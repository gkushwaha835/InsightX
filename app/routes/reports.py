from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from uuid import uuid4
import math
from types import SimpleNamespace

from flask import Blueprint, current_app, flash, render_template, request, redirect, session, url_for
from werkzeug.utils import secure_filename

from ..auth import get_current_user, login_required
from ..db import fetch_all, get_app_setting, transaction, upsert_app_settings
from ..services.analytics import (
    build_dashboard_dataset,
    build_entity_performance,
    build_monthly_trend,
    build_overview,
    fetch_monthly_rows,
    get_dimension_value,
    get_default_performer_thresholds,
    month_sort_key,
    normalize_row_limit,
)
from ..services.exporters import export_dashboard_excel, export_dashboard_pdf
from ..services.report_history import append_history
from ..services.report_history import load_history
from ..services.search_term_analysis import build_plan_rows, build_section_rows, load_or_build_report
from ..services.campaign_analysis import analyze_campaign_file
from ..services.user_flow import consume_one_time_access, issue_one_time_access, log_user_uploaded_file
from ..services.weekly_upload_service import parse_weekly_ads_excel

reports_bp = Blueprint('reports', __name__)


def _clean_display_name(value: str) -> str:
    raw = str(value or '').strip()
    if not raw:
        return ''
    if '@' in raw:
        raw = raw.split('@', 1)[0]
    for token in ('_', '.', '-', '+'):
        raw = raw.replace(token, ' ')
    parts = [part for part in raw.split() if part]
    cleaned_parts: list[str] = []
    for part in parts:
        match = re.match(r'^[A-Za-z]+', part)
        if match:
            cleaned_parts.append(match.group(0))
            break
    if not cleaned_parts and parts:
        cleaned_parts.append(parts[0])
    cleaned = ' '.join(cleaned_parts)
    return cleaned.title() if cleaned else ''


def _resolve_display_name(user: SimpleNamespace | None, fallback_email: str = '') -> str:
    if not user:
        return _clean_display_name(fallback_email) or 'User'
    role = str(getattr(user, 'role', '') or '').strip().lower()
    if role == 'admin':
        return 'Admin'
    username = str(getattr(user, 'username', '') or '').strip()
    display_name = _clean_display_name(username)
    if display_name:
        return display_name
    display_name = _clean_display_name(fallback_email)
    if display_name:
        return display_name
    return 'User'


def has_one_time_access(flow_key: str) -> bool:
    token = (request.args.get('access_token') or '').strip()
    return consume_one_time_access(flow_key, token)


def has_persistent_flow_access(flow_key: str, session_flag_key: str) -> bool:
    if has_one_time_access(flow_key):
        session[session_flag_key] = True
        return True
    return bool(session.get(session_flag_key))


def _redirect_after_report_upload_post(default_endpoint: str):
    source_page = (request.form.get('source_page') or '').strip().lower()
    if source_page == 'home':
        return redirect(url_for('main.home'))
    return redirect(url_for(default_endpoint))


def _clear_campaign_upload_session() -> None:
    session.pop('latest_campaign_file_path', None)
    session.pop('latest_campaign_source_name', None)
    session.pop('latest_campaign_range_value', None)
    session.pop('latest_campaign_range_label', None)
    session.pop('campaign_report_access_granted', None)


def _clear_search_term_upload_session() -> None:
    session.pop('latest_search_term_file_path', None)
    session.pop('latest_search_term_range_value', None)
    session.pop('latest_search_term_range_label', None)
    session.pop('latest_search_term_source_name', None)


def _app_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _legacy_root() -> Path:
    return _app_root().parent


def _history_paths(folder_name: str) -> list[Path]:
    return [
        _app_root() / f'uploads/{folder_name}/report_history.json',
        _legacy_root() / f'uploads/{folder_name}/report_history.json',
    ]


def _load_combined_history_items(folder_name: str) -> list[dict]:
    combined: list[dict] = []

    for path in _history_paths(folder_name):
        manifest = load_history(path)
        items = manifest.get('items', []) if isinstance(manifest, dict) else []
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                combined.append(item)

    # Keep chronological order by list insertion order, dedupe by id when present.
    deduped: list[dict] = []
    seen_ids: set[str] = set()
    for item in combined:
        item_id = str(item.get('id', ''))
        if item_id and item_id in seen_ids:
            continue
        if item_id:
            seen_ids.add(item_id)
        deduped.append(item)

    return list(reversed(deduped))


def _history_file_path(item: dict) -> Path | None:
    if not isinstance(item, dict):
        return None

    raw_path = str(item.get('stored_file_path') or '').strip()
    if raw_path:
        candidate = Path(raw_path)
        if candidate.exists():
            return candidate

    stored_name = str(item.get('stored_file_name') or '').strip()
    source_name = str(item.get('source_file_name') or '').strip()
    names = [n for n in [stored_name, source_name] if n]
    roots = [
        _app_root() / 'uploads/search_term_performance',
        _legacy_root() / 'uploads/search_term_performance',
    ]
    for name in names:
        for root in roots:
            candidate = root / name
            if candidate.exists():
                return candidate
    return None


def get_distinct_months(user_id: int | None = None) -> list[str]:
    if user_id is None:
        rows = fetch_all("SELECT DISTINCT month_name FROM monthly_ads WHERE COALESCE(month_name, '') <> ''")
    else:
        rows = fetch_all(
            "SELECT DISTINCT month_name FROM monthly_ads WHERE user_id = %s AND COALESCE(month_name, '') <> ''",
            (user_id,),
        )
    months = sorted({str(r.get('month_name') or '').strip() for r in rows if r.get('month_name')}, key=month_sort_key)
    return months


def get_distinct_weeks(user_id: int | None = None) -> list[str]:
    if user_id is None:
        rows = fetch_all("SELECT DISTINCT week_range FROM weekly_ads WHERE COALESCE(week_range, '') <> ''")
    else:
        rows = fetch_all(
            "SELECT DISTINCT week_range FROM weekly_ads WHERE user_id = %s AND COALESCE(week_range, '') <> ''",
            (user_id,),
        )
    weeks = sorted({str(r.get('week_range') or '').strip() for r in rows if r.get('week_range')})
    return weeks


def fetch_weekly_rows(selected_weeks: list[str] | None = None, user_id: int | None = None) -> list[SimpleNamespace]:
    if user_id is None:
        rows = fetch_all("SELECT * FROM weekly_ads")
    else:
        rows = fetch_all("SELECT * FROM weekly_ads WHERE user_id = %s", (user_id,))
    output: list[SimpleNamespace] = []
    selected = set(selected_weeks or [])
    for row in rows:
        week_range = str(row.get('week_range') or '').strip()
        if selected and week_range not in selected:
            continue

        spend = float(row.get('spend') or 0.0)
        ads_sales = float(row.get('sales') or 0.0)
        total_sales = float(row.get('total_sales') or 0.0)
        impressions = int(float(row.get('impressions') or 0.0))
        clicks = int(float(row.get('clicks') or 0.0))
        sessions = int(float(row.get('sessions') or 0.0))
        total_units = int(float(row.get('total_units') or 0.0))
        ctr = float(row.get('ctr') or ((clicks / impressions * 100.0) if impressions > 0 else 0.0))
        conversion_rate = float(row.get('conversion_rate') or ((total_units / sessions * 100.0) if sessions > 0 else 0.0))

        output.append(
            SimpleNamespace(
                sku=str(row.get('sku') or '').strip(),
                asin=str(row.get('asin') or '').strip(),
                category=str(row.get('category') or '').strip(),
                month_name=week_range,
                impressions=impressions,
                clicks=clicks,
                page_views=int(float(row.get('page_views') or 0.0)),
                sessions=sessions,
                ctr=ctr,
                spend=spend,
                sales=ads_sales,
                total_units=total_units,
                total_sales=total_sales,
                acos=float(row.get('acos') or ((spend / ads_sales * 100.0) if ads_sales > 0 else 0.0)),
                tacos=float(row.get('tacos') or ((spend / total_sales * 100.0) if total_sales > 0 else 0.0)),
                conversion_rate=conversion_rate,
            )
        )
    return output


def save_last_mode(report_key: str) -> None:
    upsert_app_settings(
        {
            'last_ads_analysis_mode': 'wow' if report_key.startswith('wow') else 'mom',
            'last_ads_analysis_time': datetime.utcnow().isoformat(),
        }
    )


def parse_dashboard_params(available_months: list[str], *, period_arg: str = 'selected_months', count_arg: str = 'month_count') -> dict:
    dimension = (request.args.get('type') or request.args.get('dimension') or 'sku').strip().lower()
    if dimension not in ('sku', 'asin', 'category'):
        dimension = 'sku'

    sales_basis = (request.args.get('sales_basis') or 'ads').strip().lower()
    if sales_basis not in ('ads', 'total'):
        sales_basis = 'ads'

    month_count = int(request.args.get(count_arg, request.args.get('month_count', 2)) or 2)
    max_count = min(6, len(available_months)) if available_months else 1
    month_count = max(1, min(month_count, max_count))

    selected_months = request.args.getlist(period_arg) or request.args.getlist('selected_months')
    selected_months = [m for m in selected_months if m in available_months]
    if not selected_months:
        selected_months = available_months[-month_count:]
    elif len(selected_months) > month_count:
        selected_months = selected_months[:month_count]

    status_filter = request.args.getlist('status_filter') or ['all']

    defaults = get_default_performer_thresholds(sales_basis)
    thresholds = {
        'top': float(request.args.get('top_threshold', defaults['top']) or defaults['top']),
        'mid_min': float(request.args.get('mid_min_threshold', defaults['mid_min']) or defaults['mid_min']),
        'mid_max': float(request.args.get('mid_max_threshold', defaults['mid_max']) or defaults['mid_max']),
        'bottom': float(request.args.get('bottom_threshold', defaults['bottom']) or defaults['bottom']),
    }

    params = {
        'dimension': dimension,
        'sales_basis': sales_basis,
        'month_count': month_count,
        'selected_months': selected_months,
        'status_filter': status_filter,
        'thresholds': thresholds,
        'analysis_row_limit': normalize_row_limit(request.args.get('analysis_row_limit'), 10),
        'top_performer_row_limit': normalize_row_limit(request.args.get('top_performer_row_limit'), 5),
        'worst_performer_row_limit': normalize_row_limit(request.args.get('worst_performer_row_limit'), 5),
        'fluctuate_row_limit': normalize_row_limit(request.args.get('fluctuate_row_limit'), 5),
        'watchlist_row_limit': normalize_row_limit(request.args.get('watchlist_row_limit'), 5),
        'opportunity_row_limit': normalize_row_limit(request.args.get('opportunity_row_limit'), 10),
        'show_results': request.args.get('analyze', '0') == '1',
        'asin': (request.args.get('asin') or 'all').strip(),
        'sku_search': (request.args.get('sku_search') or '').strip(),
        'scroll_to': (request.args.get('scroll_to') or '').strip(),
    }
    return params


def apply_limit(rows: list[dict], limit: int) -> list[dict]:
    if limit == 0:
        return rows
    return rows[:limit]


def build_critical_watchlist(analysis_rows: list[dict]) -> list[dict]:
    watchlist: list[dict] = []
    for row in analysis_rows:
        spend = float(row.get('spend', 0) or 0)
        ctr = float(row.get('ctr', 0) or 0)
        conv = float(row.get('conversion_rate', 0) or 0)
        eff = float(row.get('efficiency', 0) or 0)
        status = str(row.get('status', 'N/A'))

        issue = ''
        action = ''
        severity = 0

        if status == 'Bottom Performer' and spend >= 500:
            issue = 'High spend with weak efficiency'
            action = 'Reduce bid, tighten targeting, check placement and terms.'
            severity = 3
        elif status == 'Fluctuate' and spend >= 300:
            issue = 'Unstable trend across months'
            action = 'Stabilize bids and budgets before scaling.'
            severity = 2
        elif ctr < 0.5 and spend >= 200:
            issue = 'Low CTR on meaningful spend'
            action = 'Improve creatives, titles, and keyword relevance.'
            severity = 2
        elif conv < 3 and spend >= 200:
            issue = 'Low conversion rate'
            action = 'Audit PDP quality, pricing, and targeting intent.'
            severity = 1
        elif eff > 45 and spend >= 250:
            issue = 'ACOS is too high'
            action = 'Pause waste terms and cap expensive placements.'
            severity = 2

        if issue:
            watchlist.append(
                {
                    'label': str(row.get('label', '')),
                    'status': status,
                    'issue': issue,
                    'action': action,
                    'spend': round(spend, 2),
                    'ads_sales': round(float(row.get('ads_sales', 0) or 0), 2),
                    'total_sales': round(float(row.get('total_sales', 0) or 0), 2),
                    'acos': round(float(row.get('efficiency', 0) or 0), 2),
                    'ctr': round(ctr, 2),
                    'conversion_rate': round(conv, 2),
                    'severity': severity,
                }
            )

    watchlist.sort(key=lambda r: (r['severity'], r['spend']), reverse=True)
    return watchlist


def render_dashboard_page(
    page_title: str,
    report_key: str,
    *,
    available_periods: list[str] | None = None,
    preloaded_rows: list | None = None,
    period_label: str = 'Month',
    period_plural: str = 'Months',
):
    user = get_current_user()
    if user is None:
        return redirect(url_for('main.login'))

    available_months = available_periods if available_periods is not None else get_distinct_months(user.id)
    is_wow = report_key.startswith('wow')
    params = parse_dashboard_params(
        available_months,
        period_arg='selected_weeks' if is_wow else 'selected_months',
        count_arg='compare_count' if is_wow else 'month_count',
    )

    if preloaded_rows is not None:
        rows = preloaded_rows
        if params['selected_months']:
            selected_set = set(params['selected_months'])
            rows = [r for r in rows if (getattr(r, 'month_name', '') or '').strip() in selected_set]
    else:
        rows = fetch_monthly_rows(
            params['selected_months'] if params['selected_months'] else None,
            user_id=user.id,
        )
    asins = sorted({(r.asin or '').strip() for r in rows if (r.asin or '').strip()})
    overview = build_overview(rows)
    trend = build_monthly_trend(rows, params['sales_basis'])

    dataset = build_dashboard_dataset(
        rows,
        params['selected_months'],
        params['dimension'],
        params['sales_basis'],
        params['thresholds'],
        params['status_filter'],
        params['asin'],
        params['sku_search'],
    )

    analysis_rows = dataset['analysis_rows']
    top_performers = dataset['top_performers']
    worst_performers = dataset['worst_performers']
    fluctuate_entities = [row for row in analysis_rows if row.get('status') == 'Fluctuate']
    opportunities = dataset['opportunities']
    critical_watchlist = build_critical_watchlist(analysis_rows)

    top_entities = build_entity_performance(rows, dimension=params['dimension'], limit=10)
    top_sku_entities = build_entity_performance(rows, dimension='sku', limit=10)

    if params['show_results']:
        save_last_mode(report_key)

    ad_type_share: dict[str, dict[str, float]] = {}
    if not is_wow:
        raw_share = get_app_setting(f'mom_ad_type_share_user_{user.id}', '')
        if isinstance(raw_share, str) and raw_share.strip() != '':
            try:
                decoded = json.loads(raw_share)
                if isinstance(decoded, dict):
                    ad_type_share = decoded
            except json.JSONDecodeError:
                ad_type_share = {}

    export_type = (request.args.get('export') or '').strip().lower()
    if params['show_results'] and export_type == 'excel':
        return export_dashboard_excel(page_title, overview, trend, analysis_rows)
    if params['show_results'] and export_type == 'pdf':
        return export_dashboard_pdf(page_title, overview, trend, analysis_rows)

    return render_template(
        'dashboard.html',
        user=user,
        page_title=page_title,
        report_key=report_key,
        months_available=available_months,
        period_label=period_label,
        period_plural=period_plural,
        period_field_name=('selected_weeks' if is_wow else 'selected_months'),
        asins=asins,
        params=params,
        overview=overview,
        trend=trend,
        top_entities=top_entities,
        top_sku_entities=top_sku_entities,
        critical_watchlist=apply_limit(critical_watchlist, params['watchlist_row_limit']),
        watchlist_total=len(critical_watchlist),
        analysis_rows=apply_limit(analysis_rows, params['analysis_row_limit']),
        analysis_total=len(analysis_rows),
        top_performers=apply_limit(top_performers, params['top_performer_row_limit']),
        top_total=len(top_performers),
        worst_performers=apply_limit(worst_performers, params['worst_performer_row_limit']),
        worst_total=len(worst_performers),
        fluctuate_entities=apply_limit(fluctuate_entities, params['fluctuate_row_limit']),
        fluctuate_total=len(fluctuate_entities),
        opportunities=apply_limit(opportunities, params['opportunity_row_limit']),
        opportunities_total=len(opportunities),
        ad_type_share=ad_type_share,
    )


def render_generic_page(page_title: str, report_key: str):
    user = get_current_user()
    if user is None:
        return redirect(url_for('main.login'))

    has_access = has_persistent_flow_access('ads_overview', 'ads_overview_access_granted')
    months_available = get_distinct_months(user.id)
    params = parse_dashboard_params(months_available)
    params['show_results'] = bool(has_access)

    selected_months = params['selected_months'] if params['selected_months'] else months_available
    dimension = params['dimension']
    rows = fetch_monthly_rows(selected_months if selected_months else None, user_id=user.id) if has_access else []
    overview = build_overview(rows)
    trend = build_monthly_trend(rows)
    view_rows = normalize_row_limit(request.args.get('view_rows'), 10)
    all_top_entities = build_entity_performance(rows, dimension=dimension, limit=200)
    top_entities = apply_limit(all_top_entities, view_rows)
    top_sku_entities = build_entity_performance(rows, dimension='sku', limit=10)

    dataset = build_dashboard_dataset(
        rows,
        selected_months,
        params['dimension'],
        params['sales_basis'],
        params['thresholds'],
        params['status_filter'],
        params['asin'],
        params['sku_search'],
    ) if has_access else {
        'analysis_rows': [],
        'top_performers': [],
        'worst_performers': [],
        'opportunities': [],
    }

    analysis_rows = dataset['analysis_rows']
    top_performers = dataset['top_performers']
    worst_performers = dataset['worst_performers']
    fluctuate_entities = [row for row in analysis_rows if row.get('status') == 'Fluctuate']
    opportunities = dataset['opportunities']
    critical_watchlist = build_critical_watchlist(analysis_rows)

    ad_type_share: dict[str, dict[str, float]] = {}
    raw_share = get_app_setting(f'mom_ad_type_share_user_{user.id}', '')
    if isinstance(raw_share, str) and raw_share.strip() != '':
        try:
            decoded = json.loads(raw_share)
            if isinstance(decoded, dict):
                ad_type_share = decoded
        except json.JSONDecodeError:
            ad_type_share = {}

    export_type = (request.args.get('export') or '').strip().lower()
    if has_access and params['show_results'] and export_type == 'excel':
        return export_dashboard_excel(page_title, overview, trend, analysis_rows)
    if has_access and params['show_results'] and export_type == 'pdf':
        return export_dashboard_pdf(page_title, overview, trend, analysis_rows)

    save_last_mode(report_key)

    return render_template(
        'generic_report.html',
        user=user,
        display_name=_resolve_display_name(user, str(session.get('login_email') or user.username)),
        page_title=page_title,
        report_key=report_key,
        months_available=months_available,
        selected_months=selected_months,
        overview=overview,
        trend=trend,
        top_entities=top_entities,
        top_entities_total=len(all_top_entities),
        top_sku_entities=top_sku_entities,
        dimension=dimension,
        show_results=bool(has_access and rows),
        view_rows=view_rows,
        params=params,
        critical_watchlist=apply_limit(critical_watchlist, 10),
        watchlist_total=len(critical_watchlist),
        analysis_rows=apply_limit(analysis_rows, view_rows),
        analysis_total=len(analysis_rows),
        top_performers=apply_limit(top_performers, 10),
        top_total=len(top_performers),
        worst_performers=apply_limit(worst_performers, 10),
        worst_total=len(worst_performers),
        fluctuate_entities=apply_limit(fluctuate_entities, 10),
        fluctuate_total=len(fluctuate_entities),
        opportunities=apply_limit(opportunities, 15),
        opportunities_total=len(opportunities),
        ad_type_share=ad_type_share,
    )


@reports_bp.route('/ads-overview')
@login_required
def ads_overview():
    return render_generic_page('Ads Overview', 'ads_overview')


def render_heatmap_page(page_title: str, report_key: str):
    user = get_current_user()
    if user is None:
        return redirect(url_for('main.login'))

    is_wow = report_key.startswith('wow')
    available_periods = get_distinct_weeks(user.id) if is_wow else get_distinct_months(user.id)
    params = parse_dashboard_params(
        available_periods,
        period_arg='selected_weeks' if is_wow else 'selected_months',
        count_arg='compare_count' if is_wow else 'month_count',
    )
    if is_wow:
        rows = fetch_weekly_rows(params['selected_months'] if params['selected_months'] else None, user_id=user.id)
    else:
        rows = fetch_monthly_rows(params['selected_months'] if params['selected_months'] else None, user_id=user.id)

    metric = (request.args.get('metric') or 'acos').strip().lower()
    if metric not in ('acos', 'tacos', 'ctr', 'conversion_rate'):
        metric = 'acos'

    row_limit_raw = (request.args.get('row_limit') or '12').strip().lower()
    if row_limit_raw in ('all', '0'):
        row_limit = 0
    else:
        try:
            row_limit = int(row_limit_raw)
        except ValueError:
            row_limit = 12
        if row_limit not in {12, 25, 50, 100}:
            row_limit = 12

    hide_all_zero_rows = (request.args.get('hide_all_zero') or '1').strip() == '1'

    metric_config = {
        'acos': {'title': 'ACOS Heatmap', 'lower_is_better': True, 'good': 25.0, 'watch': 40.0},
        'tacos': {'title': 'TACOS Heatmap', 'lower_is_better': True, 'good': 10.0, 'watch': 20.0},
        'ctr': {'title': 'CTR Heatmap', 'lower_is_better': False, 'good': 1.5, 'watch': 0.5},
        'conversion_rate': {'title': 'Conversion Rate Heatmap', 'lower_is_better': False, 'good': 10.0, 'watch': 3.0},
    }[metric]

    def metric_tone(value: float | None) -> str:
        if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
            return 'tone-na'
        if metric_config['lower_is_better']:
            if value <= metric_config['good']:
                return 'tone-low'
            if value <= metric_config['watch']:
                return 'tone-mid'
            return 'tone-high'
        if value >= metric_config['good']:
            return 'tone-low'
        if value >= metric_config['watch']:
            return 'tone-mid'
        return 'tone-high'

    month_keys = params['selected_months'] or []
    entity_month_map: dict[str, dict[str, dict[str, float]]] = {}

    for row in rows:
        month_name = (row.month_name or '').strip()
        if month_keys and month_name not in month_keys:
            continue
        entity = get_dimension_value(row, params['dimension'])
        if not entity:
            continue
        entity_bucket = entity_month_map.setdefault(entity, {})
        month_bucket = entity_bucket.setdefault(
            month_name,
            {'spend': 0.0, 'ads_sales': 0.0, 'total_sales': 0.0, 'clicks': 0.0, 'impressions': 0.0, 'sessions': 0.0, 'units': 0.0},
        )
        month_bucket['spend'] += float(row.spend or 0)
        month_bucket['ads_sales'] += float(row.sales or 0)
        month_bucket['total_sales'] += float(row.total_sales or 0)
        month_bucket['clicks'] += float(row.clicks or 0)
        month_bucket['impressions'] += float(row.impressions or 0)
        month_bucket['sessions'] += float(row.sessions or 0)
        month_bucket['units'] += float(row.total_units or 0)

    summary_rows: list[dict] = []
    for entity, month_data in entity_month_map.items():
        month_values: dict[str, float | None] = {}
        present_values: list[float] = []
        has_non_zero = False

        for month in month_keys:
            cell = month_data.get(month)
            if not cell:
                month_values[month] = None
                continue

            spend = float(cell['spend'])
            ads_sales = float(cell['ads_sales'])
            total_sales = float(cell['total_sales'])
            impressions = float(cell['impressions'])
            clicks = float(cell['clicks'])
            sessions = float(cell['sessions'])
            units = float(cell['units'])

            value: float
            if metric == 'tacos':
                value = (spend / total_sales * 100.0) if total_sales > 0 else 0.0
            elif metric == 'ctr':
                value = (clicks / impressions * 100.0) if impressions > 0 else 0.0
            elif metric == 'conversion_rate':
                value = (units / sessions * 100.0) if sessions > 0 else 0.0
            else:
                value = (spend / ads_sales * 100.0) if ads_sales > 0 else 0.0

            rounded = round(value, 2)
            month_values[month] = rounded
            present_values.append(rounded)
            if rounded > 0:
                has_non_zero = True

        if hide_all_zero_rows and not has_non_zero:
            continue

        avg_value = round(sum(present_values) / len(present_values), 2) if present_values else None
        summary_rows.append(
            {
                'entity': entity,
                'months': month_values,
                'avg_value': avg_value,
                'avg_tone': metric_tone(avg_value),
                'has_non_zero_value': has_non_zero,
            }
        )

    summary_rows.sort(
        key=lambda row: (
            row['avg_value'] is None,
            (row['avg_value'] if row['avg_value'] is not None else float('inf')) if metric_config['lower_is_better'] else -(row['avg_value'] if row['avg_value'] is not None else float('-inf')),
        )
    )

    total_rows = len(summary_rows)
    if row_limit > 0:
        summary_rows = summary_rows[:row_limit]

    for row in summary_rows:
        row['tone_by_month'] = {m: metric_tone(v) for m, v in row['months'].items()}
        row['avg_display'] = 'N/A' if row['avg_value'] is None else f"{float(row['avg_value']):.2f}%"
        row['display_by_month'] = {m: ('N/A' if v is None else f"{float(v):.2f}%") for m, v in row['months'].items()}

    save_last_mode(report_key)
    return render_template(
        'heatmap.html',
        user=user,
        page_title=page_title,
        months_available=available_periods,
        params=params,
        metric=metric,
        metric_title=metric_config['title'],
        row_limit=row_limit,
        hide_all_zero_rows=hide_all_zero_rows,
        heatmap_rows=summary_rows,
        heatmap_total_rows=total_rows,
        heatmap_months=month_keys,
        entity_label=('SKU' if params['dimension'] == 'sku' else 'ASIN' if params['dimension'] == 'asin' else 'Category'),
    )


def render_advanced_feature_page(page_title: str, report_key: str):
    user = get_current_user()
    if user is None:
        return redirect(url_for('main.login'))

    analysis_mode = (request.args.get('mode') or 'mom').strip().lower()
    if analysis_mode not in ('mom', 'wow'):
        analysis_mode = 'mom'

    if analysis_mode == 'wow' and not bool(session.get('wow_dashboard_access_granted')):
        flash('Please upload WoW file first to open WoW advanced view.', 'warning')
        return redirect(url_for('reports.wow_upload'))

    if analysis_mode == 'wow':
        available_periods = get_distinct_weeks(user.id)
        params = parse_dashboard_params(available_periods, period_arg='selected_weeks', count_arg='compare_count')
        rows = fetch_weekly_rows(params['selected_months'] if params['selected_months'] else None, user_id=user.id)
        dashboard_url = url_for('reports.wow_dashboard') + '?analyze=1'
        heatmap_base_url = url_for('reports.wow_heatmap')
    else:
        available_periods = get_distinct_months(user.id)
        params = parse_dashboard_params(available_periods)
        rows = fetch_monthly_rows(params['selected_months'] if params['selected_months'] else None, user_id=user.id)
        dashboard_url = url_for('reports.dashboard') + '?analyze=1'
        heatmap_base_url = url_for('reports.heatmap')

    overview = build_overview(rows)
    top_entities = build_entity_performance(rows, dimension=params['dimension'], limit=8)
    save_last_mode('wow_advanced_feature' if analysis_mode == 'wow' else report_key)

    return render_template(
        'advanced_feature.html',
        user=user,
        page_title=page_title,
        params=params,
        overview=overview,
        top_entities=top_entities,
        analysis_mode=analysis_mode,
        dashboard_url=dashboard_url,
        heatmap_base_url=heatmap_base_url,
    )


def render_campaign_insights_page(page_title: str, report_key: str):
    user = get_current_user()
    if user is None:
        return redirect(url_for('main.login'))

    has_campaign_upload = bool(str(session.get('latest_campaign_file_path') or '').strip())
    rows = fetch_monthly_rows(user_id=user.id) if has_campaign_upload else []
    overview = build_overview(rows)
    trend = build_monthly_trend(rows, sales_basis='ads')
    top_by_sales = sorted(build_entity_performance(rows, dimension='sku', limit=30), key=lambda r: r['ads_sales'], reverse=True)[:10]
    top_by_spend = sorted(build_entity_performance(rows, dimension='sku', limit=30), key=lambda r: r['spend'], reverse=True)[:10]
    top_by_roas = sorted(
        [
            {
                **row,
                'roas': round((row['ads_sales'] / row['spend']), 2) if row['spend'] > 0 else 0.0,
            }
            for row in build_entity_performance(rows, dimension='sku', limit=40)
        ],
        key=lambda r: r['roas'],
        reverse=True,
    )[:10]

    overall_roas = round((overview['ads_sales'] / overview['total_spend']), 2) if overview['total_spend'] > 0 else 0.0

    save_last_mode(report_key)
    return render_template(
        'campaign_more_insights.html',
        user=user,
        page_title=page_title,
        has_campaign_upload=has_campaign_upload,
        overview=overview,
        trend=trend,
        top_by_sales=top_by_sales,
        top_by_spend=top_by_spend,
        top_by_roas=top_by_roas,
        overall_roas=overall_roas,
    )


@reports_bp.route('/dashboard')
@login_required
def dashboard():
    if not has_persistent_flow_access('dashboard', 'dashboard_access_granted'):
        flash('Please upload MOM file again to open dashboard.', 'warning')
        return redirect(url_for('main.home'))
    
    result = render_dashboard_page('SKU/ASIN/Category Wise MOM Analysis', 'dashboard')
    # If no data (empty rows due to DB error), show helpful message
    if not current_app.debug:
        # Check if no data by looking for specific template messages or add flag
        flash('No analysis data available. Please upload Ads + Business report via /upload first.', 'info')
    return result


@reports_bp.route('/mom')
@login_required
def mom():
    return render_dashboard_page('Month-on-Month Analysis', 'mom')


@reports_bp.route('/wow')
@login_required
def wow():
    return redirect(url_for('reports.wow_upload'))


@reports_bp.route('/wow-upload', methods=['GET', 'POST'])
@login_required
def wow_upload():
    user = get_current_user()
    if user is None:
        return redirect(url_for('main.login'))

    if request.method == 'POST':
        file = request.files.get('wow_file')
        business_file = request.files.get('wow_business_file')
        if not file or not file.filename:
            flash('Please choose a WoW file.', 'danger')
            return redirect(url_for('reports.wow_upload'))

        lower_name = file.filename.lower()
        if not lower_name.endswith(('.xlsx', '.xls')):
            flash('Only .xlsx or .xls files are supported for WoW upload.', 'danger')
            return redirect(url_for('reports.wow_upload'))

        if not business_file or not business_file.filename:
            flash('Please choose the WoW Business report file.', 'danger')
            return redirect(url_for('reports.wow_upload'))

        business_lower_name = business_file.filename.lower()
        if not business_lower_name.endswith(('.xlsx', '.xls', '.csv')):
            flash('WoW Business report must be .xlsx, .xls, or .csv.', 'danger')
            return redirect(url_for('reports.wow_upload'))

        root = Path(current_app.config['UPLOAD_FOLDER']) / 'wow_uploads'
        root.mkdir(parents=True, exist_ok=True)
        safe_name = secure_filename(file.filename)
        target = root / f'{uuid4().hex}_{safe_name}'
        file.save(target)
        log_user_uploaded_file('WoW Dashboard', target, file.filename)

        business_root = root / 'business_reports'
        business_root.mkdir(parents=True, exist_ok=True)
        business_safe_name = secure_filename(business_file.filename)
        business_target = business_root / f'{uuid4().hex}_{business_safe_name}'
        business_file.save(business_target)
        log_user_uploaded_file('WoW Business Report', business_target, business_file.filename)

        inserted, message = parse_weekly_ads_excel(target, user.id)
        if inserted > 0:
            session['wow_dashboard_access_granted'] = False
            try:
                upsert_app_settings(
                    {
                        'last_ads_analysis_mode': 'wow',
                        'last_ads_analysis_target': '/wow-dashboard',
                        'last_ads_analysis_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                current_app.logger.warning('Skipping app_settings update after WoW upload: %s', exc)
            flash(message, 'success')
            access_token = issue_one_time_access('wow_dashboard')
            return redirect(url_for('reports.wow_dashboard', analyze=1, access_token=access_token))

        flash(message, 'danger')
        return redirect(url_for('reports.wow_upload'))

    return render_template('wow_upload.html', user=user)


@reports_bp.route('/wow-dashboard')
@login_required
def wow_dashboard():
    if not has_persistent_flow_access('wow_dashboard', 'wow_dashboard_access_granted'):
        flash('Please upload WoW file again to open dashboard.', 'warning')
        return redirect(url_for('reports.wow_upload'))
    weeks = get_distinct_weeks(user.id)
    rows = fetch_weekly_rows(user_id=user.id)
    return render_dashboard_page(
        'SKU/ASIN/Category Wise WOW Analysis',
        'wow_dashboard',
        available_periods=weeks,
        preloaded_rows=rows,
        period_label='Week',
        period_plural='Weeks',
    )


@reports_bp.route('/heatmap')
@login_required
def heatmap():
    return render_heatmap_page('Heatmap (MoM)', 'heatmap')


@reports_bp.route('/wow-heatmap')
@login_required
def wow_heatmap():
    if not bool(session.get('wow_dashboard_access_granted')):
        flash('Please upload WoW file first to open WoW heatmap.', 'warning')
        return redirect(url_for('reports.wow_upload'))
    return render_heatmap_page('Heatmap (WoW)', 'wow_heatmap')


@reports_bp.route('/advanced-feature')
@login_required
def advanced_feature():
    return render_advanced_feature_page('Advanced Features', 'advanced_feature')


@reports_bp.route('/analyze-previous-ads')
@login_required
def analyze_previous_ads():
    return render_template('analyze_previous_ads.html')


@reports_bp.route('/campaign-more-insights')
@login_required
def campaign_more_insights():
    return render_campaign_insights_page('Campaign Insights', 'campaign_more_insights')


@reports_bp.route('/campaign-performance-report', methods=['GET', 'POST'])
@login_required
def campaign_performance_report():
    root = Path(current_app.config['UPLOAD_FOLDER']) / 'campaign_performance'

    if request.method == 'POST':
        user = get_current_user()
        file = request.files.get('campaign_file')
        range_value = (request.form.get('campaign_range') or '30').strip()
        if not file or not file.filename:
            flash('Please select a campaign file.', 'danger')
            return _redirect_after_report_upload_post('reports.campaign_performance_report')

        root.mkdir(parents=True, exist_ok=True)
        safe_name = secure_filename(file.filename)
        target = root / f'{uuid4().hex}_{safe_name}'
        file.save(target)
        log_user_uploaded_file('Campaign Report', target, file.filename)
        try:
            # Validate file on upload so users don't see success + error together.
            analyze_campaign_file(target)
        except Exception as exc:  # noqa: BLE001
            try:
                target.unlink(missing_ok=True)
            except OSError:
                pass
            _clear_campaign_upload_session()
            flash(f'Invalid campaign file. {str(exc).strip()}', 'danger')
            return _redirect_after_report_upload_post('reports.campaign_performance_report')

        range_label = {
            '7': 'Last 7 Days',
            '14': 'Last 14 Days',
            '30': 'Last 30 Days',
            '60': 'Last 60 Days',
        }.get(range_value, 'Last 30 Days')

        append_history(
            root / 'report_history.json',
            {
                'range_label': range_label,
                'source_file_name': file.filename,
                'saved_by': (user.username if user else ''),
            },
        )
        session['latest_campaign_file_path'] = str(target.resolve())
        session['latest_campaign_source_name'] = file.filename
        session['latest_campaign_range_value'] = range_value
        session['latest_campaign_range_label'] = range_label
        session['campaign_report_access_granted'] = False
        flash('Campaign file uploaded successfully.', 'success')
        access_token = issue_one_time_access('campaign_report')
        redirect_kwargs = {'access_token': access_token}
        source_page = (request.form.get('source_page') or '').strip().lower()
        if source_page == 'home':
            redirect_kwargs['source_page'] = 'home'
        return redirect(url_for('reports.campaign_performance_report', **redirect_kwargs))

    user = get_current_user()
    campaign_report = None
    report_error = ''
    source_page = (request.args.get('source_page') or '').strip().lower()
    range_label = str(session.get('latest_campaign_range_label') or 'Last 30 Days')
    range_value = str(session.get('latest_campaign_range_value') or '30')
    source_name = str(session.get('latest_campaign_source_name') or '')

    has_access = has_persistent_flow_access('campaign_report', 'campaign_report_access_granted')
    file_path_raw = str(session.get('latest_campaign_file_path') or '').strip()
    file_path = Path(file_path_raw) if file_path_raw else None
    if not has_access and file_path and file_path.exists():
        has_access = True

    if has_access and file_path and file_path.exists():
        try:
            campaign_report = analyze_campaign_file(file_path)
            # Add username to all report sections
            user_username = user.username
            for section in ['campaign_preview', 'top_by_sales', 'top_by_spend', 'top_by_acos', 'low_by_acos', 'top_by_roas', 'low_by_roas', 'top_by_purchases', 'low_by_purchases', 'high_spend_no_sales', 'active_without_portfolio', 'recommendation_tables']:
                for row in campaign_report.get(section, []):
                    if isinstance(row, dict):
                        row['username'] = user_username
                    elif isinstance(row, list):
                        for item in row:
                            if isinstance(item, dict):
                                item['username'] = user_username
            # Save campaign data to database
            preview_rows = campaign_report.get('campaign_preview', [])
            with transaction() as connection:
                with connection.cursor() as cursor:
                    cursor.execute('DELETE FROM campaign_report WHERE user_id = %s', (user.id,))
                    if preview_rows:
                        cursor.executemany(
                            """
                            INSERT INTO campaign_report (
                                user_id, campaign_name, impressions, clicks, spend, sales, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                            """,
                            [
                                (
                                    user.id,
                                    campaign.get('campaign_name'),
                                    int(campaign.get('impressions') or 0),
                                    int(campaign.get('clicks') or 0),
                                    float(campaign.get('spend') or 0.0),
                                    float(campaign.get('sales') or 0.0),
                                )
                                for campaign in preview_rows
                            ],
                        )
        except Exception as exc:  # noqa: BLE001
            report_error = str(exc)
    elif has_access:
        report_error = 'Uploaded campaign file not found. Please upload again.'

    if report_error and source_page == 'home':
        flash(report_error, 'danger')
        return redirect(url_for('main.home'))

    ad_type_scope = (request.args.get('ad_type_scope') or 'active').strip().lower()
    if ad_type_scope not in {'all', 'active', 'inactive'}:
        ad_type_scope = 'active'

    table_limit_raw = (request.args.get('table_limit') or '10').strip().lower()
    table_limit = 10
    if table_limit_raw == 'all':
        table_limit = 0
    elif table_limit_raw.isdigit():
        table_limit = int(table_limit_raw)
        if table_limit not in {5, 10, 20, 50}:
            table_limit = 10

    ad_type_counts = {'SP': 0, 'SB': 0, 'SBV': 0, 'SD': 0}
    ad_type_total = 0
    if isinstance(campaign_report, dict):
        active_counts = campaign_report.get('ad_type_counts_active', {}) or {}
        inactive_counts = campaign_report.get('ad_type_counts_inactive', {}) or {}
        if ad_type_scope == 'inactive':
            ad_type_counts = {k: int(inactive_counts.get(k, 0) or 0) for k in ad_type_counts.keys()}
        elif ad_type_scope == 'all':
            ad_type_counts = {
                k: int(active_counts.get(k, 0) or 0) + int(inactive_counts.get(k, 0) or 0)
                for k in ad_type_counts.keys()
            }
        else:
            ad_type_counts = {k: int(active_counts.get(k, 0) or 0) for k in ad_type_counts.keys()}
        ad_type_total = int(sum(ad_type_counts.values()))

    scope_label = {
        'active': 'Running Only',
        'inactive': 'Not Running Only',
        'all': 'All Campaigns',
    }.get(ad_type_scope, 'Running Only')

    save_last_mode('campaign_performance_report')
    return render_template(
        'campaign_report.html',
        user=user,
        display_name=_resolve_display_name(user, str(session.get('login_email') or user.username)),
        page_title='Campaign Performance Report',
        campaign_report=campaign_report,
        summary=(campaign_report.get('summary', {}) if isinstance(campaign_report, dict) else {}),
        report_error=report_error,
        range_label=range_label,
        range_value=range_value,
        saved_file_name=source_name,
        show_results=bool(campaign_report),
        ad_type_scope=ad_type_scope,
        ad_type_scope_label=scope_label,
        ad_type_counts=ad_type_counts,
        ad_type_total=ad_type_total,
        table_limit=table_limit,
    )


@reports_bp.route('/search-term-report', methods=['GET', 'POST'])
@login_required
def search_term_report():
    root = Path(current_app.config['UPLOAD_FOLDER']) / 'search_term_performance'

    if request.method == 'POST':
        file = request.files.get('search_term_file')
        source_page = (request.form.get('source_page') or '').strip().lower()
        range_value = (request.form.get('search_term_range') or '30').strip()
        if not file or not file.filename:
            flash('Please select a search term file.', 'danger')
            return _redirect_after_report_upload_post('reports.search_term_report')

        root.mkdir(parents=True, exist_ok=True)
        safe_name = secure_filename(file.filename)
        target = root / f'{uuid4().hex}_{safe_name}'
        file.save(target)
        log_user_uploaded_file('Search Term Report', target, file.filename)
        try:
            load_or_build_report(
                file_path=target,
                range_value=range_value,
                cache_dir_candidates=[
                    _app_root() / 'uploads/search_term_performance/cache',
                    _legacy_root() / 'uploads/search_term_performance/cache',
                ],
            )
        except Exception as exc:  # noqa: BLE001
            try:
                target.unlink(missing_ok=True)
            except OSError:
                pass
            _clear_search_term_upload_session()
            flash(f'Invalid search term file. {str(exc).strip()}', 'danger')
            return _redirect_after_report_upload_post('reports.search_term_report')

        range_label = {
            '7': 'Last 7 Days',
            '14': 'Last 14 Days',
            '30': 'Last 30 Days',
            '60': 'Last 60 Days',
        }.get(range_value, 'Last 30 Days')

        session['latest_search_term_file_path'] = str(target.resolve())
        session['latest_search_term_range_value'] = range_value
        session['latest_search_term_range_label'] = range_label
        session['latest_search_term_source_name'] = file.filename
        flash('Search term file uploaded successfully.', 'success')
        access_token = issue_one_time_access('search_term_report')
        redirect_kwargs = {'access_token': access_token}
        if source_page == 'home':
            redirect_kwargs['source_page'] = 'home'
        return redirect(url_for('reports.search_term_report', **redirect_kwargs))

    user = get_current_user()
    term_type_filter = (request.args.get('term_type_filter') or 'all').strip().lower()
    if term_type_filter not in {'all', 'asin', 'keyword'}:
        term_type_filter = 'all'

    rows_limit_raw = (request.args.get('rows_limit') or '10').strip().lower()
    rows_limit = 10
    if rows_limit_raw == 'all':
        rows_limit = 0
    elif rows_limit_raw.isdigit():
        rows_limit = int(rows_limit_raw)
        if rows_limit < 0:
            rows_limit = 10

    report = None
    sections = {}
    plan_rows = []
    plan_match_count = 0
    plan_no_match_count = 0
    report_error = ''
    source_page = (request.args.get('source_page') or '').strip().lower()
    range_label = str(session.get('latest_search_term_range_label') or 'Last 30 Days')
    range_value = str(session.get('latest_search_term_range_value') or '30')
    targeting_query = (request.args.get('targeting_query') or '').strip()
    selected_section = (request.args.get('section') or 'top_by_sales').strip().lower()
    allowed_sections = {
        'top_by_sales',
        'top_by_spend',
        'high_acos',
        'high_roas',
        'low_roas',
        'winners',
        'high_spend_no_sales',
        'high_clicks_no_orders',
        'low_ctr',
        'top_ctr',
        'hidden_gems',
        'plan_action',
    }
    if selected_section not in allowed_sections:
        selected_section = 'top_by_sales'

    has_access = has_one_time_access('search_term_report')
    file_path_raw = str(session.get('latest_search_term_file_path') or '').strip()
    file_path = Path(file_path_raw) if file_path_raw else None
    if has_access and file_path and file_path.exists():
        try:
            report = load_or_build_report(
                file_path=file_path,
                range_value=range_value,
                cache_dir_candidates=[
                    _app_root() / 'uploads/search_term_performance/cache',
                    _legacy_root() / 'uploads/search_term_performance/cache',
                ],
            )
            # Add username to all report sections
            user_username = user.username
            for section in ['all_terms', 'detail_rows', 'top_by_sales', 'top_by_spend', 'high_acos', 'high_roas', 'low_roas', 'winners', 'high_spend_no_sales', 'high_clicks_no_orders', 'top_ctr', 'low_ctr', 'hidden_gems', 'plan_action']:
                for row in report.get(section, []):
                    if isinstance(row, dict):
                        row['username'] = user_username
            # Save search term data to database
            all_terms = report.get('all_terms', [])
            with transaction() as connection:
                with connection.cursor() as cursor:
                    cursor.execute('DELETE FROM search_term_report WHERE user_id = %s', (user.id,))
                    if all_terms:
                        cursor.executemany(
                            """
                            INSERT INTO search_term_report (
                                user_id, search_term, impressions, clicks, spend, sales, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                            """,
                            [
                                (
                                    user.id,
                                    term.get('search_term'),
                                    int(term.get('impressions') or 0),
                                    int(term.get('clicks') or 0),
                                    float(term.get('spend') or 0.0),
                                    float(term.get('sales') or 0.0),
                                )
                                for term in all_terms
                            ],
                        )
            sections = build_section_rows(report, term_type_filter, rows_limit)
            plan_rows = build_plan_rows(report, targeting_query, term_type_filter, rows_limit)
            plan_match_count = len([row for row in plan_rows if str(row.get('match_result', '')) == 'Match'])
            plan_no_match_count = len(plan_rows) - plan_match_count
        except Exception as exc:  # noqa: BLE001
            report_error = str(exc)
    elif has_access:
        report_error = 'Uploaded file not found. Please upload again.'

    if report_error and source_page == 'home':
        flash(report_error, 'danger')
        return redirect(url_for('main.home'))

    save_last_mode('search_term_report')
    filter_label = {
        'all': 'All Terms',
        'asin': 'ASIN Only',
        'keyword': 'Keywords Only',
    }.get(term_type_filter, 'All Terms')
    rows_label = 'Show All' if rows_limit == 0 else f'Show {rows_limit}'

    summary = report.get('summary', {}) if isinstance(report, dict) else {}
    thresholds = report.get('thresholds', {'spend': 800, 'clicks': 24}) if isinstance(report, dict) else {'spend': 800, 'clicks': 24}
    all_terms = report.get('all_terms', []) if isinstance(report, dict) else []
    filtered_terms = [
        row
        for row in all_terms
        if term_type_filter == 'all'
        or (str(row.get('search_term', '')).strip().upper().startswith('B0') and term_type_filter == 'asin')
        or (not str(row.get('search_term', '')).strip().upper().startswith('B0') and term_type_filter == 'keyword')
    ]
    filtered_term_count = len(filtered_terms)

    kpi_summary = {
        'terms': filtered_term_count,
        'impressions': int(sum(float(row.get('impressions') or 0.0) for row in filtered_terms)),
        'clicks': int(sum(float(row.get('clicks') or 0.0) for row in filtered_terms)),
        'spend': float(sum(float(row.get('spend') or 0.0) for row in filtered_terms)),
        'sales': float(sum(float(row.get('sales') or 0.0) for row in filtered_terms)),
        'orders': int(sum(float(row.get('orders') or 0.0) for row in filtered_terms)),
    }
    kpi_summary['ctr'] = (
        (kpi_summary['clicks'] / kpi_summary['impressions'] * 100.0)
        if kpi_summary['impressions'] > 0
        else 0.0
    )
    kpi_summary['acos'] = (
        (kpi_summary['spend'] / kpi_summary['sales'] * 100.0)
        if kpi_summary['sales'] > 0
        else 0.0
    )
    kpi_summary['roas'] = (
        (kpi_summary['sales'] / kpi_summary['spend'])
        if kpi_summary['spend'] > 0
        else 0.0
    )

    return render_template(
        'search_term_report.html',
        user=user,
        display_name=_resolve_display_name(user, str(session.get('login_email') or user.username)),
        page_title='Search Term Report',
        report=report,
        summary=summary,
        kpi_summary=kpi_summary,
        thresholds=thresholds,
        sections=sections,
        plan_rows=plan_rows,
        plan_match_count=plan_match_count,
        plan_no_match_count=plan_no_match_count,
        report_error=report_error,
        range_label=range_label,
        range_value=range_value,
        targeting_query=targeting_query,
        selected_section=selected_section,
        term_type_filter=term_type_filter,
        filter_label=filter_label,
        rows_limit=rows_limit,
        rows_label=rows_label,
        filtered_term_count=filtered_term_count,
        brand_name='',
        saved_file_name=str(session.get('latest_search_term_source_name') or ''),
    )


@reports_bp.route('/category')
@login_required
def category():
    return render_dashboard_page('Category Wise MOM Analysis', 'category')
