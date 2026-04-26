from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from ..db import fetch_all


def safe_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def month_sort_key(month_label: str) -> tuple[int, int, str]:
    if not month_label:
        return (0, 0, month_label)
    try:
        parsed = datetime.strptime(month_label, '%b-%y')
        return (parsed.year, parsed.month, month_label)
    except ValueError:
        return (0, 0, month_label)


def fetch_monthly_rows(selected_months: list[str] | None = None, user_id: int | None = None) -> list[SimpleNamespace]:
    try:
        query = """
            SELECT
                m.sku,
                m.asin,
                m.category,
                m.month_name,
                m.impressions,
                m.clicks,
                m.page_views,
                m.sessions,
                m.ctr,
                m.spend,
                m.sales,
                m.total_units,
                m.total_sales,
                m.acos,
                m.tacos,
                m.conversion_rate,
                COALESCE(u.username, '') AS username
            FROM monthly_ads m
            LEFT JOIN users u ON u.id = m.user_id
        """
        params: list[Any] = []
        where_clauses: list[str] = []
        if user_id is not None:
            where_clauses.append("m.user_id = %s")
            params.append(user_id)
        if selected_months:
            where_clauses.append("m.month_name = ANY(%s)")
            params.append(selected_months)
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        results = fetch_all(query, tuple(params) if params else None)
        output = []
        for row in results:
            output.append(
                SimpleNamespace(
                    sku=row.get('sku'),
                    asin=row.get('asin'),
                    category=row.get('category'),
                    month_name=row.get('month_name'),
                    impressions=row.get('impressions'),
                    clicks=row.get('clicks'),
                    page_views=row.get('page_views'),
                    sessions=row.get('sessions'),
                    ctr=row.get('ctr'),
                    spend=row.get('spend'),
                    sales=row.get('sales'),
                    total_units=row.get('total_units'),
                    total_sales=row.get('total_sales'),
                    acos=row.get('acos'),
                    tacos=row.get('tacos'),
                    conversion_rate=row.get('conversion_rate'),
                    username=row.get('username') or '',
                )
            )
        return output
    except Exception as e:
        print(f"DB Error in fetch_monthly_rows: {str(e)}")
        return []


def build_overview(rows: list[Any]) -> dict[str, Any]:
    total_spend = sum(safe_float(r.spend) for r in rows)
    ads_sales = sum(safe_float(r.sales) for r in rows)
    total_sales = sum(safe_float(r.total_sales) for r in rows)
    impressions = sum(safe_int(r.impressions) for r in rows)
    clicks = sum(safe_int(r.clicks) for r in rows)
    sessions = sum(safe_int(r.sessions) for r in rows)
    total_units = sum(safe_int(r.total_units) for r in rows)

    acos = (total_spend / ads_sales * 100) if ads_sales else 0.0
    tacos = (total_spend / total_sales * 100) if total_sales else 0.0
    ctr = (clicks / impressions * 100) if impressions else 0.0
    conversion_rate = (sum(safe_int(r.total_units) for r in rows) / sessions * 100) if sessions else 0.0

    return {
        'total_rows': len(rows),
        'total_spend': round(total_spend, 2),
        'ads_sales': round(ads_sales, 2),
        'total_sales': round(total_sales, 2),
        'impressions': impressions,
        'clicks': clicks,
        'sessions': sessions,
        'total_units': total_units,
        'acos': round(acos, 2),
        'tacos': round(tacos, 2),
        'ctr': round(ctr, 2),
        'conversion_rate': round(conversion_rate, 2),
    }


def build_monthly_trend(rows: list[Any], sales_basis: str = 'ads') -> list[dict[str, Any]]:
    monthly: dict[str, dict[str, float]] = defaultdict(lambda: {
        'spend': 0.0,
        'ads_sales': 0.0,
        'total_sales': 0.0,
        'impressions': 0.0,
        'clicks': 0.0,
    })

    for row in rows:
        key = (row.month_name or '').strip() or 'Unknown'
        monthly[key]['spend'] += safe_float(row.spend)
        monthly[key]['ads_sales'] += safe_float(row.sales)
        monthly[key]['total_sales'] += safe_float(row.total_sales)
        monthly[key]['impressions'] += safe_float(row.impressions)
        monthly[key]['clicks'] += safe_float(row.clicks)

    output = []
    for month, values in sorted(monthly.items(), key=lambda item: month_sort_key(item[0])):
        ctr = (values['clicks'] / values['impressions'] * 100) if values['impressions'] else 0.0
        sales_value = values['total_sales'] if sales_basis == 'total' else values['ads_sales']
        efficiency = (values['spend'] / sales_value * 100) if sales_value else 0.0
        output.append({
            'month': month,
            'spend': round(values['spend'], 2),
            'ads_sales': round(values['ads_sales'], 2),
            'total_sales': round(values['total_sales'], 2),
            'ctr': round(ctr, 2),
            'acos': round(efficiency, 2),
        })
    return output


def get_dimension_value(row: Any, dimension: str) -> str:
    if dimension == 'asin':
        return (row.asin or '').strip()
    if dimension == 'category':
        return (row.category or '').strip()
    return (row.sku or '').strip() or (row.asin or '').strip()


def build_entity_performance(rows: list[Any], dimension: str = 'sku', limit: int = 20) -> list[dict[str, Any]]:
    entity_map: dict[str, dict[str, float]] = defaultdict(lambda: {
        'spend': 0.0,
        'ads_sales': 0.0,
        'total_sales': 0.0,
        'impressions': 0.0,
        'clicks': 0.0,
        'sessions': 0.0,
        'total_units': 0.0,
    })

    for row in rows:
        key = get_dimension_value(row, dimension)
        if not key:
            continue

        entity_map[key]['spend'] += safe_float(row.spend)
        entity_map[key]['ads_sales'] += safe_float(row.sales)
        entity_map[key]['total_sales'] += safe_float(row.total_sales)
        entity_map[key]['impressions'] += safe_float(row.impressions)
        entity_map[key]['clicks'] += safe_float(row.clicks)
        entity_map[key]['sessions'] += safe_float(row.sessions)
        entity_map[key]['total_units'] += safe_float(row.total_units)

    items = []
    for key, vals in entity_map.items():
        ctr = (vals['clicks'] / vals['impressions'] * 100) if vals['impressions'] else 0.0
        conversion_rate = (vals['total_units'] / vals['sessions'] * 100) if vals['sessions'] else 0.0
        acos = (vals['spend'] / vals['ads_sales'] * 100) if vals['ads_sales'] else 0.0
        tacos = (vals['spend'] / vals['total_sales'] * 100) if vals['total_sales'] else 0.0
        items.append({
            'name': key,
            'spend': round(vals['spend'], 2),
            'ads_sales': round(vals['ads_sales'], 2),
            'total_sales': round(vals['total_sales'], 2),
            'ctr': round(ctr, 2),
            'conversion_rate': round(conversion_rate, 2),
            'acos': round(acos, 2),
            'tacos': round(tacos, 2),
        })

    items.sort(key=lambda item: item['spend'], reverse=True)
    return items[:limit]


def normalize_row_limit(value: str | None, default: int) -> int:
    if value in ('all', '0'):
        return 0
    try:
        parsed = int(value or str(default))
    except ValueError:
        return default
    allowed = {5, 10, 15, 20, 25, 50, 100}
    return parsed if parsed in allowed else default


def get_default_performer_thresholds(sales_basis: str) -> dict[str, float]:
    if sales_basis == 'total':
        return {'top': 10.0, 'mid_min': 10.01, 'mid_max': 20.0, 'bottom': 20.01}
    return {'top': 20.0, 'mid_min': 20.01, 'mid_max': 35.0, 'bottom': 35.01}


def get_status(efficiency_rate: float, thresholds: dict[str, float]) -> str:
    if efficiency_rate <= 0.0:
        return 'N/A'
    if efficiency_rate <= thresholds['top']:
        return 'Top Performer'
    if thresholds['mid_min'] <= efficiency_rate <= thresholds['mid_max']:
        return 'Mid Performer'
    if efficiency_rate >= thresholds['bottom']:
        return 'Bottom Performer'
    return 'Fluctuate'


def is_fluctuating_trend(month_snapshots: dict[str, dict[str, float]], selected_months: list[str]) -> bool:
    # Keep behavior close to legacy PHP logic, where fluctuation is driven by
    # meaningful latest-vs-previous drop patterns in spend/sales (not tiny ACOS noise).
    if len(selected_months) < 2:
        return False

    latest_key = selected_months[-1]
    previous_key = selected_months[-2]

    latest = month_snapshots.get(latest_key, {'spend': 0.0, 'sales': 0.0, 'efficiency': 0.0})
    previous = month_snapshots.get(previous_key, {'spend': 0.0, 'sales': 0.0, 'efficiency': 0.0})

    latest_spend = float(latest.get('spend', 0.0) or 0.0)
    latest_sales = float(latest.get('sales', 0.0) or 0.0)
    previous_spend = float(previous.get('spend', 0.0) or 0.0)
    previous_sales = float(previous.get('sales', 0.0) or 0.0)
    latest_eff = float(latest.get('efficiency', 0.0) or 0.0)
    previous_eff = float(previous.get('efficiency', 0.0) or 0.0)

    if previous_spend <= 0.0 or previous_sales <= 0.0:
        return False

    spend_drop_ratio = (previous_spend - latest_spend) / previous_spend
    sales_drop_ratio = (previous_sales - latest_sales) / previous_sales

    if latest_sales <= 0.0 and latest_spend <= (previous_spend * 0.75):
        return True

    if sales_drop_ratio >= 0.70 and spend_drop_ratio >= 0.35:
        return True

    if latest_sales <= (previous_sales * 0.20) and latest_spend <= (previous_spend * 0.60):
        return True

    # Fallback: only treat as fluctuate on significant efficiency jump/drop with
    # material spend or sales shift (ignores tiny 1-2% ACOS movement).
    eff_delta = abs(latest_eff - previous_eff)
    return eff_delta >= 8.0 and (abs(spend_drop_ratio) >= 0.25 or abs(sales_drop_ratio) >= 0.25)


def build_dashboard_dataset(
    rows: list[Any],
    selected_months: list[str],
    dimension: str,
    sales_basis: str,
    thresholds: dict[str, float],
    status_filters: list[str],
    asin_filter: str = 'all',
    search_term: str = '',
) -> dict[str, Any]:
    grouped: dict[str, dict[str, Any]] = {}
    search = (search_term or '').strip().lower()

    for row in rows:
        month = (row.month_name or '').strip()
        if selected_months and month not in selected_months:
            continue

        if dimension == 'sku' and asin_filter not in ('', 'all'):
            if (row.asin or '').strip() != asin_filter:
                continue

        key = get_dimension_value(row, dimension)
        if not key:
            continue

        if search:
            key_for_search = key.lower()
            if search not in key_for_search:
                continue

        bucket = grouped.setdefault(
            key,
            {
                'label': key,
                'month_values': defaultdict(lambda: {'spend': 0.0, 'ads_sales': 0.0, 'total_sales': 0.0, 'impressions': 0.0, 'clicks': 0.0, 'sessions': 0.0, 'units': 0.0}),
            },
        )

        m = bucket['month_values'][month]
        m['spend'] += safe_float(row.spend)
        m['ads_sales'] += safe_float(row.sales)
        m['total_sales'] += safe_float(row.total_sales)
        m['impressions'] += safe_float(row.impressions)
        m['clicks'] += safe_float(row.clicks)
        m['sessions'] += safe_float(row.sessions)
        m['units'] += safe_float(row.total_units)

    analysis_rows: list[dict[str, Any]] = []

    for _, entry in grouped.items():
        month_values = entry['month_values']
        total_spend = sum(v['spend'] for v in month_values.values())
        total_ads_sales = sum(v['ads_sales'] for v in month_values.values())
        total_total_sales = sum(v['total_sales'] for v in month_values.values())
        impressions = sum(v['impressions'] for v in month_values.values())
        clicks = sum(v['clicks'] for v in month_values.values())
        sessions = sum(v['sessions'] for v in month_values.values())
        units = sum(v['units'] for v in month_values.values())

        sales_value = total_total_sales if sales_basis == 'total' else total_ads_sales
        efficiency = (total_spend / sales_value * 100) if sales_value else 0.0

        month_snapshots: dict[str, dict[str, float]] = {}
        for month_name in selected_months:
            mvals = month_values.get(month_name, {'spend': 0.0, 'ads_sales': 0.0, 'total_sales': 0.0})
            month_sales_value = mvals['total_sales'] if sales_basis == 'total' else mvals['ads_sales']
            month_efficiency = (mvals['spend'] / month_sales_value * 100) if month_sales_value else 0.0
            month_snapshots[month_name] = {
                'spend': float(mvals['spend']),
                'sales': float(month_sales_value),
                'efficiency': float(month_efficiency),
            }

        status = get_status(efficiency, thresholds)
        if status == 'Top Performer' and is_fluctuating_trend(month_snapshots, selected_months):
            status = 'Fluctuate'

        key_status = status.lower().split()[0] if status != 'N/A' else 'na'
        allowed = set(status_filters) if status_filters else {'all'}
        if 'all' not in allowed and key_status not in allowed and not (status == 'Fluctuate' and 'fluctuate' in allowed):
            continue

        ctr = (clicks / impressions * 100) if impressions else 0.0
        conversion = (units / sessions * 100) if sessions else 0.0
        if status == 'Fluctuate':
            recommendation = 'Fluctuate trend. Stabilize bids and narrow targeting month by month.'
        elif efficiency <= thresholds['top']:
            recommendation = 'Scale with controlled bids and budget.'
        elif efficiency >= thresholds['bottom']:
            recommendation = 'Reduce bids, tighten targeting, and fix listing.'
        elif ctr < 0.5:
            recommendation = 'Improve creatives and keyword relevance.'
        else:
            recommendation = 'Monitor and optimize incrementally.'

        analysis_rows.append(
            {
                'label': entry['label'],
                'spend': round(total_spend, 2),
                'ads_sales': round(total_ads_sales, 2),
                'total_sales': round(total_total_sales, 2),
                'month_details': [
                    {
                        'month': month_name,
                        'spend': round(float(month_values.get(month_name, {}).get('spend', 0.0)), 2),
                        'ads_sales': round(float(month_values.get(month_name, {}).get('ads_sales', 0.0)), 2),
                        'total_sales': round(float(month_values.get(month_name, {}).get('total_sales', 0.0)), 2),
                        'acos': round(
                            (
                                float(month_values.get(month_name, {}).get('spend', 0.0))
                                / float(month_values.get(month_name, {}).get('ads_sales', 0.0))
                                * 100.0
                            )
                            if float(month_values.get(month_name, {}).get('ads_sales', 0.0)) > 0
                            else 0.0,
                            2,
                        ),
                    }
                    for month_name in selected_months
                ],
                'efficiency': round(efficiency, 2),
                'status': status,
                'ctr': round(ctr, 2),
                'conversion_rate': round(conversion, 2),
                'recommendation': recommendation,
            }
        )

    analysis_rows.sort(key=lambda r: r['spend'], reverse=True)

    top_performers = [r for r in analysis_rows if r['status'] == 'Top Performer']
    worst_performers = [r for r in analysis_rows if r['status'] == 'Bottom Performer']

    if sales_basis == 'ads':
        opportunities = [r for r in analysis_rows if r['ctr'] < 0.5 and r['spend'] > 0]
    else:
        opportunities = [r for r in analysis_rows if r['conversion_rate'] < 3 and r['spend'] > 0]

    opportunities.sort(key=lambda r: r['spend'], reverse=True)

    return {
        'analysis_rows': analysis_rows,
        'top_performers': top_performers,
        'worst_performers': worst_performers,
        'opportunities': opportunities,
    }
