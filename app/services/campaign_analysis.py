from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


def normalize_campaign_header(value: Any) -> str:
    text = str(value or "").strip().lower()
    chars: list[str] = []
    prev_space = False
    for ch in text:
        if ch.isalnum():
            chars.append(ch)
            prev_space = False
        else:
            if not prev_space:
                chars.append(" ")
                prev_space = True
    return "".join(chars).strip()


def clean_campaign_number(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip().replace(",", "")
    filtered = []
    for ch in text:
        if ch.isdigit() or ch in ".-":
            filtered.append(ch)
    parsed = "".join(filtered)
    try:
        return float(parsed) if parsed else 0.0
    except ValueError:
        return 0.0


def is_active_campaign_row(state: str, status: str) -> bool:
    state_u = (state or "").strip().upper()
    status_u = (status or "").strip().upper()
    if state_u == "" and status_u == "":
        # Some exports include only running rows and omit explicit state/status.
        return True
    active_values = {"ENABLED", "ACTIVE"}
    return state_u in active_values or status_u in active_values


def normalize_campaign_type(raw_type: str, campaign_name: str) -> str:
    campaign_type = (raw_type or "").strip().upper()
    campaign_name_upper = (campaign_name or "").strip().upper()

    if campaign_type == "SB2":
        if "SBV" in campaign_name_upper or "VIDEO" in campaign_name_upper:
            return "SBV"
        return "SB"

    if campaign_type in {"SP", "SB", "SBV", "SD"}:
        return campaign_type

    if "SP" in campaign_type:
        return "SP"
    if "SBV" in campaign_type or "VIDEO" in campaign_type:
        return "SBV"
    if "SB" in campaign_type:
        return "SB"
    if "SD" in campaign_type:
        return "SD"
    return campaign_type or "Unknown"


def _pick_column(columns: dict[str, str], candidates: list[str]) -> str:
    for key in candidates:
        if key in columns:
            return columns[key]
    return ""


def _pick_column_contains(columns: dict[str, str], include_terms: list[str]) -> str:
    for normalized_key, original in columns.items():
        for term in include_terms:
            if term in normalized_key:
                return original
    return ""


def _pick_spend_column(columns: dict[str, str]) -> str:
    # Prefer exact/strong spend headers first.
    strong_order = [
        "spend",
        "total spend",
        "total cost",
        "cost",
        "cost usd",
        "cost inr",
        "ad spend",
    ]
    for key in strong_order:
        if key in columns:
            return columns[key]

    # Then choose a column containing spend/cost but avoid metadata fields like "cost type".
    for normalized_key, original in columns.items():
        has_spend_hint = ("spend" in normalized_key) or ("cost" in normalized_key)
        looks_like_meta = ("type" in normalized_key) or ("model" in normalized_key)
        if has_spend_hint and not looks_like_meta:
            return original

    return ""


def _load_frame(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        frame = pd.read_csv(file_path, dtype=str, keep_default_na=False)
    else:
        frame = pd.read_excel(file_path, dtype=str)
    frame = frame.fillna("")
    return frame


@dataclass
class CampaignRow:
    campaign_name: str
    portfolio: str
    campaign_type: str
    state: str
    status: str
    spend: float
    sales: float
    purchases: float
    clicks: float
    impressions: float
    cpc: float
    ctr: float
    cvr: float
    start_date: str


def _collect_rows(frame: pd.DataFrame) -> list[CampaignRow]:
    normalized_columns = {normalize_campaign_header(col): col for col in frame.columns}

    campaign_col = _pick_column(
        normalized_columns,
        ["campaign name", "campaign", "name"],
    )
    if not campaign_col:
        raise ValueError("Campaign file is invalid.Please import correct file with 'campaign name' column.")

    portfolio_col = _pick_column(normalized_columns, ["portfolio name", "portfolio"]) or _pick_column_contains(
        normalized_columns,
        ["portfolio"],
    )
    type_col = _pick_column(normalized_columns, ["campaign type", "ad type", "ad format"]) or _pick_column_contains(
        normalized_columns,
        ["campaign type", "ad type", "ad format", "type"],
    )
    state_col = _pick_column(normalized_columns, ["state"]) or _pick_column_contains(normalized_columns, ["state"])
    status_col = _pick_column(normalized_columns, ["status", "campaign status"]) or _pick_column_contains(
        normalized_columns,
        ["status"],
    )
    spend_col = _pick_spend_column(normalized_columns)
    sales_col = _pick_column(
        normalized_columns,
        ["sales", "14 day total sales", "14 day total sales ", "7 day total sales", "total sales"],
    ) or _pick_column_contains(
        normalized_columns,
        ["total sales", "sales 7 day", "sales 14 day", "sales"],
    )
    purchases_col = _pick_column(
        normalized_columns,
        ["purchases", "orders", "14 day total orders", "7 day total orders", "units sold"],
    ) or _pick_column_contains(
        normalized_columns,
        ["total orders", "orders", "purchases", "units sold"],
    )
    clicks_col = _pick_column(normalized_columns, ["clicks"]) or _pick_column_contains(normalized_columns, ["click"])
    impressions_col = _pick_column(normalized_columns, ["impressions"]) or _pick_column_contains(
        normalized_columns,
        ["impression"],
    )
    cpc_col = _pick_column(normalized_columns, ["cpc", "cost per click"]) or _pick_column_contains(
        normalized_columns,
        ["cost per click", "cpc"],
    )
    ctr_col = _pick_column(normalized_columns, ["ctr", "click through rate"]) or _pick_column_contains(
        normalized_columns,
        ["click through rate", "ctr"],
    )
    cvr_col = _pick_column(normalized_columns, ["conversion rate", "cvr"]) or _pick_column_contains(
        normalized_columns,
        ["conversion rate", "cvr"],
    )
    start_col = _pick_column(normalized_columns, ["start date", "campaign start date", "date"]) or _pick_column_contains(
        normalized_columns,
        ["start date"],
    )

    rows: list[CampaignRow] = []
    for _, raw in frame.iterrows():
        campaign_name = str(raw.get(campaign_col, "")).strip()
        if not campaign_name:
            continue

        spend = clean_campaign_number(raw.get(spend_col, "")) if spend_col else 0.0
        sales = clean_campaign_number(raw.get(sales_col, "")) if sales_col else 0.0
        purchases = clean_campaign_number(raw.get(purchases_col, "")) if purchases_col else 0.0
        clicks = clean_campaign_number(raw.get(clicks_col, "")) if clicks_col else 0.0
        impressions = clean_campaign_number(raw.get(impressions_col, "")) if impressions_col else 0.0
        cpc = clean_campaign_number(raw.get(cpc_col, "")) if cpc_col else 0.0
        ctr = clean_campaign_number(raw.get(ctr_col, "")) if ctr_col else (clicks / impressions * 100.0 if impressions > 0 else 0.0)
        cvr = clean_campaign_number(raw.get(cvr_col, "")) if cvr_col else (purchases / clicks * 100.0 if clicks > 0 else 0.0)

        campaign_type = str(raw.get(type_col, "")).strip() if type_col else ""
        if campaign_type == "":
            campaign_prefix = campaign_name.strip().upper()
            if campaign_prefix.startswith("SP"):
                campaign_type = "SP"
            elif campaign_prefix.startswith("SBV"):
                campaign_type = "SBV"
            elif campaign_prefix.startswith("SB"):
                campaign_type = "SB"
            elif campaign_prefix.startswith("SD"):
                campaign_type = "SD"

        rows.append(
            CampaignRow(
                campaign_name=campaign_name,
                portfolio=str(raw.get(portfolio_col, "")).strip() if portfolio_col else "",
                campaign_type=campaign_type,
                state=str(raw.get(state_col, "")).strip() if state_col else "",
                status=str(raw.get(status_col, "")).strip() if status_col else "",
                spend=spend,
                sales=sales,
                purchases=purchases,
                clicks=clicks,
                impressions=impressions,
                cpc=cpc,
                ctr=ctr,
                cvr=cvr,
                start_date=str(raw.get(start_col, "")).strip() if start_col else "",
            )
        )
    return rows


def _to_table_row(item: dict[str, Any]) -> dict[str, Any]:
    spend = float(item.get("spend", 0.0))
    sales = float(item.get("sales", 0.0))
    clicks = float(item.get("clicks", 0.0))
    impressions = float(item.get("impressions", 0.0))
    purchases = float(item.get("purchases", 0.0))
    return {
        "campaign_name": str(item.get("campaign_name", "")),
        "portfolio": str(item.get("portfolio", "")),
        "campaign_type": str(item.get("campaign_type", "")),
        "state": str(item.get("state", "")),
        "status": str(item.get("status", "")),
        "start_date": str(item.get("start_date", "")),
        "spend": round(spend, 2),
        "sales": round(sales, 2),
        "purchases": round(purchases, 2),
        "clicks": int(round(clicks)),
        "impressions": int(round(impressions)),
        "ctr": round(float(item.get("ctr", 0.0)), 2),
        "cvr": round(float(item.get("cvr", 0.0)), 2),
        "acos": round((spend / sales * 100.0) if sales > 0 else 0.0, 2),
        "roas": round((sales / spend) if spend > 0 else 0.0, 2),
        "cpc": round((spend / clicks) if clicks > 0 else 0.0, 2),
    }


def analyze_campaign_file(file_path: Path) -> dict[str, Any]:
    frame = _load_frame(file_path)
    raw_rows = _collect_rows(frame)
    if not raw_rows:
        raise ValueError("Invalid campaign file. No valid campaign rows found. Please check the file format and data.")

    campaign_map: dict[str, dict[str, Any]] = {}
    ad_type_counts_active = {"SP": 0, "SB": 0, "SBV": 0, "SD": 0}
    ad_type_counts_inactive = {"SP": 0, "SB": 0, "SBV": 0, "SD": 0}
    active_without_portfolio: list[dict[str, Any]] = []
    high_spend_no_sales: list[dict[str, Any]] = []

    summary = {
        "rows": 0,
        "active_rows": 0,
        "spend": 0.0,
        "sales": 0.0,
        "purchases": 0.0,
        "clicks": 0.0,
        "impressions": 0.0,
    }

    for row in raw_rows:
        summary["rows"] += 1
        summary["spend"] += row.spend
        summary["sales"] += row.sales
        summary["purchases"] += row.purchases
        summary["clicks"] += row.clicks
        summary["impressions"] += row.impressions

        normalized_type = normalize_campaign_type(row.campaign_type, row.campaign_name)
        active = is_active_campaign_row(row.state, row.status)
        if normalized_type in ad_type_counts_active:
            if active:
                ad_type_counts_active[normalized_type] += 1
            else:
                ad_type_counts_inactive[normalized_type] += 1

        if active:
            summary["active_rows"] += 1
            if row.portfolio == "":
                active_without_portfolio.append(
                    _to_table_row(
                        {
                            "campaign_name": row.campaign_name,
                            "portfolio": row.portfolio,
                            "campaign_type": normalized_type,
                            "state": row.state,
                            "status": row.status,
                            "start_date": row.start_date,
                            "spend": row.spend,
                            "sales": row.sales,
                            "purchases": row.purchases,
                            "clicks": row.clicks,
                            "impressions": row.impressions,
                            "ctr": row.ctr,
                            "cvr": row.cvr,
                        }
                    )
                )
            if row.spend > 0 and row.sales <= 0:
                high_spend_no_sales.append(
                    _to_table_row(
                        {
                            "campaign_name": row.campaign_name,
                            "portfolio": row.portfolio,
                            "campaign_type": normalized_type,
                            "state": row.state,
                            "status": row.status,
                            "start_date": row.start_date,
                            "spend": row.spend,
                            "sales": row.sales,
                            "purchases": row.purchases,
                            "clicks": row.clicks,
                            "impressions": row.impressions,
                            "ctr": row.ctr,
                            "cvr": row.cvr,
                        }
                    )
                )

        bucket = campaign_map.get(row.campaign_name)
        if bucket is None:
            bucket = {
                "campaign_name": row.campaign_name,
                "portfolio": row.portfolio,
                "campaign_type": normalized_type,
                "state": row.state,
                "status": row.status,
                "start_date": row.start_date,
                "spend": 0.0,
                "sales": 0.0,
                "purchases": 0.0,
                "clicks": 0.0,
                "impressions": 0.0,
            }
            campaign_map[row.campaign_name] = bucket

        bucket["spend"] += row.spend
        bucket["sales"] += row.sales
        bucket["purchases"] += row.purchases
        bucket["clicks"] += row.clicks
        bucket["impressions"] += row.impressions

    campaigns = [_to_table_row(item) for item in campaign_map.values()]
    campaigns = [row for row in campaigns if row["spend"] > 0 or row["sales"] > 0 or row["clicks"] > 0]

    campaigns_by_sales = sorted(campaigns, key=lambda x: x["sales"], reverse=True)
    campaigns_by_spend = sorted(campaigns, key=lambda x: x["spend"], reverse=True)
    campaigns_by_acos_high = sorted(campaigns, key=lambda x: x["acos"], reverse=True)
    campaigns_by_acos_low = sorted(campaigns, key=lambda x: x["acos"])
    roas_non_zero = [x for x in campaigns if x["roas"] > 0]
    campaigns_by_roas_high = sorted(roas_non_zero, key=lambda x: x["roas"], reverse=True)
    campaigns_by_roas_low = sorted(roas_non_zero, key=lambda x: x["roas"])
    campaigns_by_purchases_high = sorted(campaigns, key=lambda x: x["purchases"], reverse=True)
    campaigns_by_purchases_low = sorted(campaigns, key=lambda x: x["purchases"])

    total_spend = float(summary["spend"])
    high_acos_spend = sum(x["spend"] for x in campaigns if x["acos"] >= 40.0)
    summary_output = {
        "rows": int(summary["rows"]),
        "campaign_count": len(campaigns),
        "active_rows": int(summary["active_rows"]),
        "spend": round(total_spend, 2),
        "sales": round(float(summary["sales"]), 2),
        "purchases": int(round(float(summary["purchases"]))),
        "clicks": int(round(float(summary["clicks"]))),
        "impressions": int(round(float(summary["impressions"]))),
        "ctr": round((float(summary["clicks"]) / float(summary["impressions"]) * 100.0) if float(summary["impressions"]) > 0 else 0.0, 2),
        "cvr": round((float(summary["purchases"]) / float(summary["clicks"]) * 100.0) if float(summary["clicks"]) > 0 else 0.0, 2),
        "acos": round((total_spend / float(summary["sales"]) * 100.0) if float(summary["sales"]) > 0 else 0.0, 2),
        "roas": round((float(summary["sales"]) / total_spend) if total_spend > 0 else 0.0, 2),
        "high_acos_spend_pct": round((high_acos_spend / total_spend * 100.0) if total_spend > 0 else 0.0, 2),
        "spend_leak": round(sum(x["spend"] for x in high_spend_no_sales), 2),
        "active_without_portfolio_count": len(active_without_portfolio),
    }

    rec_high_acos = [x for x in campaigns_by_acos_high if x["spend"] >= 300 and x["acos"] >= 40][:15]
    rec_good_roas_low_cpc = [x for x in campaigns_by_roas_high if x["roas"] >= 4 and x["cpc"] <= 20][:15]

    return {
        "summary": summary_output,
        "campaign_preview": campaigns_by_spend[:200],
        "top_by_sales": campaigns_by_sales[:20],
        "top_by_spend": campaigns_by_spend[:20],
        "top_by_acos": campaigns_by_acos_high[:20],
        "low_by_acos": campaigns_by_acos_low[:20],
        "top_by_roas": campaigns_by_roas_high[:20],
        "low_by_roas": campaigns_by_roas_low[:20],
        "top_by_purchases": campaigns_by_purchases_high[:20],
        "low_by_purchases": campaigns_by_purchases_low[:20],
        "high_spend_no_sales": sorted(high_spend_no_sales, key=lambda x: x["spend"], reverse=True)[:20],
        "active_without_portfolio": sorted(active_without_portfolio, key=lambda x: x["spend"], reverse=True)[:20],
        "ad_type_counts_active": ad_type_counts_active,
        "ad_type_counts_inactive": ad_type_counts_inactive,
        "recommendation_tables": {
            "high_acos": rec_high_acos,
            "good_roas_low_cpc": rec_good_roas_low_cpc,
        },
    }
