"""Market/sales tools: category sales, brand vs market, monthly trend, gender breakdown."""

from __future__ import annotations

from typing import Any

from app.db.queries.market_intelligence import sales as mi_sales
from app.db.queries.basic import products as basic_products

from ._base import _json_safe, _truncate_rows


def tool_get_sales_by_category_for_brand(ps: str, pe: str, *, brand_id: int) -> dict[str, Any]:
    """Brand vs market gross PLN per parent category (only categories where brand sells)."""
    rows = mi_sales.query_sales_value_by_category(ps, pe, int(brand_id))
    return {"sales_by_parent_category": _truncate_rows(rows or [])}


def tool_get_brand_vs_market_subcategory_sales(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
) -> dict[str, Any]:
    """Subcategory-level brand vs market (media) gross PLN."""
    cat = int(parent_category_id) if parent_category_id and 1 <= int(parent_category_id) <= 10 else None
    sub = int(subcategory_id) if subcategory_id and int(subcategory_id) >= 100 else None
    rows = mi_sales.query_sales_value_brand_vs_media(ps, pe, int(brand_id), cat=cat, subcat=sub)
    return {"subcategory_brand_vs_market": _truncate_rows(rows or [])}


def tool_get_sales_trend_by_month(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
) -> dict[str, Any]:
    """Monthly gross PLN and units trend for the brand in the date window. Highlights best and worst months."""
    rows = basic_products.query_sales_trend_by_month(
        ps, pe,
        brand_id=int(brand_id),
        parent_category_id=int(parent_category_id) if parent_category_id else None,
        subcategory_id=int(subcategory_id) if subcategory_id else None,
    )
    rows = list(rows) if rows else []
    best = max(rows, key=lambda r: float(r.get("gross_pln") or 0), default=None) if rows else None
    worst = min(rows, key=lambda r: float(r.get("gross_pln") or 0), default=None) if rows else None
    return {
        "monthly_trend": _truncate_rows(rows, 60),
        "best_month": _json_safe(best),
        "worst_month": _json_safe(worst),
        "total_gross_pln": round(sum(float(r.get("gross_pln") or 0) for r in rows), 2),
    }


def tool_get_sales_by_gender(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
) -> dict[str, Any]:
    """Sales breakdown by gender (M/F): gross PLN, units, and % of total for each gender."""
    rows = basic_products.query_sales_by_gender_breakdown(
        ps, pe,
        brand_id=int(brand_id),
        parent_category_id=int(parent_category_id) if parent_category_id else None,
        subcategory_id=int(subcategory_id) if subcategory_id else None,
    )
    return {"gender_breakdown": _truncate_rows(rows or [], 10)}
