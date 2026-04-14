"""Promo tools: segment promo responsiveness, product segment breakdown, promo ROI by type."""

from __future__ import annotations

from typing import Any

from app.db.queries.basic import promo_roi as basic_promo_roi
from app.db.queries.market_intelligence import segment_sku as mi_segment_sku
from app.db.queries import promo_creator as promo_q

from ._base import _truncate_rows


def tool_get_segment_promo_responsiveness(
    ps: str,
    pe: str,
    *,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
    promo_type: str | None = None,
) -> dict[str, Any]:
    """Top segments by promo share % in category/subcategory (requires parent or sub)."""
    cat = int(parent_category_id) if parent_category_id and 1 <= int(parent_category_id) <= 10 else None
    sub = int(subcategory_id) if subcategory_id and int(subcategory_id) >= 100 else None
    if cat is None and sub is None:
        return {
            "error": "Provide parent_category_id (1–10) or subcategory_id (>=100)",
            "segments": [],
        }
    rows = promo_q.query_segment_promo_responsiveness(ps, pe, cat=cat, subcat=sub, promo_type=promo_type)
    return {"promo_responsive_segments": _truncate_rows(rows or [])}


def tool_get_product_segment_breakdown(
    ps: str,
    pe: str,
    *,
    product_id: int,
    brand_id: int,
    category_filter: int | None = None,
) -> dict[str, Any]:
    """All-sales segment breakdown for one SKU (remove-SKU / cannibalization scenarios)."""
    cid = str(int(category_filter)) if category_filter is not None else None
    rows = mi_segment_sku.query_segment_breakdown_for_product_all_sales(
        int(product_id), int(brand_id), ps, pe, category_id=cid, channel=None
    )
    total_gross = sum(float(r.get("gross_pln") or 0) for r in rows or [])
    total_units = sum(float(r.get("units") or 0) for r in rows or [])
    return {
        "product_id": int(product_id),
        "segments": _truncate_rows(rows or []),
        "totals": {"gross_pln": round(total_gross, 2), "units": round(total_units, 2)},
    }


def tool_get_promo_roi_by_type_for_brand(
    ps: str,
    pe: str,
    *,
    brand_id: int | None,
    parent_category_id: int | None = None,
) -> dict[str, Any]:
    """Average promo ROI and attributed sales by promo type/name (optional parent category filter)."""
    cat = int(parent_category_id) if parent_category_id and 1 <= int(parent_category_id) <= 10 else None
    rows = basic_promo_roi.query_promo_roi_by_type(ps, pe, cat=cat, seg=None, gender=None, brand=brand_id)
    return {"promo_roi_by_type": _truncate_rows(rows or [])}
