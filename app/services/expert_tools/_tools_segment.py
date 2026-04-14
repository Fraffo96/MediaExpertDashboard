"""Segment / needstate / marketing-summary tools."""

from __future__ import annotations

from typing import Any

from app.db.queries.marketing import segment_by_category as mkt_seg_cat
from app.services import marketing as marketing_svc

from ._base import _truncate_rows


def tool_get_segment_breakdown_for_category(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int,
    subcategory_id: int | None = None,
) -> dict[str, Any]:
    """Share of brand sales in a category by HCG segment (orders / fact_order_items)."""
    if not (1 <= int(parent_category_id) <= 10):
        return {"error": "parent_category_id must be 1–10", "rows": []}
    rows = mkt_seg_cat.query_segment_breakdown_for_category_sales(
        int(brand_id),
        ps,
        pe,
        int(parent_category_id),
        int(subcategory_id) if subcategory_id and int(subcategory_id) >= 100 else None,
        channel=None,
    )
    return {"segment_breakdown": _truncate_rows(rows or [])}


def _trim_segment_summary_payload(raw: dict[str, Any]) -> dict[str, Any]:
    """Keep segment summary small for Gemini (pain_points, needstates, top SKUs/categories)."""
    segs = raw.get("segments") or []
    out_segs: list[dict[str, Any]] = []
    for s in segs[:6]:
        out_segs.append(
            {
                "segment_id": s.get("segment_id"),
                "name": s.get("name"),
                "pain_points": s.get("pain_points"),
                "needstates": s.get("needstates"),
                "top_categories_note": s.get("top_categories_note"),
                "top_categories": _truncate_rows(s.get("top_categories") or [], 5),
                "top_skus": _truncate_rows(s.get("top_skus") or [], 5),
            }
        )
    return {"segments": out_segs, "period": raw.get("period")}


def tool_get_segment_marketing_summary(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    segment_id: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
) -> dict[str, Any]:
    """Pain points, needstate tags, top categories/SKUs for one HCG segment (optionally scoped to category/subcategory)."""
    sid = int(segment_id)
    if not (1 <= sid <= 6):
        return {"error": "segment_id must be 1–6"}
    pc = int(parent_category_id) if parent_category_id and 1 <= int(parent_category_id) <= 10 else None
    sub = int(subcategory_id) if subcategory_id and int(subcategory_id) >= 100 else None
    raw = marketing_svc.get_segment_summary(ps, pe, sid, pc, sub, int(brand_id))
    return _trim_segment_summary_payload(raw if isinstance(raw, dict) else {})


def tool_get_category_needstate_landscape(
    ps: str,
    pe: str,
    *,
    parent_category_id: int,
) -> dict[str, Any]:
    """All segments: revenue share in parent category + dominant needstate label (market-wide, not brand-only)."""
    pc = int(parent_category_id)
    if not (1 <= pc <= 10):
        return {"error": "parent_category_id must be 1–10"}
    raw = marketing_svc.get_needstates(ps, pe, pc, None)
    segs = raw.get("segments") or []
    return {
        "category_id": raw.get("category_id"),
        "period": raw.get("period"),
        "segments": _truncate_rows(segs, 12),
    }


def tool_get_needstate_dimensions_for_segment(
    ps: str,
    pe: str,
    *,
    parent_category_id: int,
    segment_id: int,
) -> dict[str, Any]:
    """Seven needstate dimensions with affinity scores for one segment in a parent category (spider / prioritisation)."""
    pc = int(parent_category_id)
    sid = int(segment_id)
    if not (1 <= pc <= 10) or not (1 <= sid <= 6):
        return {"error": "parent_category_id 1–10 and segment_id 1–6 required"}
    return marketing_svc.get_needstates(ps, pe, pc, sid)
