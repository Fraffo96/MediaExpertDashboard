"""Customer / channel / journey tools: channel mix, media touchpoints, purchasing journey, customer stats."""

from __future__ import annotations

from typing import Any

from app.db.queries.marketing import purchasing as mkt_purchasing
from app.db.queries.basic import customers as basic_customers
from app.services import marketing as marketing_svc

from ._base import _json_safe, _opt_int, _truncate_rows


def tool_get_purchasing_channel_mix(
    ps: str,
    pe: str,
    *,
    segment_id: int | None = None,
    parent_category_id: int | None = None,
) -> dict[str, Any]:
    """Channel mix (web/app/store) with buyers, orders, gross_pln; optional segment and macro category filter."""
    rows = mkt_purchasing.query_purchasing_channel_mix(
        ps, pe, segment_id=segment_id, parent_category_id=parent_category_id
    )
    return {"channel_mix": _truncate_rows(rows or [])}


def tool_get_media_touchpoints(
    *,
    segment_id: int,
    parent_category_id: int | None = None,
) -> dict[str, Any]:
    """How the segment uses media / touchpoints (social, TV-style blocks, etc.) — static model tuned by category."""
    sid = int(segment_id)
    if not (1 <= sid <= 6):
        return {"error": "segment_id must be 1–6"}
    pc = int(parent_category_id) if parent_category_id and 1 <= int(parent_category_id) <= 10 else None
    return marketing_svc.get_media_preferences(sid, pc)


def tool_get_purchasing_journey(
    ps: str,
    pe: str,
    *,
    segment_id: int | None = None,
    parent_category_id: int | None = None,
) -> dict[str, Any]:
    """Purchase channels, peak events, traffic source mix, pre-purchase search intent (for 'how they inform themselves')."""
    sid = _opt_int(segment_id)
    pc = int(parent_category_id) if parent_category_id and 1 <= int(parent_category_id) <= 10 else None
    raw = marketing_svc.get_purchasing(ps, pe, sid, pc)
    if not isinstance(raw, dict):
        return {"error": "no data"}
    return {
        "channel_mix": _truncate_rows(raw.get("channel_mix") or [], 20),
        "peak_events": _truncate_rows(raw.get("peak_events") or [], 12),
        "source_mix": raw.get("source_mix"),
        "pre_purchase_searches": raw.get("pre_purchase_searches"),
        "segment_id": raw.get("segment_id"),
        "parent_category_id": raw.get("parent_category_id"),
        "period": raw.get("period"),
    }


def tool_get_customer_stats(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int | None = None,
) -> dict[str, Any]:
    """Aggregate buyer metrics: unique customers, AOV, loyalty card %, omnichannel %, app %, channel breakdown."""
    stats = basic_customers.query_customer_stats(
        ps, pe,
        brand_id=int(brand_id),
        parent_category_id=int(parent_category_id) if parent_category_id else None,
    )
    if isinstance(stats, dict):
        return _json_safe(stats)
    return {"error": "no data"}
