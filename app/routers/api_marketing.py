"""API Marketing (segmenti, needstates, media, purchasing)."""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, Cookie
from fastapi.responses import JSONResponse

from app.auth.brand_scope import brand_category_scope_ids
from app.constants import DP, MKT_DEFAULT_PERIOD
from app.web.context import _svc, get_user
from app.web.scope import (
    reject_if_brand_param_not_allowed,
    reject_if_cat_sub_out_of_scope,
    reject_if_parent_category_out_of_scope,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/marketing/categories-by-brand", tags=["Marketing"])
async def api_marketing_categories_by_brand(
    access_token: Optional[str] = Cookie(None),
    brand_id: str | None = None,
):
    """Marketing: categories and subcategories where the brand has products. For filter dropdown when brand selected."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.can_access_tab("marketing"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    if not brand_id or not str(brand_id).strip():
        return JSONResponse({"categories": [], "subcategories": []})
    try:
        bid = int(brand_id)
    except (ValueError, TypeError):
        return JSONResponse({"categories": [], "subcategories": []})
    from app.db.queries import shared

    cats = await asyncio.to_thread(shared.query_categories_by_brand, bid) or []
    subcats = await asyncio.to_thread(shared.query_subcategories_by_brand, bid) or []
    return JSONResponse({"categories": list(cats), "subcategories": list(subcats)})


@router.get("/api/marketing/segments", tags=["Marketing"])
async def api_marketing_segments(
    access_token: Optional[str] = Cookie(None),
    period_start: str = MKT_DEFAULT_PERIOD[0],
    period_end: str = MKT_DEFAULT_PERIOD[1],
    segment_id: str | None = None,
    category_id: str | None = None,
    subcategory_id: str | None = None,
    brand_id: str | None = None,
):
    """Marketing: segment summary – pain points, needstates, top categories, top SKUs. Usa anno intero per precalc."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.can_access_tab("marketing"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    badb = reject_if_brand_param_not_allowed(user, brand_id)
    if badb:
        return badb
    badc = reject_if_cat_sub_out_of_scope(user, category_id, subcategory_id)
    if badc:
        return badc
    seg_id = int(segment_id) if segment_id and str(segment_id).strip() else None
    cat_id = int(category_id) if category_id and str(category_id).strip() else None
    sub_id = int(subcategory_id) if subcategory_id and str(subcategory_id).strip() else None
    bid = int(brand_id) if brand_id and str(brand_id).strip() else (user.brand_id if user.brand_id else None)
    return await asyncio.to_thread(_svc().get_segment_summary, period_start, period_end, seg_id, cat_id, sub_id, bid)


@router.get("/api/marketing/segment-by-category", tags=["Marketing"])
async def api_marketing_segment_by_category(
    access_token: Optional[str] = Cookie(None),
    year: str | None = None,
    category_id: str | None = None,
    subcategory_id: str | None = None,
    channel: str | None = None,
):
    """Marketing Overview: segment breakdown for brand sales in category/subcategory (calendar year)."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.can_access_tab("marketing"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    badc = reject_if_cat_sub_out_of_scope(user, category_id, subcategory_id)
    if badc:
        return badc
    bid = user.brand_id if user.brand_id else None
    try:
        y = int(year) if year and str(year).strip() else int(str(DP[0])[:4])
    except (ValueError, TypeError):
        y = int(str(DP[0])[:4])
    try:
        cat_id = int(category_id) if category_id and str(category_id).strip() else None
    except (ValueError, TypeError):
        cat_id = None
    try:
        sub_id = int(subcategory_id) if subcategory_id and str(subcategory_id).strip() else None
    except (ValueError, TypeError):
        sub_id = None
    ch = channel if channel and str(channel).strip() in ("web", "app", "store") else None
    return await asyncio.to_thread(_svc().get_segment_by_category, bid, y, cat_id, sub_id, ch)


@router.get("/api/marketing/needstates", tags=["Marketing"])
async def api_marketing_needstates(
    access_token: Optional[str] = Cookie(None),
    category_id: str | None = None,
    segment_id: str | None = None,
):
    """Marketing: spider chart – needstate dimensions for (category, segment). No date filter."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.can_access_tab("marketing"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    pset = set(brand_category_scope_ids(user)[0])
    try:
        if category_id and str(category_id).strip():
            cat_id = int(category_id)
        else:
            cat_id = min(pset) if pset else 1
        seg_id = int(segment_id) if segment_id and str(segment_id).strip() else 1
    except (ValueError, TypeError):
        cat_id, seg_id = (min(pset) if pset else 1), 1
    if cat_id not in pset:
        return JSONResponse({"error": "Category not in your brand scope"}, status_code=400)
    try:
        return await asyncio.to_thread(_svc().get_needstates_spider, cat_id, seg_id)
    except Exception as e:
        logger.exception("Needstates API error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/marketing/media-preferences", tags=["Marketing"])
async def api_marketing_media_preferences(
    access_token: Optional[str] = Cookie(None),
    segment_id: str | None = None,
    category_id: str | None = None,
):
    """Marketing: media touchpoint mix per segment (static profiles); category_id = macro-categoria per nudge dati."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.can_access_tab("marketing"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    bad = reject_if_parent_category_out_of_scope(user, category_id)
    if bad:
        return bad
    try:
        sid = int(segment_id) if segment_id and str(segment_id).strip() else None
    except (ValueError, TypeError):
        sid = None
    pc = int(category_id) if category_id and str(category_id).strip() else None
    return await asyncio.to_thread(_svc().get_media_preferences, sid, pc)


@router.get("/api/marketing/purchasing", tags=["Marketing"])
async def api_marketing_purchasing(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    segment_id: str | None = None,
    category_id: str | None = None,
):
    """Marketing: purchasing process – channel mix, peak events per segment; category_id = macro (live BQ / sintetico)."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.can_access_tab("marketing"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    bad = reject_if_parent_category_out_of_scope(user, category_id)
    if bad:
        return bad
    seg_id = int(segment_id) if segment_id and str(segment_id).strip() else None
    pc = int(category_id) if category_id and str(category_id).strip() else None
    return await asyncio.to_thread(_svc().get_purchasing, period_start, period_end, seg_id, pc)
