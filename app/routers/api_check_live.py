"""API Check Live Promo."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Cookie
from fastapi.responses import JSONResponse

from app.web.context import _svc, default_check_live_dates, get_user
from app.web.scope import reject_if_category_out_of_scope

router = APIRouter()


@router.get("/api/check-live-promo/active", tags=["Check Live Promo"])
async def api_check_live_promo_active(
    access_token: Optional[str] = Cookie(None),
    date_start: str | None = None,
    date_end: str | None = None,
    promo_id: str | None = None,
    category_id: str | None = None,
    channel: str | None = None,
):
    """Promos with actual sales in the selected period for user's brand."""
    user = get_user(access_token)
    if not user or not user.brand_id:
        return JSONResponse({"error": "Brand required for active promos"}, status_code=400)
    bad = reject_if_category_out_of_scope(user, category_id)
    if bad:
        return bad
    ds, de = default_check_live_dates()
    date_start = date_start or ds
    date_end = date_end or de
    return await _svc().get_active_promos(date_start, date_end, user.brand_id, promo_id, category_id, channel)


@router.get("/api/check-live-promo/sku", tags=["Check Live Promo"])
async def api_check_live_promo_sku(
    access_token: Optional[str] = Cookie(None),
    date_start: str | None = None,
    date_end: str | None = None,
    promo_id: str | None = None,
    category_id: str | None = None,
    channel: str | None = None,
):
    """SKU-level promo performance. Requires auth; uses user.brand_id."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Check Live Promo"}, status_code=400)
    bad = reject_if_category_out_of_scope(user, category_id)
    if bad:
        return bad
    ds, de = default_check_live_dates()
    date_start = date_start or ds
    date_end = date_end or de
    return await _svc().get_promo_sku(user.brand_id, date_start, date_end, promo_id, category_id, channel)


@router.get("/api/check-live-promo/segment-breakdown", tags=["Check Live Promo"])
async def api_check_live_promo_segment(
    access_token: Optional[str] = Cookie(None),
    product_id: str | None = None,
    date_start: str | None = None,
    date_end: str | None = None,
    promo_id: str | None = None,
    category_id: str | None = None,
    channel: str | None = None,
):
    """Segment breakdown for a product (product_id) or aggregate (no product_id)."""
    user = get_user(access_token)
    if not user or not user.brand_id:
        return JSONResponse({"error": "Brand required"}, status_code=400)
    bad = reject_if_category_out_of_scope(user, category_id)
    if bad:
        return bad
    ds, de = default_check_live_dates()
    date_start = date_start or ds
    date_end = date_end or de
    pid = int(product_id) if product_id and str(product_id).strip() else None
    return await _svc().get_segment_breakdown(
        pid, user.brand_id, date_start, date_end, promo_id, category_id, channel
    )
