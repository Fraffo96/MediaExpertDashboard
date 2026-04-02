"""API filtri e dashboard Sales legacy (basic, promo, customer, simulation, why-buy)."""
from __future__ import annotations

from fastapi import APIRouter

from app.constants import DP
from app.web.context import _svc, filters_payload

router = APIRouter()


@router.get("/api/filters")
async def api_filters():
    return await filters_payload()


@router.get("/api/basic")
async def api_basic(
    period_start: str = DP[0],
    period_end: str = DP[1],
    category_id: str | None = None,
    segment_id: str | None = None,
    gender: str | None = None,
    brand_id: str | None = None,
    subcategory_id: str | None = None,
    incremental_yoy_promo_id: str | None = None,
    channel: str | None = None,
):
    return await _svc().get_basic(
        period_start,
        period_end,
        category_id,
        segment_id,
        gender,
        brand_id,
        subcategory_id,
        incremental_yoy_promo_id,
        channel,
    )


@router.get("/api/basic/granular")
async def api_basic_granular(period_start: str = DP[0], period_end: str = DP[1], channel: str | None = None):
    """Endpoint leggero: tabelle detail per filtro client-side istantaneo. Usa questo per prefetch in background."""
    return await _svc().get_basic_granular(period_start, period_end, channel)


@router.get("/api/basic/incremental_yoy")
async def api_basic_incremental_yoy(
    period_start: str = DP[0],
    period_end: str = DP[1],
    category_id: str | None = None,
    segment_id: str | None = None,
    gender: str | None = None,
    brand_id: str | None = None,
    promo_id: str | None = None,
    promo_ids: str | None = None,
):
    """Lightweight endpoint: returns only incremental YoY (or by-promo for compare). Use promo_id for single, promo_ids (comma-separated) for compare."""
    ids = []
    if promo_ids:
        ids = [x.strip() for x in promo_ids.split(",") if x.strip()]
    elif promo_id:
        ids = [promo_id]
    return await _svc().get_incremental_yoy(
        period_start, period_end, category_id, segment_id, gender, brand_id, ids if ids else None
    )


@router.get("/api/promo")
async def api_promo(
    period_start: str = DP[0],
    period_end: str = DP[1],
    promo_type: str | None = None,
    category_id: str | None = None,
    segment_id: str | None = None,
):
    return await _svc().get_promo(period_start, period_end, promo_type, category_id, segment_id)


@router.get("/api/customer")
async def api_customer(
    period_start: str = DP[0],
    period_end: str = DP[1],
    segment_id: str | None = None,
    gender: str | None = None,
):
    return await _svc().get_customer(period_start, period_end, segment_id, gender)


@router.get("/api/simulation")
async def api_simulation(
    period_start: str = DP[0],
    period_end: str = DP[1],
    promo_type: str | None = None,
    segment_id: str | None = None,
    category_id: str | None = None,
):
    return await _svc().get_simulation(period_start, period_end, promo_type, segment_id, category_id)


@router.get("/api/why-buy")
async def api_why_buy(period_start: str = DP[0], period_end: str = DP[1], category_id: str | None = None):
    return await _svc().get_why_buy(period_start, period_end, category_id)
