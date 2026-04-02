"""API Market Intelligence."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Cookie
from fastapi.responses import JSONResponse

from app.constants import DP
from app.web.context import _svc, require_mi_user
from app.web.scope import (
    reject_if_cat_sub_id_lists_out_of_scope,
    reject_if_cat_sub_out_of_scope,
    reject_if_category_out_of_scope,
    reject_if_parent_category_out_of_scope,
)

router = APIRouter()


@router.get("/api/market-intelligence/base", tags=["Market Intelligence"])
async def api_market_intelligence_base(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
):
    """Market Intelligence: metadata (brand_cats, cat_ids, sub_ids). First call for progressive loading."""
    user, err = require_mi_user(access_token)
    if err:
        return err
    return await _svc().get_mi_base(period_start, period_end, user.brand_id)


@router.get("/api/market-intelligence/all", tags=["Market Intelligence"])
async def api_market_intelligence_all(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    discount_category_id: str | None = None,
    discount_subcategory_id: str | None = None,
):
    """Market Intelligence: batch - base + sales + promo + peak + discount in una sola chiamata. Caricamento iniziale veloce."""
    user, err = require_mi_user(access_token)
    if err:
        return err
    return await _svc().get_mi_all(
        period_start,
        period_end,
        user.brand_id,
        discount_cat=discount_category_id,
        discount_subcat=discount_subcategory_id,
    )


@router.get("/api/market-intelligence/all-years", tags=["Market Intelligence"])
async def api_market_intelligence_all_years(
    access_token: Optional[str] = Cookie(None),
    discount_category_id: str | None = None,
    discount_subcategory_id: str | None = None,
):
    """Market Intelligence: tutti gli anni in una sola chiamata. Dropdown year istantanei da subito."""
    user, err = require_mi_user(access_token)
    if err:
        return err
    bad = reject_if_cat_sub_out_of_scope(user, discount_category_id, discount_subcategory_id)
    if bad:
        return bad
    return await _svc().get_mi_all_years(
        user.brand_id,
        discount_cat=discount_category_id,
        discount_subcat=discount_subcategory_id,
    )


@router.get("/api/market-intelligence/available-years", tags=["Market Intelligence"])
async def api_market_intelligence_available_years(access_token: Optional[str] = Cookie(None)):
    """Elenco anni (solo BigQuery DISTINCT) per primo caricamento MI leggero."""
    user, err = require_mi_user(access_token)
    if err:
        return err
    return await _svc().get_mi_available_years_payload()


@router.get("/api/market-intelligence/incremental-yoy", tags=["Market Intelligence"])
async def api_market_intelligence_incremental_yoy(
    period_start: str,
    period_end: str,
    access_token: Optional[str] = Cookie(None),
):
    """Incremental YoY multi-anno (range di anni interi 01-01 … 12-31)."""
    user, err = require_mi_user(access_token)
    if err:
        return err
    return await _svc().get_mi_incremental_yoy_api(period_start, period_end, user.brand_id)


@router.get("/api/market-intelligence/sales")
async def api_market_intelligence_sales(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    cat_ids: str = "",
    sub_ids: str = "",
    subcategory_category_id: str | None = None,
):
    """Market Intelligence: sales value/volume, category/subcategory pie, prev year."""
    user, err = require_mi_user(access_token)
    if err:
        return err
    bad = reject_if_cat_sub_id_lists_out_of_scope(user, cat_ids, sub_ids)
    if bad:
        return bad
    badp = reject_if_parent_category_out_of_scope(user, subcategory_category_id)
    if badp:
        return badp
    cat_list = [x.strip() for x in cat_ids.split(",") if x.strip()] if cat_ids else []
    sub_list = [x.strip() for x in sub_ids.split(",") if x.strip()] if sub_ids else []
    sub_cat_id = subcategory_category_id or (cat_list[0] if cat_list else None)
    return await _svc().get_mi_sales(period_start, period_end, user.brand_id, cat_list, sub_list, sub_cat_id)


@router.get("/api/market-intelligence/promo")
async def api_market_intelligence_promo(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    cat_ids: str = "",
    sub_ids: str = "",
):
    """Market Intelligence: promo share and ROI. Requires cat_ids, sub_ids from /base."""
    user, err = require_mi_user(access_token)
    if err:
        return err
    base = await _svc().get_mi_base(period_start, period_end, user.brand_id)
    if base.get("error"):
        return base
    brand_cats = base.get("brand_categories", [])
    brand_subcats_map = base.get("brand_subcategories", {})
    return await _svc().get_mi_promo(period_start, period_end, user.brand_id, brand_cats, brand_subcats_map)


@router.get("/api/market-intelligence/peak")
async def api_market_intelligence_peak(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
):
    """Market Intelligence: peak events. Fetches base internally for brand_cats."""
    user, err = require_mi_user(access_token)
    if err:
        return err
    base = await _svc().get_mi_base(period_start, period_end, user.brand_id)
    if base.get("error"):
        return base
    brand_cats = base.get("brand_categories", [])
    brand_subcats_map = base.get("brand_subcategories", {})
    return await _svc().get_mi_peak(period_start, period_end, user.brand_id, brand_cats, brand_subcats_map)


@router.get("/api/market-intelligence/discount")
async def api_market_intelligence_discount(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    discount_category_id: str | None = None,
    discount_subcategory_id: str | None = None,
):
    """Market Intelligence: discount depth by category/subcategory."""
    user, err = require_mi_user(access_token)
    if err:
        return err
    bad = reject_if_cat_sub_out_of_scope(user, discount_category_id, discount_subcategory_id)
    if bad:
        return bad
    base = await _svc().get_mi_base(period_start, period_end, user.brand_id)
    if base.get("error"):
        return base
    brand_cats = base.get("brand_categories", [])
    sub_ids = base.get("sub_ids", [])
    return await _svc().get_mi_discount(
        period_start,
        period_end,
        user.brand_id,
        brand_cats,
        sub_ids,
        discount_cat=discount_category_id,
        discount_subcat=discount_subcategory_id,
    )


@router.get("/api/market-intelligence/top-products", tags=["Market Intelligence"])
async def api_market_intelligence_top_products(
    access_token: Optional[str] = Cookie(None),
    year: str | None = None,
    category_id: str | None = None,
    subcategory_id: str | None = None,
    channel: str | None = None,
):
    """Market Intelligence: top products per anno, category/subcategory, channel."""
    user, err = require_mi_user(access_token)
    if err:
        return err
    bad = reject_if_cat_sub_out_of_scope(user, category_id, subcategory_id)
    if bad:
        return bad
    y = year or str(DP[0][:4])
    return await _svc().get_mi_top_products(y, user.brand_id, category_id, subcategory_id, channel)


@router.get("/api/market-intelligence/segment-by-sku", tags=["Market Intelligence"])
async def api_market_intelligence_segment_by_sku(
    access_token: Optional[str] = Cookie(None),
    product_id: str | None = None,
    year: str | None = None,
    category_id: str | None = None,
    channel: str | None = None,
):
    """Market Intelligence: segment breakdown per SKU (tutte le vendite)."""
    user, err = require_mi_user(access_token)
    if err:
        return err
    if not product_id:
        return JSONResponse({"error": "Product ID required"}, status_code=400)
    bad = reject_if_category_out_of_scope(user, category_id)
    if bad:
        return bad
    y = year or str(DP[0][:4])
    return await _svc().get_mi_segment_by_sku(int(product_id), user.brand_id, y, category_id, channel)
