"""API Brand Comparison."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Cookie
from fastapi.responses import JSONResponse

from app.constants import DP
from app.web.context import _bc, _svc, get_user
from app.web.scope import (
    reject_if_cat_sub_out_of_scope,
    reject_if_cat_sub_id_lists_out_of_scope,
    reject_if_parent_category_out_of_scope,
)

router = APIRouter()


@router.get("/api/brand-comparison")
async def api_brand_comparison(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    category_id: str | None = None,
    subcategory_id: str | None = None,
    competitor_id: str | None = None,
):
    """Brand Comparison: competitors, sales/ROI comparison. Requires auth; uses user.brand_id."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Brand Comparison"}, status_code=400)
    bad = reject_if_cat_sub_out_of_scope(user, category_id, subcategory_id)
    if bad:
        return bad
    return await _svc().get_brand_comparison(period_start, period_end, user.brand_id, competitor_id, category_id, subcategory_id)


@router.get("/api/brand-comparison/competitors", tags=["Brand Comparison"])
async def api_brand_comparison_competitors(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
):
    """Brand Comparison: solo lista competitor. Leggero per caricamento iniziale."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Brand Comparison"}, status_code=400)
    return await _bc().get_bc_competitors(period_start, period_end, user.brand_id)


@router.get("/api/brand-comparison/base", tags=["Brand Comparison"])
async def api_brand_comparison_base(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    competitor_id: str | None = None,
):
    """Brand Comparison: metadata (brand_cats, competitors, years, channels). competitor_id → intersezione categorie con il competitor."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Brand Comparison"}, status_code=400)
    return await _bc().get_bc_base(period_start, period_end, user.brand_id, competitor_id)


@router.get("/api/brand-comparison/all", tags=["Brand Comparison"])
async def api_brand_comparison_all(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    competitor_id: str | None = None,
    discount_category_id: str | None = None,
    discount_subcategory_id: str | None = None,
):
    """Brand Comparison: batch base + sales + promo + peak + discount. Richiede competitor_id."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Brand Comparison"}, status_code=400)
    if not competitor_id:
        return JSONResponse({"error": "Competitor required"}, status_code=400)
    return await _bc().get_bc_all(
        period_start,
        period_end,
        user.brand_id,
        competitor_id,
        discount_cat=discount_category_id,
        discount_subcat=discount_subcategory_id,
    )


@router.get("/api/brand-comparison/sales", tags=["Brand Comparison"])
async def api_brand_comparison_sales(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    competitor_id: str | None = None,
    cat_ids: str = "",
    sub_ids: str = "",
    subcategory_category_id: str | None = None,
):
    """Solo sales/pie (live o precalc). Per cambio periodo su Category/Subcategory senza batch full."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required"}, status_code=400)
    if not competitor_id:
        return JSONResponse({"error": "Competitor required"}, status_code=400)
    bad = reject_if_cat_sub_id_lists_out_of_scope(user, cat_ids, sub_ids)
    if bad:
        return bad
    badp = reject_if_parent_category_out_of_scope(user, subcategory_category_id)
    if badp:
        return badp
    cat_list = [x.strip() for x in cat_ids.split(",") if x.strip()] if cat_ids else []
    sub_list = [x.strip() for x in sub_ids.split(",") if x.strip()] if sub_ids else []
    sub_cat_id = subcategory_category_id or (cat_list[0] if cat_list else None)
    return await _bc().get_bc_sales(
        period_start, period_end, user.brand_id, competitor_id, cat_list, sub_list, sub_cat_id
    )


@router.get("/api/brand-comparison/promo", tags=["Brand Comparison"])
async def api_brand_comparison_promo(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    competitor_id: str | None = None,
):
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required"}, status_code=400)
    if not competitor_id:
        return JSONResponse({"error": "Competitor required"}, status_code=400)
    base = await _bc().get_bc_base(period_start, period_end, user.brand_id, competitor_id)
    if base.get("error"):
        return base
    brand_cats = base.get("brand_categories", [])
    brand_subcats_map = base.get("brand_subcategories", {})
    return await _bc().get_bc_promo(
        period_start, period_end, user.brand_id, competitor_id, brand_cats, brand_subcats_map
    )


@router.get("/api/brand-comparison/peak", tags=["Brand Comparison"])
async def api_brand_comparison_peak(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    competitor_id: str | None = None,
):
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required"}, status_code=400)
    if not competitor_id:
        return JSONResponse({"error": "Competitor required"}, status_code=400)
    base = await _bc().get_bc_base(period_start, period_end, user.brand_id, competitor_id)
    if base.get("error"):
        return base
    brand_cats = base.get("brand_categories", [])
    brand_subcats_map = base.get("brand_subcategories", {})
    return await _bc().get_bc_peak(
        period_start, period_end, user.brand_id, competitor_id, brand_cats, brand_subcats_map
    )


@router.get("/api/brand-comparison/discount", tags=["Brand Comparison"])
async def api_brand_comparison_discount(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    competitor_id: str | None = None,
    discount_category_id: str | None = None,
    discount_subcategory_id: str | None = None,
):
    """Brand Comparison: discount depth (stesso payload di get_bc_all / discount slice)."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Brand Comparison"}, status_code=400)
    if not competitor_id:
        return JSONResponse({"error": "Competitor required"}, status_code=400)
    bad = reject_if_cat_sub_out_of_scope(user, discount_category_id, discount_subcategory_id)
    if bad:
        return bad
    base = await _bc().get_bc_base(period_start, period_end, user.brand_id, competitor_id)
    if base.get("error"):
        return base
    brand_cats = base.get("brand_categories", [])
    sub_ids = base.get("sub_ids", [])
    return await _bc().get_bc_discount(
        period_start,
        period_end,
        user.brand_id,
        competitor_id,
        brand_cats,
        sub_ids,
        discount_cat=discount_category_id,
        discount_subcat=discount_subcategory_id,
    )


@router.get("/api/brand-comparison/all-years", tags=["Brand Comparison"])
async def api_brand_comparison_all_years(
    access_token: Optional[str] = Cookie(None),
    competitor_id: str | None = None,
    discount_category_id: str | None = None,
    discount_subcategory_id: str | None = None,
):
    """Brand Comparison: tutti gli anni in una chiamata. Richiede competitor_id."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Brand Comparison"}, status_code=400)
    if not competitor_id:
        return JSONResponse({"error": "Competitor required"}, status_code=400)
    return await _bc().get_bc_all_years(
        user.brand_id,
        competitor_id,
        discount_cat=discount_category_id,
        discount_subcat=discount_subcategory_id,
    )
