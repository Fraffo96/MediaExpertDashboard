"""API Brand Comparison."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Cookie
from fastapi.responses import JSONResponse

from app.constants import DP
from app.web.context import _bc, _svc, get_user
from app.web.scope import reject_if_cat_sub_out_of_scope

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
):
    """Brand Comparison: metadata (brand_cats, competitors, years, channels). Usato da all-years."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Brand Comparison"}, status_code=400)
    return await _bc().get_bc_base(period_start, period_end, user.brand_id)


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
