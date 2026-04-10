"""Pagine: Market Intelligence, Brand Comparison, Promo Simulator, Check Live."""
from typing import Optional

from fastapi import APIRouter, Cookie, Request
from fastapi.responses import HTMLResponse

from app.auth.brand_scope import scoped_category_dropdowns
from app.jinja_env import templates
from app.web.context import (
    brand_name_for_user,
    check_live_anchor_end,
    check_tab,
    default_check_live_dates,
    filters_payload,
    page_ctx,
    require_login,
)

router = APIRouter()


@router.get("/market-intelligence", response_class=HTMLResponse)
async def page_market_intelligence(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "market_intelligence")
    if tab_redirect:
        return tab_redirect
    f = await filters_payload()
    ctx = page_ctx(f, user)
    ctx["brand_name"] = brand_name_for_user(user, f)
    ctx["brand_id"] = user.brand_id
    ctx["categories"], ctx["subcategories"] = scoped_category_dropdowns(user, f)
    return templates.TemplateResponse(request, "market_intelligence.html", {**ctx, "active": "market_intelligence"})


@router.get("/brand-comparison", response_class=HTMLResponse)
async def page_brand_comparison(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "brand_comparison")
    if tab_redirect:
        return tab_redirect
    f = await filters_payload()
    ctx = page_ctx(f, user)
    ctx["brand_name"] = brand_name_for_user(user, f)
    ctx["brand_id"] = user.brand_id
    ctx["available_years"] = f.get("available_years") or []
    ctx["categories"], ctx["subcategories"] = scoped_category_dropdowns(user, f)
    return templates.TemplateResponse(request, "brand_comparison.html", {**ctx, "active": "brand_comparison"})


@router.get("/promo-creator", response_class=HTMLResponse)
async def page_promo_creator(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "promo_creator")
    if tab_redirect:
        return tab_redirect
    f = await filters_payload()
    ctx = page_ctx(f, user)
    ctx["categories"], ctx["subcategories"] = scoped_category_dropdowns(user, f)
    ctx["promo_types"] = f.get("promo_types") or []
    return templates.TemplateResponse(request, "promo_creator.html", {**ctx, "active": "promo_creator"})


@router.get("/check-live-promo", response_class=HTMLResponse)
async def page_check_live_promo(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "check_live_promo")
    if tab_redirect:
        return tab_redirect
    f = await filters_payload()
    ctx = page_ctx(f, user)
    ctx["brand_name"] = brand_name_for_user(user, f)
    ctx["brand_id"] = user.brand_id
    ctx["categories"], ctx["subcategories"] = scoped_category_dropdowns(user, f)
    ds, de = default_check_live_dates()
    ctx["date_start_default"] = ds
    ctx["date_end_default"] = de
    ctx["clp_anchor_end"] = check_live_anchor_end()
    return templates.TemplateResponse(request, "check_live_promo.html", {**ctx, "active": "check_live_promo"})
