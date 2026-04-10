"""Route pagine HTML (Jinja2)."""
from __future__ import annotations

import asyncio
import json
import os
from typing import Optional

from fastapi import APIRouter, Cookie, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.auth.brand_scope import scoped_brands_dropdown, scoped_category_dropdowns
from app.auth.firestore_store import get_ecosystem_by_id
from app.constants import ADMIN_BRANDS, ADMIN_CATEGORIES, ADMIN_SUBCATEGORIES
from app.jinja_env import templates
from app.web.context import (
    brand_name_for_user,
    check_live_anchor_end,
    check_tab,
    default_check_live_dates,
    filters_payload,
    get_user,
    load_glossary,
    page_ctx,
    require_login,
)

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def page_root(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    if user.is_admin:
        return RedirectResponse("/admin", status_code=302)
    f = await filters_payload()
    ctx = page_ctx(f, user)
    ctx["brand_name"] = brand_name_for_user(user, f)
    return templates.TemplateResponse(request, "landing.html", {**ctx, "active": "landing"})


@router.get("/basic", response_class=HTMLResponse)
async def page_basic(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    f = await filters_payload()
    return templates.TemplateResponse(request, "basic.html", {**page_ctx(f, user), "active": "basic"})


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


@router.get("/marketing", response_class=HTMLResponse)
async def page_marketing(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "marketing")
    if tab_redirect:
        return tab_redirect
    return RedirectResponse("/marketing/overview", status_code=302)


@router.get("/marketing/overview", response_class=HTMLResponse)
async def page_marketing_overview(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "marketing")
    if tab_redirect:
        return tab_redirect
    f = await filters_payload()
    ctx = page_ctx(f, user)
    default_brand_id = user.brand_id if user and user.brand_id else None
    ctx["categories"], ctx["subcategories"] = scoped_category_dropdowns(user, f)
    ctx["brand_name"] = brand_name_for_user(user, f)
    ctx["brand_id"] = default_brand_id
    ctx["available_years"] = f.get("available_years") or [2024, 2025]
    return templates.TemplateResponse(request, "marketing/overview.html", {**ctx, "active": "marketing"})


@router.get("/marketing/segments", response_class=HTMLResponse)
async def page_marketing_segments(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "marketing")
    if tab_redirect:
        return tab_redirect
    f = await filters_payload()
    ctx = page_ctx(f, user)
    ctx["segments"] = f.get("segments") or [
        {"segment_id": i, "segment_name": n}
        for i, n in [
            (1, "Liberals"),
            (2, "Optimistic Doers"),
            (3, "Go-Getters"),
            (4, "Outcasts"),
            (5, "Contributors"),
            (6, "Floaters"),
        ]
    ]
    default_brand_id = user.brand_id if user and user.brand_id else None
    ctx["default_brand_id"] = default_brand_id
    ctx["categories"], ctx["subcategories"] = scoped_category_dropdowns(user, f)
    if default_brand_id:
        from app.db.queries.precalc.base import query_competitors_in_scope_from_precalc

        year = 2024
        ctx["brands"] = await asyncio.to_thread(query_competitors_in_scope_from_precalc, year, default_brand_id) or []
    else:
        ctx["brands"] = f.get("brands") or []
    return templates.TemplateResponse(request, "marketing/segments.html", {**ctx, "active": "marketing"})


@router.get("/marketing/needstates", response_class=HTMLResponse)
async def page_marketing_needstates(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "marketing")
    if tab_redirect:
        return tab_redirect
    f = await filters_payload()
    ctx = page_ctx(f, user)
    default_brand_id = user.brand_id if user and user.brand_id else None
    ctx["categories"], _ = scoped_category_dropdowns(user, f)
    segs = f.get("segments") or []
    if not segs:
        segs = [
            {"segment_id": i, "segment_name": n}
            for i, n in [
                (1, "Liberals"),
                (2, "Optimistic Doers"),
                (3, "Go-Getters"),
                (4, "Outcasts"),
                (5, "Contributors"),
                (6, "Floaters"),
            ]
        ]
    ctx["segments"] = segs
    from app.services.marketing import get_needstates_spider_precalc_for_template

    ctx["needstates_precalc"] = get_needstates_spider_precalc_for_template()
    return templates.TemplateResponse(request, "marketing/needstates.html", {**ctx, "active": "marketing"})


@router.get("/marketing/media-preferences", response_class=HTMLResponse)
async def page_marketing_media_preferences(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "marketing")
    if tab_redirect:
        return tab_redirect
    f = await filters_payload()
    ctx = page_ctx(f, user)
    segs = f.get("segments") or []
    if not segs:
        segs = [
            {"segment_id": i, "segment_name": n}
            for i, n in [
                (1, "Liberals"),
                (2, "Optimistic Doers"),
                (3, "Go-Getters"),
                (4, "Outcasts"),
                (5, "Contributors"),
                (6, "Floaters"),
            ]
        ]
    ctx["segments"] = segs
    ctx["categories"], _ = scoped_category_dropdowns(user, f)
    from app.services.marketing import get_media_preferences

    ctx["media_preferences_precalc"] = get_media_preferences()
    return templates.TemplateResponse(request, "marketing/media_preferences.html", {**ctx, "active": "marketing"})


@router.get("/marketing/purchasing", response_class=HTMLResponse)
async def page_marketing_purchasing(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "marketing")
    if tab_redirect:
        return tab_redirect
    f = await filters_payload()
    ctx = page_ctx(f, user)
    segs = f.get("segments") or []
    if not segs:
        segs = [
            {"segment_id": i, "segment_name": n}
            for i, n in [
                (1, "Liberals"),
                (2, "Optimistic Doers"),
                (3, "Go-Getters"),
                (4, "Outcasts"),
                (5, "Contributors"),
                (6, "Floaters"),
            ]
        ]
    ctx["segments"] = segs
    ctx["categories"], _ = scoped_category_dropdowns(user, f)
    return templates.TemplateResponse(request, "marketing/purchasing.html", {**ctx, "active": "marketing"})


@router.get("/promo", response_class=HTMLResponse)
async def page_promo(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "promo")
    if tab_redirect:
        return tab_redirect
    f = await filters_payload()
    return templates.TemplateResponse(request, "promo.html", {**page_ctx(f, user), "active": "promo"})


@router.get("/customer", response_class=HTMLResponse)
async def page_customer(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "segments")
    if tab_redirect:
        return tab_redirect
    f = await filters_payload()
    return templates.TemplateResponse(request, "customer.html", {**page_ctx(f, user), "active": "customer"})


@router.get("/simulation", response_class=HTMLResponse)
async def page_simulation(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "simulation")
    if tab_redirect:
        return tab_redirect
    f = await filters_payload()
    return templates.TemplateResponse(request, "simulation.html", {**page_ctx(f, user), "active": "simulation"})


@router.get("/compare", response_class=HTMLResponse)
async def page_compare(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "compare")
    if tab_redirect:
        return tab_redirect
    f = await filters_payload()
    return templates.TemplateResponse(request, "compare.html", {**page_ctx(f, user), "active": "compare"})


@router.get("/products", response_class=HTMLResponse)
async def page_products(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = check_tab(user, "products")
    if tab_redirect:
        return tab_redirect
    f = await filters_payload()
    return templates.TemplateResponse(request, "products.html", {**page_ctx(f, user), "active": "products"})


@router.get("/ecosystem/{eco_id}", response_class=HTMLResponse)
async def page_ecosystem(eco_id: int, request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    eco = get_ecosystem_by_id(eco_id)
    if not eco:
        return RedirectResponse("/", status_code=302)
    if not user.is_admin:
        if eco_id not in user.ecosystem_ids:
            return RedirectResponse("/", status_code=302)
    eco_dict = eco.to_dict()
    cat_ids = list(eco.category_ids)
    brand_ids = list(eco.brand_ids)
    f = await filters_payload()
    ctx = page_ctx(f, user)
    ctx["ecosystem"] = eco_dict
    ctx["ecosystem_category_ids"] = cat_ids
    ctx["ecosystem_brand_ids"] = brand_ids
    ctx["brands"] = scoped_brands_dropdown(user, f, brand_ids)
    return templates.TemplateResponse(request, "ecosystem.html", {**ctx, "active": f"eco_{eco_id}"})


@router.get("/admin", response_class=HTMLResponse)
async def page_admin(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    if not user.is_admin:
        return RedirectResponse("/", status_code=302)
    f = await filters_payload()
    categories = f.get("categories") or []
    subcategories = f.get("subcategories") or []
    brands = f.get("brands") or []
    if not categories or not any(c.get("level") == 1 for c in categories):
        categories = ADMIN_CATEGORIES
    if not subcategories:
        subcategories = ADMIN_SUBCATEGORIES
    if not brands:
        brands = ADMIN_BRANDS
    ctx = page_ctx(f, user)
    ctx["categories"] = [c for c in categories if c.get("level") == 1] or ADMIN_CATEGORIES
    ctx["categories_json"] = json.dumps(ctx["categories"])
    ctx["subcategories"] = subcategories
    ctx["brands"] = brands
    ctx["brands_json"] = json.dumps(brands)
    from app.db.client import PROJECT_ID as _gcp_pid

    ctx["gcp_project_id"] = _gcp_pid
    ctx["gcp_region"] = (os.environ.get("GCP_REGION") or os.environ.get("CLOUD_RUN_REGION") or "europe-west1").strip()
    return templates.TemplateResponse(request, "admin.html", {**ctx, "active": "admin"})


@router.get("/help", response_class=HTMLResponse)
async def page_help(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    ctx = page_ctx({}, user)
    ctx["glossary"] = load_glossary()
    return templates.TemplateResponse(request, "help.html", {**ctx, "active": None})
