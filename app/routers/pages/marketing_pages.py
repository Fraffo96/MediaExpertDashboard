"""Pagine Marketing Insights."""
import asyncio
from typing import Optional

from fastapi import APIRouter, Cookie, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.auth.brand_scope import scoped_category_dropdowns
from app.jinja_env import templates
from app.web.context import brand_name_for_user, check_tab, filters_payload, page_ctx, require_login

router = APIRouter()


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
