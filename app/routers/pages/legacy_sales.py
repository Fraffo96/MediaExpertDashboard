"""Pagine dashboard Sales legacy (promo, customer, simulation, compare, products)."""
from typing import Optional

from fastapi import APIRouter, Cookie, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.jinja_env import templates
from app.web.context import check_tab, filters_payload, page_ctx, require_login

router = APIRouter()


@router.get("/why-buy", response_class=HTMLResponse)
async def page_why_buy(request: Request, access_token: Optional[str] = Cookie(None)):
    """La UI Why Buy è nella dashboard Customer; URL dedicato per bookmark e test."""
    user, redirect = require_login(access_token)
    if redirect:
        return redirect
    return RedirectResponse("/customer", status_code=302)


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
