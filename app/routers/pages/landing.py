"""Pagine: home e dashboard Basic."""
from typing import Optional

from fastapi import APIRouter, Cookie, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.jinja_env import templates
from app.web.context import brand_name_for_user, filters_payload, get_user, page_ctx, require_login

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
