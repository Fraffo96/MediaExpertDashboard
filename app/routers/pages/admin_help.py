"""Pagine ecosystem, admin, help."""
import json
import os
from typing import Optional

from fastapi import APIRouter, Cookie, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.auth.brand_scope import scoped_brands_dropdown
from app.auth.firestore_store import get_ecosystem_by_id
from app.constants import ADMIN_BRANDS, ADMIN_CATEGORIES, ADMIN_SUBCATEGORIES
from app.jinja_env import templates
from app.web.context import filters_payload, load_glossary, page_ctx, require_login

router = APIRouter()


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
