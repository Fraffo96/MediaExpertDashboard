"""
MediaExpert Insights – dashboard platform.
Start: uvicorn app.main:app --reload
"""
from dotenv import load_dotenv
load_dotenv()

import asyncio
import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import os
from fastapi import FastAPI, Request, Cookie, Header, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.auth.database import init_db
from app.auth.firestore_store import StoredUser, get_ecosystem_by_id, list_ecosystems, list_users_active_with_brand
from app.auth.security import get_current_user
from app.auth.routes import router as auth_router
from app.constants import (
    DP,
    PC_DEFAULT_PERIOD,
    MKT_DEFAULT_PERIOD,
    CLP_DATA_MAX_DATE,
    ADMIN_CATEGORIES,
    ADMIN_SUBCATEGORIES,
    ADMIN_BRANDS,
)
from app.services.prewarm import prewarm_cache

# Lazy load: BigQuery e servizi pesanti solo alla prima richiesta che li usa (login resta veloce)
def _svc():
    from app import services
    return services

def _bc():
    from app.services import brand_comparison
    return brand_comparison

logger = logging.getLogger(__name__)

app = FastAPI(title="MediaExpert Insights")
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
app.include_router(auth_router)

_GLOSSARY: dict = {}


@app.on_event("startup")
async def on_startup():
    init_db()
    # Preload marketing needstates (JSON) so first request is fast
    try:
        from app.services.marketing import warm_needstates_spider_precalc

        warm_needstates_spider_precalc()
    except Exception:
        pass
    # Prewarm cache in background: primo utente avrà caricamento veloce
    async def _prewarm():
        try:
            r = await prewarm_cache()
            if r.get("warmed", 0) > 0:
                logger.info("Prewarm: cache ready for %s", r.get("brands", []))
        except Exception as e:
            logger.warning("Prewarm failed: %s", e)
    asyncio.create_task(_prewarm())


def _load_glossary() -> dict:
    global _GLOSSARY
    if not _GLOSSARY:
        p = BASE_DIR / "static" / "data" / "glossary.json"
        if p.exists():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    _GLOSSARY = json.load(f)
            except Exception as e:
                logger.warning("Glossary load failed: %s", e)
    return _GLOSSARY


def _get_user(access_token: Optional[str] = None) -> Optional[StoredUser]:
    if not access_token:
        return None
    return get_current_user(access_token)


def _user_ecosystems(user: Optional[StoredUser]) -> list[dict]:
    if not user:
        return []
    if user.is_admin:
        active = [e.to_dict() for e in list_ecosystems() if e.is_active]
        active.sort(key=lambda x: (x.get("name") or "").lower())
        return active
    out: list[dict] = []
    for eid in user.ecosystem_ids:
        e = get_ecosystem_by_id(eid)
        if e and e.is_active:
            out.append(e.to_dict())
    out.sort(key=lambda x: (x.get("name") or "").lower())
    return out


async def _filters():
    try:
        return await _svc().get_filters()
    except Exception as e:
        logger.warning("Filters load failed: %s", e)
        return {"categories": [], "subcategories": [], "segments": [], "brands": [], "promo_types": [], "promos": [], "genders": [], "available_years": []}


def _brand_logo_url(user: Optional[StoredUser]) -> str | None:
    """Logo brand: BRAND_LOGOS_PUBLIC_BASE + brands/{id}.png (GCS) oppure static locale."""
    if not user or user.is_admin or not user.brand_id:
        return None
    base = (os.environ.get("BRAND_LOGOS_PUBLIC_BASE") or "").strip().rstrip("/")
    if base:
        return f"{base}/{user.brand_id}.png"
    return f"/static/img/brands/{user.brand_id}.png"


def _page_ctx(f: dict, user: Optional[StoredUser] = None) -> dict:
    return {
        **f,
        "period_start": DP[0],
        "period_end": DP[1],
        "glossary": _load_glossary(),
        "user": user,
        "user_ecosystems": _user_ecosystems(user),
        "allowed_filters": user.filter_list if user else [],
        "allowed_tabs": user.tab_list if user else ["basic"],
        "brand_logo_url": _brand_logo_url(user),
    }


def _brand_name_for_user(user: Optional[StoredUser], f: dict) -> str:
    """Resolve brand name for greeting on landing page."""
    if not user or not user.brand_id:
        return "your brand"
    for b in (f.get("brands") or []):
        if b.get("brand_id") == user.brand_id:
            return b.get("brand_name", "your brand")
    return "your brand"


def _require_login(access_token: Optional[str]):
    user = _get_user(access_token)
    if not user:
        return None, RedirectResponse("/login", status_code=302)
    return user, None


def _check_tab(user: StoredUser, tab: str):
    """Return redirect if user can't access this tab."""
    if not user.can_access_tab(tab):
        tabs = user.tab_list
        first = tabs[0] if tabs else None
        url = "/market-intelligence" if first == "market_intelligence" else "/brand-comparison" if first == "brand_comparison" else "/promo-creator" if first == "promo_creator" else "/check-live-promo" if first == "check_live_promo" else "/marketing/overview" if first == "marketing" else "/"
        return RedirectResponse(url, status_code=302)
    return None


def _check_live_anchor_end() -> str:
    """End date for presets: min(today, last date in demo/production feed)."""
    from datetime import date

    max_d = date.fromisoformat(CLP_DATA_MAX_DATE)
    today = date.today()
    return min(today, max_d).isoformat()


def _default_check_live_dates():
    """Default: last 7d inclusive ending at anchor."""
    from datetime import datetime, timedelta

    end = datetime.strptime(_check_live_anchor_end(), "%Y-%m-%d").date()
    start = end - timedelta(days=6)
    return start.isoformat(), end.isoformat()


# --- Pages ---
@app.get("/", response_class=HTMLResponse)
async def page_root(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    if user.is_admin:
        return RedirectResponse("/admin", status_code=302)
    f = await _filters()
    ctx = _page_ctx(f, user)
    ctx["brand_name"] = _brand_name_for_user(user, f)
    return templates.TemplateResponse(request, "landing.html", {**ctx, "active": "landing"})


@app.get("/basic", response_class=HTMLResponse)
async def page_basic(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    f = await _filters()
    return templates.TemplateResponse(request, "basic.html", {**_page_ctx(f, user), "active": "basic"})


@app.get("/market-intelligence", response_class=HTMLResponse)
async def page_market_intelligence(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "market_intelligence")
    if tab_redirect:
        return tab_redirect
    f = await _filters()
    ctx = _page_ctx(f, user)
    ctx["brand_name"] = _brand_name_for_user(user, f)
    ctx["brand_id"] = user.brand_id
    cats = f.get("categories") or []
    subcats = f.get("subcategories") or []
    ctx["categories"] = [c for c in cats if c.get("level") == 1] or ADMIN_CATEGORIES
    ctx["subcategories"] = subcats or ADMIN_SUBCATEGORIES
    return templates.TemplateResponse(request, "market_intelligence.html", {**ctx, "active": "market_intelligence"})


@app.get("/brand-comparison", response_class=HTMLResponse)
async def page_brand_comparison(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "brand_comparison")
    if tab_redirect:
        return tab_redirect
    f = await _filters()
    ctx = _page_ctx(f, user)
    ctx["brand_name"] = _brand_name_for_user(user, f)
    ctx["brand_id"] = user.brand_id
    ctx["available_years"] = f.get("available_years") or []
    cats = f.get("categories") or []
    subcats = f.get("subcategories") or []
    ctx["categories"] = [c for c in cats if c.get("level") == 1] or ADMIN_CATEGORIES
    ctx["subcategories"] = subcats or ADMIN_SUBCATEGORIES
    return templates.TemplateResponse(request, "brand_comparison.html", {**ctx, "active": "brand_comparison"})


@app.get("/promo-creator", response_class=HTMLResponse)
async def page_promo_creator(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "promo_creator")
    if tab_redirect:
        return tab_redirect
    f = await _filters()
    ctx = _page_ctx(f, user)
    cats = f.get("categories") or []
    subcats = f.get("subcategories") or []
    ctx["categories"] = [c for c in cats if c.get("level") == 1] or ADMIN_CATEGORIES
    ctx["subcategories"] = subcats or ADMIN_SUBCATEGORIES
    ctx["promo_types"] = f.get("promo_types") or []
    return templates.TemplateResponse(request, "promo_creator.html", {**ctx, "active": "promo_creator"})


@app.get("/check-live-promo", response_class=HTMLResponse)
async def page_check_live_promo(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "check_live_promo")
    if tab_redirect:
        return tab_redirect
    f = await _filters()
    ctx = _page_ctx(f, user)
    ctx["brand_name"] = _brand_name_for_user(user, f)
    ctx["brand_id"] = user.brand_id
    cats = f.get("categories") or []
    subcats = f.get("subcategories") or []
    ctx["categories"] = [c for c in cats if c.get("level") == 1] or ADMIN_CATEGORIES
    ctx["subcategories"] = subcats or ADMIN_SUBCATEGORIES
    ds, de = _default_check_live_dates()
    ctx["date_start_default"] = ds
    ctx["date_end_default"] = de
    ctx["clp_anchor_end"] = _check_live_anchor_end()
    return templates.TemplateResponse(request, "check_live_promo.html", {**ctx, "active": "check_live_promo"})


@app.get("/marketing", response_class=HTMLResponse)
async def page_marketing(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "marketing")
    if tab_redirect:
        return tab_redirect
    return RedirectResponse("/marketing/overview", status_code=302)


@app.get("/marketing/overview", response_class=HTMLResponse)
async def page_marketing_overview(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "marketing")
    if tab_redirect:
        return tab_redirect
    f = await _filters()
    ctx = _page_ctx(f, user)
    default_brand_id = user.brand_id if user and user.brand_id else None
    cats = f.get("categories") or []
    if default_brand_id:
        from app.db.queries import shared
        ctx["categories"] = await asyncio.to_thread(shared.query_categories_by_brand, default_brand_id) or []
        ctx["subcategories"] = await asyncio.to_thread(shared.query_subcategories_by_brand, default_brand_id) or []
    else:
        ctx["categories"] = [c for c in cats if c.get("level") == 1] or ADMIN_CATEGORIES
        ctx["subcategories"] = f.get("subcategories") or ADMIN_SUBCATEGORIES
    ctx["brand_name"] = _brand_name_for_user(user, f)
    ctx["brand_id"] = default_brand_id
    ctx["available_years"] = f.get("available_years") or [2024, 2025]
    return templates.TemplateResponse(request, "marketing/overview.html", {**ctx, "active": "marketing"})


@app.get("/marketing/segments", response_class=HTMLResponse)
async def page_marketing_segments(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "marketing")
    if tab_redirect:
        return tab_redirect
    f = await _filters()
    ctx = _page_ctx(f, user)
    cats = f.get("categories") or []
    subcats = f.get("subcategories") or []
    ctx["segments"] = f.get("segments") or [{"segment_id": i, "segment_name": n} for i, n in [(1,"Liberals"),(2,"Optimistic Doers"),(3,"Go-Getters"),(4,"Outcasts"),(5,"Contributors"),(6,"Floaters")]]
    default_brand_id = user.brand_id if user and user.brand_id else None
    ctx["default_brand_id"] = default_brand_id
    if default_brand_id:
        from app.db.queries import shared
        from app.db.queries.precalc.base import query_competitors_in_scope_from_precalc
        ctx["categories"] = await asyncio.to_thread(shared.query_categories_by_brand, default_brand_id) or []
        ctx["subcategories"] = await asyncio.to_thread(shared.query_subcategories_by_brand, default_brand_id) or []
        year = 2024
        ctx["brands"] = await asyncio.to_thread(query_competitors_in_scope_from_precalc, year, default_brand_id) or []
    else:
        ctx["brands"] = f.get("brands") or []
        ctx["categories"] = [c for c in cats if c.get("level") == 1] or ADMIN_CATEGORIES
        ctx["subcategories"] = subcats or ADMIN_SUBCATEGORIES
    return templates.TemplateResponse(request, "marketing/segments.html", {**ctx, "active": "marketing"})


@app.get("/marketing/needstates", response_class=HTMLResponse)
async def page_marketing_needstates(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "marketing")
    if tab_redirect:
        return tab_redirect
    f = await _filters()
    cats = f.get("categories") or []
    ctx = _page_ctx(f, user)
    default_brand_id = user.brand_id if user and user.brand_id else None
    if default_brand_id:
        from app.db.queries import shared
        ctx["categories"] = await asyncio.to_thread(shared.query_categories_by_brand, default_brand_id) or []
    else:
        ctx["categories"] = [c for c in cats if c.get("level") == 1] or ADMIN_CATEGORIES
    segs = f.get("segments") or []
    if not segs:
        segs = [{"segment_id": i, "segment_name": n} for i, n in [(1,"Liberals"),(2,"Optimistic Doers"),(3,"Go-Getters"),(4,"Outcasts"),(5,"Contributors"),(6,"Floaters")]]
    ctx["segments"] = segs
    from app.services.marketing import get_needstates_spider_precalc_for_template

    ctx["needstates_precalc"] = get_needstates_spider_precalc_for_template()
    return templates.TemplateResponse(request, "marketing/needstates.html", {**ctx, "active": "marketing"})


@app.get("/marketing/media-preferences", response_class=HTMLResponse)
async def page_marketing_media_preferences(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "marketing")
    if tab_redirect:
        return tab_redirect
    f = await _filters()
    ctx = _page_ctx(f, user)
    segs = f.get("segments") or []
    if not segs:
        segs = [{"segment_id": i, "segment_name": n} for i, n in [(1,"Liberals"),(2,"Optimistic Doers"),(3,"Go-Getters"),(4,"Outcasts"),(5,"Contributors"),(6,"Floaters")]]
    ctx["segments"] = segs
    from app.services.marketing import get_media_preferences

    ctx["media_preferences_precalc"] = get_media_preferences()
    return templates.TemplateResponse(request, "marketing/media_preferences.html", {**ctx, "active": "marketing"})


@app.get("/marketing/purchasing", response_class=HTMLResponse)
async def page_marketing_purchasing(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "marketing")
    if tab_redirect:
        return tab_redirect
    f = await _filters()
    ctx = _page_ctx(f, user)
    segs = f.get("segments") or []
    if not segs:
        segs = [{"segment_id": i, "segment_name": n} for i, n in [(1,"Liberals"),(2,"Optimistic Doers"),(3,"Go-Getters"),(4,"Outcasts"),(5,"Contributors"),(6,"Floaters")]]
    ctx["segments"] = segs
    return templates.TemplateResponse(request, "marketing/purchasing.html", {**ctx, "active": "marketing"})


@app.get("/promo", response_class=HTMLResponse)
async def page_promo(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "promo")
    if tab_redirect:
        return tab_redirect
    f = await _filters()
    return templates.TemplateResponse(request, "promo.html", {**_page_ctx(f, user), "active": "promo"})


@app.get("/customer", response_class=HTMLResponse)
async def page_customer(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "segments")
    if tab_redirect:
        return tab_redirect
    f = await _filters()
    return templates.TemplateResponse(request, "customer.html", {**_page_ctx(f, user), "active": "customer"})


@app.get("/simulation", response_class=HTMLResponse)
async def page_simulation(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "simulation")
    if tab_redirect:
        return tab_redirect
    f = await _filters()
    return templates.TemplateResponse(request, "simulation.html", {**_page_ctx(f, user), "active": "simulation"})


@app.get("/compare", response_class=HTMLResponse)
async def page_compare(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "compare")
    if tab_redirect:
        return tab_redirect
    f = await _filters()
    return templates.TemplateResponse(request, "compare.html", {**_page_ctx(f, user), "active": "compare"})


@app.get("/products", response_class=HTMLResponse)
async def page_products(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    tab_redirect = _check_tab(user, "products")
    if tab_redirect:
        return tab_redirect
    f = await _filters()
    return templates.TemplateResponse(request, "products.html", {**_page_ctx(f, user), "active": "products"})


@app.get("/ecosystem/{eco_id}", response_class=HTMLResponse)
async def page_ecosystem(eco_id: int, request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
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
    f = await _filters()
    ctx = _page_ctx(f, user)
    ctx["ecosystem"] = eco_dict
    ctx["ecosystem_category_ids"] = cat_ids
    ctx["ecosystem_brand_ids"] = brand_ids
    return templates.TemplateResponse(request, "ecosystem.html", {**ctx, "active": f"eco_{eco_id}"})


@app.get("/admin", response_class=HTMLResponse)
async def page_admin(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    if not user.is_admin:
        return RedirectResponse("/", status_code=302)
    f = await _filters()
    # Fallback per user form quando BigQuery non disponibile
    categories = f.get("categories") or []
    subcategories = f.get("subcategories") or []
    brands = f.get("brands") or []
    if not categories or not any(c.get("level") == 1 for c in categories):
        categories = ADMIN_CATEGORIES
    if not subcategories:
        subcategories = ADMIN_SUBCATEGORIES
    if not brands:
        brands = ADMIN_BRANDS
    ctx = _page_ctx(f, user)
    ctx["categories"] = [c for c in categories if c.get("level") == 1] or ADMIN_CATEGORIES
    ctx["subcategories"] = subcategories
    ctx["brands"] = brands
    ctx["brands_json"] = json.dumps(brands)
    return templates.TemplateResponse(request, "admin.html", {**ctx, "active": "admin"})


@app.get("/help", response_class=HTMLResponse)
async def page_help(request: Request, access_token: Optional[str] = Cookie(None)):
    user, redirect = _require_login(access_token)
    if redirect:
        return redirect
    ctx = _page_ctx({}, user)
    ctx["glossary"] = _load_glossary()
    return templates.TemplateResponse(request, "help.html", {**ctx, "active": None})


# --- API ---
@app.get("/api/filters")
async def api_filters():
    return await _filters()


@app.get("/api/basic")
async def api_basic(period_start: str = DP[0], period_end: str = DP[1], category_id: str | None = None, segment_id: str | None = None, gender: str | None = None, brand_id: str | None = None, subcategory_id: str | None = None, incremental_yoy_promo_id: str | None = None, channel: str | None = None):
    return await _svc().get_basic(period_start, period_end, category_id, segment_id, gender, brand_id, subcategory_id, incremental_yoy_promo_id, channel)


@app.get("/api/basic/granular")
async def api_basic_granular(period_start: str = DP[0], period_end: str = DP[1], channel: str | None = None):
    """Endpoint leggero: tabelle detail per filtro client-side istantaneo. Usa questo per prefetch in background."""
    return await _svc().get_basic_granular(period_start, period_end, channel)


@app.get("/api/basic/incremental_yoy")
async def api_basic_incremental_yoy(
    period_start: str = DP[0], period_end: str = DP[1],
    category_id: str | None = None, segment_id: str | None = None, gender: str | None = None, brand_id: str | None = None,
    promo_id: str | None = None, promo_ids: str | None = None,
):
    """Lightweight endpoint: returns only incremental YoY (or by-promo for compare). Use promo_id for single, promo_ids (comma-separated) for compare."""
    ids = []
    if promo_ids:
        ids = [x.strip() for x in promo_ids.split(",") if x.strip()]
    elif promo_id:
        ids = [promo_id]
    return await _svc().get_incremental_yoy(period_start, period_end, category_id, segment_id, gender, brand_id, ids if ids else None)


@app.get("/api/promo")
async def api_promo(period_start: str = DP[0], period_end: str = DP[1], promo_type: str | None = None, category_id: str | None = None, segment_id: str | None = None):
    return await _svc().get_promo(period_start, period_end, promo_type, category_id, segment_id)


@app.get("/api/customer")
async def api_customer(period_start: str = DP[0], period_end: str = DP[1], segment_id: str | None = None, gender: str | None = None):
    return await _svc().get_customer(period_start, period_end, segment_id, gender)


@app.get("/api/simulation")
async def api_simulation(period_start: str = DP[0], period_end: str = DP[1], promo_type: str | None = None, segment_id: str | None = None, category_id: str | None = None):
    return await _svc().get_simulation(period_start, period_end, promo_type, segment_id, category_id)


@app.get("/api/why-buy")
async def api_why_buy(period_start: str = DP[0], period_end: str = DP[1], category_id: str | None = None):
    return await _svc().get_why_buy(period_start, period_end, category_id)


def _require_mi_user(access_token: Optional[str]):
    """Return (user, error_response). If error_response is not None, return it."""
    user = _get_user(access_token)
    if not user:
        return None, JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return None, JSONResponse({"error": "Brand required for Market Intelligence"}, status_code=400)
    return user, None


@app.get("/api/market-intelligence/base", tags=["Market Intelligence"])
async def api_market_intelligence_base(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
):
    """Market Intelligence: metadata (brand_cats, cat_ids, sub_ids). First call for progressive loading."""
    user, err = _require_mi_user(access_token)
    if err:
        return err
    return await _svc().get_mi_base(period_start, period_end, user.brand_id)


@app.get("/api/market-intelligence/all", tags=["Market Intelligence"])
async def api_market_intelligence_all(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    discount_category_id: str | None = None,
    discount_subcategory_id: str | None = None,
):
    """Market Intelligence: batch - base + sales + promo + peak + discount in una sola chiamata. Caricamento iniziale veloce."""
    user, err = _require_mi_user(access_token)
    if err:
        return err
    return await _svc().get_mi_all(
        period_start, period_end, user.brand_id,
        discount_cat=discount_category_id,
        discount_subcat=discount_subcategory_id,
    )


@app.get("/api/market-intelligence/all-years", tags=["Market Intelligence"])
async def api_market_intelligence_all_years(
    access_token: Optional[str] = Cookie(None),
    discount_category_id: str | None = None,
    discount_subcategory_id: str | None = None,
):
    """Market Intelligence: tutti gli anni in una sola chiamata. Dropdown year istantanei da subito."""
    user, err = _require_mi_user(access_token)
    if err:
        return err
    return await _svc().get_mi_all_years(
        user.brand_id,
        discount_cat=discount_category_id,
        discount_subcat=discount_subcategory_id,
    )


@app.get("/api/market-intelligence/available-years", tags=["Market Intelligence"])
async def api_market_intelligence_available_years(access_token: Optional[str] = Cookie(None)):
    """Elenco anni (solo BigQuery DISTINCT) per primo caricamento MI leggero."""
    user, err = _require_mi_user(access_token)
    if err:
        return err
    return await _svc().get_mi_available_years_payload()


@app.get("/api/market-intelligence/incremental-yoy", tags=["Market Intelligence"])
async def api_market_intelligence_incremental_yoy(
    period_start: str,
    period_end: str,
    access_token: Optional[str] = Cookie(None),
):
    """Incremental YoY multi-anno (range di anni interi 01-01 … 12-31)."""
    user, err = _require_mi_user(access_token)
    if err:
        return err
    return await _svc().get_mi_incremental_yoy_api(period_start, period_end, user.brand_id)


@app.get("/api/market-intelligence/sales")
async def api_market_intelligence_sales(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    cat_ids: str = "",
    sub_ids: str = "",
    subcategory_category_id: str | None = None,
):
    """Market Intelligence: sales value/volume, category/subcategory pie, prev year."""
    user, err = _require_mi_user(access_token)
    if err:
        return err
    cat_list = [x.strip() for x in cat_ids.split(",") if x.strip()] if cat_ids else []
    sub_list = [x.strip() for x in sub_ids.split(",") if x.strip()] if sub_ids else []
    sub_cat_id = subcategory_category_id or (cat_list[0] if cat_list else None)
    return await _svc().get_mi_sales(period_start, period_end, user.brand_id, cat_list, sub_list, sub_cat_id)


@app.get("/api/market-intelligence/promo")
async def api_market_intelligence_promo(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    cat_ids: str = "",
    sub_ids: str = "",
):
    """Market Intelligence: promo share and ROI. Requires cat_ids, sub_ids from /base."""
    user, err = _require_mi_user(access_token)
    if err:
        return err
    base = await _svc().get_mi_base(period_start, period_end, user.brand_id)
    if base.get("error"):
        return base
    brand_cats = base.get("brand_categories", [])
    brand_subcats_map = base.get("brand_subcategories", {})
    return await _svc().get_mi_promo(period_start, period_end, user.brand_id, brand_cats, brand_subcats_map)


@app.get("/api/market-intelligence/peak")
async def api_market_intelligence_peak(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
):
    """Market Intelligence: peak events. Fetches base internally for brand_cats."""
    user, err = _require_mi_user(access_token)
    if err:
        return err
    base = await _svc().get_mi_base(period_start, period_end, user.brand_id)
    if base.get("error"):
        return base
    brand_cats = base.get("brand_categories", [])
    brand_subcats_map = base.get("brand_subcategories", {})
    return await _svc().get_mi_peak(period_start, period_end, user.brand_id, brand_cats, brand_subcats_map)


@app.get("/api/market-intelligence/discount")
async def api_market_intelligence_discount(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    discount_category_id: str | None = None,
    discount_subcategory_id: str | None = None,
):
    """Market Intelligence: discount depth by category/subcategory."""
    user, err = _require_mi_user(access_token)
    if err:
        return err
    base = await _svc().get_mi_base(period_start, period_end, user.brand_id)
    if base.get("error"):
        return base
    brand_cats = base.get("brand_categories", [])
    sub_ids = base.get("sub_ids", [])
    return await _svc().get_mi_discount(
        period_start, period_end, user.brand_id, brand_cats, sub_ids,
        discount_cat=discount_category_id,
        discount_subcat=discount_subcategory_id,
    )


@app.get("/api/market-intelligence/top-products", tags=["Market Intelligence"])
async def api_market_intelligence_top_products(
    access_token: Optional[str] = Cookie(None),
    year: str | None = None,
    category_id: str | None = None,
    subcategory_id: str | None = None,
    channel: str | None = None,
):
    """Market Intelligence: top products per anno, category/subcategory, channel."""
    user, err = _require_mi_user(access_token)
    if err:
        return err
    y = year or str(DP[0][:4])
    return await _svc().get_mi_top_products(
        y, user.brand_id, category_id, subcategory_id, channel
    )


@app.get("/api/market-intelligence/segment-by-sku", tags=["Market Intelligence"])
async def api_market_intelligence_segment_by_sku(
    access_token: Optional[str] = Cookie(None),
    product_id: str | None = None,
    year: str | None = None,
    category_id: str | None = None,
    channel: str | None = None,
):
    """Market Intelligence: segment breakdown per SKU (tutte le vendite)."""
    user, err = _require_mi_user(access_token)
    if err:
        return err
    if not product_id:
        return JSONResponse({"error": "Product ID required"}, status_code=400)
    y = year or str(DP[0][:4])
    return await _svc().get_mi_segment_by_sku(
        int(product_id), user.brand_id, y, category_id, channel
    )


@app.get("/api/marketing/categories-by-brand", tags=["Marketing"])
async def api_marketing_categories_by_brand(
    access_token: Optional[str] = Cookie(None),
    brand_id: str | None = None,
):
    """Marketing: categories and subcategories where the brand has products. For filter dropdown when brand selected."""
    user = _get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.can_access_tab("marketing"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    if not brand_id or not str(brand_id).strip():
        return JSONResponse({"categories": [], "subcategories": []})
    try:
        bid = int(brand_id)
    except (ValueError, TypeError):
        return JSONResponse({"categories": [], "subcategories": []})
    from app.db.queries import shared
    cats = await asyncio.to_thread(shared.query_categories_by_brand, bid) or []
    subcats = await asyncio.to_thread(shared.query_subcategories_by_brand, bid) or []
    return JSONResponse({"categories": list(cats), "subcategories": list(subcats)})


@app.get("/api/marketing/segments", tags=["Marketing"])
async def api_marketing_segments(
    access_token: Optional[str] = Cookie(None),
    period_start: str = MKT_DEFAULT_PERIOD[0],
    period_end: str = MKT_DEFAULT_PERIOD[1],
    segment_id: str | None = None,
    category_id: str | None = None,
    subcategory_id: str | None = None,
    brand_id: str | None = None,
):
    """Marketing: segment summary – pain points, needstates, top categories, top SKUs. Usa anno intero per precalc."""
    user = _get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.can_access_tab("marketing"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    seg_id = int(segment_id) if segment_id and str(segment_id).strip() else None
    cat_id = int(category_id) if category_id and str(category_id).strip() else None
    sub_id = int(subcategory_id) if subcategory_id and str(subcategory_id).strip() else None
    bid = int(brand_id) if brand_id and str(brand_id).strip() else (user.brand_id if user.brand_id else None)
    return await asyncio.to_thread(_svc().get_segment_summary, period_start, period_end, seg_id, cat_id, sub_id, bid)


@app.get("/api/marketing/segment-by-category", tags=["Marketing"])
async def api_marketing_segment_by_category(
    access_token: Optional[str] = Cookie(None),
    year: str | None = None,
    category_id: str | None = None,
    subcategory_id: str | None = None,
    channel: str | None = None,
):
    """Marketing Overview: segment breakdown for brand sales in category/subcategory (calendar year)."""
    user = _get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.can_access_tab("marketing"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    bid = user.brand_id if user.brand_id else None
    try:
        y = int(year) if year and str(year).strip() else int(str(DP[0])[:4])
    except (ValueError, TypeError):
        y = int(str(DP[0])[:4])
    try:
        cat_id = int(category_id) if category_id and str(category_id).strip() else None
    except (ValueError, TypeError):
        cat_id = None
    try:
        sub_id = int(subcategory_id) if subcategory_id and str(subcategory_id).strip() else None
    except (ValueError, TypeError):
        sub_id = None
    ch = channel if channel and str(channel).strip() in ("web", "app", "store") else None
    return await asyncio.to_thread(_svc().get_segment_by_category, bid, y, cat_id, sub_id, ch)


@app.get("/api/marketing/needstates", tags=["Marketing"])
async def api_marketing_needstates(
    access_token: Optional[str] = Cookie(None),
    category_id: str | None = None,
    segment_id: str | None = None,
):
    """Marketing: spider chart – needstate dimensions for (category, segment). No date filter."""
    user = _get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.can_access_tab("marketing"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    try:
        cat_id = int(category_id) if category_id and str(category_id).strip() else 1
        seg_id = int(segment_id) if segment_id and str(segment_id).strip() else 1
    except (ValueError, TypeError):
        cat_id, seg_id = 1, 1
    try:
        return await asyncio.to_thread(_svc().get_needstates_spider, cat_id, seg_id)
    except Exception as e:
        logger.exception("Needstates API error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/marketing/media-preferences", tags=["Marketing"])
async def api_marketing_media_preferences(
    access_token: Optional[str] = Cookie(None),
    segment_id: str | None = None,
):
    """Marketing: media touchpoint mix per segment (static profiles)."""
    user = _get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.can_access_tab("marketing"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    try:
        sid = int(segment_id) if segment_id and str(segment_id).strip() else None
    except (ValueError, TypeError):
        sid = None
    return await asyncio.to_thread(_svc().get_media_preferences, sid)


@app.get("/api/marketing/purchasing", tags=["Marketing"])
async def api_marketing_purchasing(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    segment_id: str | None = None,
):
    """Marketing: purchasing process – channel mix, peak events per segment."""
    user = _get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.can_access_tab("marketing"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    seg_id = int(segment_id) if segment_id and str(segment_id).strip() else None
    return await asyncio.to_thread(_svc().get_purchasing, period_start, period_end, seg_id)


@app.get("/api/brand-comparison")
async def api_brand_comparison(
    request: Request,
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    category_id: str | None = None,
    subcategory_id: str | None = None,
    competitor_id: str | None = None,
):
    """Brand Comparison: competitors, sales/ROI comparison. Requires auth; uses user.brand_id."""
    user = _get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Brand Comparison"}, status_code=400)
    return await _svc().get_brand_comparison(period_start, period_end, user.brand_id, competitor_id, category_id, subcategory_id)


@app.get("/api/brand-comparison/competitors", tags=["Brand Comparison"])
async def api_brand_comparison_competitors(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
):
    """Brand Comparison: solo lista competitor. Leggero per caricamento iniziale."""
    user = _get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Brand Comparison"}, status_code=400)
    return await _bc().get_bc_competitors(period_start, period_end, user.brand_id)


@app.get("/api/brand-comparison/base", tags=["Brand Comparison"])
async def api_brand_comparison_base(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
):
    """Brand Comparison: metadata (brand_cats, competitors, years, channels). Usato da all-years."""
    user = _get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Brand Comparison"}, status_code=400)
    return await _bc().get_bc_base(period_start, period_end, user.brand_id)


@app.get("/api/brand-comparison/all", tags=["Brand Comparison"])
async def api_brand_comparison_all(
    access_token: Optional[str] = Cookie(None),
    period_start: str = DP[0],
    period_end: str = DP[1],
    competitor_id: str | None = None,
    discount_category_id: str | None = None,
    discount_subcategory_id: str | None = None,
):
    """Brand Comparison: batch base + sales + promo + peak + discount. Richiede competitor_id."""
    user = _get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Brand Comparison"}, status_code=400)
    if not competitor_id:
        return JSONResponse({"error": "Competitor required"}, status_code=400)
    return await _bc().get_bc_all(
        period_start, period_end, user.brand_id, competitor_id,
        discount_cat=discount_category_id,
        discount_subcat=discount_subcategory_id,
    )


@app.get("/api/brand-comparison/all-years", tags=["Brand Comparison"])
async def api_brand_comparison_all_years(
    access_token: Optional[str] = Cookie(None),
    competitor_id: str | None = None,
    discount_category_id: str | None = None,
    discount_subcategory_id: str | None = None,
):
    """Brand Comparison: tutti gli anni in una chiamata. Richiede competitor_id."""
    user = _get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Brand Comparison"}, status_code=400)
    if not competitor_id:
        return JSONResponse({"error": "Competitor required"}, status_code=400)
    return await _bc().get_bc_all_years(
        user.brand_id, competitor_id,
        discount_cat=discount_category_id,
        discount_subcat=discount_subcategory_id,
    )


@app.get("/api/check-live-promo/active", tags=["Check Live Promo"])
async def api_check_live_promo_active(
    access_token: Optional[str] = Cookie(None),
    date_start: str | None = None,
    date_end: str | None = None,
    promo_id: str | None = None,
    category_id: str | None = None,
    channel: str | None = None,
):
    """Promos with actual sales in the selected period for user's brand."""
    user = _get_user(access_token)
    if not user or not user.brand_id:
        return JSONResponse({"error": "Brand required for active promos"}, status_code=400)
    ds, de = _default_check_live_dates()
    date_start = date_start or ds
    date_end = date_end or de
    return await _svc().get_active_promos(
        date_start, date_end, user.brand_id, promo_id, category_id, channel
    )


@app.get("/api/check-live-promo/sku", tags=["Check Live Promo"])
async def api_check_live_promo_sku(
    access_token: Optional[str] = Cookie(None),
    date_start: str | None = None,
    date_end: str | None = None,
    promo_id: str | None = None,
    category_id: str | None = None,
    channel: str | None = None,
):
    """SKU-level promo performance. Requires auth; uses user.brand_id."""
    user = _get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Check Live Promo"}, status_code=400)
    ds, de = _default_check_live_dates()
    date_start = date_start or ds
    date_end = date_end or de
    return await _svc().get_promo_sku(user.brand_id, date_start, date_end, promo_id, category_id, channel)


@app.get("/api/check-live-promo/segment-breakdown", tags=["Check Live Promo"])
async def api_check_live_promo_segment(
    access_token: Optional[str] = Cookie(None),
    product_id: str | None = None,
    date_start: str | None = None,
    date_end: str | None = None,
    promo_id: str | None = None,
    category_id: str | None = None,
    channel: str | None = None,
):
    """Segment breakdown for a product (product_id) or aggregate (no product_id)."""
    user = _get_user(access_token)
    if not user or not user.brand_id:
        return JSONResponse({"error": "Brand required"}, status_code=400)
    ds, de = _default_check_live_dates()
    date_start = date_start or ds
    date_end = date_end or de
    pid = int(product_id) if product_id and str(product_id).strip() else None
    return await _svc().get_segment_breakdown(
        pid, user.brand_id, date_start, date_end,
        promo_id, category_id, channel,
    )


@app.get("/api/promo-creator")
async def api_promo_creator(
    request: Request,
    access_token: Optional[str] = Cookie(None),
    period_start: str = PC_DEFAULT_PERIOD[0],
    period_end: str = PC_DEFAULT_PERIOD[1],
    category_id: str | None = None,
    subcategory_id: str | None = None,
    promo_type: str | None = None,
    discount_depth: str | None = None,
):
    """Promo Creator: suggestions and benchmarks. Requires auth; uses user.brand_id."""
    user = _get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Promo Simulator"}, status_code=400)
    return await _svc().get_promo_creator_suggestions(period_start, period_end, user.brand_id, promo_type, discount_depth, category_id, subcategory_id)


@app.get("/api/products")
async def api_products(period_start: str = DP[0], period_end: str = DP[1], brand_id: str | None = None):
    return await _svc().get_products(period_start, period_end, brand_id)


@app.get("/api/compare")
async def api_compare(
    compare_type: str = "brand",
    id1: str = "",
    id2: str = "",
    period_start: str = DP[0],
    period_end: str = DP[1],
):
    """Compare two brands or categories with full metrics."""
    if compare_type == "brand":
        d1 = await _svc().get_basic(period_start, period_end, None, None, None, id1)
        d2 = await _svc().get_basic(period_start, period_end, None, None, None, id2)
        p1 = await _svc().get_promo(period_start, period_end, None, None, None)
        p2 = await _svc().get_promo(period_start, period_end, None, None, None)
    else:
        d1 = await _svc().get_basic(period_start, period_end, id1, None, None, None)
        d2 = await _svc().get_basic(period_start, period_end, id2, None, None, None)
        p1 = await _svc().get_promo(period_start, period_end, None, id1, None)
        p2 = await _svc().get_promo(period_start, period_end, None, id2, None)
    return {"item1": d1, "item2": d2, "promo1": p1, "promo2": p2}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/internal/prewarm", tags=["Internal"])
async def api_prewarm(x_prewarm_token: Optional[str] = Header(None, alias="X-Prewarm-Token")):
    """Pre-warming cache per Cloud Scheduler. Richiede X-Prewarm-Token header."""
    token = os.getenv("PREWARM_TOKEN", "").strip()
    if not token or x_prewarm_token != token:
        raise HTTPException(status_code=401, detail="Unauthorized")
    result = await prewarm_cache()
    return result


def _run_refresh_precalc() -> tuple[bool, str, float]:
    """Esegue scripts/refresh_precalc_tables.py. Ritorna (ok, message, duration_sec)."""
    script = BASE_DIR.parent / "scripts" / "refresh_precalc_tables.py"
    if not script.exists():
        return False, "Script refresh_precalc_tables.py non trovato", 0.0
    t0 = time.time()
    try:
        proc = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            timeout=600,
            cwd=str(BASE_DIR.parent),
            env={**os.environ, "GCP_PROJECT_ID": os.environ.get("GCP_PROJECT_ID", "mediaexpertdashboard")},
        )
        elapsed = time.time() - t0
        if proc.returncode != 0:
            return False, proc.stderr or proc.stdout or f"Exit code {proc.returncode}", elapsed
        return True, proc.stdout or "OK", elapsed
    except subprocess.TimeoutExpired:
        return False, "Timeout (600s)", time.time() - t0
    except Exception as e:
        return False, str(e), time.time() - t0


@app.post("/api/admin/recalculate", tags=["Admin"])
async def api_admin_recalculate(access_token: Optional[str] = Cookie(None)):
    """Ricalcola le tabelle precalcolate su BigQuery. Solo admin. Usare quando cambiano i feed dati."""
    user = _get_user(access_token)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not user.can_recalculate:
        raise HTTPException(status_code=403, detail="Non autorizzato a ricalcolare le dashboard")
    ok, message, duration = _run_refresh_precalc()
    if ok:
        return {"status": "completed", "duration_sec": round(duration, 1), "message": message}
    return JSONResponse(
        {"status": "error", "duration_sec": round(duration, 1), "error": message},
        status_code=500,
    )
