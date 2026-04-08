"""API Promo Creator, products, compare, health, prewarm, admin."""
from __future__ import annotations

import os
from typing import Optional
from unittest.mock import MagicMock

from fastapi import APIRouter, Body, Cookie, Header, HTTPException
from fastapi.responses import JSONResponse

from app.constants import DEFAULT_GCS_BRAND_LOGOS_BASE, DP, PC_DEFAULT_PERIOD
from app.web.admin_precalc import run_refresh_precalc
from app.web.brand_logo import (
    brand_logo_skip_proxy_enabled,
    brand_logo_upstream_url,
    brand_logos_force_same_origin_img,
    brand_logos_public_base,
    brand_logo_url_for_user,
    effective_brand_id_for_logo,
    local_brand_logo_path,
)
from app.web.context import _svc, get_user
from app.web.scope import reject_if_cat_sub_out_of_scope
from app.services.prewarm import prewarm_cache

router = APIRouter()


@router.get("/api/promo-creator")
async def api_promo_creator(
    access_token: Optional[str] = Cookie(None),
    period_start: str = PC_DEFAULT_PERIOD[0],
    period_end: str = PC_DEFAULT_PERIOD[1],
    category_id: str | None = None,
    subcategory_id: str | None = None,
    promo_type: str | None = None,
    discount_depth: str | None = None,
):
    """Promo Creator: suggestions and benchmarks. Requires auth; uses user.brand_id."""
    user = get_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return JSONResponse({"error": "Brand required for Promo Simulator"}, status_code=400)
    if (
        not category_id
        or not str(category_id).strip()
        or not str(category_id).strip().isdigit()
        or not (1 <= int(str(category_id).strip()) <= 10)
    ):
        return JSONResponse(
            {"error": "Choose a parent category. Promo Simulator needs category, promo type, and discount depth (%)."},
            status_code=400,
        )
    if not promo_type or not str(promo_type).strip():
        return JSONResponse(
            {"error": "Select a promo type. Promo Simulator needs category, promo type, and discount depth (%)."},
            status_code=400,
        )
    if discount_depth is None or not str(discount_depth).strip():
        return JSONResponse(
            {"error": "Enter discount depth (%). Promo Simulator needs category, promo type, and discount depth."},
            status_code=400,
        )
    try:
        dd = float(str(discount_depth).strip())
        if dd < 0 or dd > 100:
            return JSONResponse({"error": "Discount depth must be between 0 and 100."}, status_code=400)
    except ValueError:
        return JSONResponse({"error": "Discount depth must be a number."}, status_code=400)
    bad = reject_if_cat_sub_out_of_scope(user, category_id, subcategory_id)
    if bad:
        return bad
    return await _svc().get_promo_creator_suggestions(
        period_start, period_end, user.brand_id, promo_type, discount_depth, category_id, subcategory_id
    )


@router.get("/api/products")
async def api_products(period_start: str = DP[0], period_end: str = DP[1], brand_id: str | None = None):
    return await _svc().get_products(period_start, period_end, brand_id)


@router.get("/api/compare")
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


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/internal/prewarm", tags=["Internal"])
async def api_prewarm(x_prewarm_token: Optional[str] = Header(None, alias="X-Prewarm-Token")):
    """Pre-warming cache per Cloud Scheduler. Richiede X-Prewarm-Token header."""
    token = os.getenv("PREWARM_TOKEN", "").strip()
    if not token or x_prewarm_token != token:
        raise HTTPException(status_code=401, detail="Unauthorized")
    result = await prewarm_cache()
    return result


@router.get("/api/admin/brand-logo-debug", tags=["Admin"])
async def api_admin_brand_logo_debug(
    access_token: Optional[str] = Cookie(None),
    brand_id: Optional[int] = None,
):
    """Diagnostica URL loghi (GCS). Solo admin. Opzionale brand_id (es. 1 Samsung)."""
    user = get_user(access_token)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin only")
    from app.db.client import PROJECT_ID

    base = brand_logos_public_base()
    bid = int(brand_id) if brand_id is not None else (user.brand_id or 1)
    mu = MagicMock()
    mu.brand_id = bid
    eff = effective_brand_id_for_logo(mu)
    resolved = brand_logo_url_for_user(mu, _precomputed_effective_brand_id=eff)
    up = brand_logo_upstream_url(bid)
    force_so = brand_logos_force_same_origin_img()
    img_src = resolved or ""
    static_fallback = local_brand_logo_path(bid)
    return {
        "project_id_resolved": PROJECT_ID,
        "brand_logos_public_base_env": (os.environ.get("BRAND_LOGOS_PUBLIC_BASE") or "").strip() or None,
        "default_gcs_constant": DEFAULT_GCS_BRAND_LOGOS_BASE,
        "_brand_logos_public_base": base,
        "brand_logo_skip_proxy": brand_logo_skip_proxy_enabled(),
        "brand_logo_upstream_url": up,
        "brand_id_used": bid,
        "effective_brand_id_for_logo": eff,
        "brand_logo_url": resolved,
        "brand_logo_img_src_topbar_default": img_src,
        "brand_logos_force_same_origin_img": force_so,
        "uses_static_path": bool(resolved and resolved.startswith("/static/")),
        "local_static_fallback_path": str(static_fallback),
        "local_static_fallback_exists": static_fallback.is_file(),
        "note": "Default topbar = HTTPS GCS (come Cloud Run). BRAND_LOGOS_FORCE_SAME_ORIGIN_IMG=1 usa /brand-logo/ nell'HTML. Loghi su bucket GCS; Firestore ha solo brand_id.",
    }


@router.post("/api/admin/clear-cache", tags=["Admin"])
async def api_admin_clear_cache(
    access_token: Optional[str] = Cookie(None),
    body: Optional[dict] = Body(default=None),
):
    """Svuota Redis + cache in-memory (MI, BC, CLP, marketing, ecc.). Solo admin.

    Corpo JSON opzionale: {"flush_redis_db": true} esegue FLUSHDB sull'istanza Redis
    (solo se ENABLE_ADMIN_REDIS_FLUSHDB=1 sul servizio). Usare solo con Memorystore dedicato alla dashboard.
    Senza flag: DELETE per tutti i prefissi noti (inclusi legacy es. mi_all_years_v3).
    """
    user = get_user(access_token)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin only")
    from app.services._cache import clear_service_cache

    want_flush = bool((body or {}).get("flush_redis_db"))
    allow_flush = os.getenv("ENABLE_ADMIN_REDIS_FLUSHDB", "").strip().lower() in ("1", "true", "yes")
    if want_flush and not allow_flush:
        raise HTTPException(
            status_code=400,
            detail="flush_redis_db richiede ENABLE_ADMIN_REDIS_FLUSHDB=1 sull'istanza (Cloud Run / .env).",
        )
    return clear_service_cache(flush_redis_db=bool(want_flush and allow_flush))


@router.post("/api/admin/recalculate", tags=["Admin"])
async def api_admin_recalculate(access_token: Optional[str] = Cookie(None)):
    """Ricalcola le tabelle precalcolate su BigQuery. Solo admin. Usare quando cambiano i feed dati."""
    user = get_user(access_token)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not user.can_recalculate:
        raise HTTPException(status_code=403, detail="Non autorizzato a ricalcolare le dashboard")
    ok, message, duration = run_refresh_precalc()
    if ok:
        return {"status": "completed", "duration_sec": round(duration, 1), "message": message}
    return JSONResponse(
        {"status": "error", "duration_sec": round(duration, 1), "error": message},
        status_code=500,
    )
