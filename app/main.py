"""
MediaExpert Insights – dashboard platform.
Start: uvicorn app.main:app --reload
"""
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
from dotenv import load_dotenv

load_dotenv(_REPO_ROOT / ".env")

import asyncio
import logging
import os

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.auth.database import init_db
from app.auth.routes import router as auth_router
from app.jinja_env import BASE_DIR
from app.routers import (
    api_admin_dataops,
    api_brand_comparison,
    api_check_live,
    api_market_intelligence,
    api_marketing,
    api_misc,
    api_sales_basic,
    pages,
)
from app.services.prewarm import prewarm_cache
from app.web import brand_logo as brand_logo_module
from app.web.brand_logo import brand_logos_public_base, router as brand_logo_router, brand_logo_url_for_user

logger = logging.getLogger(__name__)

app = FastAPI(title="MediaExpert Insights")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
app.include_router(auth_router)
app.include_router(brand_logo_router)
app.include_router(pages.router)
app.include_router(api_sales_basic.router)
app.include_router(api_market_intelligence.router)
app.include_router(api_marketing.router)
app.include_router(api_brand_comparison.router)
app.include_router(api_check_live.router)
app.include_router(api_misc.router)
app.include_router(api_admin_dataops.router)


@app.on_event("startup")
async def on_startup():
    b = brand_logos_public_base()
    logger.info("Brand logos base: %s", b)
    fb = (os.environ.get("BRAND_LOGO_FALLBACK_BRAND_ID") or "(default 1 for admin)").strip()
    logger.info(
        "Brand logo: default topbar URL = HTTPS GCS | BRAND_LOGO_FALLBACK_BRAND_ID=%s | "
        "BRAND_LOGOS_FORCE_SAME_ORIGIN_IMG=%s (solo se 1: HTML usa /brand-logo/)",
        fb,
        (os.environ.get("BRAND_LOGOS_FORCE_SAME_ORIGIN_IMG") or "").strip() or "(off)",
    )
    logger.info("Brand logo code loaded from: %s", getattr(brand_logo_module, "__file__", "?"))
    init_db()
    try:
        from app.services.marketing import warm_needstates_spider_precalc

        warm_needstates_spider_precalc()
    except Exception:
        pass

    async def _prewarm():
        try:
            r = await prewarm_cache()
            if r.get("warmed", 0) > 0:
                logger.info("Prewarm: cache ready for %s", r.get("brands", []))
        except Exception as e:
            logger.warning("Prewarm failed: %s", e)

    asyncio.create_task(_prewarm())


# Alias legacy per script e test (es. verify_brand_logo_env, test_brand_logo_resolve)
_brand_logos_public_base = brand_logos_public_base
_brand_logo_url = brand_logo_url_for_user
