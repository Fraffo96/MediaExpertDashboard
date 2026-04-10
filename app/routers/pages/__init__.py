"""Route pagine HTML (Jinja2) — router aggregato."""
from fastapi import APIRouter

from .admin_help import router as admin_help_r
from .insights import router as insights_r
from .landing import router as landing_r
from .legacy_sales import router as legacy_sales_r
from .marketing_pages import router as marketing_r

router = APIRouter()
router.include_router(landing_r)
router.include_router(insights_r)
router.include_router(marketing_r)
router.include_router(legacy_sales_r)
router.include_router(admin_help_r)
