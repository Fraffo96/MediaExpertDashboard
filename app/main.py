"""
Dashboard Media Expert – applicazione personalizzabile.
Connessione a BigQuery (dataset mart su GCP). Avvio: uvicorn app.main:app --reload
"""
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

from app.db import (
    query_category_sales,
    query_promo_share,
    query_yoy,
    query_promo_roi,
    query_peak_events,
    query_categories,
)

app = FastAPI(title="Media Expert Dashboard")
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def default_period():
    return "2025-01-01", "2025-12-31"


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Pagina principale dashboard con filtri periodo e categoria."""
    period_start, period_end = default_period()
    categories = query_categories()
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "period_start": period_start,
            "period_end": period_end,
            "categories": categories,
        },
    )


@app.get("/api/category-sales")
async def api_category_sales(
    period_start: str = "2025-01-01",
    period_end: str = "2025-12-31",
    category_id: str | None = None,
):
    """API: vendite per categoria (per grafici/tabelle)."""
    return query_category_sales(period_start, period_end, category_id)


@app.get("/api/promo-share")
async def api_promo_share(
    period_start: str = "2025-01-01",
    period_end: str = "2025-12-31",
    category_id: str | None = None,
):
    return query_promo_share(period_start, period_end, category_id)


@app.get("/api/yoy")
async def api_yoy(
    period_start: str = "2025-01-01",
    period_end: str = "2025-12-31",
    category_id: str | None = None,
):
    return query_yoy(period_start, period_end, category_id)


@app.get("/api/promo-roi")
async def api_promo_roi(
    period_start: str = "2025-01-01",
    period_end: str = "2025-12-31",
    promo_type: str | None = None,
):
    return query_promo_roi(period_start, period_end, promo_type)


@app.get("/api/peak-events")
async def api_peak_events(
    period_start: str = "2025-01-01",
    period_end: str = "2025-12-31",
    category_id: str | None = None,
):
    return query_peak_events(period_start, period_end, category_id)


@app.get("/health")
async def health():
    return {"status": "ok"}
