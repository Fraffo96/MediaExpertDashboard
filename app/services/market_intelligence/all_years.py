"""Market Intelligence: caricamento multi-anno (dropdown anni)."""
import asyncio

from app.db.queries.shared import query_available_years
from app.services._cache import TTL_LONG, cache_key, compute_once, get_cached
from ._config import _MI_YEAR_LOAD_SEM, logger
from .base import get_mi_base
from .batch import get_mi_all
from .incremental import get_mi_incremental_yoy


async def get_mi_all_years(brand_id, discount_cat=None, discount_subcat=None):
    """Carica tutti gli anni in parallelo sul server. Una sola chiamata = tutti i dati pronti per dropdown year istantanei."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    key = cache_key(
        "mi_all_years_v4",
        brand=brand_id,
        disc_cat=discount_cat or "",
        disc_sub=discount_subcat or "",
    )

    async def _compute():
        years = await asyncio.to_thread(query_available_years)
        years = list(years) if years else []
        if not years:
            return {"error": "No years available", "by_year": {}, "available_years": []}

        y_ref = years[-1]
        base_for_incr = await get_mi_base(f"{y_ref}-01-01", f"{y_ref}-12-31", brand_id)
        if base_for_incr.get("error"):
            return {
                "error": base_for_incr.get("error"),
                "by_year": {},
                "available_years": [str(y) for y in years],
            }

        ps_full = f"{years[0]}-01-01"
        pe_full = f"{years[-1]}-12-31"
        incr_task = asyncio.create_task(
            get_mi_incremental_yoy(
                ps_full,
                pe_full,
                brand_id,
                base_for_incr.get("brand_categories") or [],
                base_for_incr.get("brand_subcategories") or {},
            )
        )

        async def _load_one_year(y: int):
            async with _MI_YEAR_LOAD_SEM:
                return y, await get_mi_all(f"{y}-01-01", f"{y}-12-31", brand_id, discount_cat, discount_subcat)

        results = await asyncio.gather(*[_load_one_year(y) for y in years], return_exceptions=True)
        by_year = {}
        for r in results:
            if isinstance(r, Exception):
                logger.warning("get_mi_all_years: year load failed brand=%s: %s", brand_id, r, exc_info=r)
                continue
            if isinstance(r, tuple) and len(r) == 2:
                y, payload = r
                if isinstance(payload, dict) and payload.get("error"):
                    logger.warning("get_mi_all_years: get_mi_all error for year=%s brand=%s: %s", y, brand_id, payload.get("error"))
                    continue
                by_year[str(y)] = payload

        incr_yoy = await incr_task

        out = {"by_year": by_year, "available_years": [str(y) for y in years]}
        if incr_yoy:
            out.update(incr_yoy)
        if years and not by_year:
            logger.error(
                "get_mi_all_years: empty by_year brand=%s years=%s — not caching (check BQ/logs).",
                brand_id,
                years,
            )
            return out
        return out

    return await compute_once(key, _compute, ttl=TTL_LONG)
