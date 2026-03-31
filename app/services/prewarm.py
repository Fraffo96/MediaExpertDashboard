"""Pre-warming cache per Cloud Run. Chiamato da Cloud Scheduler per tenere la cache calda.

- MI: /api/market-intelligence/all-years → get_mi_all_years (TTL_LONG).
- BC: primo competitor da get_bc_competitors(DP) poi get_bc_all_years (TTL_LONG).
- CLP: get_active_promos ultimi 7g (TTL 300s) come primo fetch della pagina.
Serve Redis (REDIS_URL) per condividere cache tra istanze e sessioni."""
import asyncio
import logging
from datetime import datetime, timedelta

from app.auth.firestore_store import list_users_active_with_brand
from app.constants import CLP_DATA_MAX_DATE, DP

logger = logging.getLogger(__name__)

_MI_PREWARM_SEM = asyncio.Semaphore(2)
_BC_PREWARM_SEM = asyncio.Semaphore(2)
_CLP_PREWARM_SEM = asyncio.Semaphore(3)


def _clp_default_dates() -> tuple[str, str]:
    end = min(datetime.utcnow().date(), datetime.strptime(CLP_DATA_MAX_DATE, "%Y-%m-%d").date())
    start = end - timedelta(days=6)
    return start.isoformat(), end.isoformat()


async def prewarm_cache():
    """Riscalda cache per ogni brand con utenti attivi: MI + BC (primo competitor) + CLP active + filters."""
    from app.services.brand_comparison import get_bc_all_years, get_bc_competitors
    from app.services.check_live_promo import get_active_promos
    from app.services.filters import get_filters
    from app.services.market_intelligence import get_mi_all_years

    def _brand_ids():
        users = list_users_active_with_brand()
        return sorted({u.brand_id for u in users if u.brand_id})

    try:
        brand_ids = await asyncio.to_thread(_brand_ids)
    except Exception as e:
        logger.warning("Prewarm: Firestore user list failed (%s), fallback brand_id=1", e)
        brand_ids = []
    if not brand_ids:
        brand_ids = [1]
        logger.info("Prewarm: no brand users, warming brand_id=1")

    async def _warm_mi_all_years(bid: int):
        async with _MI_PREWARM_SEM:
            return await get_mi_all_years(bid, discount_cat=None, discount_subcat=None)

    async def _warm_bc_all_years(bid: int):
        async with _BC_PREWARM_SEM:
            comp = await get_bc_competitors(DP[0], DP[1], bid)
            if isinstance(comp, Exception) or not isinstance(comp, dict):
                return None
            if comp.get("error"):
                return None
            comps = comp.get("competitors") or []
            if not comps:
                return None
            cid = comps[0].get("brand_id")
            if cid is None:
                return None
            return await get_bc_all_years(bid, cid, None, None)

    async def _warm_clp_active(bid: int):
        async with _CLP_PREWARM_SEM:
            ds, de = _clp_default_dates()
            return await get_active_promos(ds, de, bid, None, None, None)

    tasks: list = [_warm_mi_all_years(bid) for bid in brand_ids]
    tasks += [_warm_bc_all_years(bid) for bid in brand_ids]
    tasks += [_warm_clp_active(bid) for bid in brand_ids]
    tasks.append(get_filters())

    results = await asyncio.gather(*tasks, return_exceptions=True)
    n = len(brand_ids)
    mi_res = results[:n]
    bc_res = results[n : n * 2]
    clp_res = results[n * 2 : n * 3]
    filters_ok = not isinstance(results[-1], Exception)

    ok_mi = sum(
        1
        for r in mi_res
        if not isinstance(r, Exception)
        and isinstance(r, dict)
        and not r.get("error")
        and (r.get("by_year") or {})
    )
    ok_bc = sum(
        1
        for r in bc_res
        if not isinstance(r, Exception)
        and isinstance(r, dict)
        and not r.get("error")
        and (r.get("by_year") or {})
    )
    ok_clp = sum(1 for r in clp_res if not isinstance(r, Exception) and isinstance(r, dict))

    errors_n = sum(1 for r in results if isinstance(r, Exception))
    if errors_n:
        logger.warning("Prewarm: %d task errors", errors_n)
    logger.info(
        "Prewarm: MI ok %d/%d, BC ok %d/%d, CLP ok %d/%d, brands=%s, filters=%s",
        ok_mi,
        n,
        ok_bc,
        n,
        ok_clp,
        n,
        brand_ids,
        filters_ok,
    )
    return {
        "warmed": ok_mi,
        "warmed_mi_brands": ok_mi,
        "warmed_bc_brands": ok_bc,
        "warmed_clp_brands": ok_clp,
        "brands": brand_ids,
        "filters": filters_ok,
        "errors": errors_n,
    }
