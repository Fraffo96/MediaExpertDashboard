"""Pre-warming cache per Cloud Run. Chiamato da Cloud Scheduler per tenere la cache calda.

La dashboard MI carica /api/market-intelligence/all-years → get_mi_all_years (TTL_LONG).
Il vecchio prewarm usava solo get_mi_all su un anno: cache miss costanti → spinners.
Serve Redis (REDIS_URL) così la cache è condivisa tra istanze e sessioni; senza Redis ogni
istanza Cloud Run resta fredda fino al primo hit."""
import asyncio
import logging

from app.auth.firestore_store import list_users_active_with_brand

logger = logging.getLogger(__name__)

# Limita get_mi_all_years concorrenti (pesante: tutti gli anni × MI per brand)
_MI_PREWARM_BRAND_SEM = asyncio.Semaphore(2)


async def prewarm_cache():
    """Riscalda la cache per ogni brand con utenti attivi: MI all-years (come il primo fetch JS) + filters."""
    from app.services.market_intelligence import get_mi_all_years
    from app.services.filters import get_filters

    def _brand_ids():
        users = list_users_active_with_brand()
        return sorted({u.brand_id for u in users if u.brand_id})

    try:
        brand_ids = await asyncio.to_thread(_brand_ids)
    except Exception as e:
        logger.warning("Prewarm: Firestore user list failed (%s), fallback brand_id=1", e)
        brand_ids = []
    if not brand_ids:
        brand_ids = [1]  # fallback per dev locale / DB non configurato
        logger.info("Prewarm: no brand users, warming brand_id=1")

    async def _warm_mi_all_years(bid: int):
        async with _MI_PREWARM_BRAND_SEM:
            return await get_mi_all_years(bid, discount_cat=None, discount_subcat=None)

    tasks = [_warm_mi_all_years(bid) for bid in brand_ids]
    tasks.append(get_filters())

    results = await asyncio.gather(*tasks, return_exceptions=True)
    n_brand_tasks = len(brand_ids)
    brand_results = results[:n_brand_tasks]
    ok_brands = sum(
        1
        for r in brand_results
        if not isinstance(r, Exception)
        and isinstance(r, dict)
        and not r.get("error")
        and (r.get("by_year") or {})
    )
    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        logger.warning("Prewarm: %d task errors", len(errors))
    filters_ok = not isinstance(results[-1], Exception)
    logger.info(
        "Prewarm: MI all-years ok for %d/%d brands %s; filters=%s",
        ok_brands,
        len(brand_ids),
        brand_ids,
        filters_ok,
    )
    return {
        "warmed": ok_brands,
        "warmed_mi_brands": ok_brands,
        "brands": brand_ids,
        "filters": filters_ok,
        "errors": len(errors),
    }
