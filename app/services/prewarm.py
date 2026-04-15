"""Pre-warming cache per Cloud Run. Chiamato da Cloud Scheduler per tenere la cache calda.

- MI: /api/market-intelligence/all-years → get_mi_all_years (TTL_LONG).
- MI live: ultimo mese di calendario → get_mi_all (cache per finestre non annuali).
- BC: primo competitor da get_bc_competitors(DP) poi get_bc_all_years (TTL_LONG).
- Marketing: media preferences, purchasing, needstates spider (es. segmento/categoria 1).
- CLP: get_active_promos ultimi 7g (TTL 300s) come primo fetch della pagina.
Serve Redis (REDIS_URL) per condividere cache tra istanze e sessioni.

Env opzionale PREWARM_BRAND_IDS=1,2,8 : elenco brand da riscaldare (salta lettura Firestore).
Utile dopo clear-cache senza attendere login utenti."""
import asyncio
import logging
import os
from datetime import date, datetime, timedelta

from app.auth.firestore_store import list_users_active_with_brand
from app.constants import CLP_DATA_MAX_DATE, DP

logger = logging.getLogger(__name__)

_prewarm_done: bool = False


def is_prewarm_done() -> bool:
    """True se il prewarm iniziale e' completato (utile per health check e diagnostica)."""
    return _prewarm_done


_MI_PREWARM_SEM = asyncio.Semaphore(2)
_MI_LIVE_PREWARM_SEM = asyncio.Semaphore(2)
_BC_PREWARM_SEM = asyncio.Semaphore(2)
_CLP_PREWARM_SEM = asyncio.Semaphore(3)


def _clp_default_dates() -> tuple[str, str]:
    end = min(datetime.utcnow().date(), datetime.strptime(CLP_DATA_MAX_DATE, "%Y-%m-%d").date())
    start = end - timedelta(days=6)
    return start.isoformat(), end.isoformat()


def _prev_calendar_month_bounds() -> tuple[str, str]:
    """Primo e ultimo giorno del mese di calendario precedente (MI/BC live)."""
    today = date.today()
    first_this = today.replace(day=1)
    last_prev = first_this - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    return first_prev.isoformat(), last_prev.isoformat()


def _warm_marketing_sync() -> None:
    """Cache in-memory: media preferences, purchasing, needstates spider."""
    from app.services.marketing import get_media_preferences, get_needstates_spider, get_purchasing

    get_media_preferences(1, None)
    get_media_preferences(1, 1)
    get_purchasing(DP[0], DP[1], None, None)
    get_purchasing(DP[0], DP[1], 1, 1)
    get_needstates_spider(1, 1)


async def prewarm_cache():
    """Riscalda cache per ogni brand con utenti attivi: MI + BC (primo competitor) + CLP active + filters."""
    global _prewarm_done
    from app.services.brand_comparison import get_bc_all_years, get_bc_competitors
    from app.services.check_live_promo import get_active_promos
    from app.services.filters import get_filters
    from app.services.market_intelligence import get_mi_all_years

    def _brand_ids_explicit():
        raw = os.getenv("PREWARM_BRAND_IDS", "").strip()
        if not raw:
            return None
        out: list[int] = []
        for part in raw.split(","):
            p = part.strip()
            if p.isdigit():
                out.append(int(p))
        return sorted(set(out)) if out else None

    def _brand_ids_from_firestore():
        users = list_users_active_with_brand()
        return sorted({u.brand_id for u in users if u.brand_id})

    explicit = _brand_ids_explicit()
    if explicit is not None:
        brand_ids = explicit
        logger.info("Prewarm: using PREWARM_BRAND_IDS=%s", brand_ids)
    else:
        try:
            brand_ids = await asyncio.to_thread(_brand_ids_from_firestore)
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

    async def _warm_mi_prev_month(bid: int):
        from app.services.market_intelligence import get_mi_all

        ps, pe = _prev_calendar_month_bounds()
        async with _MI_LIVE_PREWARM_SEM:
            return await get_mi_all(ps, pe, bid, None, None)

    async def _warm_top_products(bid: int):
        from app.services.market_intelligence.extra import get_mi_top_products

        last_year = int(DP[1][:4])
        async with _MI_PREWARM_SEM:
            return await get_mi_top_products(last_year, bid)

    _PC_PREWARM_SEM = asyncio.Semaphore(2)

    async def _warm_promo_creator(bid: int):
        from app.services.promo_creator import get_promo_creator_suggestions

        last_year = int(DP[1][:4])
        ps, pe = f"{last_year}-01-01", f"{last_year}-12-31"
        async with _PC_PREWARM_SEM:
            return await get_promo_creator_suggestions(ps, pe, bid)

    tasks: list = [_warm_mi_all_years(bid) for bid in brand_ids]
    tasks += [_warm_bc_all_years(bid) for bid in brand_ids]
    tasks += [_warm_clp_active(bid) for bid in brand_ids]
    tasks += [_warm_top_products(bid) for bid in brand_ids]
    tasks += [_warm_promo_creator(bid) for bid in brand_ids]
    tasks.append(get_filters())

    results = await asyncio.gather(*tasks, return_exceptions=True)
    n = len(brand_ids)
    mi_res = results[:n]
    bc_res = results[n : n * 2]
    clp_res = results[n * 2 : n * 3]
    filters_ok = not isinstance(results[-1], Exception)

    mkt_ok = True
    try:
        await asyncio.to_thread(_warm_marketing_sync)
    except Exception as e:
        mkt_ok = False
        logger.warning("Prewarm: marketing bundle failed (%s)", e)

    mi_live_results = await asyncio.gather(
        *[_warm_mi_prev_month(bid) for bid in brand_ids],
        return_exceptions=True,
    )
    ok_mi_live = sum(
        1
        for r in mi_live_results
        if not isinstance(r, Exception) and isinstance(r, dict) and not r.get("error")
    )

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
    errors_n += sum(1 for r in mi_live_results if isinstance(r, Exception))
    if errors_n:
        logger.warning("Prewarm: %d task errors", errors_n)
    logger.info(
        "Prewarm: MI ok %d/%d, BC ok %d/%d, CLP ok %d/%d, MI-live-month ok %d/%d, mkt=%s, brands=%s, filters=%s",
        ok_mi,
        n,
        ok_bc,
        n,
        ok_clp,
        n,
        ok_mi_live,
        n,
        mkt_ok,
        brand_ids,
        filters_ok,
    )
    _prewarm_done = True
    return {
        "warmed": ok_mi,
        "warmed_mi_brands": ok_mi,
        "warmed_bc_brands": ok_bc,
        "warmed_clp_brands": ok_clp,
        "warmed_mi_live_month": ok_mi_live,
        "marketing_bundle": mkt_ok,
        "brands": brand_ids,
        "filters": filters_ok,
        "errors": errors_n,
    }
