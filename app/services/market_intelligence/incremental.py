"""Market Intelligence: incremental YoY multi-anno."""
import asyncio
from decimal import Decimal

from app.db.queries.market_intelligence.shared import CHANNELS
from app.db.queries.precalc import (
    get_multi_year_full_years,
    query_incremental_yoy_vendite_multi_year_from_precalc,
)
from app.services._cache import cache_key, get_cached, set_cached
from ._config import PRECALC_ONLY_ERR, _MI_INCR_SCOPE_SEM
from .base import get_mi_base


async def get_mi_incremental_yoy_api(ps: str, pe: str, brand_id: int) -> dict:
    """Incremental YoY multi-anno: richiede ps..pe come anni interi 01-01 … 12-31."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    rng = get_multi_year_full_years(ps, pe)
    if rng is None:
        return {"error": PRECALC_ONLY_ERR}
    y_ref = rng[-1]
    base = await get_mi_base(f"{y_ref}-01-01", f"{y_ref}-12-31", brand_id)
    if base.get("error"):
        return base
    return await get_mi_incremental_yoy(
        ps, pe, brand_id, base.get("brand_categories") or [], base.get("brand_subcategories") or {}
    )


async def get_mi_incremental_yoy(ps, pe, brand_id, brand_cats, brand_subcats_map):
    """Incremental YoY: year, total_gross, promo_gross per scope e channel. Per chart Average Incremental YoY."""
    if not brand_id or not str(brand_id).strip():
        return {}
    key = cache_key("mi_incr_yoy_v2_dedup_ch", ps=ps, pe=pe, brand=brand_id)
    cached = get_cached(key)
    if cached is not None:
        return cached

    scope_keys = [""]
    scope_params = [(None, None)]  # (cat, subcat)
    for c in brand_cats:
        scope_keys.append("cat_" + str(c["category_id"]))
        scope_params.append((int(c["category_id"]), None))
    for cid, subs in brand_subcats_map.items():
        for s in subs:
            scope_keys.append("sub_" + str(s["category_id"]))
            scope_params.append((None, int(s["category_id"])))

    def _to_jsonable(rows):
        out = []
        for r in (rows or []):
            d = dict(r)
            for k in ("total_gross", "promo_gross", "incremental_sales_pln"):
                if k in d and isinstance(d[k], Decimal):
                    d[k] = float(d[k])
            if "year" in d and d["year"] is not None:
                d["year"] = int(d["year"])
            out.append(d)
        return out

    multi_years = get_multi_year_full_years(ps, pe)
    if multi_years is None:
        return {}
    bid = int(brand_id) if brand_id else 0

    async def _run_precalc_scope(scope_key, cat, subcat):
        async with _MI_INCR_SCOPE_SEM:
            rows = await asyncio.to_thread(
                query_incremental_yoy_vendite_multi_year_from_precalc,
                list(multi_years),
                bid,
                cat,
                subcat,
                None,
            )
            rows = list(rows) if rows else []
            rows.sort(key=lambda x: (x.get("year") or 0))
            return {ch: rows for ch in CHANNELS}

    scope_tasks = [_run_precalc_scope(sk, cat, subcat) for sk, (cat, subcat) in zip(scope_keys, scope_params)]
    scope_results = await asyncio.gather(*scope_tasks)

    incremental_yoy_map_channel = {}
    for ch in CHANNELS:
        incremental_yoy_map_channel[ch] = {sk: _to_jsonable(scope_results[i].get(ch, [])) for i, sk in enumerate(scope_keys)}
    incremental_yoy_map = incremental_yoy_map_channel.get("", {})

    out = {"incremental_yoy_map": incremental_yoy_map, "incremental_yoy_map_channel": incremental_yoy_map_channel}
    set_cached(key, out)
    return out
