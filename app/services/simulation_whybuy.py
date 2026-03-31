"""Simulation e Why Buy dashboards."""
import asyncio

from app.db.queries import simulation, why_buy
from app.services._cache import cache_key, get_cached, set_cached, safe


async def get_simulation(ps, pe, pt=None, seg=None, cat=None):
    key = cache_key("simulation", ps=ps, pe=pe, pt=pt or "", seg=seg or "", cat=cat or "")
    cached = get_cached(key)
    if cached is not None:
        return cached
    baseline, uplift_types, seg_response = await asyncio.gather(
        asyncio.to_thread(safe, simulation.query_historical_baseline, ps, pe, pt, seg, cat),
        asyncio.to_thread(safe, simulation.query_uplift_by_promo_type, ps, pe),
        asyncio.to_thread(safe, simulation.query_segment_response, ps, pe),
    )
    out = {
        "baseline": list(baseline), "uplift_by_type": list(uplift_types),
        "segment_response": list(seg_response),
    }
    set_cached(key, out)
    return out


async def get_why_buy(ps, pe, cat=None):
    by_seg, growth, radar = await asyncio.gather(
        asyncio.to_thread(safe, why_buy.query_category_by_segment, ps, pe, cat),
        asyncio.to_thread(safe, why_buy.query_category_growth, ps, pe, cat),
        asyncio.to_thread(safe, why_buy.query_segment_radar, ps, pe, cat),
    )
    return {
        "by_segment": list(by_seg), "growth": list(growth), "radar": list(radar),
    }
