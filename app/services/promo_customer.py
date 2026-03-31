"""Promo, Incremental YoY, Customer dashboards."""
import asyncio

from app.db.queries import basic, promo, customer
from app.services._cache import cache_key, get_cached, set_cached, safe


async def get_incremental_yoy(ps, pe, cat=None, seg=None, gender=None, brand=None, promo_ids=None):
    promo_ids = promo_ids or []
    if len(promo_ids) <= 1:
        pid = int(promo_ids[0]) if promo_ids else None
        incr = await asyncio.to_thread(safe, basic.query_incremental_yoy, ps, pe, cat, seg, gender, brand, pid)
        return {"incremental_yoy": list(incr)}
    by_promo = await asyncio.to_thread(safe, basic.query_incremental_yoy_by_promo, ps, pe, cat, seg, gender, brand, promo_ids)
    return {"incremental_yoy_by_promo": list(by_promo)}


async def get_promo(ps, pe, pt=None, cat=None, seg=None):
    key = cache_key("promo", ps=ps, pe=pe, pt=pt or "", cat=cat or "", seg=seg or "")
    cached = get_cached(key)
    if cached is not None:
        return cached
    kpi, by_type, uplift_cat, timeline, ranking, roi_by_discount = await asyncio.gather(
        asyncio.to_thread(safe, promo.query_promo_kpi, ps, pe, pt, cat),
        asyncio.to_thread(safe, promo.query_performance_by_type, ps, pe, pt, cat),
        asyncio.to_thread(safe, promo.query_uplift_by_category, ps, pe, pt, cat),
        asyncio.to_thread(safe, promo.query_promo_timeline, ps, pe, pt, cat),
        asyncio.to_thread(safe, promo.query_promo_ranking, ps, pe, pt, cat),
        asyncio.to_thread(safe, promo.query_roi_by_discount, ps, pe, pt, cat, seg),
    )
    out = {
        "kpi": list(kpi), "by_type": list(by_type), "uplift_by_category": list(uplift_cat),
        "timeline": list(timeline), "ranking": list(ranking), "roi_by_discount": list(roi_by_discount),
    }
    set_cached(key, out)
    return out


async def get_customer(ps, pe, seg=None, gender=None):
    key = cache_key("customer", ps=ps, pe=pe, seg=seg or "", gender=gender or "")
    cached = get_cached(key)
    if cached is not None:
        return cached
    overview, seasonality, spend, channel, loyalty, repeat = await asyncio.gather(
        asyncio.to_thread(safe, customer.query_segment_overview, ps, pe, seg, gender),
        asyncio.to_thread(safe, customer.query_seasonality, ps, pe, seg, gender),
        asyncio.to_thread(safe, customer.query_spend_distribution, ps, pe, seg, gender),
        asyncio.to_thread(safe, customer.query_channel_mix, ps, pe, seg, gender),
        asyncio.to_thread(safe, customer.query_loyalty_penetration, ps, pe, seg, gender),
        asyncio.to_thread(safe, customer.query_repeat_rate, ps, pe, seg, gender),
    )
    out = {
        "overview": list(overview),
        "seasonality": list(seasonality), "spend_distribution": list(spend),
        "channel_mix": list(channel), "loyalty": list(loyalty), "repeat_rate": list(repeat),
    }
    set_cached(key, out)
    return out
