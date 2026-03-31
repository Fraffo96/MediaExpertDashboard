"""Check Live Promo: active promos and SKU-level performance. Fast loading via precalc."""
import asyncio
import copy
from decimal import Decimal


def _sanitize(obj):
    """Convert Decimal to float for JSON."""
    if obj is None:
        return None
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(x) for x in obj]
    return obj


async def get_active_promos(
    date_start: str,
    date_end: str,
    brand_id: int,
    promo_id: str | None = None,
    category_id: str | None = None,
    channel: str | None = None,
):
    """Promos with actual sales in the selected period for this brand (from precalc)."""
    from app.db.queries.check_live_promo import query_active_promos_from_sales
    from app.services._cache import cache_key, get_cached, set_cached, safe

    key = cache_key(
        "clp_active",
        ds=date_start,
        de=date_end,
        brand=brand_id,
        pid=promo_id or "",
        cid=category_id or "",
        ch=channel or "",
    )
    cached = get_cached(key, ttl=300)
    if cached is not None:
        return copy.deepcopy(cached)
    rows = await asyncio.to_thread(
        safe,
        query_active_promos_from_sales,
        date_start,
        date_end,
        int(brand_id),
        promo_id,
        category_id,
        channel,
    )
    out = {"active": list(rows) if rows else []}
    set_cached(key, out, ttl=300)
    return copy.deepcopy(out)


async def get_promo_sku(brand_id, date_start, date_end, promo_id=None, category_id=None, channel=None):
    """SKU-level promo performance from precalc. Cached 5 min."""
    if not brand_id:
        return {"error": "Brand required", "rows": []}
    from app.db.queries.check_live_promo import query_promo_sku_from_precalc
    from app.services._cache import cache_key, get_cached, set_cached, safe

    key = cache_key(
        "clp_sku",
        brand=brand_id,
        ds=date_start,
        de=date_end,
        pid=promo_id or "",
        cid=category_id or "",
        ch=channel or "",
    )
    cached = get_cached(key, ttl=300)
    if cached is not None:
        return copy.deepcopy(cached)
    rows = await asyncio.to_thread(
        safe,
        query_promo_sku_from_precalc,
        date_start,
        date_end,
        int(brand_id),
        promo_id,
        category_id,
        channel,
    )
    rows = list(rows) if rows else []
    total_gross = sum(float(r.get("gross_pln") or 0) for r in rows)
    total_units = sum(int(r.get("units") or 0) for r in rows)
    total_orders = sum(int(r.get("order_count") or 0) for r in rows)
    sku_count = len(set(r.get("product_id") for r in rows))
    out = {
        "rows": _sanitize(rows),
        "total_gross_pln": total_gross,
        "total_units": total_units,
        "total_orders": total_orders,
        "sku_count": sku_count,
    }
    set_cached(key, out, ttl=300)
    return copy.deepcopy(out)


async def get_segment_breakdown(product_id, brand_id, date_start, date_end, promo_id=None, category_id=None, channel=None):
    """Segment breakdown for a product or aggregate (product_id=None = all products)."""
    if not brand_id:
        return {"error": "Brand required", "rows": []}
    from app.db.queries.check_live_promo import (
        query_segment_breakdown_for_product,
        query_segment_breakdown_aggregate,
    )
    from app.services._cache import cache_key, get_cached, set_cached, safe

    key = cache_key(
        "clp_seg",
        pid=product_id if product_id else "all",
        brand=brand_id,
        ds=date_start,
        de=date_end,
        promo=promo_id or "",
        cat=category_id or "",
        ch=channel or "",
    )
    cached = get_cached(key, ttl=300)
    if cached is not None:
        return copy.deepcopy(cached)
    if product_id:
        rows = await asyncio.to_thread(
            safe,
            query_segment_breakdown_for_product,
            int(product_id),
            int(brand_id),
            date_start,
            date_end,
            promo_id,
            category_id,
            channel,
        )
    else:
        rows = await asyncio.to_thread(
            safe,
            query_segment_breakdown_aggregate,
            int(brand_id),
            date_start,
            date_end,
            promo_id,
            category_id,
            channel,
        )
    out = {"rows": _sanitize(list(rows) if rows else [])}
    set_cached(key, out, ttl=300)
    return copy.deepcopy(out)
