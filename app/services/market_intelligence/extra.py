"""Market Intelligence: top prodotti e breakdown segmenti per SKU."""
import asyncio
import copy
from decimal import Decimal

from app.services._cache import TTL_LONG, cache_key, get_cached, safe, set_cached


def _sanitize_mi(obj):
    """Convert Decimal to float for JSON."""
    if obj is None:
        return None
    if isinstance(obj, Decimal):
        return float(obj)
    if hasattr(obj, "__float__") and not isinstance(obj, (bool, int)):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _sanitize_mi(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_mi(x) for x in obj]
    return obj


async def get_mi_top_products(year, brand_id, category_id=None, subcategory_id=None, channel=None, limit=50):
    """Top products per valore (anno, brand, category/subcategory, channel)."""
    if not brand_id:
        return {"error": "Brand required", "rows": []}
    from app.db.queries.market_intelligence.segment_sku import query_top_products

    key = cache_key(
        "mi_top_prod_v2",
        year=year,
        brand=brand_id,
        cat=category_id or "",
        sub=subcategory_id or "",
        ch=channel or "",
    )
    cached = get_cached(key, ttl=TTL_LONG)
    if cached is not None:
        return copy.deepcopy(cached)
    rows = await asyncio.to_thread(
        safe,
        query_top_products,
        int(year),
        int(brand_id),
        int(category_id) if category_id and str(category_id).strip() else None,
        int(subcategory_id) if subcategory_id and str(subcategory_id).strip() else None,
        channel,
        limit,
    )
    out = {"rows": _sanitize_mi(list(rows) if rows else [])}
    set_cached(key, out, ttl=TTL_LONG)
    return copy.deepcopy(out)


async def get_mi_segment_by_sku(product_id, brand_id, year, category_id=None, channel=None):
    """Segment breakdown per un prodotto – tutte le vendite (non solo promo)."""
    if not product_id or not brand_id:
        return {"error": "Product and brand required", "rows": []}
    from app.db.queries.market_intelligence.segment_sku import (
        query_segment_breakdown_for_product_all_sales,
        query_segment_breakdown_for_product_precalc,
    )

    date_start = f"{year}-01-01"
    date_end = f"{year}-12-31"
    key = cache_key(
        "mi_seg_sku_v3_mix46",
        pid=product_id,
        brand=brand_id,
        year=year,
        cat=category_id or "",
        ch=channel or "",
    )
    cached = get_cached(key, ttl=TTL_LONG)
    if cached is not None:
        return copy.deepcopy(cached)
    rows = await asyncio.to_thread(
        safe,
        query_segment_breakdown_for_product_precalc,
        int(product_id),
        int(brand_id),
        int(year),
        category_id,
        channel,
    )
    if not rows:
        rows = await asyncio.to_thread(
            safe,
            query_segment_breakdown_for_product_all_sales,
            int(product_id),
            int(brand_id),
            date_start,
            date_end,
            category_id,
            channel,
        )
    out = {"rows": _sanitize_mi(list(rows) if rows else [])}
    set_cached(key, out, ttl=TTL_LONG)
    return copy.deepcopy(out)
