"""Products tab: vendite per categoria e top prodotti del brand."""
import asyncio

from app.db.queries import product
from app.services._cache import cache_key, get_cached, set_cached, safe


async def get_products(ps, pe, brand_id):
    if not brand_id or not str(brand_id).strip():
        return {"by_category": [], "top_products": []}
    key = cache_key("products", ps=ps, pe=pe, brand=brand_id)
    cached = get_cached(key)
    if cached is not None:
        return cached
    by_cat, top = await asyncio.gather(
        asyncio.to_thread(safe, product.query_brand_products_by_category, ps, pe, brand_id),
        asyncio.to_thread(safe, product.query_brand_top_products, ps, pe, brand_id, 15),
    )
    out = {"by_category": list(by_cat), "top_products": list(top)}
    set_cached(key, out)
    return out
