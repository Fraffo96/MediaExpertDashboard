"""Filtri condivisi: categories, subcategories, segments, brands, ecc."""
import asyncio

from app.db.queries import shared
from app.services._cache import TTL_LONG, get_cached, safe, set_cached

_FILTERS_CACHE_KEY = "filters:global"


async def get_filters():
    cached = get_cached(_FILTERS_CACHE_KEY, ttl=TTL_LONG)
    if cached is not None:
        return cached

    cats, subcats, segs, brands, ptypes, promos, genders, years = await asyncio.gather(
        asyncio.to_thread(safe, shared.query_categories),
        asyncio.to_thread(safe, shared.query_subcategories),
        asyncio.to_thread(safe, shared.query_segments),
        asyncio.to_thread(safe, shared.query_brands),
        asyncio.to_thread(safe, shared.query_promo_types),
        asyncio.to_thread(safe, shared.query_promos),
        asyncio.to_thread(safe, shared.query_genders),
        asyncio.to_thread(safe, shared.query_available_years),
    )
    cats = list(cats) if cats else []
    subcats = list(subcats) if subcats else []
    if not subcats and cats:
        subcats = [c for c in cats if c.get("level") == 2]
    result = {
        "categories": cats,
        "subcategories": subcats,
        "segments": list(segs),
        "brands": list(brands),
        "promo_types": list(ptypes),
        "promos": list(promos),
        "genders": list(genders),
        "available_years": list(years) if years else [],
    }
    set_cached(_FILTERS_CACHE_KEY, result, ttl=TTL_LONG)
    return result


def roi_cat(cat, subcategory_id):
    """Promo ROI fact is at parent level: use parent when subcategory is selected."""
    if subcategory_id and str(subcategory_id).strip():
        try:
            return int(subcategory_id) // 100
        except (ValueError, TypeError):
            pass
    if cat and str(cat).strip():
        try:
            c = int(cat)
            if 1 <= c <= 10:
                return c
        except (ValueError, TypeError):
            pass
    return None
