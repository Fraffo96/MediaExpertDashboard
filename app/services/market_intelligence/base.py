"""Market Intelligence: metadata base (brand categories, subcategories)."""
import asyncio
import copy

from app.db.queries.precalc import (
    is_full_year_period,
    query_brand_all_subcategories_from_precalc,
    query_brand_categories_from_precalc,
)
from app.db.queries.shared import query_available_years
from app.services._cache import cache_key, get_cached, set_cached
from app.services.mi_bc_live import build_mi_base_live


async def get_mi_available_years_payload() -> dict:
    """Solo elenco anni disponibili — chiamata leggera per primo paint MI."""
    yrs = await asyncio.to_thread(query_available_years)
    ys = [str(y) for y in (list(yrs) if yrs else [])]
    return {"available_years": ys}


async def get_mi_base(ps, pe, brand_id):
    """Metadata: brand_cats, brand_subcats_map, cat_ids, sub_ids, available_years."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    key = cache_key("mi_base", ps=ps, pe=pe, brand=brand_id)
    cached = get_cached(key)
    if cached is not None:
        return cached

    year = int(ps[:4])
    bid = int(brand_id) if brand_id else 0

    if not is_full_year_period(ps, pe):
        out = await build_mi_base_live(ps, pe, bid)
        set_cached(key, out)
        return copy.deepcopy(out)

    years_task = asyncio.ensure_future(asyncio.to_thread(query_available_years))

    brand_cats = await asyncio.to_thread(query_brand_categories_from_precalc, year, bid)
    brand_cats = list(brand_cats) if brand_cats else []

    first_cat = brand_cats[0]["category_id"] if brand_cats else None

    brand_subcats_map = {str(c["category_id"]): [] for c in brand_cats}
    if brand_cats:
        parent_ids = [int(c["category_id"]) for c in brand_cats]
        all_subs = await asyncio.to_thread(query_brand_all_subcategories_from_precalc, year, bid, parent_ids)
        for r in all_subs or []:
            pid = str(r.get("parent_category_id", ""))
            if pid in brand_subcats_map and r.get("category_id") is not None:
                brand_subcats_map[pid].append(
                    {"category_id": r["category_id"], "category_name": r.get("category_name")}
                )

    sub_ids = []
    for subs in brand_subcats_map.values():
        sub_ids.extend([str(s["category_id"]) for s in subs])
    cat_ids = [str(c["category_id"]) for c in brand_cats]

    _years_raw = await years_task
    available_years = list(_years_raw) if _years_raw else []

    cat_pie_id = str(first_cat) if first_cat else (cat_ids[0] if cat_ids else "")
    sub_pie_id = ""
    if first_cat and brand_subcats_map.get(str(first_cat)):
        subs = brand_subcats_map[str(first_cat)]
        sub_pie_id = str(subs[0]["category_id"]) if subs else ""

    out = {
        "available_channels": [{"id": "", "name": "All"}, {"id": "web", "name": "Web"}, {"id": "app", "name": "App"}, {"id": "store", "name": "Store"}],
        "brand_categories": brand_cats,
        "brand_subcategories": brand_subcats_map,
        "first_category_id": first_cat,
        "cat_ids": cat_ids,
        "sub_ids": sub_ids,
        "available_years": available_years,
        "category_pie_id": cat_pie_id,
        "subcategory_pie_id": sub_pie_id,
        "subcategory_category_id": first_cat,
    }
    set_cached(key, out)
    return copy.deepcopy(out)
