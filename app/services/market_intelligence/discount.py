"""Market Intelligence: discount depth."""
import asyncio
import copy

from app.db.queries.precalc import (
    is_full_year_period,
    query_discount_depth_brand_vs_media_from_precalc,
    query_discount_depth_for_all_subcategories_from_precalc,
)
from app.services._cache import cache_key, get_cached, set_cached
from app.services.mi_bc_live import get_mi_discount_live


async def get_mi_discount(ps, pe, brand_id, brand_cats, sub_ids, discount_cat=None, discount_subcat=None):
    """Discount depth by category/subcategory."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    key = cache_key("mi_discount_v2_excl_brand", ps=ps, pe=pe, brand=brand_id)
    cached = get_cached(key)
    if cached is not None:
        out = copy.deepcopy(cached)
        first_cat = brand_cats[0]["category_id"] if brand_cats else None
        disc_cat = discount_cat or (str(first_cat) if first_cat else None)
        disc_sub = discount_subcat
        if disc_sub:
            out["discount_depth_selected"] = (out.get("discount_depth_selected_map") or {}).get("sub_" + str(disc_sub))
        elif disc_cat:
            out["discount_depth_selected"] = (out.get("discount_depth_selected_map") or {}).get("cat_" + str(disc_cat))
        else:
            out["discount_depth_selected"] = None
        return out

    if not is_full_year_period(ps, pe):
        out = await get_mi_discount_live(ps, pe, int(brand_id), brand_cats, sub_ids, discount_cat, discount_subcat)
        set_cached(key, out)
        return copy.deepcopy(out)

    year = int(ps[:4])

    disc_depth = await asyncio.to_thread(query_discount_depth_brand_vs_media_from_precalc, year, int(brand_id))
    disc_depth = list(disc_depth) if disc_depth else []
    discount_depth_selected_map = {}
    for r in disc_depth:
        cid = str(r.get("category_id", ""))
        if cid:
            discount_depth_selected_map["cat_" + cid] = {
                "brand_avg_discount_depth": r.get("brand_avg_discount_depth"),
                "media_avg_discount_depth": r.get("media_avg_discount_depth"),
            }
    if sub_ids:
        sub_ids_int = [int(s) for s in sub_ids if s]
        dd_sub_all = await asyncio.to_thread(query_discount_depth_for_all_subcategories_from_precalc, year, int(brand_id), sub_ids_int)
        dd_sub_all = list(dd_sub_all) if dd_sub_all else []
        for r in dd_sub_all:
            sid = str(r.get("category_id", ""))
            if sid:
                discount_depth_selected_map["sub_" + sid] = {
                    "brand_avg_discount_depth": r.get("brand_avg_discount_depth"),
                    "media_avg_discount_depth": r.get("media_avg_discount_depth"),
                }

    first_cat = brand_cats[0]["category_id"] if brand_cats else None
    disc_cat = discount_cat or (str(first_cat) if first_cat else None)
    disc_sub = discount_subcat
    discount_selected = None
    if disc_sub:
        discount_selected = discount_depth_selected_map.get("sub_" + str(disc_sub))
    elif disc_cat:
        discount_selected = discount_depth_selected_map.get("cat_" + str(disc_cat))

    out = {
        "discount_depth": disc_depth,
        "discount_depth_selected": discount_selected,
        "discount_depth_selected_map": discount_depth_selected_map,
    }
    set_cached(key, out)
    resp = copy.deepcopy(out)
    return resp
