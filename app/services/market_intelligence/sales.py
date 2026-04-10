"""Market Intelligence: sales value/volume e pie charts."""
import asyncio
import copy

from app.db.queries.market_intelligence.shared import CHANNELS
from app.db.queries.precalc import (
    is_full_year_period,
    query_sales_by_brand_in_all_categories_all_channels_from_precalc,
    query_sales_by_brand_in_all_subcategories_all_channels_from_precalc,
    query_sales_pct_by_brand_prev_year_categories_all_channels_from_precalc,
    query_sales_pct_by_brand_prev_year_subcategories_all_channels_from_precalc,
    query_sales_value_volume_by_category_from_precalc,
    query_sales_value_volume_by_subcategory_from_precalc,
)
from app.services._cache import cache_key, get_cached, set_cached
from app.services.mi_bc_live import get_mi_sales_live


def _use_precalc_sales(ps, pe, brand_id, cat_ids, sub_ids, sub_cat_id):
    """True se usare precalc per sales (full year + brand_id valido)."""
    return is_full_year_period(ps, pe) and brand_id and str(brand_id).strip()


async def get_mi_sales(ps, pe, brand_id, cat_ids, sub_ids, sub_cat_id=None):
    """Sales value/volume, category/subcategory pie, prev year."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    key = cache_key(
        "mi_sales",
        ps=ps,
        pe=pe,
        brand=brand_id,
        cat_ids=",".join(cat_ids) if cat_ids else "",
        sub_ids=",".join(sub_ids) if sub_ids else "",
        sub_cat=sub_cat_id or "",
    )
    cached = get_cached(key)
    if cached is not None:
        return copy.deepcopy(cached)

    sub_cat_id = sub_cat_id or (cat_ids[0] if cat_ids else None)
    if not _use_precalc_sales(ps, pe, brand_id, cat_ids, sub_ids, sub_cat_id):
        out = await get_mi_sales_live(ps, pe, int(brand_id), cat_ids, sub_ids, sub_cat_id)
        set_cached(key, out)
        return copy.deepcopy(out)
    year = int(ps[:4])
    bid = int(brand_id)

    pid = int(sub_cat_id) if sub_cat_id and isinstance(sub_cat_id, str) and sub_cat_id.isdigit() else (sub_cat_id if sub_cat_id else None)
    cat_ids_int = [int(c) for c in cat_ids if c] if cat_ids else []
    sub_ids_int = [int(s) for s in sub_ids if s] if sub_ids else []

    named_tasks = [
        ("val_vol_cat", asyncio.to_thread(query_sales_value_volume_by_category_from_precalc, year, bid)),
    ]
    if pid:
        named_tasks.append(("val_vol_sub", asyncio.to_thread(query_sales_value_volume_by_subcategory_from_precalc, year, bid, pid)))
    if cat_ids_int:
        named_tasks.append(("cat_pie", asyncio.to_thread(query_sales_by_brand_in_all_categories_all_channels_from_precalc, year, cat_ids_int)))
    if sub_ids_int:
        named_tasks.append(("sub_pie", asyncio.to_thread(query_sales_by_brand_in_all_subcategories_all_channels_from_precalc, year, sub_ids_int)))
    if cat_ids_int:
        named_tasks.append(("prev_cat", asyncio.to_thread(query_sales_pct_by_brand_prev_year_categories_all_channels_from_precalc, year - 1, cat_ids_int)))
    if sub_ids_int:
        named_tasks.append(("prev_sub", asyncio.to_thread(query_sales_pct_by_brand_prev_year_subcategories_all_channels_from_precalc, year - 1, sub_ids_int)))

    results = await asyncio.gather(*[t for _, t in named_tasks])
    res_map = {k: list(v) if v else [] for (k, _), v in zip(named_tasks, results)}

    val_vol_cat = res_map.get("val_vol_cat", [])
    val_vol_sub = res_map.get("val_vol_sub", [])
    cat_pie_all = res_map.get("cat_pie", [])
    sub_pie_all = res_map.get("sub_pie", [])
    prev_cat_all = res_map.get("prev_cat", [])
    prev_sub_all = res_map.get("prev_sub", [])

    sales_value = [{"category_id": r["category_id"], "category_name": r["category_name"], "brand_gross_pln": r["brand_gross_pln"], "media_gross_pln": r["media_gross_pln"]} for r in val_vol_cat]
    sales_vol = [{"category_id": r["category_id"], "category_name": r["category_name"], "brand_units": r["brand_units"], "media_units": r["media_units"]} for r in val_vol_cat]
    sales_value_sub = [{"category_id": r["category_id"], "category_name": r["category_name"], "brand_gross_pln": r["brand_gross_pln"], "media_gross_pln": r["media_gross_pln"]} for r in val_vol_sub]
    sales_vol_sub = [{"category_id": r["category_id"], "category_name": r["category_name"], "brand_units": r["brand_units"], "media_units": r["media_units"]} for r in val_vol_sub]

    def _group_pie_by_channel(rows, has_channel=True):
        out_map = {ch: {} for ch in CHANNELS}
        for r in rows:
            ch = (r.get("channel") or "").strip() if has_channel else ""
            if ch not in out_map:
                out_map[ch] = {}
            cid = str(r.get("category_id", ""))
            if cid not in out_map[ch]:
                out_map[ch][cid] = []
            row = {k: v for k, v in r.items() if k != "channel"}
            out_map[ch][cid].append(row)
        return out_map

    def _group_prev_by_channel(rows):
        out_map = {ch: {} for ch in CHANNELS}
        for r in rows:
            ch = (r.get("channel") or "").strip()
            if ch not in out_map:
                out_map[ch] = {}
            cid = str(r.get("category_id", ""))
            bid_row = str(r.get("brand_id", ""))
            if cid and bid_row:
                if cid not in out_map[ch]:
                    out_map[ch][cid] = {}
                out_map[ch][cid][bid_row] = r.get("pct_value_prev")
        return out_map

    category_pie_brands_map_channel = _group_pie_by_channel(cat_pie_all) if cat_pie_all else {ch: {} for ch in CHANNELS}
    subcategory_pie_brands_map_channel = _group_pie_by_channel(sub_pie_all) if sub_pie_all else {ch: {} for ch in CHANNELS}
    category_pie_brands_prev_map_channel = _group_prev_by_channel(prev_cat_all) if prev_cat_all else {ch: {} for ch in CHANNELS}
    subcategory_pie_brands_prev_map_channel = _group_prev_by_channel(prev_sub_all) if prev_sub_all else {ch: {} for ch in CHANNELS}

    if sales_value or sales_vol:
        out = {
            "sales_value": sales_value,
            "sales_volume": sales_vol,
            "sales_value_subcategory": sales_value_sub,
            "sales_volume_subcategory": sales_vol_sub,
            "subcategory_category_id": sub_cat_id,
            "category_pie_brands_map_channel": category_pie_brands_map_channel,
            "subcategory_pie_brands_map_channel": subcategory_pie_brands_map_channel,
            "category_pie_brands_prev_map_channel": category_pie_brands_prev_map_channel,
            "subcategory_pie_brands_prev_map_channel": subcategory_pie_brands_prev_map_channel,
        }
        set_cached(key, out)
        return copy.deepcopy(out)
    out = {
        "sales_value": sales_value,
        "sales_volume": sales_vol,
        "sales_value_subcategory": [],
        "sales_volume_subcategory": [],
        "subcategory_category_id": sub_cat_id,
        "category_pie_brands_map_channel": {ch: {} for ch in CHANNELS},
        "subcategory_pie_brands_map_channel": {ch: {} for ch in CHANNELS},
        "category_pie_brands_prev_map_channel": {ch: {} for ch in CHANNELS},
        "subcategory_pie_brands_prev_map_channel": {ch: {} for ch in CHANNELS},
    }
    set_cached(key, out)
    return copy.deepcopy(out)
