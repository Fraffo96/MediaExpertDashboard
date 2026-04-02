"""Path live BigQuery per MI/BC quando il periodo non è anno intero (precalc)."""
from __future__ import annotations

import asyncio

from app.db.queries.brand_comparison import (
    query_competitors_in_scope,
    query_discount_depth_brand_vs_competitor_all_categories,
    query_discount_depth_for_all_subcategories_bc,
    query_peak_events_brand_vs_competitor,
    query_promo_roi_brand_vs_competitor,
    query_promo_share_by_category_brand_vs_competitor,
    query_promo_share_by_subcategory_brand_vs_competitor,
    query_sales_by_brand_in_all_categories_bc_all_channels,
    query_sales_by_brand_in_all_subcategories_bc_all_channels,
)
from app.db.queries.market_intelligence import (
    query_brand_categories,
    query_brand_subcategories,
    query_discount_depth_brand_vs_media,
    query_discount_depth_for_all_subcategories,
    query_peak_events_brand_vs_media,
    query_promo_roi_brand_vs_media,
    query_promo_share_by_category_brand_vs_media,
    query_promo_share_by_subcategory_brand_vs_media,
    query_sales_by_brand_in_all_categories_all_channels,
    query_sales_by_brand_in_all_subcategories_all_channels,
    query_sales_pct_by_brand_prev_year_categories_all_channels,
    query_sales_pct_by_brand_prev_year_subcategories_all_channels,
    query_sales_value_by_category,
    query_sales_value_by_subcategory,
    query_sales_volume_by_category,
    query_sales_volume_by_subcategory,
)
from app.db.queries.market_intelligence.shared import CHANNELS
from app.db.queries.shared import query_available_years
from app.services.period_live import shift_period_years


def _merge_category_val_vol(val_rows: list, vol_rows: list) -> tuple[list, list]:
    vidx = {int(r["category_id"]): r for r in (vol_rows or []) if r.get("category_id") is not None}
    sales_value, sales_vol = [], []
    for r in val_rows or []:
        cid = r.get("category_id")
        vr = vidx.get(int(cid)) if cid is not None else None
        sales_value.append({
            "category_id": cid,
            "category_name": r.get("category_name"),
            "brand_gross_pln": r.get("brand_gross_pln"),
            "media_gross_pln": r.get("media_gross_pln"),
        })
        sales_vol.append({
            "category_id": cid,
            "category_name": r.get("category_name"),
            "brand_units": (vr or {}).get("brand_units") or 0,
            "media_units": (vr or {}).get("media_units") or 0,
        })
    return sales_value, sales_vol


def _merge_sub_val_vol(val_rows: list, vol_rows: list) -> tuple[list, list]:
    vidx = {int(r["category_id"]): r for r in (vol_rows or []) if r.get("category_id") is not None}
    sales_value, sales_vol = [], []
    for r in val_rows or []:
        cid = r.get("category_id")
        vr = vidx.get(int(cid)) if cid is not None else None
        sales_value.append({
            "category_id": cid,
            "category_name": r.get("category_name"),
            "brand_gross_pln": r.get("brand_gross_pln"),
            "media_gross_pln": r.get("media_gross_pln"),
        })
        sales_vol.append({
            "category_id": cid,
            "category_name": r.get("category_name"),
            "brand_units": (vr or {}).get("brand_units") or 0,
            "media_units": (vr or {}).get("media_units") or 0,
        })
    return sales_value, sales_vol


def _group_pie_by_channel(rows, has_channel=True):
    out_map = {ch: {} for ch in CHANNELS}
    for r in rows or []:
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
    for r in rows or []:
        ch = (r.get("channel") or "").strip()
        if ch not in out_map:
            out_map[ch] = {}
        cid = str(r.get("category_id", ""))
        bid = str(r.get("brand_id", ""))
        if cid and bid:
            if cid not in out_map[ch]:
                out_map[ch][cid] = {}
            out_map[ch][cid][bid] = r.get("pct_value_prev")
    return out_map


async def build_mi_base_live(ps: str, pe: str, brand_id: int) -> dict:
    bid = int(brand_id)
    brand_cats = await asyncio.to_thread(query_brand_categories, ps, pe, bid)
    brand_cats = list(brand_cats) if brand_cats else []
    brand_subcats_map = {str(c["category_id"]): [] for c in brand_cats}

    async def _subs_for(pid: int):
        return await asyncio.to_thread(query_brand_subcategories, ps, pe, bid, pid)

    if brand_cats:
        sub_tasks = [_subs_for(int(c["category_id"])) for c in brand_cats]
        sub_results = await asyncio.gather(*sub_tasks)
        for c, subs in zip(brand_cats, sub_results):
            pid = str(c["category_id"])
            for row in subs or []:
                brand_subcats_map[pid].append(
                    {"category_id": row["category_id"], "category_name": row.get("category_name")},
                )

    sub_ids = []
    for subs in brand_subcats_map.values():
        sub_ids.extend([str(s["category_id"]) for s in subs])
    cat_ids = [str(c["category_id"]) for c in brand_cats]
    available_years = await asyncio.to_thread(query_available_years)
    available_years = list(available_years) if available_years else []
    first_cat = brand_cats[0]["category_id"] if brand_cats else None
    cat_pie_id = str(first_cat) if first_cat else (cat_ids[0] if cat_ids else "")
    sub_pie_id = ""
    if first_cat and brand_subcats_map.get(str(first_cat)):
        subs = brand_subcats_map[str(first_cat)]
        sub_pie_id = str(subs[0]["category_id"]) if subs else ""

    return {
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
        "period_data_source": "live",
    }


async def get_mi_sales_live(ps: str, pe: str, brand_id: int, cat_ids: list, sub_ids: list, sub_cat_id):
    bid = int(brand_id)
    sub_cat_id = sub_cat_id or (cat_ids[0] if cat_ids else None)
    ps_prev, pe_prev = shift_period_years(ps, pe, -1)
    cat_ids_int = [int(c) for c in cat_ids if c] if cat_ids else []
    sub_ids_int = [int(s) for s in sub_ids if s] if sub_ids else []
    pid = int(sub_cat_id) if sub_cat_id and str(sub_cat_id).isdigit() else (int(sub_cat_id) if sub_cat_id else None)

    named_tasks = [
        ("val_vol_cat", asyncio.to_thread(query_sales_value_by_category, ps, pe, bid)),
        ("vol_cat", asyncio.to_thread(query_sales_volume_by_category, ps, pe, bid)),
    ]
    if pid:
        named_tasks.append(("val_vol_sub", asyncio.to_thread(query_sales_value_by_subcategory, ps, pe, bid, pid)))
        named_tasks.append(("vol_sub", asyncio.to_thread(query_sales_volume_by_subcategory, ps, pe, bid, pid)))
    if cat_ids_int:
        named_tasks.append(("cat_pie", asyncio.to_thread(query_sales_by_brand_in_all_categories_all_channels, ps, pe, cat_ids_int)))
        named_tasks.append(("prev_cat", asyncio.to_thread(query_sales_pct_by_brand_prev_year_categories_all_channels, ps_prev, pe_prev, cat_ids_int)))
    if sub_ids_int:
        named_tasks.append(("sub_pie", asyncio.to_thread(query_sales_by_brand_in_all_subcategories_all_channels, ps, pe, sub_ids_int)))
        named_tasks.append(("prev_sub", asyncio.to_thread(query_sales_pct_by_brand_prev_year_subcategories_all_channels, ps_prev, pe_prev, sub_ids_int)))

    results = await asyncio.gather(*[t for _, t in named_tasks])
    res_map = {k: list(v) if v else [] for (k, _), v in zip(named_tasks, results)}

    val_cat = res_map.get("val_vol_cat", [])
    vol_cat = res_map.get("vol_cat", [])
    sales_value, sales_vol = _merge_category_val_vol(val_cat, vol_cat)

    val_sub = res_map.get("val_vol_sub", [])
    vol_sub = res_map.get("vol_sub", [])
    sales_value_sub, sales_vol_sub = _merge_sub_val_vol(val_sub, vol_sub)

    cat_pie_all = res_map.get("cat_pie", [])
    sub_pie_all = res_map.get("sub_pie", [])
    prev_cat_all = res_map.get("prev_cat", [])
    prev_sub_all = res_map.get("prev_sub", [])

    category_pie_brands_map_channel = _group_pie_by_channel(cat_pie_all) if cat_pie_all else {ch: {} for ch in CHANNELS}
    subcategory_pie_brands_map_channel = _group_pie_by_channel(sub_pie_all) if sub_pie_all else {ch: {} for ch in CHANNELS}
    category_pie_brands_prev_map_channel = _group_prev_by_channel(prev_cat_all) if prev_cat_all else {ch: {} for ch in CHANNELS}
    subcategory_pie_brands_prev_map_channel = _group_prev_by_channel(prev_sub_all) if prev_sub_all else {ch: {} for ch in CHANNELS}

    return {
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


def _row_to_ps(r):
    return {
        "category_id": r.get("category_id"),
        "category_name": r.get("category_name") or "",
        "brand_promo_share_pct": r.get("brand_promo_share_pct"),
        "media_promo_share_pct": r.get("media_promo_share_pct"),
    }


def _row_to_roi(r):
    return {
        "promo_type": r.get("promo_type"),
        "brand_avg_roi": r.get("brand_avg_roi"),
        "media_avg_roi": r.get("media_avg_roi"),
    }


async def get_mi_promo_live(ps: str, pe: str, brand_id: int, brand_cats: list, brand_subcats_map: dict) -> dict:
    bid = int(brand_id)
    allowed = {c["category_id"] for c in (brand_cats or [])}

    async def cat_ch(ch: str):
        chv = ch if ch else None
        rows = await asyncio.to_thread(query_promo_share_by_category_brand_vs_media, ps, pe, bid, None, None, chv)
        out = []
        for r in rows or []:
            rr = dict(r)
            rr["channel"] = ch
            out.append(rr)
        return out

    tasks_cat = [cat_ch(ch) for ch in CHANNELS]
    cat_results = await asyncio.gather(*tasks_cat)
    ps_cat_all = []
    for block in cat_results:
        ps_cat_all.extend(block)

    async def sub_for_parent_ch(pid: int, ch: str):
        chv = ch if ch else None
        rows = await asyncio.to_thread(query_promo_share_by_subcategory_brand_vs_media, ps, pe, bid, pid, chv)
        out = []
        for r in rows or []:
            rr = dict(r)
            rr["channel"] = ch
            rr["parent_category_id"] = pid
            out.append(rr)
        return out

    sub_tasks = []
    for c in brand_cats or []:
        pid = int(c["category_id"])
        for ch in CHANNELS:
            sub_tasks.append(sub_for_parent_ch(pid, ch))
    sub_blocks = await asyncio.gather(*sub_tasks) if sub_tasks else []
    ps_sub_all = []
    for blk in sub_blocks:
        ps_sub_all.extend(blk)

    roi_tasks = [asyncio.to_thread(query_promo_roi_brand_vs_media, ps, pe, bid, c["category_id"], None) for c in (brand_cats or [])]
    roi_by_cat = await asyncio.gather(*roi_tasks) if roi_tasks else []
    roi_all = []
    for c, roi_rows in zip(brand_cats or [], roi_by_cat):
        cid = c["category_id"]
        for r in roi_rows or []:
            rr = dict(r)
            rr["category_id"] = cid
            roi_all.append(rr)

    promo_share_cat = [_row_to_ps(r) for r in ps_cat_all if (r.get("channel") or "").strip() == "" and r.get("category_id") in allowed]

    by_parent = {}
    for r in ps_sub_all:
        if (r.get("channel") or "").strip() != "":
            continue
        pid = str(r.get("parent_category_id", ""))
        by_parent.setdefault(pid, []).append(_row_to_ps(r))
    promo_share_by_subcategory_map = {str(c["category_id"]): by_parent.get(str(c["category_id"]), []) for c in (brand_cats or [])}

    roi_by_pt = {}
    for r in roi_all:
        pt = r.get("promo_type")
        roi_by_pt.setdefault(pt, {"brand": [], "media": []})
        roi_by_pt[pt]["brand"].append(r.get("brand_avg_roi") or 0)
        roi_by_pt[pt]["media"].append(r.get("media_avg_roi") or 0)
    roi = [
        {"promo_type": pt, "brand_avg_roi": sum(v["brand"]) / len(v["brand"]) if v["brand"] else 0, "media_avg_roi": sum(v["media"]) / len(v["media"]) if v["media"] else 0}
        for pt, v in roi_by_pt.items()
    ]
    roi.sort(key=lambda x: (x.get("media_avg_roi") or 0), reverse=True)

    def _norm_cat_id(raw):
        if raw is None:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    def _roi_for_cat(cat_id):
        cid = _norm_cat_id(cat_id)
        if cid is None:
            return []
        rows = [r for r in roi_all if _norm_cat_id(r.get("category_id")) == cid]
        by_pt = {}
        for r in rows:
            by_pt[r.get("promo_type")] = _row_to_roi(r)
        return list(by_pt.values())

    promo_roi_map = {"": roi}
    for c in brand_cats or []:
        promo_roi_map["cat_" + str(c["category_id"])] = _roi_for_cat(c["category_id"])
    for pid, subs in (brand_subcats_map or {}).items():
        cat_roi = _roi_for_cat(int(pid))
        for s in subs:
            sid = s.get("category_id")
            if sid is not None:
                promo_roi_map["sub_" + str(sid)] = cat_roi or roi

    def _by_channel(rows):
        ch_map = {}
        for r in rows:
            ch = (r.get("channel") or "").strip()
            ch_map.setdefault(ch, []).append(r)
        return ch_map

    ps_cat_by_ch = _by_channel(ps_cat_all)
    ps_sub_by_ch = _by_channel(ps_sub_all)
    promo_share_by_category_channel = {ch: [_row_to_ps(r) for r in ps_cat_by_ch.get(ch, []) if r.get("category_id") in allowed] for ch in CHANNELS}
    promo_share_by_subcategory_map_channel = {}
    for ch in CHANNELS:
        sub_rows = ps_sub_by_ch.get(ch, [])
        by_parent_ch = {}
        for r in sub_rows:
            pid = str(r.get("parent_category_id", ""))
            by_parent_ch.setdefault(pid, []).append(_row_to_ps(r))
        promo_share_by_subcategory_map_channel[ch] = {str(c["category_id"]): by_parent_ch.get(str(c["category_id"]), []) for c in (brand_cats or [])}

    return {
        "promo_share_by_category": promo_share_cat,
        "promo_share_by_subcategory_map": promo_share_by_subcategory_map,
        "promo_roi": roi,
        "promo_roi_map": promo_roi_map,
        "promo_share_by_category_channel": promo_share_by_category_channel,
        "promo_share_by_subcategory_map_channel": promo_share_by_subcategory_map_channel,
    }


async def get_mi_peak_live(ps: str, pe: str, brand_id: int, brand_cats: list, brand_subcats_map: dict) -> dict:
    bid = int(brand_id)

    async def one_peak(cat_f, sub_f, ch_f):
        chv = ch_f if ch_f else None
        cat = None
        subcat = None
        if sub_f is not None and str(sub_f).strip().isdigit() and int(sub_f) >= 100:
            subcat = int(sub_f)
        elif cat_f is not None and str(cat_f).strip().isdigit():
            ci = int(cat_f)
            if 1 <= ci <= 10:
                cat = ci
        return await asyncio.to_thread(query_peak_events_brand_vs_media, ps, pe, bid, cat, subcat, chv)

    async def peak_scope(cat_f, sub_f, ch_f):
        rows = await one_peak(cat_f, sub_f, ch_f)
        return [{"peak_event": r.get("peak_event"), "brand_pct_of_annual": float(r.get("brand_pct_of_annual") or 0), "media_pct_of_annual": float(r.get("media_pct_of_annual") or 0)} for r in (rows or [])]

    peak = await peak_scope(None, None, "")
    peak_events_map = {"": peak}
    for c in brand_cats or []:
        peak_events_map["cat_" + str(c["category_id"])] = await peak_scope(c["category_id"], None, "")
    for pid, subs in (brand_subcats_map or {}).items():
        for s in subs:
            sid = s.get("category_id")
            if sid is not None:
                peak_events_map["sub_" + str(sid)] = await peak_scope(None, sid, "")

    peak_events_map_channel = {}
    for ch in CHANNELS:
        ch_val = ch if ch else ""
        peak_map = {"": await peak_scope(None, None, ch_val)}
        for c in brand_cats or []:
            peak_map["cat_" + str(c["category_id"])] = await peak_scope(c["category_id"], None, ch_val)
        for pid, subs in (brand_subcats_map or {}).items():
            for s in subs:
                sid = s.get("category_id")
                if sid is not None:
                    peak_map["sub_" + str(sid)] = await peak_scope(None, sid, ch_val)
        peak_events_map_channel[ch] = peak_map

    return {
        "peak_events": peak,
        "peak_events_map": peak_events_map,
        "peak_events_map_channel": peak_events_map_channel,
    }


async def get_mi_discount_live(ps: str, pe: str, brand_id: int, brand_cats: list, sub_ids: list, discount_cat=None, discount_subcat=None):
    bid = int(brand_id)
    disc_depth = await asyncio.to_thread(query_discount_depth_brand_vs_media, ps, pe, bid)
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
        dd_sub = await asyncio.to_thread(query_discount_depth_for_all_subcategories, ps, pe, bid, sub_ids_int)
        for r in dd_sub or []:
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
    return {
        "discount_depth": disc_depth,
        "discount_depth_selected": discount_selected,
        "discount_depth_selected_map": discount_depth_selected_map,
    }


def intersect_brand_category_trees_live(ps: str, pe: str, brand_id: int, competitor_id: int) -> tuple[list, dict]:
    cats_a = query_brand_categories(ps, pe, brand_id) or []
    cats_b = query_brand_categories(ps, pe, competitor_id) or []
    ids_b = {c["category_id"] for c in cats_b}
    brand_cats = [c for c in cats_a if c["category_id"] in ids_b]
    if not brand_cats:
        return [], {}
    brand_subcats_map = {str(c["category_id"]): [] for c in brand_cats}
    for c in brand_cats:
        pid = int(c["category_id"])
        subs_a = query_brand_subcategories(ps, pe, brand_id, pid) or []
        subs_b = query_brand_subcategories(ps, pe, competitor_id, pid) or []
        ids_sb = {s["category_id"] for s in subs_b}
        for s in subs_a:
            if s["category_id"] in ids_sb:
                brand_subcats_map[str(pid)].append({"category_id": s["category_id"], "category_name": s.get("category_name")})
    return brand_cats, brand_subcats_map


async def build_bc_base_live(ps: str, pe: str, brand_id: int, competitor_id=None) -> dict:
    bid = int(brand_id)
    cid_opt = int(competitor_id) if competitor_id is not None and str(competitor_id).strip() else None
    if cid_opt is not None:
        brand_cats, brand_subcats_map = await asyncio.to_thread(intersect_brand_category_trees_live, ps, pe, bid, cid_opt)
    else:
        base = await build_mi_base_live(ps, pe, bid)
        brand_cats = base["brand_categories"]
        brand_subcats_map = base["brand_subcategories"]
    sub_ids = []
    for subs in brand_subcats_map.values():
        sub_ids.extend([str(s["category_id"]) for s in subs])
    cat_ids = [str(c["category_id"]) for c in brand_cats]
    available_years = await asyncio.to_thread(query_available_years)
    available_years = list(available_years) if available_years else []
    competitors = await asyncio.to_thread(query_competitors_in_scope, ps, pe, bid)
    first_cat = brand_cats[0]["category_id"] if brand_cats else None
    cat_pie_id = str(first_cat) if first_cat else (cat_ids[0] if cat_ids else "")
    sub_pie_id = ""
    if first_cat and brand_subcats_map.get(str(first_cat)):
        subs = brand_subcats_map[str(first_cat)]
        sub_pie_id = str(subs[0]["category_id"]) if subs else ""
    return {
        "available_channels": [{"id": "", "name": "All"}, {"id": "web", "name": "Web"}, {"id": "app", "name": "App"}, {"id": "store", "name": "Store"}],
        "brand_categories": brand_cats,
        "brand_subcategories": brand_subcats_map,
        "first_category_id": first_cat,
        "cat_ids": cat_ids,
        "sub_ids": sub_ids,
        "available_years": available_years,
        "competitors": list(competitors) if competitors else [],
        "category_pie_id": cat_pie_id,
        "subcategory_pie_id": sub_pie_id,
        "subcategory_category_id": first_cat,
        "period_data_source": "live",
    }


async def get_bc_sales_live(ps: str, pe: str, brand_id: int, competitor_id: int, cat_ids: list, sub_ids: list, sub_cat_id):
    bid, cid = int(brand_id), int(competitor_id)
    sub_cat_id = sub_cat_id or (cat_ids[0] if cat_ids else None)
    cat_ids_int = [int(c) for c in cat_ids if c] if cat_ids else []
    sub_ids_int = [int(s) for s in sub_ids if s] if sub_ids else []
    ps_prev, pe_prev = shift_period_years(ps, pe, -1)

    async def cat_bundle():
        if not cat_ids_int:
            return [], []
        pie, prev = await asyncio.gather(
            asyncio.to_thread(
                query_sales_by_brand_in_all_categories_bc_all_channels, ps, pe, cat_ids_int, bid, cid
            ),
            asyncio.to_thread(
                query_sales_pct_by_brand_prev_year_categories_all_channels, ps_prev, pe_prev, cat_ids_int
            ),
        )
        return list(pie or []), list(prev or [])

    async def sub_bundle():
        if not sub_ids_int:
            return [], []
        pie, prev = await asyncio.gather(
            asyncio.to_thread(
                query_sales_by_brand_in_all_subcategories_bc_all_channels, ps, pe, sub_ids_int, bid, cid
            ),
            asyncio.to_thread(
                query_sales_pct_by_brand_prev_year_subcategories_all_channels, ps_prev, pe_prev, sub_ids_int
            ),
        )
        return list(pie or []), list(prev or [])

    (cat_pie_all, prev_cat_all), (sub_pie_all, prev_sub_all) = await asyncio.gather(cat_bundle(), sub_bundle())

    def _group_bc(rows):
        out = {ch: {} for ch in CHANNELS}
        for r in rows:
            ch = (r.get("channel") or "").strip()
            if ch not in out:
                out[ch] = {}
            cidk = str(r.get("category_id", ""))
            if cidk not in out[ch]:
                out[ch][cidk] = []
            out[ch][cidk].append({k: v for k, v in r.items() if k != "channel"})
        return out

    return {
        "sales_value": [],
        "sales_volume": [],
        "sales_value_subcategory": [],
        "sales_volume_subcategory": [],
        "subcategory_category_id": sub_cat_id,
        "category_pie_brands_map_channel": _group_bc(cat_pie_all),
        "subcategory_pie_brands_map_channel": _group_bc(sub_pie_all),
        "category_pie_brands_prev_map_channel": _group_prev_by_channel(prev_cat_all) if prev_cat_all else {c: {} for c in CHANNELS},
        "subcategory_pie_brands_prev_map_channel": _group_prev_by_channel(prev_sub_all) if prev_sub_all else {c: {} for c in CHANNELS},
    }


async def get_bc_promo_live(ps: str, pe: str, brand_id: int, competitor_id: int, brand_cats: list, brand_subcats_map: dict) -> dict:
    bid, cid = int(brand_id), int(competitor_id)
    allowed = {c["category_id"] for c in (brand_cats or [])}

    async def cat_ch(ch: str):
        chv = ch if ch else None
        rows = await asyncio.to_thread(query_promo_share_by_category_brand_vs_competitor, ps, pe, bid, cid, None, None, chv)
        out = []
        for r in rows or []:
            rr = dict(r)
            rr["channel"] = ch
            out.append(rr)
        return out

    cat_results = await asyncio.gather(*[cat_ch(ch) for ch in CHANNELS])
    ps_cat_all = []
    for block in cat_results:
        ps_cat_all.extend(block)

    async def sub_pc(pid: int, ch: str):
        chv = ch if ch else None
        rows = await asyncio.to_thread(query_promo_share_by_subcategory_brand_vs_competitor, ps, pe, bid, cid, pid, chv)
        out = []
        for r in rows or []:
            rr = dict(r)
            rr["channel"] = ch
            rr["parent_category_id"] = pid
            out.append(rr)
        return out

    st = []
    for c in brand_cats or []:
        pid = int(c["category_id"])
        for ch in CHANNELS:
            st.append(sub_pc(pid, ch))
    sub_blocks = await asyncio.gather(*st) if st else []
    ps_sub_all = []
    for blk in sub_blocks:
        ps_sub_all.extend(blk)

    roi_all = []
    for c in brand_cats or []:
        cidp = c["category_id"]
        rows = await asyncio.to_thread(query_promo_roi_brand_vs_competitor, ps, pe, bid, cid, cidp, None)
        for r in rows or []:
            rr = dict(r)
            rr["category_id"] = cidp
            roi_all.append(rr)

    promo_share_cat = [_row_to_ps(r) for r in ps_cat_all if (r.get("channel") or "").strip() == "" and r.get("category_id") in allowed]
    by_parent = {}
    for r in ps_sub_all:
        if (r.get("channel") or "").strip() != "":
            continue
        pid = str(r.get("parent_category_id", ""))
        by_parent.setdefault(pid, []).append(_row_to_ps(r))
    promo_share_by_subcategory_map = {str(c["category_id"]): by_parent.get(str(c["category_id"]), []) for c in (brand_cats or [])}

    roi_by_pt = {}
    for r in roi_all:
        pt = r.get("promo_type")
        roi_by_pt.setdefault(pt, {"brand": [], "media": []})
        roi_by_pt[pt]["brand"].append(r.get("brand_avg_roi") or 0)
        roi_by_pt[pt]["media"].append(r.get("media_avg_roi") or 0)
    roi = [
        {"promo_type": pt, "brand_avg_roi": sum(v["brand"]) / len(v["brand"]) if v["brand"] else 0, "media_avg_roi": sum(v["media"]) / len(v["media"]) if v["media"] else 0}
        for pt, v in roi_by_pt.items()
    ]
    roi.sort(key=lambda x: (x.get("media_avg_roi") or 0), reverse=True)

    def _roi_for_cat(cat_id):
        rows = [r for r in roi_all if r.get("category_id") == cat_id]
        by_pt = {r.get("promo_type"): _row_to_roi(r) for r in rows}
        return list(by_pt.values())

    promo_roi_map = {"": roi}
    for c in brand_cats or []:
        promo_roi_map["cat_" + str(c["category_id"])] = _roi_for_cat(c["category_id"])
    for pid, subs in (brand_subcats_map or {}).items():
        cat_roi = _roi_for_cat(int(pid))
        for s in subs:
            sid = s.get("category_id")
            if sid is not None:
                promo_roi_map["sub_" + str(sid)] = cat_roi or roi

    def _by_channel(rows):
        ch_map = {}
        for r in rows:
            ch = (r.get("channel") or "").strip()
            ch_map.setdefault(ch, []).append(r)
        return ch_map

    ps_cat_by_ch = _by_channel(ps_cat_all)
    ps_sub_by_ch = _by_channel(ps_sub_all)
    promo_share_by_category_channel = {ch: [_row_to_ps(r) for r in ps_cat_by_ch.get(ch, []) if r.get("category_id") in allowed] for ch in CHANNELS}
    promo_share_by_subcategory_map_channel = {}
    for ch in CHANNELS:
        sub_rows = ps_sub_by_ch.get(ch, [])
        by_parent_ch = {}
        for r in sub_rows:
            pid = str(r.get("parent_category_id", ""))
            by_parent_ch.setdefault(pid, []).append(_row_to_ps(r))
        promo_share_by_subcategory_map_channel[ch] = {str(c["category_id"]): by_parent_ch.get(str(c["category_id"]), []) for c in (brand_cats or [])}

    return {
        "promo_share_by_category": promo_share_cat,
        "promo_share_by_subcategory_map": promo_share_by_subcategory_map,
        "promo_roi": roi,
        "promo_roi_map": promo_roi_map,
        "promo_share_by_category_channel": promo_share_by_category_channel,
        "promo_share_by_subcategory_map_channel": promo_share_by_subcategory_map_channel,
    }


async def get_bc_peak_live(ps: str, pe: str, brand_id: int, competitor_id: int, brand_cats: list, brand_subcats_map: dict) -> dict:
    bid, cid = int(brand_id), int(competitor_id)

    async def peak_scope(cat_f, sub_f, ch_f):
        chv = ch_f if ch_f else None
        cat = None
        subcat = None
        if sub_f is not None and str(sub_f).strip().isdigit() and int(sub_f) >= 100:
            subcat = int(sub_f)
        elif cat_f is not None and str(cat_f).strip().isdigit():
            ci = int(cat_f)
            if 1 <= ci <= 10:
                cat = ci
        rows = await asyncio.to_thread(query_peak_events_brand_vs_competitor, ps, pe, bid, cid, cat, subcat, chv)
        return [{"peak_event": r.get("peak_event"), "brand_pct_of_annual": float(r.get("brand_pct_of_annual") or 0), "media_pct_of_annual": float(r.get("media_pct_of_annual") or 0)} for r in (rows or [])]

    peak = await peak_scope(None, None, "")
    peak_events_map = {"": peak}
    for c in brand_cats or []:
        peak_events_map["cat_" + str(c["category_id"])] = await peak_scope(c["category_id"], None, "")
    for pid, subs in (brand_subcats_map or {}).items():
        for s in subs:
            sid = s.get("category_id")
            if sid is not None:
                peak_events_map["sub_" + str(sid)] = await peak_scope(None, sid, "")

    peak_events_map_channel = {}
    for ch in CHANNELS:
        ch_val = ch if ch else ""
        peak_map = {"": await peak_scope(None, None, ch_val)}
        for c in brand_cats or []:
            peak_map["cat_" + str(c["category_id"])] = await peak_scope(c["category_id"], None, ch_val)
        for pid, subs in (brand_subcats_map or {}).items():
            for s in subs:
                sid = s.get("category_id")
                if sid is not None:
                    peak_map["sub_" + str(sid)] = await peak_scope(None, sid, ch_val)
        peak_events_map_channel[ch] = peak_map

    return {
        "peak_events": peak,
        "peak_events_map": peak_events_map,
        "peak_events_map_channel": peak_events_map_channel,
    }


async def get_bc_discount_live(ps: str, pe: str, brand_id: int, competitor_id: int, brand_cats: list, sub_ids: list, discount_cat=None, discount_subcat=None):
    bid, cid = int(brand_id), int(competitor_id)
    disc_depth = await asyncio.to_thread(query_discount_depth_brand_vs_competitor_all_categories, ps, pe, bid, cid)
    disc_depth = list(disc_depth) if disc_depth else []
    discount_depth_selected_map = {}
    for r in disc_depth:
        cidk = str(r.get("category_id", ""))
        if cidk:
            discount_depth_selected_map["cat_" + cidk] = {
                "brand_avg_discount_depth": r.get("brand_avg_discount_depth"),
                "media_avg_discount_depth": r.get("media_avg_discount_depth"),
            }
    if sub_ids:
        sub_ids_int = [int(s) for s in sub_ids if s]
        dd_sub = await asyncio.to_thread(query_discount_depth_for_all_subcategories_bc, ps, pe, bid, cid, sub_ids_int)
        for r in dd_sub or []:
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
    return {
        "discount_depth": disc_depth,
        "discount_depth_selected": discount_selected,
        "discount_depth_selected_map": discount_depth_selected_map,
    }


async def get_bc_competitors_live(ps: str, pe: str, brand_id: int) -> dict:
    rows = await asyncio.to_thread(query_competitors_in_scope, ps, pe, int(brand_id))
    seen = {}
    for r in rows or []:
        bid = r.get("brand_id")
        if bid is not None and bid not in seen:
            seen[bid] = {"brand_id": bid, "brand_name": r.get("brand_name", "")}
    competitors = sorted(seen.values(), key=lambda x: (x.get("brand_name") or "").lower())
    return {"competitors": competitors}
