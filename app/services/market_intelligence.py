"""Market Intelligence: brand vs media category/subcategory benchmarks.
Split into 5 endpoints for progressive loading: base, sales, promo, peak, discount.
Solo tabelle precalcolate (precalc_*). Periodo deve essere anno intero (YYYY-01-01 a YYYY-12-31).
"""
PRECALC_ONLY_ERR = "Precalc tables only. Use full year period (YYYY-01-01 to YYYY-12-31)."
import asyncio
import copy
from decimal import Decimal

from app.db.queries.market_intelligence.shared import CHANNELS
from app.db.queries.precalc import (
    get_multi_year_full_years,
    is_full_year_period,
    query_brand_all_subcategories_from_precalc,
    query_brand_categories_from_precalc,
    query_incremental_yoy_vendite_multi_year_from_precalc,
    query_sales_value_volume_by_category_from_precalc,
    query_sales_value_volume_by_subcategory_from_precalc,
    query_sales_by_brand_in_all_categories_all_channels_from_precalc,
    query_sales_by_brand_in_all_subcategories_all_channels_from_precalc,
    query_sales_pct_by_brand_prev_year_categories_all_channels_from_precalc,
    query_sales_pct_by_brand_prev_year_subcategories_all_channels_from_precalc,
    query_promo_share_mi_all_channels_from_precalc,
    query_promo_share_sub_mi_all_channels_from_precalc,
    query_promo_roi_mi_all_from_precalc,
    query_peak_mi_raw_all_from_precalc,
    query_discount_depth_brand_vs_media_from_precalc,
    query_discount_depth_for_all_subcategories_from_precalc,
)
from app.db.queries.shared import query_available_years
from app.services._cache import cache_key, get_cached, set_cached, TTL_LONG
from app.services.mi_bc_live import (
    build_mi_base_live,
    get_mi_discount_live,
    get_mi_peak_live,
    get_mi_promo_live,
    get_mi_sales_live,
)

# Parallelismo caricamento anni (all-years). 4 ≈ tipici 4 anni di seed senza appiattire troppo BQ.
_MI_YEAR_LOAD_SEM = asyncio.Semaphore(4)
# Limita job BigQuery simultanei per incremental YoY (~80 scope): evita coda lato BQ/client.
_MI_INCR_SCOPE_SEM = asyncio.Semaphore(28)


async def get_mi_available_years_payload() -> dict:
    """Solo elenco anni disponibili — chiamata leggera per primo paint MI."""
    yrs = await asyncio.to_thread(query_available_years)
    ys = [str(y) for y in (list(yrs) if yrs else [])]
    return {"available_years": ys}


async def get_mi_incremental_yoy_api(ps: str, pe: str, brand_id: int) -> dict:
    """Incremental YoY multi-anno: richiede ps..pe come anni interi 01-01 … 12-31."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    rng = get_multi_year_full_years(ps, pe)
    if rng is None:
        return {"error": PRECALC_ONLY_ERR}
    y_ref = rng[-1]
    base = await get_mi_base(f"{y_ref}-01-01", f"{y_ref}-12-31", brand_id)
    if base.get("error"):
        return base
    return await get_mi_incremental_yoy(
        ps, pe, brand_id, base.get("brand_categories") or [], base.get("brand_subcategories") or {}
    )


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

    available_years = await asyncio.to_thread(query_available_years)
    available_years = list(available_years) if available_years else []

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

    # Query consolidate: 1 value+volume, 1 sub, 4 pie+prev (invece di 20+)
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
            bid = str(r.get("brand_id", ""))
            if cid and bid:
                if cid not in out_map[ch]:
                    out_map[ch][cid] = {}
                out_map[ch][cid][bid] = r.get("pct_value_prev")
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


def _row_to_ps(r):
    return {
        "category_id": r.get("category_id"),
        "category_name": r.get("category_name"),
        "brand_promo_share_pct": r.get("brand_promo_share_pct"),
        "media_promo_share_pct": r.get("media_promo_share_pct"),
    }


def _row_to_roi(r):
    return {
        "promo_type": r.get("promo_type"),
        "brand_avg_roi": r.get("brand_avg_roi"),
        "media_avg_roi": r.get("media_avg_roi"),
    }


async def get_mi_promo(ps, pe, brand_id, brand_cats, brand_subcats_map):
    """Promo share and ROI: brand vs media. 3 query consolidate invece di 100+."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    key = cache_key("mi_promo_v4_weighted_scope", ps=ps, pe=pe, brand=brand_id)
    cached = get_cached(key)
    if cached is not None:
        return cached

    if not is_full_year_period(ps, pe):
        out = await get_mi_promo_live(ps, pe, int(brand_id), brand_cats, brand_subcats_map)
        set_cached(key, out)
        return out

    year = int(ps[:4])
    bid = int(brand_id)

    ps_cat_all, ps_sub_all, roi_all = await asyncio.gather(
        asyncio.to_thread(query_promo_share_mi_all_channels_from_precalc, year, bid),
        asyncio.to_thread(query_promo_share_sub_mi_all_channels_from_precalc, year, bid),
        asyncio.to_thread(query_promo_roi_mi_all_from_precalc, year, bid),
    )
    ps_cat_all = list(ps_cat_all) if ps_cat_all else []
    ps_sub_all = list(ps_sub_all) if ps_sub_all else []
    roi_all = list(roi_all) if roi_all else []

    promo_share_cat = [_row_to_ps(r) for r in ps_cat_all if (r.get("channel") or "").strip() == ""]
    by_parent = {}
    for r in ps_sub_all:
        if (r.get("channel") or "").strip() != "":
            continue
        pid = str(r.get("parent_category_id", ""))
        if pid not in by_parent:
            by_parent[pid] = []
        by_parent[pid].append(_row_to_ps(r))
    promo_share_by_subcategory_map = {str(c["category_id"]): by_parent.get(str(c["category_id"]), []) for c in brand_cats}

    roi_by_pt = {}
    for r in roi_all:
        pt = r.get("promo_type")
        if pt not in roi_by_pt:
            roi_by_pt[pt] = {"brand": [], "media": []}
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
            pt = r.get("promo_type")
            by_pt[pt] = _row_to_roi(r)
        return list(by_pt.values())

    promo_roi_map = {"": roi}
    for c in brand_cats:
        cid = _norm_cat_id(c.get("category_id"))
        if cid is not None:
            promo_roi_map["cat_" + str(cid)] = _roi_for_cat(cid)
    for pid, subs in brand_subcats_map.items():
        pid_int = _norm_cat_id(pid)
        for s in subs:
            sid = s.get("category_id")
            if sid is not None:
                sub_rows = _roi_for_cat(sid) or (_roi_for_cat(pid_int) if pid_int is not None else []) or roi
                promo_roi_map["sub_" + str(sid)] = sub_rows

    def _by_channel(rows):
        ch_map = {}
        for r in rows:
            ch = (r.get("channel") or "").strip()
            if ch not in ch_map:
                ch_map[ch] = []
            ch_map[ch].append(r)
        return ch_map

    ps_cat_by_ch = _by_channel(ps_cat_all)
    ps_sub_by_ch = _by_channel(ps_sub_all)
    promo_share_by_category_channel = {ch: [_row_to_ps(r) for r in ps_cat_by_ch.get(ch, [])] for ch in CHANNELS}
    promo_share_by_subcategory_map_channel = {}
    for ch in CHANNELS:
        sub_rows = ps_sub_by_ch.get(ch, [])
        by_parent_ch = {}
        for r in sub_rows:
            pid = str(r.get("parent_category_id", ""))
            if pid not in by_parent_ch:
                by_parent_ch[pid] = []
            by_parent_ch[pid].append(_row_to_ps(r))
        promo_share_by_subcategory_map_channel[ch] = {str(c["category_id"]): by_parent_ch.get(str(c["category_id"]), []) for c in brand_cats}

    out = {
        "promo_share_by_category": promo_share_cat,
        "promo_share_by_subcategory_map": promo_share_by_subcategory_map,
        "promo_roi": roi,
        "promo_roi_map": promo_roi_map,
        "promo_share_by_category_channel": promo_share_by_category_channel,
        "promo_share_by_subcategory_map_channel": promo_share_by_subcategory_map_channel,
    }
    set_cached(key, out)
    return out


def _build_peak_mi_from_raw(
    peak_brand,
    annual_brand,
    peak_competitors,
    annual_competitors,
    cat_filter,
    sub_filter,
    ch_filter,
):
    """Costruisce peak MI: brand vs media = media aritmetica dei % annui dei singoli competitor."""
    want_ch = (ch_filter or "").strip()

    def _match(r):
        if (r.get("channel") or "").strip() != want_ch:
            return False
        if cat_filter is not None and 1 <= cat_filter <= 10:
            if r.get("parent_category_id") != cat_filter:
                return False
        if sub_filter is not None and sub_filter >= 100:
            if r.get("category_id") != sub_filter:
                return False
        return True

    def _num(v):
        return float(v) if v is not None else 0

    annual_b = sum(_num(r.get("annual_gross")) for r in annual_brand if _match(r))
    peak_by_event_brand = {}
    for r in peak_brand:
        if not _match(r):
            continue
        ev = r.get("peak_event")
        if ev:
            peak_by_event_brand[ev] = peak_by_event_brand.get(ev, 0) + _num(r.get("gross_pln"))

    comp_ids = set()
    for r in annual_competitors:
        if _match(r):
            bid = r.get("brand_id")
            if bid is not None:
                comp_ids.add(int(bid))
    for r in peak_competitors:
        if _match(r):
            bid = r.get("brand_id")
            if bid is not None:
                comp_ids.add(int(bid))

    media_pct_by_event: dict[str, list[float]] = {}
    for bid in comp_ids:
        ann = sum(
            _num(r.get("annual_gross"))
            for r in annual_competitors
            if _match(r) and r.get("brand_id") is not None and int(r["brand_id"]) == bid
        )
        if ann <= 0:
            continue
        ev_gross: dict[str, float] = {}
        for r in peak_competitors:
            if not _match(r) or r.get("brand_id") is None or int(r["brand_id"]) != bid:
                continue
            ev = r.get("peak_event")
            if not ev:
                continue
            ev_gross[ev] = ev_gross.get(ev, 0) + _num(r.get("gross_pln"))
        for ev, g in ev_gross.items():
            media_pct_by_event.setdefault(ev, []).append(100.0 * g / ann)

    all_events = set(peak_by_event_brand.keys()) | set(media_pct_by_event.keys())
    out = []
    for ev in all_events:
        bpct = round(100.0 * peak_by_event_brand.get(ev, 0) / (annual_b or 1), 1)
        plist = media_pct_by_event.get(ev) or []
        mpct = round(sum(plist) / len(plist), 1) if plist else 0.0
        out.append({"peak_event": ev, "brand_pct_of_annual": bpct, "media_pct_of_annual": mpct})
    out.sort(key=lambda x: (x.get("media_pct_of_annual") or 0), reverse=True)
    return out


async def get_mi_peak(ps, pe, brand_id, brand_cats, brand_subcats_map):
    """Peak events: brand vs media. 4 query consolidate invece di 100+."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    key = cache_key("mi_peak_v3_comp_avg", ps=ps, pe=pe, brand=brand_id)
    cached = get_cached(key)
    if cached is not None:
        return cached

    if not is_full_year_period(ps, pe):
        out = await get_mi_peak_live(ps, pe, int(brand_id), brand_cats, brand_subcats_map)
        set_cached(key, out)
        return out

    year = int(ps[:4])
    bid = int(brand_id)

    peak_brand, annual_brand, peak_comp, annual_comp = await asyncio.to_thread(
        query_peak_mi_raw_all_from_precalc, year, bid
    )
    peak_brand = list(peak_brand) if peak_brand else []
    annual_brand = list(annual_brand) if annual_brand else []
    peak_comp = list(peak_comp) if peak_comp else []
    annual_comp = list(annual_comp) if annual_comp else []

    def _peak(cat_f, sub_f, ch_f):
        return _build_peak_mi_from_raw(peak_brand, annual_brand, peak_comp, annual_comp, cat_f, sub_f, ch_f)

    peak = _peak(None, None, "")
    peak_events_map = {"": peak}
    for c in brand_cats:
        peak_events_map["cat_" + str(c["category_id"])] = _peak(c["category_id"], None, "")
    for pid, subs in brand_subcats_map.items():
        for s in subs:
            sid = s.get("category_id")
            if sid is not None:
                peak_events_map["sub_" + str(sid)] = _peak(None, sid, "")

    peak_events_map_channel = {}
    for ch in CHANNELS:
        ch_val = ch if ch else ""
        peak_map = {"": _peak(None, None, ch_val)}
        for c in brand_cats:
            peak_map["cat_" + str(c["category_id"])] = _peak(c["category_id"], None, ch_val)
        for pid, subs in brand_subcats_map.items():
            for s in subs:
                sid = s.get("category_id")
                if sid is not None:
                    peak_map["sub_" + str(sid)] = _peak(None, sid, ch_val)
        peak_events_map_channel[ch] = peak_map

    out = {
        "peak_events": peak,
        "peak_events_map": peak_events_map,
        "peak_events_map_channel": peak_events_map_channel,
    }
    set_cached(key, out)
    return out


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


async def get_mi_all(ps, pe, brand_id, discount_cat=None, discount_subcat=None):
    """Batch: base + sales + promo + peak + discount in una sola chiamata. Riduce round-trip per caricamento iniziale."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    base = await get_mi_base(ps, pe, brand_id)
    if base.get("error"):
        return base
    brand_cats = base.get("brand_categories") or []
    brand_subcats = base.get("brand_subcategories") or {}
    cat_ids = base.get("cat_ids") or []
    sub_ids = base.get("sub_ids") or []
    sub_cat_id = base.get("subcategory_category_id") or (cat_ids[0] if cat_ids else None)
    disc_cat = discount_cat or (str(brand_cats[0]["category_id"]) if brand_cats else None)
    disc_sub = discount_subcat

    sales, promo, peak, discount = await asyncio.gather(
        get_mi_sales(ps, pe, brand_id, cat_ids, sub_ids, sub_cat_id),
        get_mi_promo(ps, pe, brand_id, brand_cats, brand_subcats),
        get_mi_peak(ps, pe, brand_id, brand_cats, brand_subcats),
        get_mi_discount(ps, pe, brand_id, brand_cats, sub_ids, disc_cat, disc_sub),
    )
    out = {**base}
    if not sales.get("error"):
        out.update(sales)
    if not promo.get("error"):
        out.update(promo)
    if not peak.get("error"):
        out.update(peak)
    if not discount.get("error"):
        out.update(discount)
    return out


async def get_mi_incremental_yoy(ps, pe, brand_id, brand_cats, brand_subcats_map):
    """Incremental YoY: year, total_gross, promo_gross per scope e channel. Per chart Average Incremental YoY."""
    if not brand_id or not str(brand_id).strip():
        return {}
    key = cache_key("mi_incr_yoy_v2_dedup_ch", ps=ps, pe=pe, brand=brand_id)
    cached = get_cached(key)
    if cached is not None:
        return cached

    scope_keys = [""]
    scope_params = [(None, None)]  # (cat, subcat)
    for c in brand_cats:
        scope_keys.append("cat_" + str(c["category_id"]))
        scope_params.append((int(c["category_id"]), None))
    for cid, subs in brand_subcats_map.items():
        for s in subs:
            scope_keys.append("sub_" + str(s["category_id"]))
            scope_params.append((None, int(s["category_id"])))

    def _to_jsonable(rows):
        out = []
        for r in (rows or []):
            d = dict(r)
            for k in ("total_gross", "promo_gross", "incremental_sales_pln"):
                if k in d and isinstance(d[k], Decimal):
                    d[k] = float(d[k])
            if "year" in d and d["year"] is not None:
                d["year"] = int(d["year"])
            out.append(d)
        return out

    multi_years = get_multi_year_full_years(ps, pe)
    if multi_years is None:
        return {}
    bid = int(brand_id) if brand_id else 0

    async def _run_precalc_scope(scope_key, cat, subcat):
        async with _MI_INCR_SCOPE_SEM:
            # Una query BigQuery per scope (tutti gli anni), non N×anno.
            rows = await asyncio.to_thread(
                query_incremental_yoy_vendite_multi_year_from_precalc,
                list(multi_years),
                bid,
                cat,
                subcat,
                None,
            )
            rows = list(rows) if rows else []
            rows.sort(key=lambda x: (x.get("year") or 0))
            return {ch: rows for ch in CHANNELS}

    scope_tasks = [_run_precalc_scope(sk, cat, subcat) for sk, (cat, subcat) in zip(scope_keys, scope_params)]
    scope_results = await asyncio.gather(*scope_tasks)

    incremental_yoy_map_channel = {}
    for ch in CHANNELS:
        incremental_yoy_map_channel[ch] = {sk: _to_jsonable(scope_results[i].get(ch, [])) for i, sk in enumerate(scope_keys)}
    incremental_yoy_map = incremental_yoy_map_channel.get("", {})

    out = {"incremental_yoy_map": incremental_yoy_map, "incremental_yoy_map_channel": incremental_yoy_map_channel}
    set_cached(key, out)
    return out


async def get_mi_all_years(brand_id, discount_cat=None, discount_subcat=None):
    """Carica tutti gli anni in parallelo sul server. Una sola chiamata = tutti i dati pronti per dropdown year istantanei."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    key = cache_key(
        "mi_all_years_v3",
        brand=brand_id,
        disc_cat=discount_cat or "",
        disc_sub=discount_subcat or "",
    )
    cached = get_cached(key, ttl=TTL_LONG)
    if cached is not None:
        return cached
    years = await asyncio.to_thread(query_available_years)
    years = list(years) if years else []
    if not years:
        return {"error": "No years available", "by_year": {}, "available_years": []}

    y_ref = years[-1]
    base_for_incr = await get_mi_base(f"{y_ref}-01-01", f"{y_ref}-12-31", brand_id)
    if base_for_incr.get("error"):
        return {
            "error": base_for_incr.get("error"),
            "by_year": {},
            "available_years": [str(y) for y in years],
        }

    ps_full = f"{years[0]}-01-01"
    pe_full = f"{years[-1]}-12-31"
    incr_task = asyncio.create_task(
        get_mi_incremental_yoy(
            ps_full,
            pe_full,
            brand_id,
            base_for_incr.get("brand_categories") or [],
            base_for_incr.get("brand_subcategories") or {},
        )
    )

    async def _load_one_year(y: int):
        async with _MI_YEAR_LOAD_SEM:
            return y, await get_mi_all(f"{y}-01-01", f"{y}-12-31", brand_id, discount_cat, discount_subcat)

    results = await asyncio.gather(*[_load_one_year(y) for y in years], return_exceptions=True)
    by_year = {}
    for r in results:
        if isinstance(r, Exception):
            continue
        if isinstance(r, tuple) and len(r) == 2:
            y, payload = r
            if not (isinstance(payload, dict) and payload.get("error")):
                by_year[str(y)] = payload

    incr_yoy = await incr_task

    out = {"by_year": by_year, "available_years": [str(y) for y in years]}
    if incr_yoy:
        out.update(incr_yoy)
    set_cached(key, out, ttl=TTL_LONG)
    return out


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
    from app.services._cache import cache_key, get_cached, set_cached, safe

    key = cache_key(
        "mi_top_prod_v2",
        year=year,
        brand=brand_id,
        cat=category_id or "",
        sub=subcategory_id or "",
        ch=channel or "",
    )
    cached = get_cached(key, ttl=600)
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
    set_cached(key, out, ttl=600)
    return copy.deepcopy(out)


async def get_mi_segment_by_sku(product_id, brand_id, year, category_id=None, channel=None):
    """Segment breakdown per un prodotto – tutte le vendite (non solo promo)."""
    if not product_id or not brand_id:
        return {"error": "Product and brand required", "rows": []}
    from app.db.queries.market_intelligence.segment_sku import (
        query_segment_breakdown_for_product_all_sales,
        query_segment_breakdown_for_product_precalc,
    )
    from app.services._cache import cache_key, get_cached, set_cached, safe

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
    cached = get_cached(key, ttl=600)
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
    set_cached(key, out, ttl=600)
    return copy.deepcopy(out)
