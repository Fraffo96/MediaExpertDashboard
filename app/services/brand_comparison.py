"""Brand Comparison: stessi grafici di Market Intelligence ma Brand vs Competitor invece di Brand vs Category Avg.
Solo tabelle precalcolate. Periodo deve essere anno intero o range multi-anno completo."""
PRECALC_ONLY_ERR = "Precalc tables only. Use full year period (YYYY-01-01 to YYYY-12-31)."
import asyncio
import copy
from decimal import Decimal


def _to_float(v):
    """Converte Decimal/None in float per evitare 'float * Decimal'."""
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def _sanitize_decimals(obj):
    """Converte ricorsivamente Decimal in float per JSON e operazioni."""
    if obj is None:
        return None
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _sanitize_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_decimals(x) for x in obj]
    return obj

from app.db.queries.market_intelligence.shared import CHANNELS
from app.db.queries.precalc import (
    get_multi_year_full_years,
    is_full_year_period,
    query_brand_all_subcategories_from_precalc,
    query_brand_categories_from_precalc,
    query_competitors_in_scope_from_precalc,
    query_sales_pie_bc_categories_all_channels_from_precalc,
    query_sales_pie_bc_subcategories_all_channels_from_precalc,
    query_promo_share_bc_all_channels_from_precalc,
    query_promo_share_sub_bc_all_channels_from_precalc,
    query_promo_roi_bc_all_categories_from_precalc,
    query_peak_bc_raw_all_from_precalc,
    query_discount_depth_brand_vs_competitor_all_categories_from_precalc,
    query_discount_depth_for_all_subcategories_bc_from_precalc,
)
from app.db.queries.shared import query_available_years
from app.services._cache import cache_key, get_cached, set_cached, TTL_LONG


def intersect_brand_category_trees(year: int, brand_id: int, competitor_id: int) -> tuple[list[dict], dict[str, list]]:
    """Macro-categorie e sub dove entrambi i brand hanno vendite (stesso anno, precalc). Ordine parent = come il brand principale (per fatturato)."""
    cats_a = query_brand_categories_from_precalc(year, brand_id) or []
    cats_b = query_brand_categories_from_precalc(year, competitor_id) or []
    ids_b = {c["category_id"] for c in cats_b}
    brand_cats = [c for c in cats_a if c["category_id"] in ids_b]
    if not brand_cats:
        return [], {}
    parent_ids = [int(c["category_id"]) for c in brand_cats]
    all_a = query_brand_all_subcategories_from_precalc(year, brand_id, parent_ids) or []
    all_b = query_brand_all_subcategories_from_precalc(year, competitor_id, parent_ids) or []
    subs_b_by_parent: dict[int, set[int]] = {}
    for r in all_b:
        pid = r.get("parent_category_id")
        sid = r.get("category_id")
        if pid is None or sid is None:
            continue
        subs_b_by_parent.setdefault(int(pid), set()).add(int(sid))
    brand_subcats_map = {str(c["category_id"]): [] for c in brand_cats}
    for r in all_a:
        pid = r.get("parent_category_id")
        sid = r.get("category_id")
        if pid is None or sid is None:
            continue
        pid_i, sid_i = int(pid), int(sid)
        pkey = str(pid_i)
        if pkey not in brand_subcats_map:
            continue
        if sid_i not in subs_b_by_parent.get(pid_i, set()):
            continue
        brand_subcats_map[pkey].append({"category_id": sid_i, "category_name": r.get("category_name")})
    return brand_cats, brand_subcats_map


async def get_bc_competitors(ps, pe, brand_id):
    """Solo lista competitor. Leggero, per caricamento iniziale dropdown."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    key = cache_key("bc_competitors", ps=ps, pe=pe, brand=brand_id)
    cached = get_cached(key)
    if cached is not None:
        return copy.deepcopy(cached)

    multi_years = get_multi_year_full_years(ps, pe)
    if not multi_years:
        return {"error": PRECALC_ONLY_ERR}
    bid = int(brand_id)
    results = await asyncio.gather(*[
        asyncio.to_thread(query_competitors_in_scope_from_precalc, y, bid)
        for y in multi_years
    ])
    seen = {}
    for rows in results:
        for r in (rows or []):
            bid_comp = r.get("brand_id")
            if bid_comp is not None and bid_comp not in seen:
                seen[bid_comp] = {"brand_id": bid_comp, "brand_name": r.get("brand_name", "")}
    competitors = list(seen.values())
    competitors.sort(key=lambda x: (x.get("brand_name") or "").lower())

    out = {"competitors": competitors}
    set_cached(key, out)
    return copy.deepcopy(out)


async def get_bc_base(ps, pe, brand_id, competitor_id=None):
    """Metadata per BC: brand_cats, brand_subcats_map, years, channels + competitors. Solo precalc.
    Con competitor_id: solo categorie/sub dove entrambi i brand hanno vendite."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    if not is_full_year_period(ps, pe):
        return {"error": PRECALC_ONLY_ERR}
    cid_opt: int | None = None
    if competitor_id is not None and str(competitor_id).strip():
        try:
            cid_opt = int(competitor_id)
        except (TypeError, ValueError):
            cid_opt = None
    comp_key = str(cid_opt) if cid_opt is not None else "none"
    key = cache_key("bc_base", ps=ps, pe=pe, brand=brand_id, comp=comp_key)
    cached = get_cached(key)
    if cached is not None:
        return copy.deepcopy(cached)

    year = int(ps[:4])
    bid = int(brand_id) if brand_id else 0

    if cid_opt is not None:
        brand_cats, brand_subcats_map = await asyncio.to_thread(intersect_brand_category_trees, year, bid, cid_opt)
        brand_cats = list(brand_cats) if brand_cats else []
        brand_subcats_map = dict(brand_subcats_map) if brand_subcats_map else {}
    else:
        brand_cats = await asyncio.to_thread(query_brand_categories_from_precalc, year, bid)
        brand_cats = list(brand_cats) if brand_cats else []

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

    first_cat = brand_cats[0]["category_id"] if brand_cats else None

    sub_ids = []
    for subs in brand_subcats_map.values():
        sub_ids.extend([str(s["category_id"]) for s in subs])
    cat_ids = [str(c["category_id"]) for c in brand_cats]

    available_years = await asyncio.to_thread(query_available_years)
    available_years = list(available_years) if available_years else []

    competitors = await asyncio.to_thread(query_competitors_in_scope_from_precalc, year, bid)

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
        "competitors": list(competitors) if competitors else [],
        "category_pie_id": cat_pie_id,
        "subcategory_pie_id": sub_pie_id,
        "subcategory_category_id": first_cat,
    }
    set_cached(key, out)
    return copy.deepcopy(out)


async def get_bc_sales(ps, pe, brand_id, competitor_id, cat_ids, sub_ids, sub_cat_id=None):
    """Sales pie: brand + competitor. Output come get_mi_sales."""
    if not brand_id or not competitor_id:
        return {"error": "Brand and competitor required"}
    key = cache_key(
        "bc_sales",
        ps=ps,
        pe=pe,
        brand=brand_id,
        comp=competitor_id,
        cat_ids=",".join(cat_ids) if cat_ids else "",
        sub_ids=",".join(sub_ids) if sub_ids else "",
        sub_cat=sub_cat_id or "",
    )
    cached = get_cached(key)
    if cached is not None:
        return copy.deepcopy(cached)

    sub_cat_id = sub_cat_id or (cat_ids[0] if cat_ids else None)

    if not is_full_year_period(ps, pe):
        return {"error": PRECALC_ONLY_ERR}
    year = int(ps[:4])
    bid, cid = int(brand_id), int(competitor_id)

    def _group_pie_by_channel(rows):
        out = {ch: {} for ch in CHANNELS}
        for r in rows:
            ch = (r.get("channel") or "").strip()
            if ch not in out:
                out[ch] = {}
            cat_id = str(r.get("category_id", ""))
            if cat_id not in out[ch]:
                out[ch][cat_id] = []
            out[ch][cat_id].append({k: v for k, v in r.items() if k != "channel"})
        return out

    cat_pie_all, sub_pie_all = [], []
    if cat_ids:
        cat_ids_int = [int(c) for c in cat_ids if c]
        cat_pie_all = await asyncio.to_thread(query_sales_pie_bc_categories_all_channels_from_precalc, year, cat_ids_int, bid, cid)
        cat_pie_all = list(cat_pie_all) if cat_pie_all else []
    if sub_ids:
        sub_ids_int = [int(s) for s in sub_ids if s]
        sub_pie_all = await asyncio.to_thread(query_sales_pie_bc_subcategories_all_channels_from_precalc, year, sub_ids_int, bid, cid)
        sub_pie_all = list(sub_pie_all) if sub_pie_all else []

    category_pie_brands_map_channel = _group_pie_by_channel(cat_pie_all) if cat_pie_all else {ch: {} for ch in CHANNELS}
    subcategory_pie_brands_map_channel = _group_pie_by_channel(sub_pie_all) if sub_pie_all else {ch: {} for ch in CHANNELS}

    out = {
        "sales_value": [],
        "sales_volume": [],
        "sales_value_subcategory": [],
        "sales_volume_subcategory": [],
        "subcategory_category_id": sub_cat_id,
        "category_pie_brands_map_channel": category_pie_brands_map_channel,
        "subcategory_pie_brands_map_channel": subcategory_pie_brands_map_channel,
        "category_pie_brands_prev_map_channel": {},
        "subcategory_pie_brands_prev_map_channel": {},
    }
    set_cached(key, out)
    return copy.deepcopy(out)


def _row_to_ps(r):
    """Estrae category_id, category_name, brand_promo_share_pct, media_promo_share_pct da riga promo share."""
    return {
        "category_id": r.get("category_id"),
        "category_name": r.get("category_name") or "",
        "brand_promo_share_pct": r.get("brand_promo_share_pct"),
        "media_promo_share_pct": r.get("media_promo_share_pct"),
    }


def _row_to_roi(r):
    """Estrae promo_type, brand_avg_roi, media_avg_roi da riga ROI."""
    return {
        "promo_type": r.get("promo_type"),
        "brand_avg_roi": r.get("brand_avg_roi"),
        "media_avg_roi": r.get("media_avg_roi"),
    }


async def get_bc_promo(ps, pe, brand_id, competitor_id, brand_cats, brand_subcats_map):
    """Promo share e ROI: brand vs competitor. 3 query consolidate invece di 20+."""
    if not brand_id or not competitor_id:
        return {"error": "Brand and competitor required"}
    key = cache_key("bc_promo", ps=ps, pe=pe, brand=brand_id, comp=competitor_id)
    cached = get_cached(key)
    if cached is not None:
        return copy.deepcopy(cached)

    if not is_full_year_period(ps, pe):
        return {"error": PRECALC_ONLY_ERR}
    year = int(ps[:4])
    bid, cid = int(brand_id), int(competitor_id)

    ps_cat_all, ps_sub_all, roi_all = await asyncio.gather(
        asyncio.to_thread(query_promo_share_bc_all_channels_from_precalc, year, bid, cid),
        asyncio.to_thread(query_promo_share_sub_bc_all_channels_from_precalc, year, bid, cid),
        asyncio.to_thread(query_promo_roi_bc_all_categories_from_precalc, year, bid, cid),
    )
    ps_cat_all = list(ps_cat_all) if ps_cat_all else []
    ps_sub_all = list(ps_sub_all) if ps_sub_all else []
    roi_all = list(roi_all) if roi_all else []

    allowed_parents = {c["category_id"] for c in brand_cats} if brand_cats else set()

    def _parent_ok_row(r):
        if not allowed_parents:
            return False
        return r.get("category_id") in allowed_parents

    # promo_share_by_category: canale '' (all)
    promo_share_cat = [
        _row_to_ps(r)
        for r in ps_cat_all
        if (r.get("channel") or "").strip() == "" and _parent_ok_row(r)
    ]

    # promo_share_by_subcategory_map: canale '', raggruppato per parent
    by_parent = {}
    for r in ps_sub_all:
        if (r.get("channel") or "").strip() != "":
            continue
        pid = str(r.get("parent_category_id", ""))
        if pid not in by_parent:
            by_parent[pid] = []
        by_parent[pid].append(_row_to_ps(r))
    promo_share_by_subcategory_map = {str(c["category_id"]): by_parent.get(str(c["category_id"]), []) for c in brand_cats}

    # promo_roi: aggregato su tutte le categorie (1-10)
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

    # promo_roi_map: '' + cat_X + sub_Y
    def _roi_for_cat(cat_id):
        rows = [r for r in roi_all if r.get("category_id") == cat_id]
        by_pt = {}
        for r in rows:
            pt = r.get("promo_type")
            by_pt[pt] = _row_to_roi(r)
        return list(by_pt.values())

    promo_roi_map = {"": roi}
    for c in brand_cats:
        promo_roi_map["cat_" + str(c["category_id"])] = _roi_for_cat(c["category_id"])
    for pid, subs in brand_subcats_map.items():
        cat_roi = _roi_for_cat(int(pid))
        for s in subs:
            sid = s.get("category_id")
            if sid is not None:
                promo_roi_map["sub_" + str(sid)] = cat_roi

    # promo_share_by_category_channel, promo_share_by_subcategory_map_channel
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
    promo_share_by_category_channel = {
        ch: [_row_to_ps(r) for r in ps_cat_by_ch.get(ch, []) if _parent_ok_row(r)] for ch in CHANNELS
    }
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
    return copy.deepcopy(out)


def _build_peak_from_raw(peak_rows, annual_rows, bid, cid, cat_filter, sub_filter, ch_filter):
    """Costruisce lista peak_event, brand_pct_of_annual, media_pct_of_annual da raw rows."""
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

    annual_brand = sum(_to_float(r.get("annual_gross")) for r in annual_rows if _match(r) and r.get("brand_id") == bid)
    annual_comp = sum(_to_float(r.get("annual_gross")) for r in annual_rows if _match(r) and r.get("brand_id") == cid)
    peak_by_event = {}
    for r in peak_rows:
        if not _match(r):
            continue
        ev = r.get("peak_event")
        if not ev:
            continue
        if ev not in peak_by_event:
            peak_by_event[ev] = {"brand": 0.0, "media": 0.0}
        if r.get("brand_id") == bid:
            peak_by_event[ev]["brand"] += _to_float(r.get("gross_pln"))
        elif r.get("brand_id") == cid:
            peak_by_event[ev]["media"] += _to_float(r.get("gross_pln"))

    out = []
    for ev, vals in peak_by_event.items():
        bpct = round(100.0 * vals["brand"] / (annual_brand or 1), 1)
        mpct = round(100.0 * vals["media"] / (annual_comp or 1), 1)
        out.append({"peak_event": ev, "brand_pct_of_annual": bpct, "media_pct_of_annual": mpct})
    out.sort(key=lambda x: (x.get("media_pct_of_annual") or 0), reverse=True)
    return out


async def get_bc_peak(ps, pe, brand_id, competitor_id, brand_cats, brand_subcats_map):
    """Peak events: brand vs competitor. 1 query raw invece di 20+."""
    if not brand_id or not competitor_id:
        return {"error": "Brand and competitor required"}
    key = cache_key("bc_peak", ps=ps, pe=pe, brand=brand_id, comp=competitor_id)
    cached = get_cached(key)
    if cached is not None:
        return copy.deepcopy(cached)

    if not is_full_year_period(ps, pe):
        return {"error": PRECALC_ONLY_ERR}
    year = int(ps[:4])
    bid, cid = int(brand_id), int(competitor_id)

    peak_rows, annual_rows = await asyncio.to_thread(query_peak_bc_raw_all_from_precalc, year, bid, cid)
    peak_rows = list(peak_rows) if peak_rows else []
    annual_rows = list(annual_rows) if annual_rows else []

    def _peak(cat_f, sub_f, ch_f):
        return _build_peak_from_raw(peak_rows, annual_rows, bid, cid, cat_f, sub_f, ch_f)

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
    return copy.deepcopy(out)


async def get_bc_discount(ps, pe, brand_id, competitor_id, brand_cats, sub_ids, discount_cat=None, discount_subcat=None):
    """Discount depth: brand vs competitor. Output come get_mi_discount."""
    if not brand_id or not competitor_id:
        return {"error": "Brand and competitor required"}
    key = cache_key("bc_discount", ps=ps, pe=pe, brand=brand_id, comp=competitor_id)
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

    use_precalc = is_full_year_period(ps, pe) and brand_id and competitor_id
    year = int(ps[:4]) if use_precalc else None
    bid, comp_id = int(brand_id), int(competitor_id)

    if not use_precalc:
        return {"error": PRECALC_ONLY_ERR}
    disc_depth = await asyncio.to_thread(query_discount_depth_brand_vs_competitor_all_categories_from_precalc, year, bid, comp_id)
    disc_depth = list(disc_depth) if disc_depth else []

    discount_depth_selected_map = {}
    for r in disc_depth:
        cid_str = str(r.get("category_id", ""))
        if cid_str:
            discount_depth_selected_map["cat_" + cid_str] = {
                "brand_avg_discount_depth": r.get("brand_avg_discount_depth"),
                "media_avg_discount_depth": r.get("media_avg_discount_depth"),
            }
    if sub_ids:
        sub_ids_int = [int(s) for s in sub_ids if s]
        dd_sub_all = await asyncio.to_thread(query_discount_depth_for_all_subcategories_bc_from_precalc, year, bid, comp_id, sub_ids_int)
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
    return copy.deepcopy(out)


async def get_bc_all(ps, pe, brand_id, competitor_id, discount_cat=None, discount_subcat=None):
    """Batch: base + sales + promo + peak + discount. Richiede competitor_id."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    if not competitor_id:
        return {"error": "Competitor required"}

    base = await get_bc_base(ps, pe, brand_id, competitor_id)
    if base.get("error"):
        return base

    competitor_name = ""
    for c in (base.get("competitors") or []):
        if str(c.get("brand_id")) == str(competitor_id):
            competitor_name = c.get("brand_name", "")
            break

    brand_cats = base.get("brand_categories") or []
    brand_subcats = base.get("brand_subcategories") or {}
    cat_ids = base.get("cat_ids") or []
    sub_ids = base.get("sub_ids") or []
    sub_cat_id = base.get("subcategory_category_id") or (cat_ids[0] if cat_ids else None)
    disc_cat = discount_cat or (str(brand_cats[0]["category_id"]) if brand_cats else None)
    disc_sub = discount_subcat

    sales, promo, peak, discount = await asyncio.gather(
        get_bc_sales(ps, pe, brand_id, competitor_id, cat_ids, sub_ids, sub_cat_id),
        get_bc_promo(ps, pe, brand_id, competitor_id, brand_cats, brand_subcats),
        get_bc_peak(ps, pe, brand_id, competitor_id, brand_cats, brand_subcats),
        get_bc_discount(ps, pe, brand_id, competitor_id, brand_cats, sub_ids, disc_cat, disc_sub),
    )
    out = {**base, "competitor_id": competitor_id, "competitor_name": competitor_name}
    if not sales.get("error"):
        out.update(sales)
    if not promo.get("error"):
        out.update(promo)
    if not peak.get("error"):
        out.update(peak)
    if not discount.get("error"):
        out.update(discount)
    return _sanitize_decimals(out)


async def get_bc_all_years(brand_id, competitor_id, discount_cat=None, discount_subcat=None):
    """Tutti gli anni in una chiamata. Richiede competitor_id."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    if not competitor_id:
        return {"error": "Competitor required"}

    key = cache_key("bc_all_years", brand=brand_id, comp=competitor_id)
    cached = get_cached(key, ttl=TTL_LONG)
    if cached is not None:
        return copy.deepcopy(cached)

    from app.db.queries.shared import query_available_years
    years = await asyncio.to_thread(query_available_years)
    years = list(years) if years else []
    if not years:
        return {"error": "No years available", "by_year": {}, "available_years": []}

    tasks = [
        get_bc_all(f"{y}-01-01", f"{y}-12-31", brand_id, competitor_id, discount_cat, discount_subcat)
        for y in years
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    by_year = {}
    first_error = None
    for y, r in zip(years, results):
        if isinstance(r, Exception):
            first_error = first_error or str(r)
            continue
        if r.get("error"):
            first_error = first_error or r.get("error")
            continue
        by_year[str(y)] = r

    out = {"by_year": by_year, "available_years": [str(y) for y in years]}
    if not by_year and first_error:
        out["error"] = first_error
    set_cached(key, out, ttl=TTL_LONG)
    return copy.deepcopy(out)


async def get_brand_comparison(ps, pe, brand_id, competitor_id=None, cat=None, subcat=None):
    """Legacy: solo precalc. Usare /api/brand-comparison/base e /api/brand-comparison/all."""
    return {"error": PRECALC_ONLY_ERR}
