"""Market Intelligence: peak events."""
import asyncio

from app.db.queries.market_intelligence.shared import CHANNELS
from app.db.queries.precalc import is_full_year_period, query_peak_mi_raw_all_from_precalc
from app.services._cache import cache_key, get_cached, set_cached
from app.services.mi_bc_live import get_mi_peak_live


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
