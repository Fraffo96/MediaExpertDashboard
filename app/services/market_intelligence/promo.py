"""Market Intelligence: promo share e ROI."""
import asyncio

from app.db.queries.market_intelligence.shared import CHANNELS
from app.db.queries.precalc import (
    is_full_year_period,
    query_promo_roi_mi_all_from_precalc,
    query_promo_share_mi_all_channels_from_precalc,
    query_promo_share_sub_mi_all_channels_from_precalc,
)
from app.services._cache import cache_key, get_cached, set_cached
from app.services.mi_bc_live import get_mi_promo_live


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
