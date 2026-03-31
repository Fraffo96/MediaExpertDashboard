"""Basic dashboard: KPI, sales by category/subcategory, promo, peak, ROI, ecc."""
import asyncio

from app.db.queries import basic
from app.services._cache import cache_key, get_cached, set_cached, safe
from app.services.filters import roi_cat


def _agg_repeat_from_channels(rows):
    """Aggrega repeat_rate_by_channel in un singolo repeat_rate (tutti i canali)."""
    def to_f(v):
        if v is None:
            return 0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0
    if not rows:
        return []
    if len(rows) == 1 and rows[0].get("channel") is None:
        return [dict(rows[0])]
    tot_buyers = sum(to_f(r.get("total_buyers")) for r in rows)
    tot_repeat = sum(to_f(r.get("repeat_buyers")) for r in rows)
    return [{
        "total_buyers": tot_buyers,
        "repeat_buyers": tot_repeat,
        "repeat_rate_pct": round(100.0 * tot_repeat / tot_buyers, 1) if tot_buyers else None,
        "avg_frequency": None,
        "avg_lifetime_spend": None,
    }]


async def get_basic(ps, pe, cat=None, seg=None, gender=None, brand=None, subcategory_id=None, incremental_yoy_promo_id=None, channel=None):
    effective_cat = (subcategory_id or cat) if (subcategory_id or cat) else None
    roi_c = roi_cat(cat, subcategory_id)
    key = cache_key("basic", ps=ps, pe=pe, cat=effective_cat or "", seg=seg or "", gender=gender or "", brand=brand or "", channel=channel or "")
    cached = get_cached(key)
    if cached is not None:
        return cached
    (kpi, by_cat, by_subcat, promo_share, promo_share_cat, promo_share_subcat, promo_share_detail, yoy, incr_yoy,
     peak, roi_type, roi_by_cat, roi_by_brand, roi_detail, disc_depth, disc_depth_detail, detail, by_brand, by_brand_detail, brand_cat,
     by_cat_seg, by_cat_gender,
     channel_mix, loyalty, buyer_segs, buyer_demo, repeat, channel_seg) = await asyncio.gather(
        asyncio.to_thread(safe, basic.query_kpi, ps, pe, effective_cat, seg, gender, brand),
        asyncio.to_thread(safe, basic.query_sales_by_category, ps, pe, effective_cat, seg, gender, brand),
        asyncio.to_thread(safe, basic.query_sales_by_subcategory, ps, pe, effective_cat, seg, gender, brand),
        asyncio.to_thread(safe, basic.query_promo_share, ps, pe, effective_cat, seg, gender, brand),
        asyncio.to_thread(safe, basic.query_promo_share_by_category, ps, pe, effective_cat, seg, gender, brand),
        asyncio.to_thread(safe, basic.query_promo_share_by_subcategory, ps, pe, effective_cat, seg, gender, brand),
        asyncio.to_thread(safe, basic.query_promo_share_detail, ps, pe),
        asyncio.to_thread(safe, basic.query_yoy, ps, pe, effective_cat, seg, gender, brand),
        asyncio.to_thread(safe, basic.query_incremental_yoy, ps, pe, effective_cat, seg, gender, brand, incremental_yoy_promo_id),
        asyncio.to_thread(safe, basic.query_peak_events, ps, pe, effective_cat, seg, gender, brand),
        asyncio.to_thread(safe, basic.query_promo_roi_by_type, ps, pe, roi_c, None, None, brand),
        asyncio.to_thread(safe, basic.query_promo_roi_by_category, ps, pe, roi_c, brand),
        asyncio.to_thread(safe, basic.query_promo_roi_by_brand, ps, pe, roi_c, brand),
        asyncio.to_thread(safe, basic.query_promo_roi_detail, ps, pe, None, None),
        asyncio.to_thread(safe, basic.query_discount_depth_by_category, ps, pe, effective_cat, seg, gender, brand),
        asyncio.to_thread(safe, basic.query_discount_depth_detail, ps, pe),
        asyncio.to_thread(safe, basic.query_sales_detail, ps, pe, effective_cat, seg, gender, brand),
        asyncio.to_thread(safe, basic.query_sales_by_brand, ps, pe, effective_cat, seg, gender, brand),
        asyncio.to_thread(safe, basic.query_sales_by_brand_detail, ps, pe),
        asyncio.to_thread(safe, basic.query_sales_brand_category_crosstab, ps, pe, seg, gender, effective_cat, brand),
        asyncio.to_thread(safe, basic.query_sales_by_category_by_segment, ps, pe, effective_cat, seg, gender, brand),
        asyncio.to_thread(safe, basic.query_sales_by_category_by_gender, ps, pe, effective_cat, seg, gender, brand),
        asyncio.to_thread(safe, basic.query_channel_mix, ps, pe, channel),
        asyncio.to_thread(safe, basic.query_loyalty_breakdown, ps, pe, channel),
        asyncio.to_thread(safe, basic.query_buyer_segments, ps, pe, channel),
        asyncio.to_thread(safe, basic.query_buyer_demographics, ps, pe, channel),
        asyncio.to_thread(safe, basic.query_repeat_rate, ps, pe, channel),
        asyncio.to_thread(safe, basic.query_channel_by_segment, ps, pe, channel),
    )
    out = {
        "kpi": list(kpi),
        "sales_by_category": list(by_cat),
        "sales_by_subcategory": list(by_subcat),
        "promo_share": list(promo_share),
        "promo_share_by_category": list(promo_share_cat),
        "promo_share_by_subcategory": list(promo_share_subcat),
        "promo_share_detail": list(promo_share_detail),
        "yoy": list(yoy),
        "incremental_yoy": list(incr_yoy),
        "peak_events": list(peak),
        "promo_roi_by_type": list(roi_type),
        "promo_roi_by_category": list(roi_by_cat),
        "promo_roi_by_brand": list(roi_by_brand),
        "promo_roi_detail": list(roi_detail),
        "discount_depth_by_category": list(disc_depth),
        "discount_depth_detail": list(disc_depth_detail),
        "sales_detail": list(detail),
        "sales_by_brand": list(by_brand),
        "sales_by_brand_detail": list(by_brand_detail),
        "sales_brand_category": list(brand_cat),
        "sales_by_category_by_segment": list(by_cat_seg),
        "sales_by_category_by_gender": list(by_cat_gender),
        "channel_mix": list(channel_mix),
        "loyalty_breakdown": list(loyalty),
        "buyer_segments": list(buyer_segs),
        "buyer_demographics": list(buyer_demo),
        "repeat_rate": list(repeat),
        "channel_by_segment": list(channel_seg),
    }
    set_cached(key, out)
    return out


async def get_basic_granular(ps, pe, channel=None):
    """Endpoint leggero: tabelle detail per filtro client-side. Fetch completo (channel ignorato)."""
    key = cache_key("basic_granular", ps=ps, pe=pe)
    cached = get_cached(key)
    if cached is not None:
        return cached

    def _f(v):
        if v is None:
            return 0
        return float(v)

    (
        promo_share_detail,
        discount_depth_detail,
        by_brand_detail,
        by_cat_gender,
        by_cat_seg,
        yoy_detail,
        peak_detail,
        roi_type,
        roi_by_cat,
        roi_by_brand,
        roi_detail,
        incr_yoy,
        channel_mix,
        loyalty_by_channel,
        buyer_segs_by_channel,
        buyer_demo_by_channel,
        repeat_by_channel,
        channel_seg,
        top_products,
    ) = await asyncio.gather(
        asyncio.to_thread(safe, basic.query_promo_share_detail, ps, pe),
        asyncio.to_thread(safe, basic.query_discount_depth_detail, ps, pe),
        asyncio.to_thread(safe, basic.query_sales_by_brand_detail, ps, pe),
        asyncio.to_thread(safe, basic.query_sales_by_category_by_gender, ps, pe, None, None, None),
        asyncio.to_thread(safe, basic.query_sales_by_category_by_segment, ps, pe, None, None, None),
        asyncio.to_thread(safe, basic.query_yoy_detail, ps, pe),
        asyncio.to_thread(safe, basic.query_peak_events_detail, ps, pe),
        asyncio.to_thread(safe, basic.query_promo_roi_by_type, ps, pe, None, None, None, None),
        asyncio.to_thread(safe, basic.query_promo_roi_by_category, ps, pe, None, None),
        asyncio.to_thread(safe, basic.query_promo_roi_by_brand, ps, pe, None, None),
        asyncio.to_thread(safe, basic.query_promo_roi_detail, ps, pe, None, None),
        asyncio.to_thread(safe, basic.query_incremental_yoy, ps, pe, None, None, None, None, None),
        asyncio.to_thread(safe, basic.query_channel_mix, ps, pe, None),
        asyncio.to_thread(safe, basic.query_loyalty_breakdown_by_channel, ps, pe),
        asyncio.to_thread(safe, basic.query_buyer_segments_by_channel, ps, pe),
        asyncio.to_thread(safe, basic.query_buyer_demographics_by_channel, ps, pe),
        asyncio.to_thread(safe, basic.query_repeat_rate_by_channel, ps, pe),
        asyncio.to_thread(safe, basic.query_channel_by_segment, ps, pe, None),
        asyncio.to_thread(safe, basic.query_top_products, ps, pe, 100, None, None),
    )
    psd = list(promo_share_detail)
    ddd = list(discount_depth_detail)
    bbd = list(by_brand_detail)
    by_gen = list(by_cat_gender)
    by_seg = list(by_cat_seg)
    yoy_d = list(yoy_detail)
    peak_d = list(peak_detail)

    def _agg_psd(rows):
        by_cat, by_sub, tot = {}, {}, {"gross": 0.0, "promo": 0.0}
        for r in rows:
            tg, pg = _f(r.get("total_gross")), _f(r.get("promo_gross"))
            tot["gross"] += tg
            tot["promo"] += pg
            k = r.get("parent_category_id"), r.get("parent_name")
            if k not in by_cat:
                by_cat[k] = {"total_gross": 0.0, "promo_gross": 0.0, "category_id": r.get("parent_category_id"), "category_name": r.get("parent_name")}
            by_cat[k]["total_gross"] += tg
            by_cat[k]["promo_gross"] += pg
            sk = r.get("category_id"), r.get("category_name")
            if sk not in by_sub:
                by_sub[sk] = {"total_gross": 0.0, "promo_gross": 0.0, "category_id": r.get("category_id"), "category_name": r.get("category_name")}
            by_sub[sk]["total_gross"] += tg
            by_sub[sk]["promo_gross"] += pg
        promo_share = [{"total_gross": tot["gross"], "promo_gross": tot["promo"], "promo_share_pct": round(100 * tot["promo"] / tot["gross"], 1) if tot["gross"] else 0}]
        promo_cat = [{"category_id": k[0], "category_name": k[1], "total_gross": v["total_gross"], "promo_gross": v["promo_gross"], "promo_share_pct": round(100 * v["promo_gross"] / v["total_gross"], 1) if v["total_gross"] else 0} for k, v in by_cat.items()]
        promo_sub = [{"category_id": k[0], "category_name": k[1], "total_gross": v["total_gross"], "promo_gross": v["promo_gross"], "promo_share_pct": round(100 * v["promo_gross"] / v["total_gross"], 1) if v["total_gross"] else 0} for k, v in by_sub.items()]
        return promo_share, promo_cat, promo_sub

    def _agg_ddd(rows):
        by_cat = {}
        for r in rows:
            k = r.get("parent_category_id"), r.get("parent_name")
            if k not in by_cat:
                by_cat[k] = {"weighted_sum": 0.0, "gross_sum": 0.0}
            w = _f(r.get("promo_gross")) or _f(r.get("gross_pln")) or 1.0
            by_cat[k]["weighted_sum"] += _f(r.get("avg_discount_depth")) * w
            by_cat[k]["gross_sum"] += w
        return [{"category_id": k[0], "category_name": k[1], "avg_discount_depth": round(10 * v["weighted_sum"] / v["gross_sum"], 1) if v["gross_sum"] else 0} for k, v in by_cat.items()]

    def _agg_bbd(rows):
        by_b = {}
        for r in rows:
            bid = r.get("brand_id")
            if bid not in by_b:
                by_b[bid] = {"brand_id": bid, "brand_name": r.get("brand_name"), "gross_pln": 0.0, "units": 0}
            by_b[bid]["gross_pln"] += _f(r.get("gross_pln"))
            by_b[bid]["units"] += _f(r.get("units"))
        return sorted(by_b.values(), key=lambda x: -(x.get("gross_pln") or 0))

    def _agg_by_cat(rows, key_id="category_id", key_name="category_name"):
        by_c = {}
        for r in rows:
            cid = r.get(key_id)
            if cid not in by_c:
                by_c[cid] = {"category_id": cid, "category_name": r.get(key_name), "gross_pln": 0.0, "units": 0}
            by_c[cid]["gross_pln"] += _f(r.get("gross_pln"))
            by_c[cid]["units"] += _f(r.get("units"))
        return sorted(by_c.values(), key=lambda x: -(x.get("gross_pln") or 0))

    def _agg_yoy(rows):
        by_y = {}
        for r in rows:
            y = r.get("year")
            if y not in by_y:
                by_y[y] = {"year": y, "total_gross": 0.0, "promo_gross": 0.0}
            by_y[y]["total_gross"] += _f(r.get("total_gross"))
            by_y[y]["promo_gross"] += _f(r.get("promo_gross"))
        sorted_y = sorted(by_y.keys())
        out_y = []
        for i, y in enumerate(sorted_y):
            v = by_y[y]
            prior = by_y.get(sorted_y[i - 1], {}).get("total_gross", 0) if i > 0 else 0
            yoy_pct = round(100.0 * (v["total_gross"] - prior) / prior, 1) if prior else None
            out_y.append({"year": y, "total_gross": v["total_gross"], "promo_gross": v["promo_gross"], "prior_gross": prior, "yoy_pct": yoy_pct})
        return out_y

    def _agg_peak(rows):
        by_p = {}
        total = 0.0
        for r in rows:
            p = r.get("peak_event")
            if p not in by_p:
                by_p[p] = {"peak_event": p, "gross_pln": 0.0, "units": 0.0, "days_count": 0}
            by_p[p]["gross_pln"] += _f(r.get("gross_pln"))
            by_p[p]["units"] += _f(r.get("units"))
            by_p[p]["days_count"] += r.get("days_count") or 0
            total += _f(r.get("gross_pln"))
        return [{"peak_event": k, "gross_pln": v["gross_pln"], "units": v["units"], "days_count": v["days_count"], "pct_of_annual": round(100.0 * v["gross_pln"] / total, 1) if total else 0} for k, v in by_p.items()]

    promo_share, promo_cat, promo_sub = _agg_psd(psd) if psd else ([{"total_gross": 0, "promo_gross": 0, "promo_share_pct": 0}], [], [])
    disc_cat = _agg_ddd(ddd) if ddd else []
    by_brand = _agg_bbd(bbd) if bbd else []
    by_cat = _agg_by_cat(by_gen) if by_gen else []
    by_sub_raw = {}
    for r in psd:
        cid, cname = r.get("category_id"), r.get("category_name")
        if cid not in by_sub_raw:
            by_sub_raw[cid] = {"category_id": cid, "category_name": cname, "gross_pln": 0, "units": 0}
        by_sub_raw[cid]["gross_pln"] += r.get("total_gross") or 0
    by_sub = sorted(by_sub_raw.values(), key=lambda x: -(x.get("gross_pln") or 0))
    yoy = _agg_yoy(yoy_d)
    peak = _agg_peak(peak_d)
    tot_units = sum(_f(r.get("units")) for r in bbd) if bbd else 0
    avg_disc = round(10 * sum(_f(r.get("avg_discount_depth")) for r in disc_cat) / len(disc_cat), 1) if disc_cat else None
    ps0 = promo_share[0] if promo_share else {}
    kpi = [{"total_gross": ps0.get("total_gross", 0), "total_units": tot_units, "promo_share_pct": ps0.get("promo_share_pct"), "promo_gross": ps0.get("promo_gross"), "avg_discount_depth": avg_disc}]

    out = {
        "promo_share_detail": psd,
        "discount_depth_detail": ddd,
        "sales_by_brand_detail": bbd,
        "sales_by_category_by_gender": by_gen,
        "sales_by_category_by_segment": by_seg,
        "yoy_detail": yoy_d,
        "peak_events_detail": peak_d,
        "kpi": kpi,
        "sales_by_category": by_cat,
        "sales_by_subcategory": by_sub,
        "promo_share": promo_share,
        "promo_share_by_category": promo_cat,
        "promo_share_by_subcategory": promo_sub,
        "discount_depth_by_category": disc_cat,
        "sales_by_brand": by_brand,
        "sales_brand_category": [],
        "yoy": yoy,
        "peak_events": peak,
        "promo_roi_by_type": list(roi_type),
        "promo_roi_by_category": list(roi_by_cat),
        "promo_roi_by_brand": list(roi_by_brand),
        "promo_roi_detail": list(roi_detail),
        "incremental_yoy": list(incr_yoy),
        "channel_mix": list(channel_mix),
        "loyalty_breakdown": list(loyalty_by_channel),
        "buyer_segments": list(buyer_segs_by_channel),
        "buyer_demographics": list(buyer_demo_by_channel),
        "repeat_rate": _agg_repeat_from_channels(repeat_by_channel),
        "repeat_rate_by_channel": list(repeat_by_channel),
        "channel_by_segment": list(channel_seg),
        "top_products": list(top_products),
    }
    set_cached(key, out)
    return out
