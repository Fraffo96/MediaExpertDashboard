"""Promo Creator: suggestions and benchmarks.
Periodo anno intero. Discount benchmark: precalc a livello macro-categoria; con sottocategoria selezionata, media sconto da fact_sales_daily su quella sub.
ROI atteso e top competitor: fact_promo_performance (solo parent 1–10) oppure, con subcategory, stesso modello ROI di MI su fact_sales_daily per quella sub."""
PRECALC_ONLY_ERR = "Precalc tables only. Use full year period (YYYY-01-01 to YYYY-12-31)."
import asyncio
import logging
import time

from app.db.queries import promo_creator
from app.db.queries.precalc import (
    is_full_year_period,
    query_category_discount_benchmark_from_precalc,
    query_promo_creator_subcat_from_precalc,
    query_promo_creator_subcat_type_from_precalc,
    query_roi_benchmark_by_type_from_precalc,
    query_top_competitor_roi_from_precalc,
)
from app.services._cache import TTL_LONG, cache_key, compute_once, get_cached, set_cached, safe

logger = logging.getLogger(__name__)

_PC_BQ_SEM = asyncio.Semaphore(3)

# Sotto questa soglia il benchmark sconto è poco affidabile (dati sparsi / arrotondamenti): non penalizzare ROI.
_MIN_RELIABLE_MEDIA_DISCOUNT = 3.0


def _pc_log(event: str, **fields) -> None:
    msg = " ".join(f"{k}={fields[k]}" for k in sorted(fields) if fields[k] is not None)
    logger.info("promo_creator %s %s", event, msg)


def _pc_discount_cache_key(discount_depth) -> str:
    if discount_depth is None or not str(discount_depth).strip():
        return ""
    try:
        return f"{float(str(discount_depth).strip()):g}"
    except (ValueError, TypeError):
        return str(discount_depth).strip()


def _parse_merged_roi_competitor(rows) -> tuple[list, object]:
    """Da query_roi_and_top_competitor_* → (roi_benchmark list, top_competitor dict|None)."""
    if not rows:
        return [], None
    r0 = rows[0]
    n = int(r0.get("n_promos") or 0)
    roi_bench = [{"avg_roi": r0.get("avg_roi"), "n_promos": n}] if n > 0 else []
    name = r0.get("top_competitor_name")
    if not name:
        return roi_bench, None
    tc = {"brand_name": name, "avg_roi": float(r0.get("top_competitor_avg_roi") or 0)}
    return roi_bench, tc


def _pick_top_competitor_from_rows(top_brands: list, user_brand_id: int, eligible_brand_ids: set[int] | None = None):
    for tb in top_brands or []:
        try:
            tbid = int(tb.get("brand_id") or 0)
        except (TypeError, ValueError):
            tbid = 0
        if tbid == user_brand_id:
            continue
        if eligible_brand_ids is not None and tbid not in eligible_brand_ids:
            continue
        return {
            "brand_name": tb.get("brand_name"),
            "avg_roi": float(tb.get("avg_roi") or 0),
        }
    return None


async def get_promo_creator_suggestions(ps, pe, brand_id, promo_type=None, discount_depth=None, cat=None, subcat=None):
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required", "suggestions": []}
    key = cache_key(
        "pc_v9_promo_diag",
        ps=ps,
        pe=pe,
        brand=brand_id,
        pt=promo_type or "",
        dd=_pc_discount_cache_key(discount_depth),
        cat=cat or "",
        subcat=subcat or "",
    )

    async def _compute():
        async with _PC_BQ_SEM:
            return await _get_promo_creator_suggestions_inner(
                ps, pe, brand_id, promo_type, discount_depth, cat, subcat
            )

    return await compute_once(key, _compute, ttl=TTL_LONG)


async def _get_promo_creator_suggestions_inner(ps, pe, brand_id, promo_type, discount_depth, cat, subcat):
    t_req = time.perf_counter()
    data_path = "unknown"
    precalc_type_aggregate_used = False
    if not is_full_year_period(ps, pe):
        return {"error": PRECALC_ONLY_ERR, "suggestions": []}
    year = int(ps[:4])
    roi_c = int(cat) if cat and 1 <= int(cat) <= 10 else (int(subcat) // 100 if subcat and int(subcat) >= 100 else None)
    try:
        subcat_int = int(subcat) if subcat and str(subcat).strip() else None
    except (TypeError, ValueError):
        subcat_int = None
    use_subcat_roi = subcat_int is not None and subcat_int >= 100

    use_discount_filter = discount_depth is not None and str(discount_depth).strip()
    use_subcat_merge = use_subcat_roi and promo_type and str(promo_type).strip()

    used_precalc_fast = False
    discount_precalc_fb = None
    eligible_brand_ids: set[int] | None = None
    if use_subcat_merge and use_discount_filter and roi_c is not None:
        try:
            ud_fast = float(str(discount_depth).strip())
        except (ValueError, TypeError):
            ud_fast = None
        if ud_fast is not None:
            t_g = time.perf_counter()
            pc_rows, discount_precalc_fb, eligible_brand_rows = await asyncio.gather(
                asyncio.to_thread(
                    safe,
                    query_promo_creator_subcat_from_precalc,
                    year,
                    roi_c,
                    subcat_int,
                    str(promo_type).strip(),
                    ud_fast,
                ),
                asyncio.to_thread(
                    safe, query_category_discount_benchmark_from_precalc, year, int(brand_id), roi_c
                ),
                asyncio.to_thread(
                    safe, promo_creator.query_brand_ids_by_subcategory, subcat_int
                ),
            )
            eligible_brand_ids = {int(r.get("brand_id")) for r in (eligible_brand_rows or []) if r.get("brand_id") is not None}
            np0 = int(pc_rows[0].get("n_promos") or 0) if pc_rows else 0
            _pc_log(
                "phase_gather_done",
                step="precalc_discount_bucket",
                ms=round((time.perf_counter() - t_g) * 1000, 1),
                n_rows=len(pc_rows or []),
                n_promos=np0,
                user_discount=ud_fast,
            )
            if pc_rows and np0 > 0:
                used_precalc_fast = True
                data_path = "subcat_precalc_discount_bucket"
                r0 = pc_rows[0]
                n0 = int(r0.get("n_promos") or 0)
                roi_bench = [{"avg_roi": r0.get("avg_roi"), "n_promos": n0}]
                bid = int(brand_id)
                top_competitor = _pick_top_competitor_from_rows(
                    r0.get("top_brands") or [], bid, eligible_brand_ids
                )
                top_segments = [
                    {
                        "segment_name": s.get("segment_name"),
                        "promo_share_pct": float(s.get("promo_share_pct") or 0),
                    }
                    for s in (r0.get("top_segments") or [])[:3]
                ]
                fb0 = discount_precalc_fb[0] if discount_precalc_fb else {}
                discount_bench = [
                    {
                        "media_avg_discount": float(r0.get("media_avg_discount") or 0),
                        "brand_avg_discount": fb0.get("brand_avg_discount"),
                    }
                ]
                if top_competitor is None:
                    _pc_log(
                        "no_competitor_after_catalog_filter",
                        step="precalc_discount_bucket",
                        n_catalog_brands=len(eligible_brand_ids),
                    )

    if use_subcat_merge and not used_precalc_fast and roi_c is not None:
        t_g = time.perf_counter()
        pc_rows, discount_precalc_fb, eligible_brand_rows = await asyncio.gather(
            asyncio.to_thread(
                safe,
                query_promo_creator_subcat_type_from_precalc,
                year,
                roi_c,
                subcat_int,
                str(promo_type).strip(),
            ),
            asyncio.to_thread(
                safe, query_category_discount_benchmark_from_precalc, year, int(brand_id), roi_c
            ),
            asyncio.to_thread(
                safe, promo_creator.query_brand_ids_by_subcategory, subcat_int
            ),
        )
        eligible_brand_ids = {int(r.get("brand_id")) for r in (eligible_brand_rows or []) if r.get("brand_id") is not None}
        np0 = int(pc_rows[0].get("n_promos") or 0) if pc_rows else 0
        _pc_log(
            "phase_gather_done",
            step="precalc_type_aggregate",
            ms=round((time.perf_counter() - t_g) * 1000, 1),
            n_rows=len(pc_rows or []),
            n_promos=np0,
        )
        if pc_rows and np0 > 0:
            used_precalc_fast = True
            data_path = "subcat_precalc_type_aggregate"
            precalc_type_aggregate_used = True
            r0 = pc_rows[0]
            n0 = int(r0.get("n_promos") or 0)
            roi_bench = [{"avg_roi": r0.get("avg_roi"), "n_promos": n0}]
            bid = int(brand_id)
            top_competitor = _pick_top_competitor_from_rows(
                r0.get("top_brands") or [], bid, eligible_brand_ids
            )
            top_segments = [
                {
                    "segment_name": s.get("segment_name"),
                    "promo_share_pct": float(s.get("promo_share_pct") or 0),
                }
                for s in (r0.get("top_segments") or [])[:3]
            ]
            fb0 = discount_precalc_fb[0] if discount_precalc_fb else {}
            discount_bench = [
                {
                    "media_avg_discount": float(r0.get("media_avg_discount") or 0),
                    "brand_avg_discount": fb0.get("brand_avg_discount"),
                }
            ]
            _pc_log(
                "precalc_type_aggregate_note",
                note="ROI aggregato su tutti i bucket sconto precalc (manca riga dedicata per ±7pp sul tuo sconto).",
            )
            if top_competitor is None:
                _pc_log(
                    "no_competitor_after_catalog_filter",
                    step="precalc_type_aggregate",
                    n_catalog_brands=len(eligible_brand_ids),
                )
        else:
            _pc_log("precalc_type_aggregate_empty", next_step="live_subcat_merge")

    if not used_precalc_fast:
        _pc_log(
            "live_path_enter",
            use_subcat_merge=use_subcat_merge,
            use_discount_filter=use_discount_filter,
            use_subcat_roi=use_subcat_roi,
        )
        if not use_subcat_merge:
            if use_subcat_roi and use_discount_filter:
                roi_task = asyncio.to_thread(
                    safe,
                    promo_creator.query_roi_benchmark_by_type_and_discount_subcat,
                    ps,
                    pe,
                    promo_type,
                    subcat_int,
                    discount_depth,
                )
            elif use_subcat_roi:
                roi_task = asyncio.to_thread(
                    safe, promo_creator.query_roi_benchmark_by_type_subcat, ps, pe, promo_type, subcat_int
                )
            elif use_discount_filter:
                roi_task = asyncio.to_thread(
                    safe,
                    promo_creator.query_roi_benchmark_by_type_and_discount,
                    ps,
                    pe,
                    promo_type,
                    roi_c,
                    discount_depth,
                )
            else:
                roi_task = asyncio.to_thread(
                    safe, query_roi_benchmark_by_type_from_precalc, year, promo_type, roi_c, None
                )

            if use_subcat_roi and use_discount_filter and promo_type:
                competitor_task = asyncio.to_thread(
                    safe,
                    promo_creator.query_top_competitor_by_discount_subcat,
                    ps,
                    pe,
                    subcat_int,
                    promo_type,
                    discount_depth,
                    int(brand_id),
                )
            elif use_subcat_roi and promo_type:
                competitor_task = asyncio.to_thread(
                    safe,
                    promo_creator.query_top_competitor_by_type_subcat,
                    ps,
                    pe,
                    subcat_int,
                    promo_type,
                    int(brand_id),
                )
            elif use_discount_filter and roi_c is not None and promo_type:
                competitor_task = asyncio.to_thread(
                    safe,
                    promo_creator.query_top_competitor_by_discount,
                    ps,
                    pe,
                    roi_c,
                    promo_type,
                    discount_depth,
                    int(brand_id),
                )
            elif roi_c is not None and promo_type:
                competitor_task = asyncio.to_thread(
                    safe, query_top_competitor_roi_from_precalc, year, roi_c, promo_type, int(brand_id)
                )
            else:
                competitor_task = asyncio.to_thread(lambda: [])

        if use_subcat_roi:
            discount_bench_task = asyncio.to_thread(
                safe,
                promo_creator.query_category_discount_benchmark,
                ps,
                pe,
                int(brand_id),
                cat,
                subcat_int,
            )
        else:
            discount_bench_task = asyncio.to_thread(
                safe, query_category_discount_benchmark_from_precalc, year, int(brand_id), roi_c
            )

        segment_task = asyncio.to_thread(
            safe, promo_creator.query_segment_promo_responsiveness, ps, pe, cat, subcat, promo_type
        )

        if use_subcat_merge:
            if use_discount_filter:
                merged_task = asyncio.to_thread(
                    safe,
                    promo_creator.query_roi_and_top_competitor_discount_subcat,
                    ps,
                    pe,
                    promo_type,
                    subcat_int,
                    discount_depth,
                    int(brand_id),
                )
            else:
                merged_task = asyncio.to_thread(
                    safe,
                    promo_creator.query_roi_and_top_competitor_subcat,
                    ps,
                    pe,
                    promo_type,
                    subcat_int,
                    int(brand_id),
                )
            precalc_fb_task = asyncio.to_thread(
                safe, query_category_discount_benchmark_from_precalc, year, int(brand_id), roi_c
            )
            t_g = time.perf_counter()
            discount_bench, merged_rows, top_segments, discount_precalc_fb = await asyncio.gather(
                discount_bench_task,
                merged_task,
                segment_task,
                precalc_fb_task,
            )
            data_path = "live_subcat_merged_discount" if use_discount_filter else "live_subcat_merged_open"
            _pc_log(
                "phase_gather_done",
                step=data_path,
                ms=round((time.perf_counter() - t_g) * 1000, 1),
            )
            discount_bench = list(discount_bench) if discount_bench else []
            top_segments = list(top_segments) if top_segments else []
            roi_bench, top_competitor = _parse_merged_roi_competitor(merged_rows)
        else:
            t_g = time.perf_counter()
            if use_subcat_roi and roi_c is not None:
                precalc_fb_task = asyncio.to_thread(
                    safe, query_category_discount_benchmark_from_precalc, year, int(brand_id), roi_c
                )
                discount_bench, roi_bench, top_segments, top_competitor, discount_precalc_fb = await asyncio.gather(
                    discount_bench_task,
                    roi_task,
                    segment_task,
                    competitor_task,
                    precalc_fb_task,
                )
            else:
                discount_bench, roi_bench, top_segments, top_competitor = await asyncio.gather(
                    discount_bench_task,
                    roi_task,
                    segment_task,
                    competitor_task,
                )
                discount_precalc_fb = None

            data_path = "live_mixed_parallel"
            _pc_log(
                "phase_gather_done",
                step=data_path,
                ms=round((time.perf_counter() - t_g) * 1000, 1),
            )
            discount_bench = list(discount_bench) if discount_bench else []
            top_segments = list(top_segments) if top_segments else []
            roi_bench = list(roi_bench) if roi_bench else []
            top_competitor = list(top_competitor)[0] if top_competitor and len(top_competitor) > 0 else None

    if use_discount_filter and (not roi_bench or len(roi_bench) == 0 or int((roi_bench or [{}])[0].get("n_promos") or 0) == 0):
        _pc_log(
            "roi_followup_n_promos_zero",
            use_subcat_roi=use_subcat_roi,
            data_path=data_path,
        )
        if use_subcat_roi:
            if (not top_competitor) and promo_type:
                roi_tb, tc_list = await asyncio.gather(
                    asyncio.to_thread(
                        safe,
                        promo_creator.query_roi_benchmark_by_type_subcat,
                        ps,
                        pe,
                        promo_type,
                        subcat_int,
                    ),
                    asyncio.to_thread(
                        safe,
                        promo_creator.query_top_competitor_by_type_subcat,
                        ps,
                        pe,
                        subcat_int,
                        promo_type,
                        int(brand_id),
                    ),
                )
                roi_bench = list(roi_tb) if roi_tb else []
                top_competitor = list(tc_list)[0] if tc_list and len(tc_list) > 0 else None
            else:
                roi_tb = await asyncio.to_thread(
                    safe,
                    promo_creator.query_roi_benchmark_by_type_subcat,
                    ps,
                    pe,
                    promo_type,
                    subcat_int,
                )
                roi_bench = list(roi_tb) if roi_tb else []
        else:
            if roi_c is not None and promo_type and not top_competitor:
                roi_tb, tc_list = await asyncio.gather(
                    asyncio.to_thread(safe, query_roi_benchmark_by_type_from_precalc, year, promo_type, roi_c, None),
                    asyncio.to_thread(
                        safe, query_top_competitor_roi_from_precalc, year, roi_c, promo_type, int(brand_id)
                    ),
                )
                roi_bench = list(roi_tb) if roi_tb else []
                top_competitor = list(tc_list)[0] if tc_list and len(tc_list) > 0 else None
            else:
                roi_tb = await asyncio.to_thread(
                    safe, query_roi_benchmark_by_type_from_precalc, year, promo_type, roi_c, None
                )
                roi_bench = list(roi_tb) if roi_tb else []
        use_discount_filter = False

    suggestions = []
    expected_roi = None
    media_avg_discount = None
    if discount_bench and len(discount_bench) > 0:
        row = discount_bench[0]
        media_d = float(row.get("media_avg_discount") or 0)
        media_avg_discount = media_d
        if discount_depth is not None:
            try:
                dd = float(discount_depth)
                if media_d >= _MIN_RELIABLE_MEDIA_DISCOUNT:
                    diff = dd - media_d
                    if diff > 5:
                        suggestions.append({"type": "info", "text": f"Your discount ({dd}%) is {diff:.0f}pp above category average ({media_d}%)."})
                    elif diff < -5:
                        suggestions.append({"type": "info", "text": f"Your discount ({dd}%) is {-diff:.0f}pp below category average ({media_d}%)."})
            except (ValueError, TypeError):
                pass

    if (media_avg_discount is None or float(media_avg_discount or 0) < _MIN_RELIABLE_MEDIA_DISCOUNT) and roi_c is not None:
        fb = discount_precalc_fb
        if fb is None:
            _pc_log("media_discount_fetch_parent_precalc", reason="subcat_media_low_or_missing")
            fb = await asyncio.to_thread(
                safe, query_category_discount_benchmark_from_precalc, year, int(brand_id), roi_c
            )
        if fb and len(fb) > 0:
            alt = float(fb[0].get("media_avg_discount") or 0)
            if alt >= _MIN_RELIABLE_MEDIA_DISCOUNT:
                _pc_log(
                    "media_discount_substitute",
                    from_val=float(media_avg_discount or 0),
                    to_val=alt,
                )
                media_avg_discount = alt
    if roi_bench and len(roi_bench) > 0:
        row = roi_bench[0]
        avg_roi = float(row.get("avg_roi") or 0)
        n = int(row.get("n_promos") or 0)
        if n > 0:
            expected_roi = avg_roi
            if use_discount_filter:
                suggestions.append({"type": "benchmark", "text": f"Similar promos at ~{float(discount_depth):.0f}% discount: avg ROI {avg_roi:.2f}x (n={n})."})
            else:
                suggestions.append({"type": "benchmark", "text": f"Similar promos in category: avg ROI {avg_roi:.2f}x (n={n})."})

    # Penalità monotona solo se la media sconto categoria è attendibile (evita "~0% avg" artefatti).
    if expected_roi is not None and discount_depth is not None and str(discount_depth).strip():
        try:
            dd = float(discount_depth)
            if media_avg_discount is None or float(media_avg_discount) < _MIN_RELIABLE_MEDIA_DISCOUNT:
                pass
            else:
                media_ref = float(media_avg_discount)
                delta = dd - media_ref
                threshold_pp = 10.0
                excess = max(0.0, delta - threshold_pp)
                penalty = 0.09 * excess + 0.0018 * (excess**2)
                if delta > 22:
                    penalty += 0.35
                base_roi = float(expected_roi)
                adj = base_roi - penalty
                if adj < -3.0:
                    adj = -3.0
                if penalty >= 0.05:
                    expected_roi = adj
                    if adj < 0.0 or penalty >= 0.2:
                        suggestions.append(
                            {
                                "type": "warning",
                                "text": (
                                    "ROI atteso ribassato rispetto al benchmark: sconti molto sopra la media storica "
                                    f"nel periodo ({dd:.0f}% vs ~{media_ref:.0f}% media categoria) spesso riducono il rendimento incrementale."
                                ),
                            }
                        )
        except (ValueError, TypeError):
            pass

    if precalc_type_aggregate_used:
        suggestions.insert(
            0,
            {
                "type": "info",
                "text": (
                    "Benchmark: nessuna riga precalc dedicata al tuo sconto (±7pp). "
                    "ROI e competitor usano l'aggregazione precalc su tutti i bucket sconto per questa subcategoria e tipo promo."
                ),
            },
        )

    seg_pcts = [float(s.get("promo_share_pct") or 0) for s in (top_segments or [])[:3]]
    if len(seg_pcts) >= 2:
        spread = max(seg_pcts) - min(seg_pcts)
        if spread < 0.6:
            _pc_log(
                "low_segment_differentiation",
                promo_share_pcts=seg_pcts,
                spread_pp=round(spread, 2),
            )

    n_fin = int((roi_bench or [{}])[0].get("n_promos") or 0) if roi_bench else 0
    if 0 < n_fin < 25:
        _pc_log("low_n_promos_slice", n_promos=n_fin, data_path=data_path)

    out = {
        "suggestions": suggestions,
        "expected_roi": expected_roi,
        "top_competitor": {"brand_name": top_competitor["brand_name"], "avg_roi": float(top_competitor["avg_roi"])} if top_competitor else None,
        "top_segments": [{"segment_name": r["segment_name"], "promo_share_pct": float(r.get("promo_share_pct") or 0)} for r in top_segments[:3]],
        "discount_benchmark": list(discount_bench),
        "roi_benchmark": list(roi_bench) if roi_bench else [],
        "seasonality": [],
        "discount_depth_used": float(discount_depth) if use_discount_filter else None,
        "benchmark_scope": "subcategory" if use_subcat_roi else "parent_category",
    }
    _pc_log(
        "response_done",
        brand_id=brand_id,
        cat=cat,
        subcat=subcat,
        data_path=data_path,
        discount_filter=use_discount_filter,
        expected_roi=expected_roi,
        n_promos=n_fin,
        top_competitor=top_competitor["brand_name"] if top_competitor else None,
        total_ms=round((time.perf_counter() - t_req) * 1000, 1),
    )
    return out
