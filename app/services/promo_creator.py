"""Promo Creator: suggestions and benchmarks.
Periodo anno intero. Discount benchmark: precalc a livello macro-categoria; con sottocategoria selezionata, media sconto da fact_sales_daily su quella sub.
ROI atteso e top competitor: fact_promo_performance (solo parent 1–10) oppure, con subcategory, stesso modello ROI di MI su fact_sales_daily per quella sub."""
PRECALC_ONLY_ERR = "Precalc tables only. Use full year period (YYYY-01-01 to YYYY-12-31)."
import asyncio

from app.db.queries import promo_creator
from app.db.queries.precalc import (
    is_full_year_period,
    query_category_discount_benchmark_from_precalc,
    query_roi_benchmark_by_type_from_precalc,
    query_top_competitor_roi_from_precalc,
)
from app.services._cache import TTL_LONG, cache_key, get_cached, set_cached, safe

# Sotto questa soglia il benchmark sconto è poco affidabile (dati sparsi / arrotondamenti): non penalizzare ROI.
_MIN_RELIABLE_MEDIA_DISCOUNT = 3.0


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


async def get_promo_creator_suggestions(ps, pe, brand_id, promo_type=None, discount_depth=None, cat=None, subcat=None):
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required", "suggestions": []}
    key = cache_key(
        "pc_v6_benchfix",
        ps=ps,
        pe=pe,
        brand=brand_id,
        pt=promo_type or "",
        dd=_pc_discount_cache_key(discount_depth),
        cat=cat or "",
        subcat=subcat or "",
    )
    cached = get_cached(key)
    if cached is not None:
        return cached
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
                safe, promo_creator.query_roi_benchmark_by_type_and_discount, ps, pe, promo_type, roi_c, discount_depth
            )
        else:
            roi_task = asyncio.to_thread(safe, query_roi_benchmark_by_type_from_precalc, year, promo_type, roi_c, None)

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
        discount_bench, merged_rows, top_segments = await asyncio.gather(
            discount_bench_task,
            merged_task,
            segment_task,
        )
        discount_bench = list(discount_bench) if discount_bench else []
        top_segments = list(top_segments) if top_segments else []
        roi_bench, top_competitor = _parse_merged_roi_competitor(merged_rows)
    else:
        tasks = [
            discount_bench_task,
            roi_task,
            segment_task,
            competitor_task,
        ]
        discount_bench, roi_bench, top_segments, top_competitor = await asyncio.gather(*tasks)

        discount_bench = list(discount_bench) if discount_bench else []
        top_segments = list(top_segments) if top_segments else []
        roi_bench = list(roi_bench) if roi_bench else []
        top_competitor = list(top_competitor)[0] if top_competitor and len(top_competitor) > 0 else None

    if use_discount_filter and (not roi_bench or len(roi_bench) == 0 or int((roi_bench or [{}])[0].get("n_promos") or 0) == 0):
        if use_subcat_roi:
            roi_bench = await asyncio.to_thread(
                safe, promo_creator.query_roi_benchmark_by_type_subcat, ps, pe, promo_type, subcat_int
            )
            if (not top_competitor) and promo_type:
                tc = await asyncio.to_thread(
                    safe,
                    promo_creator.query_top_competitor_by_type_subcat,
                    ps,
                    pe,
                    subcat_int,
                    promo_type,
                    int(brand_id),
                )
                top_competitor = list(tc)[0] if tc and len(tc) > 0 else None
        else:
            roi_bench = await asyncio.to_thread(safe, query_roi_benchmark_by_type_from_precalc, year, promo_type, roi_c, None)
            if roi_c is not None and promo_type and not top_competitor:
                tc = await asyncio.to_thread(safe, query_top_competitor_roi_from_precalc, year, roi_c, promo_type, int(brand_id))
                top_competitor = list(tc)[0] if tc and len(tc) > 0 else None
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
        fb = await asyncio.to_thread(
            safe, query_category_discount_benchmark_from_precalc, year, int(brand_id), roi_c
        )
        if fb and len(fb) > 0:
            alt = float(fb[0].get("media_avg_discount") or 0)
            if alt >= _MIN_RELIABLE_MEDIA_DISCOUNT:
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
    set_cached(key, out, TTL_LONG)
    return out
