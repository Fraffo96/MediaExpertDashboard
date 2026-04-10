"""Compila profilo seed v2 in strutture per preview + patch SQL."""
from __future__ import annotations

import json
import math
from datetime import datetime
from typing import Any

from seed_planner.defaults import (
    DEFAULT_DISCOUNT_BIAS,
    DEFAULT_PROMO_CURVE,
    DEFAULT_SEG_BOUNDARIES,
    DEFAULT_SEG_PREF_ROWS,
    DEFAULT_SEGMENT_BEHAVIOR,
)


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _int_seg(k: Any) -> int:
    try:
        return int(str(k))
    except (TypeError, ValueError):
        return 0


def _normalize_shares(raw: dict[Any, Any] | None) -> dict[int, float]:
    if not raw:
        return {i: 1.0 / 6 for i in range(1, 7)}
    vals: dict[int, float] = {}
    for k, v in raw.items():
        seg = _int_seg(k)
        if 1 <= seg <= 6:
            vals[seg] = max(0.0, _f(v, 0.0))
    s = sum(vals.get(i, 0.0) for i in range(1, 7))
    if s <= 0:
        return {i: 1.0 / 6 for i in range(1, 7)}
    return {i: vals.get(i, 0.0) / s for i in range(1, 7)}


def _boundaries_from_shares(num_customers: int, shares: dict[int, float]) -> list[int]:
    """Ritorna 5 soglie (<= soglia -> segmento 1..5), oltre -> 6."""
    bounds: list[int] = []
    cum = 0.0
    for seg in range(1, 6):
        cum += shares[seg] * num_customers
        bounds.append(max(1, min(num_customers, int(math.floor(cum)))))
    # evita non-monotonic per arrotondamenti
    for i in range(1, len(bounds)):
        if bounds[i] < bounds[i - 1]:
            bounds[i] = bounds[i - 1]
    return bounds


def _build_seg_behavior_sql(beh: dict[int, dict]) -> str:
    lines: list[str] = []
    for seg in range(1, 7):
        b = beh.get(seg) or DEFAULT_SEGMENT_BEHAVIOR[seg]
        ps = _f(b.get("promo_sens"), DEFAULT_SEGMENT_BEHAVIOR[seg]["promo_sens"])
        cw = int(b.get("ch_web", DEFAULT_SEGMENT_BEHAVIOR[seg]["ch_web"]))
        ca = int(b.get("ch_app", DEFAULT_SEGMENT_BEHAVIOR[seg]["ch_app"]))
        cs = int(b.get("ch_store", DEFAULT_SEGMENT_BEHAVIOR[seg]["ch_store"]))
        lp = _f(b.get("loyalty_prob"), DEFAULT_SEGMENT_BEHAVIOR[seg]["loyalty_prob"])
        prem = _f(b.get("prem"), DEFAULT_SEGMENT_BEHAVIOR[seg]["prem"])
        inc = str(b.get("inc", DEFAULT_SEGMENT_BEHAVIOR[seg]["inc"])).replace("'", "")
        ch = _f(b.get("churn"), DEFAULT_SEGMENT_BEHAVIOR[seg]["churn"])
        if seg == 1:
            lines.append(
                f"  SELECT {seg} AS seg, {ps} AS promo_sens, {cw} AS ch_web, {ca} AS ch_app, {cs} AS ch_store, "
                f"{lp} AS loyalty_prob, {prem} AS prem, '{inc}' AS inc, {ch} AS churn UNION ALL"
            )
        elif seg < 6:
            lines.append(
                f"  SELECT {seg}, {ps}, {cw}, {ca}, {cs}, {lp}, {prem}, '{inc}', {ch} UNION ALL"
            )
        else:
            lines.append(
                f"  SELECT {seg}, {ps}, {cw}, {ca}, {cs}, {lp}, {prem}, '{inc}', {ch}"
            )
    return "\n".join(lines)


def _build_seg_assign_case(bounds: list[int]) -> str:
    b1, b2, b3, b4, b5 = bounds
    return (
        f"    WHEN id.customer_id <= {b1} THEN 1\n"
        f"    WHEN id.customer_id <= {b2} THEN 2\n"
        f"    WHEN id.customer_id <= {b3} THEN 3\n"
        f"    WHEN id.customer_id <= {b4} THEN 4\n"
        f"    WHEN id.customer_id <= {b5} THEN 5\n"
        f"    ELSE 6"
    )


def _build_promo_sens_case(beh: dict[int, dict]) -> str:
    parts = []
    for seg in range(1, 7):
        ps = _f((beh.get(seg) or DEFAULT_SEGMENT_BEHAVIOR[seg]).get("promo_sens"))
        parts.append(f"            WHEN {seg} THEN {ps}")
    parts.append("            ELSE 0.52")
    return "\n".join(parts)


def _build_discount_bias_case(bias: dict[int, float]) -> str:
    # Stesso ordine CASE dello schema (4,5,6,1,2,3 poi ELSE 0.0)
    order = [4, 5, 6, 1, 2, 3]
    out = []
    for seg in order:
        v = bias.get(seg, DEFAULT_DISCOUNT_BIAS.get(seg, 0.0))
        out.append(f"          WHEN {seg} THEN {v}")
    out.append("          ELSE 0.0")
    return "\n".join(out)


def _build_seg_pref_sql(rows: list[tuple[int, int]]) -> str:
    lines: list[str] = []
    for i, (seg, pc) in enumerate(rows):
        if i == 0:
            lines.append(f"  SELECT {seg} AS segment_id, {pc} AS parent_category_id")
        else:
            lines.append(f"  UNION ALL SELECT {seg}, {pc}")
    return "\n".join(lines)


def _parse_date_range(g: dict) -> tuple[str, int]:
    dr = g.get("date_range")
    start = "2023-01-01"
    span = 1461
    if isinstance(dr, dict) and dr.get("start") and dr.get("end"):
        try:
            s = datetime.strptime(str(dr["start"])[:10], "%Y-%m-%d").date()
            e = datetime.strptime(str(dr["end"])[:10], "%Y-%m-%d").date()
            start = s.isoformat()
            span = max(1, (e - s).days + 1)
        except ValueError:
            pass
    if g.get("order_date_span_days") is not None:
        try:
            span = max(1, int(g["order_date_span_days"]))
        except (TypeError, ValueError):
            pass
    if g.get("order_date_start"):
        start = str(g["order_date_start"])[:10]
    return start, span


def compile_seed_profile(profile: dict) -> dict[str, Any]:
    """Ritorna dict con patch SQL + preview + env hints."""
    g = profile.get("global") or {}
    num_c = int(g.get("num_customers") or 24000)
    num_o = int(g.get("num_orders") or 380000)

    sr = profile.get("segment_rules") or {}
    shares = _normalize_shares(sr.get("customer_share_by_segment"))
    bounds = _boundaries_from_shares(num_c, shares)
    if sr.get("use_legacy_boundaries"):
        bounds = list(DEFAULT_SEG_BOUNDARIES)

    beh_src = profile.get("segment_behavior") or {}
    beh: dict[int, dict] = {}
    for seg in range(1, 7):
        base = dict(DEFAULT_SEGMENT_BEHAVIOR[seg])
        ov = beh_src.get(str(seg)) or beh_src.get(seg) or {}
        if isinstance(ov, dict):
            base.update(ov)
        beh[seg] = base

    curve = dict(DEFAULT_PROMO_CURVE)
    pc = (profile.get("promo_rules") or {}).get("promo_curve") or g.get("promo_curve")
    if isinstance(pc, dict):
        if pc.get("slope") is not None:
            curve["slope"] = _f(pc["slope"], curve["slope"])
        if pc.get("intercept") is not None:
            curve["intercept"] = _f(pc["intercept"], curve["intercept"])

    disc_bias = dict(DEFAULT_DISCOUNT_BIAS)
    db = (profile.get("promo_rules") or {}).get("segment_discount_bias") or {}
    if isinstance(db, dict):
        for k, v in db.items():
            seg = _int_seg(k)
            if 1 <= seg <= 6:
                disc_bias[seg] = _f(v, 0.0)

    # seg_pref
    pcmap = sr.get("parent_categories") or {}
    rows: list[tuple[int, int]] = []
    if isinstance(pcmap, dict) and pcmap:
        for k, v in pcmap.items():
            seg = _int_seg(k)
            if not (1 <= seg <= 6):
                continue
            if isinstance(v, list):
                for x in v:
                    try:
                        pid = int(x)
                        if 1 <= pid <= 10:
                            rows.append((seg, pid))
                    except (TypeError, ValueError):
                        pass
    if not rows:
        rows = list(DEFAULT_SEG_PREF_ROWS)

    order_start, order_span = _parse_date_range(g)

    seg_behavior_sql = _build_seg_behavior_sql(beh)
    seg_assign_case = _build_seg_assign_case(bounds)
    promo_sens_case = _build_promo_sens_case(beh)
    discount_case = _build_discount_bias_case(disc_bias)
    seg_pref_sql = _build_seg_pref_sql(rows)

    mean_promo_sens = sum(_f(beh[i].get("promo_sens")) for i in range(1, 7)) / 6.0
    approx_thresh = max(0.22, min(0.87, mean_promo_sens * curve["slope"] + curve["intercept"]))

    brand_focus = (profile.get("brand_rules") or {}).get("focus_override")
    brand_promo_aff = (profile.get("brand_rules") or {}).get("promo_affinity")

    compiled = {
        "global": {
            "num_customers": num_c,
            "num_orders": num_o,
            "num_products": int(g.get("num_products") or 1200),
            "order_date_start": order_start,
            "order_date_span_days": order_span,
            "promo_curve": curve,
        },
        "segment_customer_shares": shares,
        "segment_boundaries": bounds,
        "sql": {
            "seg_behavior_body": seg_behavior_sql,
            "seg_assign_case": seg_assign_case,
            "promo_sens_case": promo_sens_case,
            "discount_bias_case": discount_case,
            "seg_pref_body": seg_pref_sql,
        },
        "env": {
            "SEED_BRAND_FOCUS_JSON": json.dumps(brand_focus) if isinstance(brand_focus, dict) else "",
            "SEED_BRAND_PROMO_AFFINITY_JSON": json.dumps(brand_promo_aff) if isinstance(brand_promo_aff, dict) else "",
        },
        "preview": {
            "segment_shares_expected": {str(i): shares[i] for i in range(1, 7)},
            "segment_boundaries": bounds,
            "mean_promo_sensitivity": round(mean_promo_sens, 4),
            "approx_order_promo_threshold": round(approx_thresh, 4),
            "parent_category_rows": len(rows),
        },
    }
    return compiled


def compile_from_json(s: str) -> dict[str, Any]:
    return compile_seed_profile(json.loads(s))
