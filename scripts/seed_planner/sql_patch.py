"""Applica output di compile_seed_profile al testo di schema_and_seed.sql."""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any


def _replace_seg_behavior(content: str, body: str) -> str:
    pat = re.compile(
        r"seg_behavior AS \(\n(?:.*\n)*?  SELECT 6,.*?\n\),\nids AS",
        re.MULTILINE,
    )
    repl = f"seg_behavior AS (\n{body}\n),\nids AS"
    m = pat.search(content)
    if not m:
        raise ValueError("sql_patch: blocco seg_behavior non trovato")
    return content[: m.start()] + repl + content[m.end() :]


def _replace_seg_assign_case(content: str, case_block: str) -> str:
    pat = re.compile(
        r"JOIN seg_behavior sb ON sb\.seg = CASE\n(?:    WHEN id\.customer_id <= \d+ THEN \d+\n)+    ELSE 6\n  END",
        re.MULTILINE,
    )
    repl = f"JOIN seg_behavior sb ON sb.seg = CASE\n{case_block}\n  END"
    m = pat.search(content)
    if not m:
        raise ValueError("sql_patch: blocco seg_assign CASE non trovato")
    return content[: m.start()] + repl + content[m.end() :]


def _replace_order_dates(content: str, start_date: str, span_days: int) -> str:
    pat = re.compile(
        r"order_dates AS \(\n"
        r"  SELECT i, DATE_ADD\(DATE\('[^']+'\), INTERVAL MOD\(ABS\(FARM_FINGERPRINT\(CAST\(i AS STRING\)\)\), \d+\) DAY\) AS dt\n"
        r"  FROM nums\n\),",
        re.MULTILINE,
    )
    repl = (
        f"order_dates AS (\n"
        f"  SELECT i, DATE_ADD(DATE('{start_date}'), INTERVAL MOD(ABS(FARM_FINGERPRINT(CAST(i AS STRING))), {int(span_days)}) DAY) AS dt\n"
        f"  FROM nums\n),"
    )
    m = pat.search(content)
    if not m:
        raise ValueError("sql_patch: blocco order_dates non trovato")
    return content[: m.start()] + repl + content[m.end() :]


def _replace_promo_sensitivity(content: str, case_lines: str, slope: float, intercept: float) -> str:
    pat = re.compile(
        r"          CASE op\.segment_id\n"
        r"            WHEN 1 THEN 0\.35 WHEN 2 THEN 0\.42 WHEN 3 THEN 0\.28\n"
        r"            WHEN 4 THEN 0\.58 WHEN 5 THEN 0\.48 ELSE 0\.52\n"
        r"          END \* 0\.58 \+ 0\.17",
        re.MULTILINE,
    )
    inner = f"          CASE op.segment_id\n{case_lines}\n          END * {slope} + {intercept}"
    m = pat.search(content)
    if not m:
        raise ValueError("sql_patch: blocco promo_sens non trovato")
    return content[: m.start()] + inner + content[m.end() :]


def _replace_discount_bias(content: str, case_block: str) -> str:
    pat = re.compile(
        r"\+ CASE gen\.segment_id\n          WHEN 4 THEN 4\.0 WHEN 5 THEN 3\.0 WHEN 6 THEN 3\.5\n"
        r"          WHEN 1 THEN -1\.5 WHEN 2 THEN -1\.0 WHEN 3 THEN -0\.5\n          ELSE 0\.0\n        END",
        re.MULTILINE,
    )
    repl = f"+ CASE gen.segment_id\n{case_block}\n        END"
    m = pat.search(content)
    if not m:
        raise ValueError("sql_patch: blocco discount_depth non trovato")
    return content[: m.start()] + repl + content[m.end() :]


def _replace_seg_pref(content: str, body: str) -> str:
    pat = re.compile(
        r"seg_pref AS \(\n(?:  SELECT.*\n|  UNION ALL SELECT.*\n)+?\),\nch_pref AS",
        re.MULTILINE,
    )
    repl = f"seg_pref AS (\n{body}\n),\nch_pref AS"
    m = pat.search(content)
    if not m:
        raise ValueError("sql_patch: blocco seg_pref non trovato")
    return content[: m.start()] + repl + content[m.end() :]


def apply_compiled_to_sql_content(content: str, compiled: dict[str, Any]) -> str:
    sql = compiled.get("sql") or {}
    g = compiled.get("global") or {}
    curve = g.get("promo_curve") or {"slope": 0.58, "intercept": 0.17}
    slope = float(curve.get("slope", 0.58))
    intercept = float(curve.get("intercept", 0.17))

    c = content
    c = _replace_seg_behavior(c, sql["seg_behavior_body"])
    c = _replace_seg_assign_case(c, sql["seg_assign_case"].strip())
    c = _replace_order_dates(c, str(g.get("order_date_start", "2023-01-01")), int(g.get("order_date_span_days", 1461)))
    c = _replace_promo_sensitivity(c, sql["promo_sens_case"], slope, intercept)
    c = _replace_discount_bias(c, sql["discount_bias_case"])
    c = _replace_seg_pref(c, sql["seg_pref_body"])
    return c


def load_compiled_from_env() -> dict[str, Any] | None:
    path = (os.environ.get("SEED_COMPILED_PATH") or "").strip()
    if path:
        fp = Path(path)
        if fp.is_file():
            return json.loads(fp.read_text(encoding="utf-8"))
    raw = (os.environ.get("SEED_COMPILED_JSON") or "").strip()
    if raw:
        return json.loads(raw)
    return None
