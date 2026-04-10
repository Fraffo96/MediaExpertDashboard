"""Smoke test seed planner (no BigQuery)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from seed_planner.compiler import compile_seed_profile  # noqa: E402
from seed_planner.sql_patch import apply_compiled_to_sql_content  # noqa: E402
from app.services.seed_profile_v2 import validate_profile_v2  # noqa: E402


def test_compile_and_patch_schema():
    p = {
        "profile_version": 2,
        "global": {"num_orders": 1000, "num_customers": 500, "num_products": 100},
        "segment_rules": {"customer_share_by_segment": {"1": 2, "2": 1, "3": 1, "4": 1, "5": 1, "6": 1}},
    }
    c = compile_seed_profile(p)
    assert c["preview"]["segment_boundaries"]
    assert len(c["sql"]["seg_pref_body"]) > 20
    raw = (ROOT / "bigquery" / "schema_and_seed.sql").read_text(encoding="utf-8")
    out = apply_compiled_to_sql_content(raw, c)
    assert "seg_behavior AS (" in out
    assert "order_dates AS (" in out


def test_validate_v2_guards():
    ok = {
        "profile_version": 2,
        "global": {"num_orders": 100, "num_customers": 50, "num_products": 10},
        "guards": {"max_num_orders": 200, "max_num_customers": 100},
    }
    norm, err = validate_profile_v2(ok)
    assert not err
    bad = dict(ok)
    bad["global"] = dict(ok["global"])
    bad["global"]["num_orders"] = 999999
    _, err2 = validate_profile_v2(bad)
    assert err2
