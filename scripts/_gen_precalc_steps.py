"""One-off: genera scripts/precalc_refresh/sql_steps.py da refresh_precalc_tables.py."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent
src = (ROOT / "refresh_precalc_tables.py").read_text(encoding="utf-8")
start = src.index("    # 1. precalc_sales_agg")
_m = '    if not run_query(client, sql_mkt_peak, "precalc_mkt_purchasing_peak"):\n        sys.exit(1)'
end = src.index(_m) + len(_m)
body = src[start:end]
lines_out = []
for line in body.splitlines():
    if line.startswith("    "):
        lines_out.append(line[4:])
    else:
        lines_out.append(line)
core = "\n".join(lines_out)
# Trasforma pattern esecuzione in append a steps
replacements = [
    (
        'if not run_query(client, sql_sales, "precalc_sales_agg"):\n    sys.exit(1)',
        'steps.append(("precalc_sales_agg", sql_sales))',
    ),
    (
        'if not run_query(client, sql_peak, "precalc_peak_agg"):\n    sys.exit(1)',
        'steps.append(("precalc_peak_agg", sql_peak))',
    ),
    (
        'if not run_query(client, sql_roi, "precalc_roi_agg"):\n    sys.exit(1)',
        'steps.append(("precalc_roi_agg", sql_roi))',
    ),
    (
        'if not run_query(client, sql_inc, "precalc_incremental_yoy"):\n    sys.exit(1)',
        'steps.append(("precalc_incremental_yoy", sql_inc))',
    ),
    (
        'if not run_query(client, sql_pie_cat, "precalc_pie_brands_category"):\n    sys.exit(1)',
        'steps.append(("precalc_pie_brands_category", sql_pie_cat))',
    ),
    (
        'if not run_query(client, sql_pie_sub, "precalc_pie_brands_subcategory"):\n    sys.exit(1)',
        'steps.append(("precalc_pie_brands_subcategory", sql_pie_sub))',
    ),
    (
        'if not run_query(client, sql_prev, "precalc_prev_year_pct"):\n    sys.exit(1)',
        'steps.append(("precalc_prev_year_pct", sql_prev))',
    ),
    (
        'if not run_query(client, sql_bar_cat, "precalc_sales_bar_category"):\n    sys.exit(1)',
        'steps.append(("precalc_sales_bar_category", sql_bar_cat))',
    ),
    (
        'if not run_query(client, sql_bar_sub, "precalc_sales_bar_subcategory"):\n    sys.exit(1)',
        'steps.append(("precalc_sales_bar_subcategory", sql_bar_sub))',
    ),
    (
        'if not run_query(client, sql_pc, "precalc_promo_creator_benchmark"):\n    sys.exit(1)',
        'steps.append(("precalc_promo_creator_benchmark", sql_pc))',
    ),
    (
        'if not run_query(client, sql_promo_live, "precalc_promo_live_sku"):\n    sys.exit(1)',
        'steps.append(("precalc_promo_live_sku", sql_promo_live))',
    ),
    (
        'if not run_query(client, sql_mkt_cat, "precalc_mkt_segment_categories"):\n    sys.exit(1)',
        'steps.append(("precalc_mkt_segment_categories", sql_mkt_cat))',
    ),
    (
        'if not run_query(client, sql_mkt_skus, "precalc_mkt_segment_skus"):\n    sys.exit(1)',
        'steps.append(("precalc_mkt_segment_skus", sql_mkt_skus))',
    ),
    (
        'if not run_query(client, sql_mi_seg_by_prod, "precalc_mi_segment_by_product"):\n    sys.exit(1)',
        'steps.append(("precalc_mi_segment_by_product", sql_mi_seg_by_prod))',
    ),
    (
        'for _tbl in ("precalc_mkt_purchasing_channel", "precalc_mkt_purchasing_peak"):\n'
        '    run_query(client, f"DROP TABLE IF EXISTS `{PROJECT_ID}.{DATASET}.{_tbl}`", f"drop {_tbl} (recreate clustering)")',
        'for _tbl in ("precalc_mkt_purchasing_channel", "precalc_mkt_purchasing_peak"):\n'
        '    steps.append((f"drop_{_tbl}", f"DROP TABLE IF EXISTS `{project_id}.{dataset}.{_tbl}`"))',
    ),
    (
        'if not run_query(client, sql_mkt_ch, "precalc_mkt_purchasing_channel"):\n    sys.exit(1)',
        'steps.append(("precalc_mkt_purchasing_channel", sql_mkt_ch))',
    ),
    (
        'if not run_query(client, sql_mkt_peak, "precalc_mkt_purchasing_peak"):\n    sys.exit(1)',
        'steps.append(("precalc_mkt_purchasing_peak", sql_mkt_peak))',
    ),
]
for old, new in replacements:
    if old not in core:
        raise SystemExit(f"missing block:\n{old[:80]}")
    core = core.replace(old, new)
# DATASET -> dataset, PROJECT_ID -> project_id nelle f-string delle query (già usano {DATASET})
core = core.replace("{DATASET}", "{dataset}").replace("{PROJECT_ID}", "{project_id}")
raw_indented = "\n".join(("    " + line if line.strip() else line) for line in core.splitlines())


def _dedent_fstring_sql_bodies(text: str) -> str:
    lines = text.splitlines()
    out: list[str] = []
    in_sql = False
    for line in lines:
        if not in_sql and "= f" in line and '"""' in line:
            in_sql = True
            out.append(line)
            continue
        if in_sql:
            if line.strip() == '"""':
                in_sql = False
                out.append('"""')
                continue
            if line.startswith("    "):
                out.append(line[4:])
            else:
                out.append(line)
            continue
        out.append(line)
    return "\n".join(out)


indented = _dedent_fstring_sql_bodies(raw_indented)
header = '''"""Step SQL precalc — generato da _gen_precalc_steps.py; non editare a mano (rigenera)."""
from __future__ import annotations


def build_sql_steps(dataset: str, project_id: str) -> list[tuple[str, str]]:
    steps: list[tuple[str, str]] = []
'''
footer = "\n    return steps\n"
out_dir = ROOT / "precalc_refresh"
out_dir.mkdir(exist_ok=True)
(out_dir / "sql_steps.py").write_text(header + indented + footer, encoding="utf-8")
print("Wrote", out_dir / "sql_steps.py")
