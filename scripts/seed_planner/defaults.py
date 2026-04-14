"""Default seed (allineati a bigquery/schema_and_seed.sql).

Per tuning senza profilo compilato: modificare qui e le CTE corrispondenti nello schema, oppure
solo lo schema e poi aggiornare questi default per il compiler. Riferimento tabellare: docs/SEED_PIPELINE_AND_WEIGHTS.md (§6).
"""

from __future__ import annotations

# (segment_id, parent_category_id) — da seg_pref nello schema
DEFAULT_SEG_PREF_ROWS: list[tuple[int, int]] = [
    (1, 8),
    (1, 9),
    (1, 3),
    (1, 2),
    (1, 6),
    (1, 1),
    (1, 7),
    (1, 5),
    (2, 1),
    (2, 2),
    (2, 7),
    (2, 3),
    (2, 6),
    (2, 8),
    (2, 5),
    (2, 9),
    (3, 3),
    (3, 2),
    (3, 7),
    (3, 1),
    (3, 6),
    (3, 8),
    (3, 4),
    (3, 9),
    (3, 5),
    (4, 2),
    (4, 7),
    (4, 4),
    (4, 1),
    (4, 8),
    (4, 6),
    (4, 3),
    (4, 5),
    (5, 5),
    (5, 6),
    (5, 1),
    (5, 2),
    (5, 9),
    (5, 8),
    (6, 5),
    (6, 6),
    (6, 1),
    (6, 2),
    (6, 9),
    (6, 8),
    (6, 7),
]

# seg_behavior default (promo_sens, ch_web, ch_app, ch_store, loyalty_prob, prem, inc, churn)
DEFAULT_SEGMENT_BEHAVIOR: dict[int, dict] = {
    1: {"promo_sens": 0.35, "ch_web": 1, "ch_app": 1, "ch_store": 0, "loyalty_prob": 0.65, "prem": 0.5, "inc": "high", "churn": 0.12},
    2: {"promo_sens": 0.42, "ch_web": 1, "ch_app": 1, "ch_store": 0, "loyalty_prob": 0.55, "prem": 0.7, "inc": "high", "churn": 0.10},
    3: {"promo_sens": 0.28, "ch_web": 1, "ch_app": 1, "ch_store": 1, "loyalty_prob": 0.70, "prem": 0.75, "inc": "high", "churn": 0.05},
    4: {"promo_sens": 0.58, "ch_web": 1, "ch_app": 1, "ch_store": 0, "loyalty_prob": 0.25, "prem": 0.2, "inc": "low", "churn": 0.28},
    5: {"promo_sens": 0.48, "ch_web": 1, "ch_app": 0, "ch_store": 1, "loyalty_prob": 0.80, "prem": 0.35, "inc": "low", "churn": 0.08},
    6: {"promo_sens": 0.52, "ch_web": 0, "ch_app": 0, "ch_store": 1, "loyalty_prob": 0.45, "prem": 0.25, "inc": "low", "churn": 0.15},
}

# Default quote boundaries (24k customers) — usati solo se non c'è customer_share
DEFAULT_SEG_BOUNDARIES: list[int] = [3000, 6500, 11800, 18800, 22200]

DEFAULT_PROMO_CURVE = {"slope": 0.58, "intercept": 0.17}

# discount_depth segment bias (schema fact_orders)
DEFAULT_DISCOUNT_BIAS: dict[int, float] = {
    4: 4.0,
    5: 3.0,
    6: 3.5,
    1: -1.5,
    2: -1.0,
    3: -0.5,
}
