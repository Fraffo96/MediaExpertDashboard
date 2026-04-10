"""Promo Creator: suggestions and benchmarks for promo planning."""
from google.cloud import bigquery
from app.db.client import run_query


def query_category_discount_benchmark(ps, pe, brand_id, cat=None, subcat=None):
    """Average discount depth in category for benchmark."""
    roi_cat = int(cat) if cat and 1 <= int(cat) <= 10 else (int(subcat) // 100 if subcat and int(subcat) >= 100 else None)
    # Con categoria + sottocategoria: solo righe coerenti (parent = cat e category_id = subcat).
    try:
        c_int = int(cat) if cat and str(cat).strip() and 1 <= int(cat) <= 10 else None
        s_int = int(subcat) if subcat and str(subcat).strip() and int(subcat) >= 100 else None
    except (TypeError, ValueError):
        c_int, s_int = None, None
    if c_int is not None and s_int is not None:
        where_cat = "AND f.parent_category_id = @cat AND f.category_id = @subcat"
        where_subcat = ""
    else:
        where_cat = "AND (@cat IS NULL OR f.parent_category_id = @cat OR f.category_id = @cat)"
        where_subcat = "AND (@subcat IS NULL OR f.category_id = @subcat)"
    q = f"""
    SELECT
      ROUND(COALESCE(SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
            / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0), 0), 1) AS media_avg_discount,
      ROUND(COALESCE(SUM(CASE WHEN f.promo_flag AND f.brand_id = @brand THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
            / NULLIF(SUM(CASE WHEN f.promo_flag AND f.brand_id = @brand THEN f.gross_pln ELSE 0 END), 0), 0), 1) AS brand_avg_discount
    FROM mart.fact_sales_daily f
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      {where_cat}
      {where_subcat}
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id) if brand_id else None),
        bigquery.ScalarQueryParameter("cat", "INT64", int(cat) if cat and 1 <= int(cat) <= 10 else None),
        bigquery.ScalarQueryParameter("subcat", "INT64", int(subcat) if subcat and int(subcat) >= 100 else None),
    ]
    return run_query(q, params)


# promo_id -> nominal discount % (from derive pcfg)
_PROMO_DISCOUNT_MAP = {1: 10, 2: 20, 3: 30, 4: 15, 5: 15, 6: 12, 7: 8, 8: 18, 9: 25, 10: 20}

# ROI da fact_sales_daily per subcategoria (stessa logica di market_intelligence/promo.py query_promo_roi_brand_vs_media).
# fact_promo_performance è solo a livello parent 1–10; qui il benchmark dipende dalla subcategory scelta.
_ROI_SUBCAT_CTE = """
WITH pcfg AS (
  SELECT 1 AS pid, 1.52 AS br UNION ALL SELECT 2 AS pid, 1.02 AS br UNION ALL SELECT 3 AS pid, 0.68 AS br
  UNION ALL SELECT 4 AS pid, 1.38 AS br UNION ALL SELECT 5 AS pid, 1.18 AS br UNION ALL SELECT 6 AS pid, 1.92 AS br
  UNION ALL SELECT 7 AS pid, 1.48 AS br UNION ALL SELECT 8 AS pid, 1.02 AS br UNION ALL SELECT 9 AS pid, 0.88 AS br UNION ALL SELECT 10 AS pid, 0.82 AS br
),
yadj AS (
  SELECT 2023 AS yr, 1.15 AS ra UNION ALL SELECT 2024, 1.00 UNION ALL SELECT 2025, 0.85 UNION ALL SELECT 2026, 0.85
),
agg AS (
  SELECT f.promo_id, f.brand_id, f.date,
    ANY_VALUE(f.parent_category_id) AS pcat,
    SUM(f.gross_pln) AS att
  FROM mart.fact_sales_daily f
  WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    AND f.promo_flag AND f.promo_id IS NOT NULL AND f.category_id = @subcat
    {agg_promo_filter}
  GROUP BY f.promo_id, f.brand_id, f.date
),
non_promo AS (
  SELECT brand_id, category_id, date, SUM(gross_pln) AS gross
  FROM mart.fact_sales_daily
  WHERE NOT promo_flag AND category_id = @subcat AND date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
  GROUP BY brand_id, category_id, date
),
baseline AS (
  SELECT a.promo_id, a.brand_id, a.date, a.pcat, a.att,
    AVG(np.gross) AS bl
  FROM agg a
  LEFT JOIN non_promo np ON np.brand_id = a.brand_id
    AND np.date BETWEEN DATE_SUB(a.date, INTERVAL 28 DAY) AND DATE_SUB(a.date, INTERVAL 1 DAY)
  GROUP BY a.promo_id, a.brand_id, a.date, a.pcat, a.att
),
roi_computed AS (
  SELECT b.promo_id, b.brand_id,
    ROUND(
      (
        (p.br * y.ra + 0.04 * (MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(b.date AS STRING), CAST(b.promo_id AS STRING), CAST(b.brand_id AS STRING)))), 21) - 10) / 10.0)
        * (0.76 + 0.42 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('bmul', CAST(b.brand_id AS STRING)))), 1000) / 1000.0))
        + 0.28 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('padj', CAST(b.brand_id AS STRING), '|', CAST(b.promo_id AS STRING)))), 21) - 10) / 10.0
      )
      * (0.80 + 0.42 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('pcat', CAST(b.pcat AS STRING)))), 1000) / 1000.0))
      * (0.74 + 0.48 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('ptype', COALESCE(dm.promo_type, 'na'), '|', CAST(b.pcat AS STRING)))), 1000) / 1000.0)),
      4) AS roi
  FROM baseline b
  JOIN pcfg p ON p.pid = b.promo_id
  JOIN yadj y ON y.yr = EXTRACT(YEAR FROM b.date)
  JOIN mart.dim_promo dm ON dm.promo_id = b.promo_id AND dm.promo_type = @pt
)
"""


def query_roi_benchmark_by_type(ps, pe, brand_id, promo_type, cat=None):
    """ROI benchmark for similar promo type in category."""
    q = """
    SELECT ROUND(AVG(fp.roi), 2) AS avg_roi, COUNT(*) AS n_promos
    FROM mart.fact_promo_performance fp
    JOIN mart.dim_promo p ON p.promo_id = fp.promo_id
    WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND (@pt IS NULL OR p.promo_type = @pt)
      AND (@cat IS NULL OR fp.category_id = @cat)
      AND (@brand IS NULL OR fp.brand_id = @brand)
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("pt", "STRING", promo_type if promo_type else None),
        bigquery.ScalarQueryParameter("cat", "INT64", int(cat) if cat and 1 <= int(cat) <= 10 else None),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id) if brand_id else None),
    ])


def query_roi_benchmark_by_type_and_discount(ps, pe, promo_type, roi_cat, discount_depth, dd_tolerance=7):
    """ROI benchmark for promos with similar discount depth. Uses fact_promo_performance + promo_id discount mapping."""
    try:
        dd = float(discount_depth)
    except (TypeError, ValueError):
        return []
    dd_min = max(0, dd - dd_tolerance)
    dd_max = min(100, dd + dd_tolerance)
    promo_ids = [pid for pid, d in _PROMO_DISCOUNT_MAP.items() if dd_min <= d <= dd_max]
    if not promo_ids:
        return []
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ArrayQueryParameter("pids", "INT64", promo_ids),
    ]
    where_pt = "AND (@pt IS NULL OR p.promo_type = @pt)"
    if promo_type:
        params.append(bigquery.ScalarQueryParameter("pt", "STRING", promo_type))
    else:
        where_pt = ""
    where_cat = "AND (@roi_cat IS NULL OR fp.category_id = @roi_cat)"
    if roi_cat is not None:
        params.append(bigquery.ScalarQueryParameter("roi_cat", "INT64", roi_cat))
    else:
        where_cat = ""
    q = f"""
    SELECT ROUND(AVG(fp.roi), 2) AS avg_roi, COUNT(*) AS n_promos
    FROM mart.fact_promo_performance fp
    JOIN mart.dim_promo p ON p.promo_id = fp.promo_id
    WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND fp.promo_id IN UNNEST(@pids) {where_pt} {where_cat}
    """
    return run_query(q, params)


def query_top_competitor_by_discount(ps, pe, roi_cat, promo_type, discount_depth, exclude_brand_id, dd_tolerance=7):
    """Top competitor by ROI for promos with similar discount depth."""
    try:
        dd = float(discount_depth)
    except (TypeError, ValueError):
        return []
    dd_min = max(0, dd - dd_tolerance)
    dd_max = min(100, dd + dd_tolerance)
    promo_ids = [pid for pid, d in _PROMO_DISCOUNT_MAP.items() if dd_min <= d <= dd_max]
    if not promo_ids or roi_cat is None or not promo_type:
        return []
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("cat", "INT64", roi_cat),
        bigquery.ScalarQueryParameter("pt", "STRING", promo_type),
        bigquery.ScalarQueryParameter("exclude", "INT64", exclude_brand_id),
        bigquery.ArrayQueryParameter("pids", "INT64", promo_ids),
    ]
    q = """
    SELECT b.brand_name, ROUND(AVG(fp.roi), 2) AS avg_roi
    FROM mart.fact_promo_performance fp
    JOIN mart.dim_promo p ON p.promo_id = fp.promo_id
    JOIN mart.dim_brand b ON b.brand_id = fp.brand_id
    WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND fp.category_id = @cat AND p.promo_type = @pt AND fp.brand_id != @exclude
      AND fp.promo_id IN UNNEST(@pids)
    GROUP BY fp.brand_id, b.brand_name
    ORDER BY AVG(fp.roi) DESC
    LIMIT 1
    """
    return run_query(q, params)


def query_roi_benchmark_by_type_and_discount_subcat(
    ps, pe, promo_type, subcat, discount_depth, dd_tolerance=7
):
    """ROI medio su subcategoria + tipo promo + promo_id con profondità sconto simile (da sales daily, non parent)."""
    rows = query_roi_and_top_competitor_discount_subcat(
        ps, pe, promo_type, subcat, discount_depth, exclude_brand_id=-1, dd_tolerance=dd_tolerance
    )
    if not rows:
        return []
    r = rows[0]
    return [{"avg_roi": r.get("avg_roi"), "n_promos": r.get("n_promos")}]


def query_roi_and_top_competitor_discount_subcat(
    ps, pe, promo_type, subcat, discount_depth, exclude_brand_id, dd_tolerance=7
):
    """Stesso CTE ROI con filtro promo_id per bucket sconto; benchmark + top competitor in una query."""
    try:
        sid = int(subcat)
        if sid < 100:
            return []
        dd = float(discount_depth)
    except (TypeError, ValueError):
        return []
    dd_min = max(0, dd - dd_tolerance)
    dd_max = min(100, dd + dd_tolerance)
    promo_ids = [pid for pid, d in _PROMO_DISCOUNT_MAP.items() if dd_min <= d <= dd_max]
    if not promo_ids or not (promo_type and str(promo_type).strip()):
        return []
    excl = int(exclude_brand_id)
    params: list = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("subcat", "INT64", sid),
        bigquery.ScalarQueryParameter("pt", "STRING", promo_type),
        bigquery.ArrayQueryParameter("pids", "INT64", promo_ids),
    ]
    if excl >= 0:
        competitor_sql = """
, topc AS (
  SELECT b.brand_name AS top_competitor_name, ROUND(AVG(r.roi), 2) AS top_competitor_avg_roi
  FROM roi_computed r
  JOIN mart.dim_brand b ON b.brand_id = r.brand_id
  WHERE r.brand_id != @exclude
  GROUP BY r.brand_id, b.brand_name
  ORDER BY AVG(r.roi) DESC
  LIMIT 1
)
SELECT o.avg_roi, o.n_promos, t.top_competitor_name, t.top_competitor_avg_roi
FROM overall o
LEFT JOIN topc t ON TRUE
"""
        params.append(bigquery.ScalarQueryParameter("exclude", "INT64", excl))
    else:
        competitor_sql = """
SELECT o.avg_roi, o.n_promos, CAST(NULL AS STRING) AS top_competitor_name, CAST(NULL AS FLOAT64) AS top_competitor_avg_roi
FROM overall o
"""
    q = (
        _ROI_SUBCAT_CTE.format(agg_promo_filter="AND f.promo_id IN UNNEST(@pids)")
        + """
, overall AS (
  SELECT ROUND(AVG(roi), 2) AS avg_roi, COUNT(*) AS n_promos FROM roi_computed
)
"""
        + competitor_sql
    )
    return run_query(q, params)


def query_roi_benchmark_by_type_subcat(ps, pe, promo_type, subcat):
    """ROI medio subcategoria + tipo promo (tutti i promo_id con vendite promo in quel periodo/subcat)."""
    rows = query_roi_and_top_competitor_subcat(ps, pe, promo_type, subcat, exclude_brand_id=-1)
    if not rows:
        return []
    r = rows[0]
    return [{"avg_roi": r.get("avg_roi"), "n_promos": r.get("n_promos")}]


def query_roi_and_top_competitor_subcat(ps, pe, promo_type, subcat, exclude_brand_id):
    """Una sola passata sul CTE ROI: benchmark globale + top competitor (esclude brand utente).

    exclude_brand_id=-1 disabilita il ramo competitor (solo avg_roi / n_promos significativi).
    """
    try:
        sid = int(subcat)
        if sid < 100 or not (promo_type and str(promo_type).strip()):
            return []
    except (TypeError, ValueError):
        return []
    excl = int(exclude_brand_id)
    competitor_sql = ""
    params: list = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("subcat", "INT64", sid),
        bigquery.ScalarQueryParameter("pt", "STRING", promo_type),
    ]
    if excl >= 0:
        competitor_sql = """
, topc AS (
  SELECT b.brand_name AS top_competitor_name, ROUND(AVG(r.roi), 2) AS top_competitor_avg_roi
  FROM roi_computed r
  JOIN mart.dim_brand b ON b.brand_id = r.brand_id
  WHERE r.brand_id != @exclude
  GROUP BY r.brand_id, b.brand_name
  ORDER BY AVG(r.roi) DESC
  LIMIT 1
)
SELECT o.avg_roi, o.n_promos, t.top_competitor_name, t.top_competitor_avg_roi
FROM overall o
LEFT JOIN topc t ON TRUE
"""
        params.append(bigquery.ScalarQueryParameter("exclude", "INT64", excl))
    else:
        competitor_sql = """
SELECT o.avg_roi, o.n_promos, CAST(NULL AS STRING) AS top_competitor_name, CAST(NULL AS FLOAT64) AS top_competitor_avg_roi
FROM overall o
"""
    q = (
        _ROI_SUBCAT_CTE.format(agg_promo_filter="")
        + """
, overall AS (
  SELECT ROUND(AVG(roi), 2) AS avg_roi, COUNT(*) AS n_promos FROM roi_computed
)
"""
        + competitor_sql
    )
    return run_query(q, params)


def query_top_competitor_by_discount_subcat(
    ps, pe, subcat, promo_type, discount_depth, exclude_brand_id, dd_tolerance=7
):
    """Top competitor per ROI in subcategoria, stesso bucket sconto e tipo promo."""
    try:
        sid = int(subcat)
        if sid < 100:
            return []
        dd = float(discount_depth)
    except (TypeError, ValueError):
        return []
    dd_min = max(0, dd - dd_tolerance)
    dd_max = min(100, dd + dd_tolerance)
    promo_ids = [pid for pid, d in _PROMO_DISCOUNT_MAP.items() if dd_min <= d <= dd_max]
    if not promo_ids or not (promo_type and str(promo_type).strip()):
        return []
    q = (
        _ROI_SUBCAT_CTE.format(agg_promo_filter="AND f.promo_id IN UNNEST(@pids)")
        + """
SELECT b.brand_name, ROUND(AVG(r.roi), 2) AS avg_roi
FROM roi_computed r
JOIN mart.dim_brand b ON b.brand_id = r.brand_id
WHERE r.brand_id != @exclude
GROUP BY r.brand_id, b.brand_name
ORDER BY AVG(r.roi) DESC
LIMIT 1
"""
    )
    return run_query(
        q,
        [
            bigquery.ScalarQueryParameter("ps", "STRING", ps),
            bigquery.ScalarQueryParameter("pe", "STRING", pe),
            bigquery.ScalarQueryParameter("subcat", "INT64", sid),
            bigquery.ScalarQueryParameter("pt", "STRING", promo_type),
            bigquery.ScalarQueryParameter("exclude", "INT64", int(exclude_brand_id)),
            bigquery.ArrayQueryParameter("pids", "INT64", promo_ids),
        ],
    )


def query_top_competitor_by_type_subcat(ps, pe, subcat, promo_type, exclude_brand_id):
    """Top competitor per ROI in subcategoria e tipo promo (senza filtro bucket sconto)."""
    try:
        sid = int(subcat)
        if sid < 100 or not (promo_type and str(promo_type).strip()):
            return []
    except (TypeError, ValueError):
        return []
    q = (
        _ROI_SUBCAT_CTE.format(agg_promo_filter="")
        + """
SELECT b.brand_name, ROUND(AVG(r.roi), 2) AS avg_roi
FROM roi_computed r
JOIN mart.dim_brand b ON b.brand_id = r.brand_id
WHERE r.brand_id != @exclude
GROUP BY r.brand_id, b.brand_name
ORDER BY AVG(r.roi) DESC
LIMIT 1
"""
    )
    return run_query(
        q,
        [
            bigquery.ScalarQueryParameter("ps", "STRING", ps),
            bigquery.ScalarQueryParameter("pe", "STRING", pe),
            bigquery.ScalarQueryParameter("subcat", "INT64", sid),
            bigquery.ScalarQueryParameter("pt", "STRING", promo_type),
            bigquery.ScalarQueryParameter("exclude", "INT64", int(exclude_brand_id)),
        ],
    )


def query_seasonality_by_month(ps, pe, promo_type, cat=None):
    """Promo performance by month for seasonality insight."""
    q = """
    SELECT EXTRACT(MONTH FROM fp.date) AS month,
      ROUND(AVG(fp.roi), 2) AS avg_roi,
      SUM(fp.attributed_sales_pln) AS attributed
    FROM mart.fact_promo_performance fp
    JOIN mart.dim_promo p ON p.promo_id = fp.promo_id
    WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND (@pt IS NULL OR p.promo_type = @pt)
      AND (@cat IS NULL OR fp.category_id = @cat)
    GROUP BY 1 ORDER BY 1
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("pt", "STRING", promo_type if promo_type else None),
        bigquery.ScalarQueryParameter("cat", "INT64", int(cat) if cat and 1 <= int(cat) <= 10 else None),
    ])


def query_segment_promo_responsiveness(ps, pe, cat=None, subcat=None, promo_type=None):
    """Segmenti HCG più reattivi: quota fatturato in categoria su promo (per tipo se promo_type è valorizzato).

    promo_type: se impostato (es. app_only, flash_sale), il numeratore include solo righe promo_flag
    con dim_promo.promo_type corrispondente; il denominatore resta tutto il fatturato categoria del segmento.
    Se None o '', stessa logica di prima (tutte le righe promo_flag).
    """
    where_cat = ""
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("pt", "STRING", (promo_type or "").strip() or None),
    ]
    if subcat and int(subcat) >= 100:
        where_cat = "AND f.category_id = @subcat"
        params.append(bigquery.ScalarQueryParameter("subcat", "INT64", int(subcat)))
    elif cat and 1 <= int(cat) <= 10:
        where_cat = "AND f.parent_category_id = @cat"
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(cat)))
    else:
        return []
    q = f"""
    SELECT
      s.segment_id,
      s.segment_name,
      ROUND(COALESCE(SUM(
        CASE WHEN f.promo_flag AND (@pt IS NULL OR p.promo_type = @pt) THEN f.gross_pln ELSE 0 END
      ), 0) / NULLIF(SUM(f.gross_pln), 0) * 100, 1) AS promo_share_pct,
      SUM(f.gross_pln) AS total_gross
    FROM mart.fact_sales_daily f
    JOIN mart.dim_segment s ON s.segment_id = f.segment_id
    LEFT JOIN mart.dim_promo p ON p.promo_id = f.promo_id
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND f.gross_pln > 0 {where_cat}
    GROUP BY s.segment_id, s.segment_name
    ORDER BY promo_share_pct DESC
    LIMIT 3
    """
    return run_query(q, params)


def query_active_promos_overlap(ps, pe, brand_id, cat=None, subcat=None):
    """Active promos in same category (cannibalization check)."""
    where_cat = "AND (@cat IS NULL OR f.parent_category_id = @cat OR f.category_id = @cat)"
    where_subcat = "AND (@subcat IS NULL OR f.category_id = @subcat)"
    q = f"""
    SELECT DISTINCT f.promo_id, p.promo_name, p.promo_type,
      MIN(CAST(f.date AS STRING)) AS start_date,
      MAX(CAST(f.date AS STRING)) AS end_date
    FROM mart.fact_sales_daily f
    JOIN mart.dim_promo p ON p.promo_id = f.promo_id
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND f.brand_id = @brand
      AND f.promo_flag
      {where_cat}
      {where_subcat}
    GROUP BY f.promo_id, p.promo_name, p.promo_type
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id) if brand_id else None),
        bigquery.ScalarQueryParameter("cat", "INT64", int(cat) if cat and 1 <= int(cat) <= 10 else None),
        bigquery.ScalarQueryParameter("subcat", "INT64", int(subcat) if subcat and int(subcat) >= 100 else None),
    ])
