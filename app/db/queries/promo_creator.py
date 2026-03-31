"""Promo Creator: suggestions and benchmarks for promo planning."""
from google.cloud import bigquery
from app.db.client import run_query


def query_category_discount_benchmark(ps, pe, brand_id, cat=None, subcat=None):
    """Average discount depth in category for benchmark."""
    roi_cat = int(cat) if cat and 1 <= int(cat) <= 10 else (int(subcat) // 100 if subcat and int(subcat) >= 100 else None)
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
_PROMO_DISCOUNT_MAP = {1: 10, 2: 20, 3: 30, 4: 15, 5: 15, 6: 12, 7: 8, 8: 18, 9: 22, 10: 20}


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


def query_segment_promo_responsiveness(ps, pe, cat=None, subcat=None):
    """Promo Creator: segmenti HCG più reattivi alle promo nella categoria (promo share % per segmento)."""
    where_cat = ""
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
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
      ROUND(COALESCE(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0)
        / NULLIF(SUM(f.gross_pln), 0) * 100, 1) AS promo_share_pct,
      SUM(f.gross_pln) AS total_gross
    FROM mart.fact_sales_daily f
    JOIN mart.dim_segment s ON s.segment_id = f.segment_id
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
