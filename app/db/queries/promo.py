"""Query per dashboard PROMO."""
from google.cloud import bigquery
from app.db.client import run_query

_P = lambda ps, pe, pt=None, cat=None, seg=None: [
    bigquery.ScalarQueryParameter("ps", "STRING", ps),
    bigquery.ScalarQueryParameter("pe", "STRING", pe),
    bigquery.ScalarQueryParameter("pt", "STRING", pt if pt else None),
    bigquery.ScalarQueryParameter("cat", "INT64", int(cat) if cat else None),
    bigquery.ScalarQueryParameter("seg", "INT64", int(seg) if seg else None),
]

_WHERE = """
  WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    AND (@pt IS NULL OR p.promo_type = @pt)
    AND (@cat IS NULL OR fp.category_id = @cat)
"""


def query_promo_kpi(ps, pe, pt=None, cat=None):
    q = f"""
    SELECT
      SUM(fp.attributed_sales_pln) AS total_promo_sales,
      ROUND(AVG(fp.uplift_pct), 2) AS avg_uplift_pct,
      ROUND(AVG(fp.roi), 4) AS avg_roi,
      SUM(fp.discount_cost_pln + fp.media_cost_pln) AS total_cost
    FROM mart.fact_promo_performance fp
    JOIN mart.dim_promo p ON p.promo_id = fp.promo_id {_WHERE}
    """
    return run_query(q, _P(ps, pe, pt, cat))


def query_performance_by_type(ps, pe, pt=None, cat=None):
    q = f"""
    SELECT p.promo_type, p.promo_mechanic,
      SUM(fp.attributed_sales_pln) AS attributed_sales,
      SUM(fp.incremental_sales_pln) AS incremental_sales,
      ROUND(AVG(fp.roi), 4) AS avg_roi,
      ROUND(AVG(fp.uplift_pct), 2) AS avg_uplift
    FROM mart.fact_promo_performance fp
    JOIN mart.dim_promo p ON p.promo_id = fp.promo_id {_WHERE}
    GROUP BY p.promo_type, p.promo_mechanic ORDER BY attributed_sales DESC
    """
    return run_query(q, _P(ps, pe, pt, cat))


def query_uplift_by_category(ps, pe, pt=None, cat=None):
    q = f"""
    SELECT c.category_name, ROUND(AVG(fp.uplift_pct), 2) AS avg_uplift,
      SUM(fp.incremental_sales_pln) AS incremental_sales
    FROM mart.fact_promo_performance fp
    JOIN mart.dim_promo p ON p.promo_id = fp.promo_id
    JOIN mart.dim_category c ON c.category_id = fp.category_id {_WHERE}
    GROUP BY c.category_name ORDER BY avg_uplift DESC
    """
    return run_query(q, _P(ps, pe, pt, cat))


def query_promo_timeline(ps, pe, pt=None, cat=None):
    q = f"""
    SELECT CAST(fp.date AS STRING) AS date,
      SUM(fp.attributed_sales_pln) AS attributed,
      SUM(fp.baseline_sales_pln) AS baseline
    FROM mart.fact_promo_performance fp
    JOIN mart.dim_promo p ON p.promo_id = fp.promo_id {_WHERE}
    GROUP BY fp.date ORDER BY fp.date
    """
    return run_query(q, _P(ps, pe, pt, cat))


def query_promo_ranking(ps, pe, pt=None, cat=None, seg=None):
    q = f"""
    SELECT p.promo_name, p.promo_type, p.promo_mechanic,
      SUM(fp.attributed_sales_pln) AS attributed_sales,
      SUM(fp.incremental_sales_pln) AS incremental,
      ROUND(AVG(fp.roi), 4) AS roi,
      ROUND(AVG(fp.uplift_pct), 2) AS uplift_pct,
      SUM(fp.discount_cost_pln) AS discount_cost,
      SUM(fp.media_cost_pln) AS media_cost
    FROM mart.fact_promo_performance fp
    JOIN mart.dim_promo p ON p.promo_id = fp.promo_id {_WHERE}
    GROUP BY p.promo_name, p.promo_type, p.promo_mechanic
    ORDER BY attributed_sales DESC LIMIT 20
    """
    return run_query(q, _P(ps, pe, pt, cat, seg))


def query_roi_by_discount(ps, pe, pt=None, cat=None, seg=None):
    """ROI e costo per promo con profondità sconto media (da fact_sales_daily + fact_promo_performance)."""
    q = """
    WITH sales_discount AS (
      SELECT f.promo_id, ROUND(AVG(f.discount_depth_pct), 2) AS avg_discount_depth_pct
      FROM mart.fact_sales_daily f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.promo_flag = TRUE AND f.promo_id IS NOT NULL
        AND (@cat IS NULL OR f.category_id = @cat)
        AND (@seg IS NULL OR f.segment_id = @seg)
      GROUP BY f.promo_id
    ),
    promo_perf AS (
      SELECT fp.promo_id,
        SUM(fp.attributed_sales_pln) AS attributed_sales_pln,
        SUM(fp.discount_cost_pln + fp.media_cost_pln) AS total_cost_pln,
        ROUND(AVG(fp.roi), 4) AS roi
      FROM mart.fact_promo_performance fp
      JOIN mart.dim_promo p ON p.promo_id = fp.promo_id
      WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND (@pt IS NULL OR p.promo_type = @pt)
        AND (@cat IS NULL OR fp.category_id = @cat)
      GROUP BY fp.promo_id
    )
    SELECT p.promo_id, p.promo_name, p.promo_type,
      s.avg_discount_depth_pct,
      pr.roi, pr.total_cost_pln, pr.attributed_sales_pln
    FROM mart.dim_promo p
    INNER JOIN sales_discount s ON s.promo_id = p.promo_id
    INNER JOIN promo_perf pr ON pr.promo_id = p.promo_id
    ORDER BY s.avg_discount_depth_pct
    """
    return run_query(q, _P(ps, pe, pt, cat, seg))
