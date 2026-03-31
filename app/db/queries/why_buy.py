"""Query per dashboard WHY DO CONSUMERS BUY – usa fact_orders + dim_customer."""
from google.cloud import bigquery
from app.db.client import run_query


def query_category_by_segment(ps, pe, category_id):
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("cat", "INT64", int(category_id) if category_id else None),
    ]
    q = """
    SELECT s.segment_name, s.segment_id, s.top_driver AS motivation,
      COUNT(DISTINCT o.customer_id) AS n_buyers,
      SUM(o.gross_pln) AS total_spend,
      SUM(o.units) AS total_units,
      ROUND(AVG(o.gross_pln), 2) AS avg_order_value
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    JOIN mart.dim_segment s ON s.segment_id = c.segment_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY s.segment_name, s.segment_id, s.top_driver
    ORDER BY total_spend DESC
    """
    return run_query(q, params)


def query_category_growth(ps, pe, category_id):
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("cat", "INT64", int(category_id) if category_id else None),
    ]
    q = """
    WITH by_year AS (
      SELECT s.segment_name, EXTRACT(YEAR FROM o.date) AS year,
        COUNT(DISTINCT o.customer_id) AS n_buyers, SUM(o.gross_pln) AS spend
      FROM mart.fact_orders o
      JOIN mart.dim_customer c ON c.customer_id = o.customer_id
      JOIN mart.dim_segment s ON s.segment_id = c.segment_id
      WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      GROUP BY s.segment_name, year
    )
    SELECT segment_name, year, n_buyers, spend,
      ROUND(100.0 * (spend - LAG(spend) OVER (PARTITION BY segment_name ORDER BY year))
        / NULLIF(LAG(spend) OVER (PARTITION BY segment_name ORDER BY year), 0), 2) AS growth_pct
    FROM by_year ORDER BY segment_name, year
    """
    return run_query(q, params)


def query_segment_radar(ps, pe, category_id):
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("cat", "INT64", int(category_id) if category_id else None),
    ]
    q = """
    SELECT s.segment_name,
      COUNT(DISTINCT o.customer_id) AS buyers,
      SUM(o.gross_pln) AS spend,
      SUM(o.units) AS units,
      ROUND(AVG(o.discount_depth_pct), 2) AS avg_discount,
      ROUND(100.0 * COUNTIF(o.promo_flag) / NULLIF(COUNT(*), 0), 2) AS promo_dependency
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    JOIN mart.dim_segment s ON s.segment_id = c.segment_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY s.segment_name ORDER BY spend DESC
    """
    return run_query(q, params)
