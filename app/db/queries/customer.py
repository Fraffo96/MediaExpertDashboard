"""Query per dashboard CUSTOMER segments – usa fact_orders + dim_customer."""
from google.cloud import bigquery
from app.db.client import run_query

_P = lambda ps, pe, seg=None, gender=None: [
    bigquery.ScalarQueryParameter("ps", "STRING", ps),
    bigquery.ScalarQueryParameter("pe", "STRING", pe),
    bigquery.ScalarQueryParameter("seg", "INT64", int(seg) if seg else None),
    bigquery.ScalarQueryParameter("gender", "STRING", gender if gender else None),
]


def query_segment_overview(ps, pe, seg=None, gender=None):
    q = """
    SELECT s.segment_id, s.segment_name, s.age_range, s.income_level, s.gender_skew, s.top_driver,
      COUNT(DISTINCT o.customer_id) AS n_customers,
      SUM(o.gross_pln) AS total_spend,
      ROUND(AVG(o.gross_pln), 2) AS avg_order_value,
      SUM(o.units) AS total_units
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    JOIN mart.dim_segment s ON s.segment_id = c.segment_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND (@seg IS NULL OR c.segment_id = @seg)
      AND (@gender IS NULL OR c.gender = @gender)
    GROUP BY s.segment_id, s.segment_name, s.age_range, s.income_level, s.gender_skew, s.top_driver
    ORDER BY total_spend DESC
    """
    return run_query(q, _P(ps, pe, seg, gender))


def query_seasonality(ps, pe, seg=None, gender=None):
    q = """
    SELECT s.segment_name, EXTRACT(QUARTER FROM o.date) AS quarter,
      SUM(o.gross_pln) AS gross_pln, SUM(o.units) AS units
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    JOIN mart.dim_segment s ON s.segment_id = c.segment_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND (@seg IS NULL OR c.segment_id = @seg)
      AND (@gender IS NULL OR c.gender = @gender)
    GROUP BY s.segment_name, quarter ORDER BY s.segment_name, quarter
    """
    return run_query(q, _P(ps, pe, seg, gender))


def query_spend_distribution(ps, pe, seg=None, gender=None):
    q = """
    WITH buyer_stats AS (
      SELECT c.customer_id, s.segment_name,
        SUM(o.gross_pln) AS lifetime_spend, COUNT(*) AS lifetime_orders
      FROM mart.fact_orders o
      JOIN mart.dim_customer c ON c.customer_id = o.customer_id
      JOIN mart.dim_segment s ON s.segment_id = c.segment_id
      WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND (@seg IS NULL OR c.segment_id = @seg)
        AND (@gender IS NULL OR c.gender = @gender)
      GROUP BY c.customer_id, s.segment_name
    )
    SELECT segment_name,
      ROUND(AVG(lifetime_spend), 2) AS avg_lifetime_spend,
      ROUND(MIN(lifetime_spend), 2) AS min_spend,
      ROUND(MAX(lifetime_spend), 2) AS max_spend,
      COUNT(*) AS n_customers,
      ROUND(AVG(lifetime_orders), 1) AS avg_orders
    FROM buyer_stats
    GROUP BY segment_name ORDER BY avg_lifetime_spend DESC
    """
    return run_query(q, _P(ps, pe, seg, gender))


def query_channel_mix(ps, pe, seg=None, gender=None):
    q = """
    SELECT o.channel,
      COUNT(DISTINCT o.customer_id) AS buyers, COUNT(*) AS orders,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND (@seg IS NULL OR c.segment_id = @seg)
      AND (@gender IS NULL OR c.gender = @gender)
    GROUP BY o.channel ORDER BY gross_pln DESC
    """
    return run_query(q, _P(ps, pe, seg, gender))


def query_loyalty_penetration(ps, pe, seg=None, gender=None):
    q = """
    SELECT c.loyalty_tier,
      COUNT(DISTINCT o.customer_id) AS buyers, COUNT(*) AS orders,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND (@seg IS NULL OR c.segment_id = @seg)
      AND (@gender IS NULL OR c.gender = @gender)
    GROUP BY c.loyalty_tier ORDER BY gross_pln DESC
    """
    return run_query(q, _P(ps, pe, seg, gender))


def query_repeat_rate(ps, pe, seg=None, gender=None):
    q = """
    WITH buyer_freq AS (
      SELECT o.customer_id, COUNT(*) AS n_orders
      FROM mart.fact_orders o
      JOIN mart.dim_customer c ON c.customer_id = o.customer_id
      WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND (@seg IS NULL OR c.segment_id = @seg)
        AND (@gender IS NULL OR c.gender = @gender)
      GROUP BY o.customer_id
    )
    SELECT
      COUNT(*) AS total_buyers,
      COUNTIF(n_orders > 1) AS repeat_buyers,
      ROUND(100.0 * COUNTIF(n_orders > 1) / NULLIF(COUNT(*), 0), 1) AS repeat_rate_pct,
      ROUND(AVG(n_orders), 1) AS avg_frequency
    FROM buyer_freq
    """
    return run_query(q, _P(ps, pe, seg, gender))
