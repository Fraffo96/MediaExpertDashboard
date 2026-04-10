"""Buyer analytics (fact_orders + dim_customer)."""
from google.cloud import bigquery

from app.db.client import run_query


def query_channel_mix(ps, pe, channel=None):
    channel_clause = " AND o.channel = @channel" if (channel and str(channel).strip()) else ""
    q = f"""
    SELECT o.channel,
      COUNT(DISTINCT o.customer_id) AS buyers, COUNT(*) AS orders,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov
    FROM mart.fact_orders o
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe){channel_clause}
    GROUP BY o.channel ORDER BY gross_pln DESC
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    if channel and str(channel).strip():
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", (channel or "").strip()))
    return run_query(q, params)


def query_loyalty_breakdown(ps, pe, channel=None):
    channel_clause = " AND o.channel = @channel" if (channel and str(channel).strip()) else ""
    q = f"""
    SELECT c.loyalty_tier,
      COUNT(DISTINCT o.customer_id) AS buyers, COUNT(*) AS orders,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe){channel_clause}
    GROUP BY c.loyalty_tier ORDER BY gross_pln DESC
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    if channel and str(channel).strip():
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", (channel or "").strip()))
    return run_query(q, params)


def query_buyer_segments(ps, pe, channel=None):
    channel_clause = " AND o.channel = @channel" if (channel and str(channel).strip()) else ""
    q = f"""
    SELECT s.segment_name, s.segment_id,
      COUNT(DISTINCT o.customer_id) AS buyers, COUNT(*) AS orders,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov,
      ROUND(100.0 * COUNTIF(o.promo_flag) / NULLIF(COUNT(*), 0), 1) AS promo_pct
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    JOIN mart.dim_segment s ON s.segment_id = c.segment_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe){channel_clause}
    GROUP BY s.segment_name, s.segment_id ORDER BY gross_pln DESC
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    if channel and str(channel).strip():
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", (channel or "").strip()))
    return run_query(q, params)


def query_buyer_demographics(ps, pe, channel=None):
    channel_clause = " AND o.channel = @channel" if (channel and str(channel).strip()) else ""
    q = f"""
    SELECT c.gender, c.age_band,
      COUNT(DISTINCT o.customer_id) AS buyers,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe){channel_clause}
    GROUP BY c.gender, c.age_band ORDER BY gross_pln DESC
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    if channel and str(channel).strip():
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", (channel or "").strip()))
    return run_query(q, params)


def query_loyalty_breakdown_by_channel(ps, pe):
    """Loyalty per canale per filtro client-side istantaneo."""
    q = """
    SELECT o.channel, c.loyalty_tier,
      COUNT(DISTINCT o.customer_id) AS buyers, COUNT(*) AS orders,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY o.channel, c.loyalty_tier ORDER BY o.channel, gross_pln DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ])


def query_buyer_segments_by_channel(ps, pe):
    """Buyer segments per canale per filtro client-side istantaneo."""
    q = """
    SELECT o.channel, s.segment_name, s.segment_id,
      COUNT(DISTINCT o.customer_id) AS buyers, COUNT(*) AS orders,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov,
      ROUND(100.0 * COUNTIF(o.promo_flag) / NULLIF(COUNT(*), 0), 1) AS promo_pct
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    JOIN mart.dim_segment s ON s.segment_id = c.segment_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY o.channel, s.segment_name, s.segment_id ORDER BY o.channel, gross_pln DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ])


def query_buyer_demographics_by_channel(ps, pe):
    """Buyer demographics per canale per filtro client-side istantaneo."""
    q = """
    SELECT o.channel, c.gender, c.age_band,
      COUNT(DISTINCT o.customer_id) AS buyers,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY o.channel, c.gender, c.age_band ORDER BY o.channel, gross_pln DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ])


def query_repeat_rate_by_channel(ps, pe):
    """Repeat rate per canale per filtro client-side istantaneo."""
    q = """
    WITH bf AS (
      SELECT customer_id, channel, COUNT(*) AS n_orders, SUM(gross_pln) AS spend
      FROM mart.fact_orders
      WHERE date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      GROUP BY customer_id, channel
    )
    SELECT channel,
      COUNT(*) AS total_buyers,
      COUNTIF(n_orders > 1) AS repeat_buyers,
      ROUND(100.0 * COUNTIF(n_orders > 1) / NULLIF(COUNT(*), 0), 1) AS repeat_rate_pct,
      ROUND(AVG(n_orders), 1) AS avg_frequency,
      ROUND(AVG(spend), 2) AS avg_lifetime_spend
    FROM bf GROUP BY channel ORDER BY total_buyers DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ])


def query_repeat_rate(ps, pe, channel=None):
    channel_clause = " AND channel = @channel" if (channel and str(channel).strip()) else ""
    q = f"""
    WITH bf AS (
      SELECT customer_id, COUNT(*) AS n_orders, SUM(gross_pln) AS spend
      FROM mart.fact_orders
      WHERE date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe){channel_clause}
      GROUP BY customer_id
    )
    SELECT COUNT(*) AS total_buyers,
      COUNTIF(n_orders > 1) AS repeat_buyers,
      ROUND(100.0 * COUNTIF(n_orders > 1) / NULLIF(COUNT(*), 0), 1) AS repeat_rate_pct,
      ROUND(AVG(n_orders), 1) AS avg_frequency,
      ROUND(AVG(spend), 2) AS avg_lifetime_spend
    FROM bf
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    if channel and str(channel).strip():
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", (channel or "").strip()))
    return run_query(q, params)


def query_channel_by_segment(ps, pe, channel=None):
    channel_clause = " AND o.channel = @channel" if (channel and str(channel).strip()) else ""
    q = f"""
    SELECT s.segment_name, o.channel,
      COUNT(DISTINCT o.customer_id) AS buyers, SUM(o.gross_pln) AS gross_pln
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    JOIN mart.dim_segment s ON s.segment_id = c.segment_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe){channel_clause}
    GROUP BY s.segment_name, o.channel ORDER BY s.segment_name, gross_pln DESC
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    if channel and str(channel).strip():
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", (channel or "").strip()))
    return run_query(q, params)
