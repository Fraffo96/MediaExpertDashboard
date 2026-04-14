"""Customer metrics for the expert chatbot: unique buyers, AOV, loyalty, app/omnichannel stats."""
from google.cloud import bigquery

from app.db.client import run_query


def query_customer_stats(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int | None = None,
) -> dict:
    """
    Aggregated customer metrics for a brand's buyers in the period.
    Returns: unique_customers, total_orders, avg_order_value_pln, avg_orders_per_customer,
             pct_loyalty_card, pct_omnichannel, pct_app_users,
             channel_breakdown (list: channel, orders, unique_customers, gross_pln).
    Optionally scoped to a parent category (1-10).
    """
    params: list = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
    ]
    cat_join = ""
    cat_clause = ""
    if parent_category_id and 1 <= int(parent_category_id) <= 10:
        cat_join = """
        JOIN mart.fact_order_items oi ON oi.order_id = o.order_id
        JOIN mart.dim_product p ON p.product_id = oi.product_id
        """
        cat_clause = " AND p.category_id = @cat"
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(parent_category_id)))

    # Main aggregate stats
    q_stats = f"""
    SELECT
      COUNT(DISTINCT o.customer_id) AS unique_customers,
      COUNT(DISTINCT o.order_id) AS total_orders,
      ROUND(SUM(o.gross_pln) / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS avg_order_value_pln,
      ROUND(COUNT(DISTINCT o.order_id) / NULLIF(COUNT(DISTINCT o.customer_id), 0), 2) AS avg_orders_per_customer,
      ROUND(100.0 * COUNTIF(c.has_loyalty_card) / NULLIF(COUNT(DISTINCT o.customer_id), 0), 1) AS pct_loyalty_card,
      ROUND(100.0 * COUNTIF(c.omnichannel_flag) / NULLIF(COUNT(DISTINCT o.customer_id), 0), 1) AS pct_omnichannel,
      ROUND(100.0 * COUNTIF(c.has_app) / NULLIF(COUNT(DISTINCT o.customer_id), 0), 1) AS pct_app_users
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    {cat_join}
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND EXISTS (
        SELECT 1 FROM mart.fact_order_items oi2
        JOIN mart.dim_product p2 ON p2.product_id = oi2.product_id
        WHERE oi2.order_id = o.order_id AND p2.brand_id = @brand
      )
      {cat_clause}
    """
    # Channel breakdown
    q_channel = f"""
    SELECT
      o.channel,
      COUNT(DISTINCT o.order_id) AS orders,
      COUNT(DISTINCT o.customer_id) AS unique_customers,
      ROUND(SUM(o.gross_pln), 2) AS gross_pln
    FROM mart.fact_orders o
    {cat_join}
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND EXISTS (
        SELECT 1 FROM mart.fact_order_items oi2
        JOIN mart.dim_product p2 ON p2.product_id = oi2.product_id
        WHERE oi2.order_id = o.order_id AND p2.brand_id = @brand
      )
      {cat_clause}
    GROUP BY o.channel
    ORDER BY gross_pln DESC
    """
    stats_rows = run_query(q_stats, params) or []
    channel_rows = run_query(q_channel, params) or []
    stats = dict(stats_rows[0]) if stats_rows else {}
    stats["channel_breakdown"] = list(channel_rows)
    return stats
