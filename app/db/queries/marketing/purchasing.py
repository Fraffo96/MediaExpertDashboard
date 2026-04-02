"""Marketing: purchasing process - channel mix, peak events per segment."""
from google.cloud import bigquery

from app.db.client import run_query


def query_purchasing_channel_mix(
    ps: str, pe: str, segment_id: int | None = None, parent_category_id: int | None = None,
) -> list[dict]:
    """Channel mix (web/app/store) for orders. Per segment or all. Optional: ordini con almeno una riga nella macro-categoria."""
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("pcat", "INT64", int(parent_category_id) if parent_category_id else None),
    ]
    where_seg = ""
    if segment_id is not None:
        where_seg = "AND c.segment_id = @seg"
        params.append(bigquery.ScalarQueryParameter("seg", "INT64", segment_id))
    q = f"""
    SELECT c.segment_id, s.segment_name, o.channel,
      COUNT(DISTINCT o.customer_id) AS buyers,
      COUNT(*) AS orders,
      SUM(o.gross_pln) AS gross_pln,
      ROUND(AVG(o.gross_pln), 2) AS aov
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    JOIN mart.dim_segment s ON s.segment_id = c.segment_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      {where_seg}
      AND (
        @pcat IS NULL
        OR EXISTS (
          SELECT 1 FROM mart.fact_order_items oi
          JOIN mart.dim_product p ON p.product_id = oi.product_id
          JOIN mart.dim_category dc ON dc.category_id = p.category_id
          WHERE oi.order_id = o.order_id
            AND (dc.parent_category_id = @pcat OR (dc.level = 1 AND dc.category_id = @pcat))
        )
      )
    GROUP BY c.segment_id, s.segment_name, o.channel
    ORDER BY c.segment_id, gross_pln DESC
    """
    return run_query(q, params)


def query_purchasing_peak_events(
    ps: str, pe: str, segment_id: int | None = None, parent_category_id: int | None = None,
) -> list[dict]:
    """Orders and sales by peak event (Black Friday, Christmas, etc.) per segment."""
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("pcat", "INT64", int(parent_category_id) if parent_category_id else None),
    ]
    where_seg = ""
    if segment_id is not None:
        where_seg = "AND c.segment_id = @seg"
        params.append(bigquery.ScalarQueryParameter("seg", "INT64", segment_id))
    q = f"""
    SELECT c.segment_id, s.segment_name, d.peak_event,
      COUNT(*) AS orders,
      SUM(o.gross_pln) AS gross_pln,
      ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY c.segment_id), 1) AS orders_pct
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    JOIN mart.dim_segment s ON s.segment_id = c.segment_id
    JOIN mart.dim_date d ON d.date = o.date
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      {where_seg}
      AND (
        @pcat IS NULL
        OR EXISTS (
          SELECT 1 FROM mart.fact_order_items oi
          JOIN mart.dim_product p ON p.product_id = oi.product_id
          JOIN mart.dim_category dc ON dc.category_id = p.category_id
          WHERE oi.order_id = o.order_id
            AND (dc.parent_category_id = @pcat OR (dc.level = 1 AND dc.category_id = @pcat))
        )
      )
    GROUP BY c.segment_id, s.segment_name, d.peak_event
    ORDER BY c.segment_id, gross_pln DESC
    """
    return run_query(q, params)
