"""Marketing Overview: segment share of sales for a category/subcategory (brand scope)."""
from google.cloud import bigquery

from app.db.client import run_query


def query_segment_breakdown_for_category_sales(
    brand_id: int,
    date_start: str,
    date_end: str,
    category_id: int,
    subcategory_id: int | None = None,
    channel: str | None = None,
) -> list[dict]:
    """All sales for brand in category (and optional subcategory), broken down by customer segment."""
    params = [
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("date_start", "DATE", date_start),
        bigquery.ScalarQueryParameter("date_end", "DATE", date_end),
        bigquery.ScalarQueryParameter("category_id", "INT64", category_id),
    ]
    where_parts = [
        "o.date BETWEEN @date_start AND @date_end",
        "p.brand_id = @brand",
        "p.category_id = @category_id",
    ]
    if subcategory_id and subcategory_id >= 100:
        where_parts.append("p.subcategory_id = @subcategory_id")
        params.append(bigquery.ScalarQueryParameter("subcategory_id", "INT64", subcategory_id))
    if channel and str(channel).strip() in ("web", "app", "store"):
        where_parts.append("o.channel = @channel")
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))

    where_clause = " AND ".join(where_parts)
    q = f"""
    SELECT c.segment_id, s.segment_name,
      SUM(oi.gross_pln) AS gross_pln, SUM(oi.quantity) AS units
    FROM mart.fact_order_items oi
    JOIN mart.fact_orders o ON o.order_id = oi.order_id
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    JOIN mart.dim_segment s ON s.segment_id = c.segment_id
    JOIN mart.dim_product p ON p.product_id = oi.product_id
    WHERE {where_clause}
    GROUP BY c.segment_id, s.segment_name
    ORDER BY gross_pln DESC
    """
    return run_query(q, params)
