"""Market Intelligence: top products e segment breakdown per SKU (tutte le vendite, non solo promo)."""
from google.cloud import bigquery

from app.db.client import run_query


def query_top_products(
    year: int,
    brand_id: int,
    category_id: int | None = None,
    subcategory_id: int | None = None,
    channel: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Top products per valore (gross_pln) per anno, brand, category/subcategory, channel."""
    date_start = f"{year}-01-01"
    date_end = f"{year}-12-31"
    params = [
        bigquery.ScalarQueryParameter("date_start", "DATE", date_start),
        bigquery.ScalarQueryParameter("date_end", "DATE", date_end),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
    ]
    where_parts = [
        "o.date BETWEEN @date_start AND @date_end",
        "p.brand_id = @brand",
    ]
    if category_id and 1 <= category_id <= 10:
        where_parts.append("p.category_id = @category_id")
        params.append(bigquery.ScalarQueryParameter("category_id", "INT64", category_id))
    if subcategory_id and subcategory_id >= 100:
        where_parts.append("p.subcategory_id = @subcategory_id")
        params.append(bigquery.ScalarQueryParameter("subcategory_id", "INT64", subcategory_id))
    if channel and str(channel).strip() in ("web", "app", "store"):
        where_parts.append("o.channel = @channel")
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))

    where_clause = " AND ".join(where_parts)
    q = f"""
    SELECT oi.product_id, p.product_name,
      SUM(oi.gross_pln) AS gross_pln, SUM(oi.quantity) AS units
    FROM mart.fact_order_items oi
    JOIN mart.fact_orders o ON o.order_id = oi.order_id
    JOIN mart.dim_product p ON p.product_id = oi.product_id
    WHERE {where_clause}
    GROUP BY oi.product_id, p.product_name
    ORDER BY gross_pln DESC
    LIMIT @limit
    """
    return run_query(q, params)


def query_segment_breakdown_for_product_precalc(
    product_id: int,
    brand_id: int,
    year: int,
    category_id: str | None = None,
    channel: str | None = None,
) -> list[dict]:
    """Segment breakdown da precalc (allineato a query live su fact_order_items)."""
    params = [
        bigquery.ScalarQueryParameter("product_id", "INT64", product_id),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("year", "INT64", year),
    ]
    where_parts = [
        "ps.year = @year",
        "ps.product_id = @product_id",
        "ps.brand_id = @brand",
    ]
    ch = str(channel).strip() if channel else ""
    if ch in ("web", "app", "store"):
        where_parts.append("ps.channel = @channel")
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", ch))
    else:
        where_parts.append("ps.channel = ''")

    join_product = ""
    if category_id and str(category_id).strip():
        cid = int(category_id)
        if cid <= 10:
            where_parts.append("p.category_id = @category_id")
        else:
            where_parts.append("p.subcategory_id = @category_id")
        params.append(bigquery.ScalarQueryParameter("category_id", "INT64", cid))
        join_product = "JOIN mart.dim_product p ON p.product_id = ps.product_id"

    where_clause = " AND ".join(where_parts)
    q = f"""
    SELECT ps.segment_id, ANY_VALUE(ps.segment_name) AS segment_name,
      SUM(ps.gross_pln) AS gross_pln, SUM(ps.units) AS units
    FROM mart.precalc_mi_segment_by_product ps
    {join_product}
    WHERE {where_clause}
    GROUP BY ps.segment_id
    ORDER BY gross_pln DESC
    """
    return run_query(q, params)


def query_segment_breakdown_for_product_all_sales(
    product_id: int,
    brand_id: int,
    date_start: str,
    date_end: str,
    category_id: str | None = None,
    channel: str | None = None,
) -> list[dict]:
    """Segment breakdown per un prodotto – TUTTE le vendite (non solo promo). Per Market Intelligence."""
    params = [
        bigquery.ScalarQueryParameter("product_id", "INT64", product_id),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("date_start", "DATE", date_start),
        bigquery.ScalarQueryParameter("date_end", "DATE", date_end),
    ]
    where_parts = [
        "oi.product_id = @product_id",
        "p.brand_id = @brand",
        "o.date BETWEEN @date_start AND @date_end",
    ]
    if category_id and str(category_id).strip():
        cid = int(category_id)
        if cid <= 10:
            where_parts.append("p.category_id = @category_id")
        else:
            where_parts.append("p.subcategory_id = @category_id")
        params.append(bigquery.ScalarQueryParameter("category_id", "INT64", cid))
    if channel and str(channel).strip() in ("web", "app", "store"):
        where_parts.append("o.channel = @channel")
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))

    where_clause = " AND ".join(where_parts)
    q = f"""
    SELECT c.segment_id, ANY_VALUE(s.segment_name) AS segment_name,
      SUM(oi.gross_pln) AS gross_pln, SUM(oi.quantity) AS units
    FROM mart.fact_order_items oi
    JOIN mart.fact_orders o ON o.order_id = oi.order_id
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    JOIN mart.dim_segment s ON s.segment_id = c.segment_id
    JOIN mart.dim_product p ON p.product_id = oi.product_id
    WHERE {where_clause}
    GROUP BY c.segment_id
    ORDER BY gross_pln DESC
    """
    return run_query(q, params)
