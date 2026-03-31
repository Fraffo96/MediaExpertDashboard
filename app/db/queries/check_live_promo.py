"""Check Live Promo: active promos and SKU-level performance from precalc."""
from google.cloud import bigquery

from app.db.client import run_query


def query_active_promos_from_sales(
    date_start: str,
    date_end: str,
    brand_id: int,
    promo_id: str | None = None,
    category_id: str | None = None,
    channel: str | None = None,
) -> list[dict]:
    """Promos active in the selected period = those with actual sales in precalc for this brand."""
    params = [
        bigquery.ScalarQueryParameter("date_start", "DATE", date_start),
        bigquery.ScalarQueryParameter("date_end", "DATE", date_end),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    where_parts = ["date BETWEEN @date_start AND @date_end", "brand_id = @brand"]
    if promo_id and str(promo_id).strip():
        where_parts.append("promo_id = @promo_id")
        params.append(bigquery.ScalarQueryParameter("promo_id", "INT64", int(promo_id)))
    if category_id and str(category_id).strip():
        if int(category_id) <= 10:
            where_parts.append("parent_category_id = @category_id")
        else:
            where_parts.append("category_id = @category_id")
        params.append(bigquery.ScalarQueryParameter("category_id", "INT64", int(category_id)))
    if channel is not None and str(channel).strip() in ("web", "app", "store"):
        where_parts.append("channel = @channel")
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))
    else:
        where_parts.append("channel = ''")

    where_clause = " AND ".join(where_parts)
    q = f"""
    SELECT DISTINCT promo_id, promo_name
    FROM mart.precalc_promo_live_sku
    WHERE {where_clause}
    ORDER BY promo_name
    """
    return run_query(q, params)


def query_promo_sku_from_precalc(
    date_start: str,
    date_end: str,
    brand_id: int,
    promo_id: str | None = None,
    category_id: str | None = None,
    channel: str | None = None,
) -> list[dict]:
    """SKU-level promo performance from precalc. Fast: partition by date."""
    params = [
        bigquery.ScalarQueryParameter("date_start", "DATE", date_start),
        bigquery.ScalarQueryParameter("date_end", "DATE", date_end),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    where_parts = ["date BETWEEN @date_start AND @date_end", "brand_id = @brand"]
    if promo_id and str(promo_id).strip():
        where_parts.append("promo_id = @promo_id")
        params.append(bigquery.ScalarQueryParameter("promo_id", "INT64", int(promo_id)))
    if category_id and str(category_id).strip():
        if int(category_id) <= 10:
            where_parts.append("parent_category_id = @category_id")
        else:
            where_parts.append("category_id = @category_id")
        params.append(bigquery.ScalarQueryParameter("category_id", "INT64", int(category_id)))
    if channel is not None and str(channel).strip() in ("web", "app", "store"):
        where_parts.append("channel = @channel")
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))
    # when channel is All: no filter -> get aggregated (channel='') and per-channel (web,app,store) for donut by channel

    where_clause = " AND ".join(where_parts)
    q = f"""
    SELECT product_id, product_name, brand_id, brand_name, category_id, category_name,
      parent_category_id, promo_id, promo_name, channel,
      SUM(gross_pln) AS gross_pln, SUM(units) AS units, SUM(order_count) AS order_count
    FROM mart.precalc_promo_live_sku
    WHERE {where_clause}
    GROUP BY product_id, product_name, brand_id, brand_name, category_id, category_name,
      parent_category_id, promo_id, promo_name, channel
    ORDER BY gross_pln DESC
    """
    return run_query(q, params)


def query_segment_breakdown_for_product(
    product_id: int,
    brand_id: int,
    date_start: str,
    date_end: str,
    promo_id: str | None = None,
    category_id: str | None = None,
    channel: str | None = None,
) -> list[dict]:
    """Segment breakdown for a product (which segments react to the promo). Live query from fact_orders + dim_customer."""
    params = [
        bigquery.ScalarQueryParameter("product_id", "INT64", product_id),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("date_start", "DATE", date_start),
        bigquery.ScalarQueryParameter("date_end", "DATE", date_end),
    ]
    where_parts = [
        "o.promo_flag",
        "o.promo_id IS NOT NULL",
        "oi.product_id = @product_id",
        "p.brand_id = @brand",
        "o.date BETWEEN @date_start AND @date_end",
    ]
    if promo_id and str(promo_id).strip():
        where_parts.append("o.promo_id = @promo_id")
        params.append(bigquery.ScalarQueryParameter("promo_id", "INT64", int(promo_id)))
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


def query_segment_breakdown_aggregate(
    brand_id: int,
    date_start: str,
    date_end: str,
    promo_id: str | None = None,
    category_id: str | None = None,
    channel: str | None = None,
) -> list[dict]:
    """Segment breakdown for ALL products in promo (aggregate). Same filters as SKU query."""
    params = [
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("date_start", "DATE", date_start),
        bigquery.ScalarQueryParameter("date_end", "DATE", date_end),
    ]
    where_parts = [
        "o.promo_flag",
        "o.promo_id IS NOT NULL",
        "p.brand_id = @brand",
        "o.date BETWEEN @date_start AND @date_end",
    ]
    if promo_id and str(promo_id).strip():
        where_parts.append("o.promo_id = @promo_id")
        params.append(bigquery.ScalarQueryParameter("promo_id", "INT64", int(promo_id)))
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
