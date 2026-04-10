"""Top prodotti e vendite per prodotto (fact_order_items)."""
from google.cloud import bigquery

from app.db.client import run_query


def query_top_products(ps, pe, limit=20, cat=None, brand=None):
    """Top prodotti per vendite (gross_pln). Include channel per filtro client-side."""
    cat_clause = " AND (p.category_id = @cat OR p.subcategory_id = @cat)" if (cat and str(cat).strip()) else ""
    brand_clause = " AND p.brand_id = @brand" if (brand and str(brand).strip()) else ""
    q = f"""
    SELECT p.product_id, p.product_name, p.brand_id, b.brand_name,
      p.category_id, c.category_name, o.channel,
      SUM(oi.gross_pln) AS gross_pln, SUM(oi.quantity) AS units
    FROM mart.fact_order_items oi
    JOIN mart.fact_orders o ON o.order_id = oi.order_id
    JOIN mart.dim_product p ON p.product_id = oi.product_id
    JOIN mart.dim_brand b ON b.brand_id = p.brand_id
    JOIN mart.dim_category c ON c.category_id = p.category_id AND c.level = 1
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe){cat_clause}{brand_clause}
    GROUP BY p.product_id, p.product_name, p.brand_id, b.brand_name, p.category_id, c.category_name, o.channel
    ORDER BY gross_pln DESC
    LIMIT {int(limit)}
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    if cat and str(cat).strip():
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(cat)))
    if brand and str(brand).strip():
        params.append(bigquery.ScalarQueryParameter("brand", "INT64", int(brand)))
    return run_query(q, params)


def query_products_by_category(ps, pe, cat=None):
    """Vendite per prodotto raggruppate per categoria. Per grafici prodotti."""
    q = """
    SELECT p.product_id, p.product_name, b.brand_name, c.category_name,
      SUM(oi.gross_pln) AS gross_pln, SUM(oi.quantity) AS units
    FROM mart.fact_order_items oi
    JOIN mart.fact_orders o ON o.order_id = oi.order_id
    JOIN mart.dim_product p ON p.product_id = oi.product_id
    JOIN mart.dim_brand b ON b.brand_id = p.brand_id
    JOIN mart.dim_category c ON c.category_id = p.category_id AND c.level = 1
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND (@cat IS NULL OR p.category_id = @cat OR p.subcategory_id = @cat)
    GROUP BY p.product_id, p.product_name, b.brand_name, c.category_name
    ORDER BY gross_pln DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("cat", "INT64", int(cat) if cat else None),
    ])
