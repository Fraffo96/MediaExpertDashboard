"""Top prodotti e vendite per prodotto (fact_order_items)."""
import re

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


def query_products_any_token_match(
    ps: str,
    pe: str,
    *,
    tokens: list[str],
    brand_id: int | None = None,
    candidate_limit: int = 800,
) -> list[dict]:
    """
    Products (with sales in window) whose name matches at least one token (substring).
    Used as a broad candidate set for natural-language SKU resolution; refine in Python.
    """
    clean: list[str] = []
    for t in tokens:
        s = re.sub(r"[^a-z0-9]", "", (t or "").lower())
        if 2 <= len(s) <= 48:
            clean.append(s)
    if not clean:
        return []
    lim = max(50, min(int(candidate_limit), 2000))
    brand_clause = ""
    params: list = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ArrayQueryParameter("tokens", "STRING", clean),
        bigquery.ScalarQueryParameter("lim", "INT64", lim),
    ]
    if brand_id is not None:
        brand_clause = " AND p.brand_id = @brand"
        params.append(bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)))
    q = f"""
    WITH products AS (
      SELECT p.product_id, p.product_name,
        SUM(oi.gross_pln) AS gross_pln, SUM(oi.quantity) AS units
      FROM mart.fact_order_items oi
      JOIN mart.fact_orders o ON o.order_id = oi.order_id
      JOIN mart.dim_product p ON p.product_id = oi.product_id
      WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        {brand_clause}
      GROUP BY p.product_id, p.product_name
    )
    SELECT product_id, product_name, gross_pln, units
    FROM products p
    WHERE (
      SELECT COUNT(1) FROM UNNEST(@tokens) AS tok
      WHERE CONTAINS_SUBSTR(LOWER(p.product_name), tok)
    ) >= 1
    ORDER BY gross_pln DESC
    LIMIT @lim
    """
    return run_query(q, params)
