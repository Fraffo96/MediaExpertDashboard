"""Query per tab Products: performance prodotti per brand."""
from google.cloud import bigquery

from app.db.client import run_query


def query_brand_products_by_category(ps, pe, brand_id):
    """Vendite per categoria/sottocategoria per i prodotti del brand."""
    if not brand_id or not str(brand_id).strip():
        return []
    q = """
    SELECT
      pc.category_id AS parent_category_id,
      pc.category_name AS parent_category_name,
      sc.category_id AS subcategory_id,
      sc.category_name AS subcategory_name,
      SUM(oi.gross_pln) AS gross_pln,
      SUM(oi.quantity) AS units
    FROM mart.fact_order_items oi
    JOIN mart.fact_orders o ON o.order_id = oi.order_id
    JOIN mart.dim_product p ON p.product_id = oi.product_id
    JOIN mart.dim_category pc ON pc.category_id = p.category_id AND pc.level = 1
    JOIN mart.dim_category sc ON sc.category_id = p.subcategory_id AND sc.level = 2
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND p.brand_id = @brand
    GROUP BY pc.category_id, pc.category_name, sc.category_id, sc.category_name
    ORDER BY gross_pln DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
    ])


def query_brand_top_products(ps, pe, brand_id, limit=15):
    """Top prodotti del brand con gross_pln, units, promo_share, avg_discount."""
    if not brand_id or not str(brand_id).strip():
        return []
    q = f"""
    WITH prod_sales AS (
      SELECT
        p.product_id,
        p.product_name,
        sc.category_name AS subcategory_name,
        SUM(oi.gross_pln) AS gross_pln,
        SUM(oi.quantity) AS units,
        SUM(CASE WHEN o.promo_flag THEN oi.gross_pln ELSE 0 END) AS promo_gross,
        SUM(CASE WHEN o.promo_flag THEN oi.gross_pln * o.discount_depth_pct ELSE 0 END) AS discount_weighted
      FROM mart.fact_order_items oi
      JOIN mart.fact_orders o ON o.order_id = oi.order_id
      JOIN mart.dim_product p ON p.product_id = oi.product_id
      JOIN mart.dim_category sc ON sc.category_id = p.subcategory_id AND sc.level = 2
      WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND p.brand_id = @brand
      GROUP BY p.product_id, p.product_name, sc.category_name
    )
    SELECT
      product_id,
      product_name,
      subcategory_name,
      gross_pln,
      units,
      ROUND(100.0 * promo_gross / NULLIF(gross_pln, 0), 1) AS promo_share_pct,
      ROUND(discount_weighted / NULLIF(promo_gross, 0), 1) AS avg_discount_pct
    FROM prod_sales
    ORDER BY gross_pln DESC
    LIMIT {int(limit)}
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
    ])
