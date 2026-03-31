"""Query per categorie e sottocategorie del brand (metadata)."""
from google.cloud import bigquery

from app.db.client import run_query
from .shared import params


def query_brand_categories(ps, pe, brand_id):
    """Parent categories (1-10) dove il brand ha vendite, ordinate per vendite brand desc."""
    q = """
    WITH brand_sales AS (
      SELECT parent_category_id AS category_id, SUM(gross_pln) AS gross_pln
      FROM mart.fact_sales_daily
      WHERE date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND brand_id = @brand AND gross_pln > 0
      GROUP BY parent_category_id
    )
    SELECT c.category_id, c.category_name
    FROM mart.dim_category c
    JOIN brand_sales b ON b.category_id = c.category_id
    WHERE c.level = 1
    ORDER BY b.gross_pln DESC
    """
    return run_query(q, params(ps, pe, brand_id, None, None))


def query_brand_subcategories(ps, pe, brand_id, parent_id):
    """Sottocategorie dove il brand ha vendite nella parent category data."""
    p = int(parent_id) if parent_id else None
    pparams = params(ps, pe, brand_id, parent_id, None) + [
        bigquery.ScalarQueryParameter("parent", "INT64", p),
    ]
    q = """
    SELECT c.category_id, c.category_name
    FROM mart.dim_category c
    WHERE c.level = 2 AND c.parent_category_id = @parent
      AND EXISTS (
        SELECT 1 FROM mart.fact_sales_daily f
        WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
          AND f.brand_id = @brand
          AND f.category_id = c.category_id
          AND f.gross_pln > 0
      )
    ORDER BY c.category_id
    """
    return run_query(q, pparams)
