"""Query precalc: base (brand categories, subcategories, competitors)."""
from google.cloud import bigquery

from app.db.client import run_query


def query_brand_categories_from_precalc(year: int, brand_id: int) -> list[dict]:
    """Parent categories (1-10) dove il brand ha vendite."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    q = """
    WITH brand_sales AS (
      SELECT parent_category_id AS category_id, SUM(gross_pln) AS gross_pln
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND channel = '' AND gross_pln > 0
      GROUP BY parent_category_id
    )
    SELECT c.category_id, c.category_name
    FROM mart.dim_category c
    JOIN brand_sales b ON b.category_id = c.category_id
    WHERE c.level = 1
    ORDER BY b.gross_pln DESC
    """
    return run_query(q, p)


def query_brand_all_subcategories_from_precalc(year: int, brand_id: int, parent_ids: list[int]) -> list[dict]:
    """Tutte le subcategorie con vendite per più parent in una query (sostituisce N chiamate separate)."""
    if not parent_ids:
        return []
    pids = sorted({int(x) for x in parent_ids})
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ArrayQueryParameter("parents", "INT64", pids),
    ]
    q = """
    SELECT c.parent_category_id, c.category_id, c.category_name
    FROM mart.dim_category c
    WHERE c.level = 2
      AND c.parent_category_id IN UNNEST(@parents)
      AND EXISTS (
        SELECT 1 FROM mart.precalc_sales_agg f
        WHERE f.year = @year AND f.brand_id = @brand AND f.channel = ''
          AND f.category_id = c.category_id AND f.gross_pln > 0
      )
    ORDER BY c.parent_category_id, c.category_id
    """
    return run_query(q, p)


def query_brand_subcategories_from_precalc(year: int, brand_id: int, parent_id: int) -> list[dict]:
    """Subcategorie dove il brand ha vendite nella parent category."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("parent", "INT64", parent_id),
    ]
    q = """
    SELECT c.category_id, c.category_name
    FROM mart.dim_category c
    WHERE c.level = 2 AND c.parent_category_id = @parent
      AND EXISTS (
        SELECT 1 FROM mart.precalc_sales_agg f
        WHERE f.year = @year AND f.brand_id = @brand AND f.channel = ''
          AND f.category_id = c.category_id AND f.gross_pln > 0
      )
    ORDER BY c.category_id
    """
    return run_query(q, p)


def query_competitors_in_scope_from_precalc(year: int, brand_id: int) -> list[dict]:
    """Brands che vendono nelle stesse categorie del brand."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    q = """
    WITH brand_cats AS (
      SELECT DISTINCT parent_category_id
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND channel = '' AND gross_pln > 0
    )
    SELECT c.brand_id, MAX(c.brand_name) AS brand_name
    FROM mart.precalc_sales_agg c
    JOIN brand_cats bc ON c.parent_category_id = bc.parent_category_id
    WHERE c.year = @year AND c.brand_id != @brand AND c.channel = ''
    GROUP BY c.brand_id
    ORDER BY brand_name
    """
    return run_query(q, p)
