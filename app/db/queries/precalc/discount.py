"""Query precalc: discount depth (BC e MI)."""
from google.cloud import bigquery
from google.cloud.bigquery import ArrayQueryParameter

from app.db.client import run_query


def query_discount_depth_brand_vs_competitor_all_categories_from_precalc(
    year: int, brand_id: int, competitor_id: int
) -> list[dict]:
    """Discount depth per categoria: brand vs competitor."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("competitor", "INT64", competitor_id),
    ]
    q = """
    WITH brand_data AS (
      SELECT f.parent_category_id AS category_id, dc.category_name,
        ROUND(COALESCE(SUM(f.discount_depth_weighted) / NULLIF(SUM(f.promo_gross), 0), 0), 1) AS brand_avg_discount_depth
      FROM mart.precalc_sales_agg f
      JOIN mart.dim_category dc ON dc.category_id = f.parent_category_id AND dc.level = 1
      WHERE f.year = @year AND f.brand_id = @brand AND f.channel = '' AND f.gross_pln > 0
      GROUP BY f.parent_category_id, dc.category_name
    ),
    comp_data AS (
      SELECT f.parent_category_id AS category_id,
        ROUND(COALESCE(SUM(f.discount_depth_weighted) / NULLIF(SUM(f.promo_gross), 0), 0), 1) AS media_avg_discount_depth
      FROM mart.precalc_sales_agg f
      WHERE f.year = @year AND f.brand_id = @competitor AND f.channel = ''
      GROUP BY f.parent_category_id
    )
    SELECT b.category_id, b.category_name,
      COALESCE(b.brand_avg_discount_depth, 0) AS brand_avg_discount_depth,
      COALESCE(c.media_avg_discount_depth, 0) AS media_avg_discount_depth
    FROM brand_data b
    LEFT JOIN comp_data c ON b.category_id = c.category_id
    ORDER BY b.brand_avg_discount_depth DESC
    """
    return run_query(q, p)


def query_discount_depth_for_all_subcategories_bc_from_precalc(
    year: int, brand_id: int, competitor_id: int, subcategory_ids: list[int]
) -> list[dict]:
    """Discount depth per subcategoria: brand vs competitor."""
    if not subcategory_ids:
        return []
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("competitor", "INT64", competitor_id),
        ArrayQueryParameter("sub_ids", "INT64", subcategory_ids),
    ]
    q = """
    WITH brand_data AS (
      SELECT category_id,
        ROUND(COALESCE(SUM(discount_depth_weighted) / NULLIF(SUM(promo_gross), 0), 0), 1) AS brand_avg_discount_depth
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND channel = '' AND category_id IN UNNEST(@sub_ids) AND gross_pln > 0
      GROUP BY category_id
    ),
    comp_data AS (
      SELECT category_id,
        ROUND(COALESCE(SUM(discount_depth_weighted) / NULLIF(SUM(promo_gross), 0), 0), 1) AS media_avg_discount_depth
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @competitor AND channel = '' AND category_id IN UNNEST(@sub_ids)
      GROUP BY category_id
    )
    SELECT COALESCE(b.category_id, c.category_id) AS category_id,
      COALESCE(b.brand_avg_discount_depth, 0) AS brand_avg_discount_depth,
      COALESCE(c.media_avg_discount_depth, 0) AS media_avg_discount_depth
    FROM brand_data b
    FULL OUTER JOIN comp_data c ON b.category_id = c.category_id
    ORDER BY COALESCE(b.category_id, c.category_id)
    """
    return run_query(q, p)


def query_discount_depth_brand_vs_media_from_precalc(year: int, brand_id: int) -> list[dict]:
    """Discount depth per parent category: brand vs media."""
    q = """
    WITH brand_data AS (
      SELECT parent_category_id AS category_id,
        ROUND(COALESCE(SUM(discount_depth_weighted) / NULLIF(SUM(promo_gross), 0), 0), 1) AS avg_discount_depth
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND channel = '' AND gross_pln > 0
      GROUP BY parent_category_id
    ),
    media_data AS (
      SELECT parent_category_id AS category_id,
        ROUND(COALESCE(SUM(discount_depth_weighted) / NULLIF(SUM(promo_gross), 0), 0), 1) AS avg_discount_depth
      FROM mart.precalc_sales_agg
      WHERE year = @year AND channel = '' AND brand_id != @brand
      GROUP BY parent_category_id
    )
    SELECT c.category_id, c.category_name,
      COALESCE(b.avg_discount_depth, 0) AS brand_avg_discount_depth,
      COALESCE(m.avg_discount_depth, 0) AS media_avg_discount_depth
    FROM mart.dim_category c
    JOIN brand_data b ON b.category_id = c.category_id
    LEFT JOIN media_data m ON m.category_id = c.category_id
    WHERE c.level = 1
    ORDER BY b.avg_discount_depth DESC
    """
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    return run_query(q, p)


def query_discount_depth_for_all_subcategories_from_precalc(
    year: int, brand_id: int, subcategory_ids: list[int]
) -> list[dict]:
    """Discount depth per subcategory: brand vs media."""
    if not subcategory_ids:
        return []
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        ArrayQueryParameter("sub_ids", "INT64", subcategory_ids),
    ]
    q = """
    WITH brand_data AS (
      SELECT category_id,
        ROUND(COALESCE(SUM(discount_depth_weighted) / NULLIF(SUM(promo_gross), 0), 0), 1) AS brand_avg_discount_depth
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND channel = '' AND category_id IN UNNEST(@sub_ids) AND gross_pln > 0
      GROUP BY category_id
    ),
    media_data AS (
      SELECT category_id,
        ROUND(COALESCE(SUM(discount_depth_weighted) / NULLIF(SUM(promo_gross), 0), 0), 1) AS media_avg_discount_depth
      FROM mart.precalc_sales_agg
      WHERE year = @year AND channel = '' AND brand_id != @brand AND category_id IN UNNEST(@sub_ids)
      GROUP BY category_id
    )
    SELECT COALESCE(b.category_id, m.category_id) AS category_id,
      COALESCE(b.brand_avg_discount_depth, 0) AS brand_avg_discount_depth,
      COALESCE(m.media_avg_discount_depth, 0) AS media_avg_discount_depth
    FROM brand_data b
    FULL OUTER JOIN media_data m ON b.category_id = m.category_id
    ORDER BY COALESCE(b.category_id, m.category_id)
    """
    return run_query(q, p)
