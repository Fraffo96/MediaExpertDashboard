"""Query discount depth brand vs media per Market Intelligence."""
from google.cloud.bigquery import ArrayQueryParameter
from google.cloud import bigquery

from app.db.client import run_query
from .shared import params, where_cat_subcat


def query_discount_depth_brand_vs_media(ps, pe, brand_id, cat=None, subcat=None):
    """Media discount depth: brand vs media per category. Solo categorie con prodotti brand."""
    q = """
    WITH brand_data AS (
      SELECT c.category_id, c.category_name,
        ROUND(COALESCE(
          SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
          / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0)
        , 0), 1) AS avg_discount_depth
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.parent_category_id AND c.level = 1
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand AND f.gross_pln > 0
      GROUP BY c.category_id, c.category_name
    ),
    media_data AS (
      SELECT c.category_id, c.category_name,
        ROUND(COALESCE(
          SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
          / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0)
        , 0), 1) AS avg_discount_depth
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.parent_category_id AND c.level = 1
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      GROUP BY c.category_id, c.category_name
    )
    SELECT b.category_id, b.category_name,
      COALESCE(b.avg_discount_depth, 0) AS brand_avg_discount_depth,
      COALESCE(m.avg_discount_depth, 0) AS media_avg_discount_depth
    FROM brand_data b
    LEFT JOIN media_data m ON b.category_id = m.category_id
    ORDER BY b.avg_discount_depth DESC
    """
    return run_query(q, params(ps, pe, brand_id, None, None))


def query_discount_depth_single(ps, pe, brand_id, cat=None, subcat=None):
    """Singola riga: brand vs media avg discount depth per scope category/subcategory."""
    q = f"""
    WITH brand_data AS (
      SELECT ROUND(COALESCE(
        SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
        / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0)
      , 0), 1) AS avg_discount_depth
      FROM mart.fact_sales_daily f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand
        {where_cat_subcat()}
    ),
    media_data AS (
      SELECT ROUND(COALESCE(
        SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
        / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0)
      , 0), 1) AS avg_discount_depth
      FROM mart.fact_sales_daily f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        {where_cat_subcat()}
    )
    SELECT b.avg_discount_depth AS brand_avg_discount_depth, m.avg_discount_depth AS media_avg_discount_depth
    FROM brand_data b, media_data m
    """
    return run_query(q, params(ps, pe, brand_id, cat, subcat))


def query_discount_depth_for_all_subcategories(ps, pe, brand_id, subcategory_ids):
    """Discount depth brand vs media per sottocategoria. Per tutte le sub in una query."""
    if not subcategory_ids:
        return []
    ids = [int(x) for x in subcategory_ids if x]
    if not ids:
        return []
    pparams = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id) if brand_id else None),
        ArrayQueryParameter("sub_ids", "INT64", ids),
    ]
    q = """
    WITH brand_data AS (
      SELECT f.category_id,
        ROUND(COALESCE(
          SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
          / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0)
        , 0), 1) AS brand_avg_discount_depth
      FROM mart.fact_sales_daily f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand AND f.category_id IN UNNEST(@sub_ids) AND f.gross_pln > 0
      GROUP BY f.category_id
    ),
    media_data AS (
      SELECT f.category_id,
        ROUND(COALESCE(
          SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
          / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0)
        , 0), 1) AS media_avg_discount_depth
      FROM mart.fact_sales_daily f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.category_id IN UNNEST(@sub_ids)
      GROUP BY f.category_id
    )
    SELECT COALESCE(b.category_id, m.category_id) AS category_id,
      COALESCE(b.brand_avg_discount_depth, 0) AS brand_avg_discount_depth,
      COALESCE(m.media_avg_discount_depth, 0) AS media_avg_discount_depth
    FROM brand_data b
    FULL OUTER JOIN media_data m ON b.category_id = m.category_id
    ORDER BY COALESCE(b.category_id, m.category_id)
    """
    return run_query(q, pparams)
