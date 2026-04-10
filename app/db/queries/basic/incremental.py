"""Incremental YoY da fact_promo_performance."""
from google.cloud import bigquery

from app.db.client import run_query

from ._common import _P


def query_incremental_yoy(ps, pe, cat=None, seg=None, gender=None, brand=None, promo_id=None):
    q = """
    WITH yp AS (
      SELECT EXTRACT(YEAR FROM fp.date) AS year,
        SUM(fp.incremental_sales_pln) AS incremental_sales,
        SUM(fp.attributed_sales_pln) AS attributed_sales
      FROM mart.fact_promo_performance fp
      WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND (@cat IS NULL OR fp.category_id = @cat)
        AND (@brand IS NULL OR fp.brand_id = @brand)
        AND (@promo IS NULL OR fp.promo_id = @promo)
      GROUP BY 1
    ),
    yt AS (
      SELECT EXTRACT(YEAR FROM f.date) AS year, SUM(f.gross_pln) AS total_gross
      FROM mart.fact_sales_daily f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND (@cat IS NULL OR f.parent_category_id = @cat OR f.category_id = @cat)
        AND (@seg IS NULL OR f.segment_id = @seg)
        AND (@brand IS NULL OR f.brand_id = @brand)
        AND (@gender IS NULL OR f.gender = @gender)
      GROUP BY 1
    )
    SELECT yp.year, yp.incremental_sales, yp.attributed_sales, yt.total_gross,
      ROUND(100.0 * yp.incremental_sales / NULLIF(yt.total_gross, 0), 1) AS incremental_pct
    FROM yp JOIN yt ON yt.year = yp.year ORDER BY yp.year
    """
    params = _P(ps, pe, cat, seg, gender, brand) + [
        bigquery.ScalarQueryParameter("promo", "INT64", int(promo_id) if promo_id else None),
    ]
    return run_query(q, params)


def query_incremental_yoy_by_promo(ps, pe, cat=None, seg=None, gender=None, brand=None, promo_ids=None):
    """Incremental YoY broken down by promo (for compare mode). promo_ids = list of int or None for all."""
    if not promo_ids:
        promo_ids = []
    q = """
    WITH yp AS (
      SELECT EXTRACT(YEAR FROM fp.date) AS year, fp.promo_id,
        SUM(fp.incremental_sales_pln) AS incremental_sales,
        SUM(fp.attributed_sales_pln) AS attributed_sales
      FROM mart.fact_promo_performance fp
      WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND (@cat IS NULL OR fp.category_id = @cat)
        AND (@brand IS NULL OR fp.brand_id = @brand)
        AND fp.promo_id IN UNNEST(@promo_ids)
      GROUP BY 1, 2
    ),
    yt AS (
      SELECT EXTRACT(YEAR FROM f.date) AS year, SUM(f.gross_pln) AS total_gross
      FROM mart.fact_sales_daily f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND (@cat IS NULL OR f.parent_category_id = @cat OR f.category_id = @cat)
        AND (@seg IS NULL OR f.segment_id = @seg)
        AND (@brand IS NULL OR f.brand_id = @brand)
        AND (@gender IS NULL OR f.gender = @gender)
      GROUP BY 1
    )
    SELECT yp.year, yp.promo_id, p.promo_name, yp.incremental_sales, yp.attributed_sales, yt.total_gross,
      ROUND(100.0 * yp.incremental_sales / NULLIF(yt.total_gross, 0), 1) AS incremental_pct
    FROM yp
    JOIN mart.dim_promo p ON p.promo_id = yp.promo_id
    JOIN yt ON yt.year = yp.year
    ORDER BY yp.year, yp.promo_id
    """
    params = _P(ps, pe, cat, seg, gender, brand) + [
        bigquery.ArrayQueryParameter("promo_ids", "INT64", [int(x) for x in promo_ids] if promo_ids else []),
    ]
    return run_query(q, params)
