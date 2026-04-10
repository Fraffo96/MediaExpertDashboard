"""ROI promo e discount depth per categoria."""
from google.cloud import bigquery

from app.db.client import run_query

from ._common import _P, _WHERE, _is_parent_cat


def query_promo_roi_by_type(ps, pe, cat=None, seg=None, gender=None, brand=None):
    q = """
    SELECT p.promo_name, p.promo_type,
      ROUND(AVG(fp.roi), 2) AS avg_roi,
      SUM(fp.attributed_sales_pln) AS total_attributed,
      SUM(fp.incremental_sales_pln) AS total_incremental
    FROM mart.fact_promo_performance fp
    JOIN mart.dim_promo p ON p.promo_id = fp.promo_id
    WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND (@cat IS NULL OR fp.category_id = @cat)
      AND (@brand IS NULL OR fp.brand_id = @brand)
    GROUP BY p.promo_name, p.promo_type ORDER BY avg_roi DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("cat", "INT64", int(cat) if cat and int(cat) < 100 else None),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand) if brand else None),
    ])


def query_promo_roi_by_category(ps, pe, cat=None, brand=None):
    """ROI promo aggregato per categoria (parent). Per selettore Dimension su chart ROI."""
    q = """
    SELECT c.category_id, c.category_name,
      ROUND(AVG(fp.roi), 2) AS avg_roi,
      SUM(fp.attributed_sales_pln) AS total_attributed,
      SUM(fp.incremental_sales_pln) AS total_incremental
    FROM mart.fact_promo_performance fp
    JOIN mart.dim_category c ON c.category_id = fp.category_id AND c.level = 1
    WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND (@cat IS NULL OR fp.category_id = @cat)
      AND (@brand IS NULL OR fp.brand_id = @brand)
    GROUP BY c.category_id, c.category_name ORDER BY avg_roi DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("cat", "INT64", int(cat) if cat and int(cat) < 100 else None),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand) if brand else None),
    ])


def query_promo_roi_by_brand(ps, pe, cat=None, brand=None):
    """ROI promo aggregato per brand. Per selettore Dimension su chart ROI."""
    q = """
    SELECT fp.brand_id, fp.brand_name,
      ROUND(AVG(fp.roi), 2) AS avg_roi,
      SUM(fp.attributed_sales_pln) AS total_attributed,
      SUM(fp.incremental_sales_pln) AS total_incremental
    FROM mart.fact_promo_performance fp
    WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND (@cat IS NULL OR fp.category_id = @cat)
      AND (@brand IS NULL OR fp.brand_id = @brand)
    GROUP BY fp.brand_id, fp.brand_name ORDER BY avg_roi DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("cat", "INT64", int(cat) if cat and int(cat) < 100 else None),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand) if brand else None),
    ])


def query_promo_roi_detail(ps, pe, cat=None, brand=None):
    """ROI promo a grana (promo, brand, category) per filtro chart: filtra per brand/category lato client."""
    q = """
    SELECT p.promo_id, p.promo_name, p.promo_type,
      fp.brand_id, fp.brand_name, fp.category_id AS category_id, c.category_name,
      ROUND(AVG(fp.roi), 2) AS avg_roi,
      SUM(fp.attributed_sales_pln) AS total_attributed,
      SUM(fp.incremental_sales_pln) AS total_incremental
    FROM mart.fact_promo_performance fp
    JOIN mart.dim_promo p ON p.promo_id = fp.promo_id
    JOIN mart.dim_category c ON c.category_id = fp.category_id AND c.level = 1
    WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND (@cat IS NULL OR fp.category_id = @cat)
      AND (@brand IS NULL OR fp.brand_id = @brand)
    GROUP BY p.promo_id, p.promo_name, p.promo_type, fp.brand_id, fp.brand_name, fp.category_id, c.category_name
    ORDER BY p.promo_type, fp.brand_name, c.category_name
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("cat", "INT64", int(cat) if cat and int(cat) < 100 else None),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand) if brand else None),
    ])


def query_discount_depth_by_category(ps, pe, cat=None, seg=None, gender=None, brand=None):
    wavg = "ROUND(COALESCE(SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END) / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0), 0), 1) AS avg_discount_depth"
    if _is_parent_cat(cat):
        q = f"""
        SELECT c.category_name, c.category_id, {wavg}
        FROM mart.fact_sales_daily f
        JOIN mart.dim_category c ON c.category_id = f.category_id
        {_WHERE}
        GROUP BY c.category_name, c.category_id ORDER BY avg_discount_depth DESC
        """
    else:
        q = f"""
        SELECT pc.category_name, pc.category_id, {wavg}
        FROM mart.fact_sales_daily f
        JOIN mart.dim_category pc ON pc.category_id = f.parent_category_id
        {_WHERE}
        GROUP BY pc.category_name, pc.category_id ORDER BY avg_discount_depth DESC
        """
    return run_query(q, _P(ps, pe, cat, seg, gender, brand))
