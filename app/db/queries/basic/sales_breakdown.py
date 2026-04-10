"""Brand, dettaglio vendite, crosstab, segmenti e gender."""
from google.cloud import bigquery

from app.db.client import run_query

from ._common import _P, _WHERE


def query_sales_by_brand_detail(ps, pe):
    """Vendite per brand con breakdown category/gender/segment/channel per filtro client-side istantaneo."""
    q = """
    SELECT b.brand_id, b.brand_name, f.parent_category_id, f.category_id,
      f.gender, f.segment_id, f.channel,
      SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units
    FROM mart.v_sales_daily_by_channel f
    JOIN mart.dim_brand b ON b.brand_id = f.brand_id
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY b.brand_id, b.brand_name, f.parent_category_id, f.category_id, f.gender, f.segment_id, f.channel
    ORDER BY b.brand_name, gross_pln DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ])


def query_sales_by_brand(ps, pe, cat=None, seg=None, gender=None, brand=None):
    q = f"""
    SELECT b.brand_name, b.brand_id, SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units
    FROM mart.fact_sales_daily f
    JOIN mart.dim_brand b ON b.brand_id = f.brand_id
    {_WHERE}
    GROUP BY b.brand_name, b.brand_id ORDER BY gross_pln DESC
    """
    return run_query(q, _P(ps, pe, cat, seg, gender, brand))


def query_sales_detail(ps, pe, cat=None, seg=None, gender=None, brand=None):
    q = f"""
    SELECT CAST(f.date AS STRING) AS date,
      pc.category_name AS parent_category, c.category_name AS subcategory,
      b.brand_name, s.segment_name, f.gender,
      f.gross_pln, f.net_pln, f.units, f.promo_flag, f.discount_depth_pct,
      f.parent_category_id, f.category_id
    FROM mart.fact_sales_daily f
    JOIN mart.dim_category c  ON c.category_id = f.category_id
    JOIN mart.dim_category pc ON pc.category_id = f.parent_category_id
    JOIN mart.dim_brand b     ON b.brand_id = f.brand_id
    JOIN mart.dim_segment s   ON s.segment_id = f.segment_id
    {_WHERE}
    ORDER BY f.date DESC LIMIT 200
    """
    return run_query(q, _P(ps, pe, cat, seg, gender, brand))


def query_sales_brand_category_crosstab(ps, pe, seg=None, gender=None, cat=None, brand=None):
    q = f"""
    SELECT b.brand_id, b.brand_name, pc.category_name, SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units
    FROM mart.fact_sales_daily f
    JOIN mart.dim_brand b     ON b.brand_id = f.brand_id
    JOIN mart.dim_category pc ON pc.category_id = f.parent_category_id
    {_WHERE}
    GROUP BY b.brand_id, b.brand_name, pc.category_name ORDER BY b.brand_name, gross_pln DESC
    """
    return run_query(q, _P(ps, pe, cat, seg, gender, brand))


def query_sales_by_category_by_segment(ps, pe, cat=None, seg=None, gender=None, brand=None):
    q = f"""
    SELECT pc.category_name, pc.category_id, s.segment_name, s.segment_id, f.channel,
      SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units
    FROM mart.v_sales_daily_by_channel f
    JOIN mart.dim_category pc ON pc.category_id = f.parent_category_id
    JOIN mart.dim_segment s   ON s.segment_id = f.segment_id
    {_WHERE}
    GROUP BY pc.category_name, pc.category_id, s.segment_name, s.segment_id, f.channel
    ORDER BY pc.category_name, gross_pln DESC
    """
    return run_query(q, _P(ps, pe, cat, seg, gender, brand))


def query_sales_by_category_by_gender(ps, pe, cat=None, seg=None, gender=None, brand=None):
    q = f"""
    SELECT pc.category_name, pc.category_id, f.gender, f.channel,
      SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units
    FROM mart.v_sales_daily_by_channel f
    JOIN mart.dim_category pc ON pc.category_id = f.parent_category_id
    {_WHERE}
    GROUP BY pc.category_name, pc.category_id, f.gender, f.channel
    ORDER BY pc.category_name, gross_pln DESC
    """
    return run_query(q, _P(ps, pe, cat, seg, gender, brand))
