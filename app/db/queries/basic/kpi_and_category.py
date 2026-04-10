"""KPI, categorie, promo share, discount detail."""
from app.db.client import run_query

from ._common import _P, _WHERE, _is_parent_cat


def query_kpi(ps, pe, cat=None, seg=None, gender=None, brand=None):
    q = f"""
    SELECT
      SUM(gross_pln) AS total_gross, SUM(net_pln) AS total_net, SUM(units) AS total_units,
      SUM(CASE WHEN promo_flag THEN gross_pln ELSE 0 END) AS promo_gross,
      ROUND(100.0 * SUM(CASE WHEN promo_flag THEN gross_pln ELSE 0 END)
            / NULLIF(SUM(gross_pln), 0), 1) AS promo_share_pct,
      ROUND(COALESCE(
        SUM(CASE WHEN promo_flag THEN discount_depth_pct * gross_pln ELSE 0 END)
        / NULLIF(SUM(CASE WHEN promo_flag THEN gross_pln ELSE 0 END), 0)
      , 0), 1) AS avg_discount_depth,
      COUNT(DISTINCT parent_category_id) AS n_categories,
      COUNT(DISTINCT segment_id) AS n_segments
    FROM mart.fact_sales_daily f
    {_WHERE}
    """
    return run_query(q, _P(ps, pe, cat, seg, gender, brand))


def query_sales_by_category(ps, pe, cat=None, seg=None, gender=None, brand=None):
    if _is_parent_cat(cat):
        q = f"""
        SELECT c.category_name, c.category_id, SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units
        FROM mart.fact_sales_daily f
        JOIN mart.dim_category c ON c.category_id = f.category_id
        {_WHERE}
        GROUP BY c.category_name, c.category_id ORDER BY gross_pln DESC
        """
    else:
        q = f"""
        SELECT pc.category_name, pc.category_id, SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units
        FROM mart.fact_sales_daily f
        JOIN mart.dim_category pc ON pc.category_id = f.parent_category_id
        {_WHERE}
        GROUP BY pc.category_name, pc.category_id ORDER BY gross_pln DESC
        """
    return run_query(q, _P(ps, pe, cat, seg, gender, brand))


def query_promo_share(ps, pe, cat=None, seg=None, gender=None, brand=None):
    q = f"""
    SELECT
      SUM(gross_pln) AS total_gross,
      SUM(CASE WHEN promo_flag THEN gross_pln ELSE 0 END) AS promo_gross,
      ROUND(100.0 * SUM(CASE WHEN promo_flag THEN gross_pln ELSE 0 END)
            / NULLIF(SUM(gross_pln), 0), 1) AS promo_share_pct
    FROM mart.fact_sales_daily f
    {_WHERE}
    """
    return run_query(q, _P(ps, pe, cat, seg, gender, brand))


def query_promo_share_by_category(ps, pe, cat=None, seg=None, gender=None, brand=None):
    if _is_parent_cat(cat):
        q = f"""
        SELECT c.category_name, c.category_id,
          SUM(f.gross_pln) AS total_gross,
          SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross,
          ROUND(100.0 * SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END)
                / NULLIF(SUM(f.gross_pln), 0), 1) AS promo_share_pct
        FROM mart.fact_sales_daily f
        JOIN mart.dim_category c ON c.category_id = f.category_id
        {_WHERE}
        GROUP BY c.category_name, c.category_id ORDER BY promo_share_pct DESC
        """
    else:
        q = f"""
        SELECT pc.category_name, pc.category_id,
          SUM(f.gross_pln) AS total_gross,
          SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross,
          ROUND(100.0 * SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END)
                / NULLIF(SUM(f.gross_pln), 0), 1) AS promo_share_pct
        FROM mart.fact_sales_daily f
        JOIN mart.dim_category pc ON pc.category_id = f.parent_category_id
        {_WHERE}
        GROUP BY pc.category_name, pc.category_id ORDER BY promo_share_pct DESC
        """
    return run_query(q, _P(ps, pe, cat, seg, gender, brand))


def query_sales_by_subcategory(ps, pe, cat=None, seg=None, gender=None, brand=None):
    """Vendite per subcategoria (sempre livello category_id). Per grafici “per subcategoria”."""
    q = f"""
    SELECT c.category_name, c.category_id, SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units
    FROM mart.fact_sales_daily f
    JOIN mart.dim_category c ON c.category_id = f.category_id
    {_WHERE}
    GROUP BY c.category_name, c.category_id ORDER BY gross_pln DESC
    """
    return run_query(q, _P(ps, pe, cat, seg, gender, brand))


def query_promo_share_by_subcategory(ps, pe, cat=None, seg=None, gender=None, brand=None):
    """Promo share per subcategoria. Per grafici “per subcategoria”."""
    q = f"""
    SELECT c.category_name, c.category_id,
      SUM(f.gross_pln) AS total_gross,
      SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross,
      ROUND(100.0 * SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END)
            / NULLIF(SUM(f.gross_pln), 0), 1) AS promo_share_pct
    FROM mart.fact_sales_daily f
    JOIN mart.dim_category c ON c.category_id = f.category_id
    {_WHERE}
    GROUP BY c.category_name, c.category_id ORDER BY promo_share_pct DESC
    """
    return run_query(q, _P(ps, pe, cat, seg, gender, brand))


def query_promo_share_detail(ps, pe):
    """Promo share a grana (category, brand, segment, gender, channel) da v_sales_daily_by_channel."""
    q = """
    SELECT f.category_id AS category_id, c.category_name,
      f.parent_category_id, pc.category_name AS parent_name,
      f.brand_id, b.brand_name,
      f.segment_id, f.gender, f.channel,
      SUM(f.gross_pln) AS total_gross,
      SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross
    FROM mart.v_sales_daily_by_channel f
    JOIN mart.dim_category c ON c.category_id = f.category_id
    JOIN mart.dim_category pc ON pc.category_id = f.parent_category_id
    JOIN mart.dim_brand b ON b.brand_id = f.brand_id
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY f.category_id, c.category_name, f.parent_category_id, pc.category_name, f.brand_id, b.brand_name, f.segment_id, f.gender, f.channel
    ORDER BY f.parent_category_id, f.category_id, b.brand_name
    """
    return run_query(q, _P(ps, pe, None, None, None, None))


def query_discount_depth_detail(ps, pe):
    """Discount depth a grana (category, brand, segment, gender, channel) da v_sales_daily_by_channel."""
    q = """
    SELECT f.category_id AS category_id, c.category_name,
      f.parent_category_id, pc.category_name AS parent_name,
      f.brand_id, b.brand_name,
      f.segment_id, f.gender, f.channel,
      ROUND(COALESCE(
        SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
        / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0)
      , 0), 1) AS avg_discount_depth,
      SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross
    FROM mart.v_sales_daily_by_channel f
    JOIN mart.dim_category c ON c.category_id = f.category_id
    JOIN mart.dim_category pc ON pc.category_id = f.parent_category_id
    JOIN mart.dim_brand b ON b.brand_id = f.brand_id
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY f.category_id, c.category_name, f.parent_category_id, pc.category_name, f.brand_id, b.brand_name, f.segment_id, f.gender, f.channel
    ORDER BY f.parent_category_id, f.category_id, b.brand_name
    """
    return run_query(q, _P(ps, pe, None, None, None, None))
