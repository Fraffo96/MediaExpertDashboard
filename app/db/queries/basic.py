"""
Query per dashboard BASIC – struttura McKinsey/Nielsen.
Supporta parent_category_id per raggruppamento top-level
e drill-down a subcategoria quando si filtra una parent category.
"""
from google.cloud import bigquery
from app.db.client import run_query

_P = lambda ps, pe, cat, seg, gender=None, brand=None: [
    bigquery.ScalarQueryParameter("ps", "STRING", ps),
    bigquery.ScalarQueryParameter("pe", "STRING", pe),
    bigquery.ScalarQueryParameter("cat", "INT64", int(cat) if cat else None),
    bigquery.ScalarQueryParameter("seg", "INT64", int(seg) if seg else None),
    bigquery.ScalarQueryParameter("gender", "STRING", gender if gender else None),
    bigquery.ScalarQueryParameter("brand", "INT64", int(brand) if brand else None),
]

_WHERE = """
  WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    AND (@cat IS NULL OR f.parent_category_id = @cat OR f.category_id = @cat)
    AND (@seg IS NULL OR f.segment_id = @seg)
    AND (@brand IS NULL OR f.brand_id = @brand)
    AND (@gender IS NULL OR f.gender = @gender)
"""


def _is_parent_cat(cat):
    """Parent categories have IDs 1-10, subcategories 101+."""
    return cat is not None and 1 <= int(cat) <= 10


# ─── KPI CARDS ────────────────────────────────────────────────────────────────

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


# ─── CATEGORY PERFORMANCE ─────────────────────────────────────────────────────

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


# ─── PROMOTION PERFORMANCE ────────────────────────────────────────────────────

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


# ─── YoY ──────────────────────────────────────────────────────────────────────

def query_yoy_detail(ps, pe):
    """YoY con breakdown year/gender/segment per filtro client-side."""
    q = """
    SELECT EXTRACT(YEAR FROM f.date) AS year, f.gender, f.segment_id,
      SUM(f.gross_pln) AS total_gross,
      SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross
    FROM mart.fact_sales_daily f
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY 1, 2, 3 ORDER BY year, gender, segment_id
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ])


def query_yoy(ps, pe, cat=None, seg=None, gender=None, brand=None):
    q = """
    WITH yearly AS (
      SELECT EXTRACT(YEAR FROM f.date) AS year,
        SUM(f.gross_pln) AS total_gross,
        SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross
      FROM mart.fact_sales_daily f
      WHERE (@cat IS NULL OR f.parent_category_id = @cat OR f.category_id = @cat)
        AND (@seg IS NULL OR f.segment_id = @seg)
        AND (@brand IS NULL OR f.brand_id = @brand)
        AND (@gender IS NULL OR f.gender = @gender)
      GROUP BY 1
    )
    SELECT year, total_gross, promo_gross,
      LAG(total_gross) OVER (ORDER BY year) AS prior_gross,
      ROUND(100.0 * (total_gross - LAG(total_gross) OVER (ORDER BY year))
            / NULLIF(LAG(total_gross) OVER (ORDER BY year), 0), 1) AS yoy_pct
    FROM yearly ORDER BY year
    """
    return run_query(q, _P(ps, pe, cat, seg, gender, brand))


# ─── EVENT DEPENDENCE ─────────────────────────────────────────────────────────

def query_peak_events_detail(ps, pe):
    """Peak events con breakdown peak_event/gender/segment per filtro client-side."""
    q = """
    SELECT d.peak_event, f.gender, f.segment_id,
      SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units,
      COUNT(DISTINCT f.date) AS days_count
    FROM mart.fact_sales_daily f
    JOIN mart.dim_date d ON d.date = f.date
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY d.peak_event, f.gender, f.segment_id ORDER BY peak_event, gross_pln DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ])


def query_peak_events(ps, pe, cat=None, seg=None, gender=None, brand=None):
    q = f"""
    WITH annual AS (
      SELECT SUM(f.gross_pln) AS annual_gross FROM mart.fact_sales_daily f {_WHERE}
    )
    SELECT d.peak_event, SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units,
      COUNT(DISTINCT f.date) AS days_count,
      ROUND(SUM(f.gross_pln) / NULLIF(COUNT(DISTINCT f.date), 0), 2) AS avg_daily_gross,
      ROUND(100.0 * SUM(f.gross_pln) / NULLIF((SELECT annual_gross FROM annual), 0), 1) AS pct_of_annual
    FROM mart.fact_sales_daily f
    JOIN mart.dim_date d ON d.date = f.date
    {_WHERE}
    GROUP BY d.peak_event ORDER BY gross_pln DESC
    """
    return run_query(q, _P(ps, pe, cat, seg, gender, brand))


# ─── ADDITIONAL ───────────────────────────────────────────────────────────────

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


# ─── PRODUCT ANALYTICS (fact_order_items + dim_product) ────────────────────────

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


# ─── BUYER ANALYTICS (fact_orders + dim_customer) ────────────────────────────

def query_channel_mix(ps, pe, channel=None):
    channel_clause = " AND o.channel = @channel" if (channel and str(channel).strip()) else ""
    q = f"""
    SELECT o.channel,
      COUNT(DISTINCT o.customer_id) AS buyers, COUNT(*) AS orders,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov
    FROM mart.fact_orders o
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe){channel_clause}
    GROUP BY o.channel ORDER BY gross_pln DESC
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    if channel and str(channel).strip():
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", (channel or "").strip()))
    return run_query(q, params)


def query_loyalty_breakdown(ps, pe, channel=None):
    channel_clause = " AND o.channel = @channel" if (channel and str(channel).strip()) else ""
    q = f"""
    SELECT c.loyalty_tier,
      COUNT(DISTINCT o.customer_id) AS buyers, COUNT(*) AS orders,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe){channel_clause}
    GROUP BY c.loyalty_tier ORDER BY gross_pln DESC
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    if channel and str(channel).strip():
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", (channel or "").strip()))
    return run_query(q, params)


def query_buyer_segments(ps, pe, channel=None):
    channel_clause = " AND o.channel = @channel" if (channel and str(channel).strip()) else ""
    q = f"""
    SELECT s.segment_name, s.segment_id,
      COUNT(DISTINCT o.customer_id) AS buyers, COUNT(*) AS orders,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov,
      ROUND(100.0 * COUNTIF(o.promo_flag) / NULLIF(COUNT(*), 0), 1) AS promo_pct
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    JOIN mart.dim_segment s ON s.segment_id = c.segment_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe){channel_clause}
    GROUP BY s.segment_name, s.segment_id ORDER BY gross_pln DESC
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    if channel and str(channel).strip():
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", (channel or "").strip()))
    return run_query(q, params)


def query_buyer_demographics(ps, pe, channel=None):
    channel_clause = " AND o.channel = @channel" if (channel and str(channel).strip()) else ""
    q = f"""
    SELECT c.gender, c.age_band,
      COUNT(DISTINCT o.customer_id) AS buyers,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe){channel_clause}
    GROUP BY c.gender, c.age_band ORDER BY gross_pln DESC
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    if channel and str(channel).strip():
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", (channel or "").strip()))
    return run_query(q, params)


def query_loyalty_breakdown_by_channel(ps, pe):
    """Loyalty per canale per filtro client-side istantaneo."""
    q = """
    SELECT o.channel, c.loyalty_tier,
      COUNT(DISTINCT o.customer_id) AS buyers, COUNT(*) AS orders,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY o.channel, c.loyalty_tier ORDER BY o.channel, gross_pln DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ])


def query_buyer_segments_by_channel(ps, pe):
    """Buyer segments per canale per filtro client-side istantaneo."""
    q = """
    SELECT o.channel, s.segment_name, s.segment_id,
      COUNT(DISTINCT o.customer_id) AS buyers, COUNT(*) AS orders,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov,
      ROUND(100.0 * COUNTIF(o.promo_flag) / NULLIF(COUNT(*), 0), 1) AS promo_pct
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    JOIN mart.dim_segment s ON s.segment_id = c.segment_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY o.channel, s.segment_name, s.segment_id ORDER BY o.channel, gross_pln DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ])


def query_buyer_demographics_by_channel(ps, pe):
    """Buyer demographics per canale per filtro client-side istantaneo."""
    q = """
    SELECT o.channel, c.gender, c.age_band,
      COUNT(DISTINCT o.customer_id) AS buyers,
      SUM(o.gross_pln) AS gross_pln, ROUND(AVG(o.gross_pln), 2) AS aov
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    GROUP BY o.channel, c.gender, c.age_band ORDER BY o.channel, gross_pln DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ])


def query_repeat_rate_by_channel(ps, pe):
    """Repeat rate per canale per filtro client-side istantaneo."""
    q = """
    WITH bf AS (
      SELECT customer_id, channel, COUNT(*) AS n_orders, SUM(gross_pln) AS spend
      FROM mart.fact_orders
      WHERE date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      GROUP BY customer_id, channel
    )
    SELECT channel,
      COUNT(*) AS total_buyers,
      COUNTIF(n_orders > 1) AS repeat_buyers,
      ROUND(100.0 * COUNTIF(n_orders > 1) / NULLIF(COUNT(*), 0), 1) AS repeat_rate_pct,
      ROUND(AVG(n_orders), 1) AS avg_frequency,
      ROUND(AVG(spend), 2) AS avg_lifetime_spend
    FROM bf GROUP BY channel ORDER BY total_buyers DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ])


def query_repeat_rate(ps, pe, channel=None):
    channel_clause = " AND channel = @channel" if (channel and str(channel).strip()) else ""
    q = f"""
    WITH bf AS (
      SELECT customer_id, COUNT(*) AS n_orders, SUM(gross_pln) AS spend
      FROM mart.fact_orders
      WHERE date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe){channel_clause}
      GROUP BY customer_id
    )
    SELECT COUNT(*) AS total_buyers,
      COUNTIF(n_orders > 1) AS repeat_buyers,
      ROUND(100.0 * COUNTIF(n_orders > 1) / NULLIF(COUNT(*), 0), 1) AS repeat_rate_pct,
      ROUND(AVG(n_orders), 1) AS avg_frequency,
      ROUND(AVG(spend), 2) AS avg_lifetime_spend
    FROM bf
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    if channel and str(channel).strip():
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", (channel or "").strip()))
    return run_query(q, params)


def query_channel_by_segment(ps, pe, channel=None):
    channel_clause = " AND o.channel = @channel" if (channel and str(channel).strip()) else ""
    q = f"""
    SELECT s.segment_name, o.channel,
      COUNT(DISTINCT o.customer_id) AS buyers, SUM(o.gross_pln) AS gross_pln
    FROM mart.fact_orders o
    JOIN mart.dim_customer c ON c.customer_id = o.customer_id
    JOIN mart.dim_segment s ON s.segment_id = c.segment_id
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe){channel_clause}
    GROUP BY s.segment_name, o.channel ORDER BY s.segment_name, gross_pln DESC
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    if channel and str(channel).strip():
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", (channel or "").strip()))
    return run_query(q, params)
