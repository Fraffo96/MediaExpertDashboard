"""YoY vendite e peak events."""
from google.cloud import bigquery

from app.db.client import run_query

from ._common import _P, _WHERE


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
