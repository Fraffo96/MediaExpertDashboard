"""Query peak events brand vs media per Market Intelligence."""
from app.db.client import run_query
from .shared import params, where_cat_subcat, from_table, where_channel


def query_peak_events_brand_vs_media(ps, pe, brand_id, cat=None, subcat=None, channel=None):
    """Dipendenza da peak events: brand vs media % dell'annuale."""
    tbl = from_table(channel)
    wch = where_channel(channel)
    q = f"""
    WITH annual_brand AS (
      SELECT SUM(f.gross_pln) AS annual_gross
      FROM {tbl} f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand
        {where_cat_subcat()}
        {wch}
    ),
    annual_media AS (
      SELECT SUM(f.gross_pln) AS annual_gross
      FROM {tbl} f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        {where_cat_subcat()}
        {wch}
    ),
    brand_events AS (
      SELECT d.peak_event, SUM(f.gross_pln) AS gross_pln
      FROM {tbl} f
      JOIN mart.dim_date d ON d.date = f.date
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand
        {where_cat_subcat()}
        {wch}
      GROUP BY d.peak_event
    ),
    media_events AS (
      SELECT d.peak_event, SUM(f.gross_pln) AS gross_pln
      FROM {tbl} f
      JOIN mart.dim_date d ON d.date = f.date
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        {where_cat_subcat()}
        {wch}
      GROUP BY d.peak_event
    )
    SELECT COALESCE(be.peak_event, me.peak_event) AS peak_event,
      ROUND(100.0 * COALESCE(be.gross_pln, 0) / NULLIF((SELECT annual_gross FROM annual_brand), 0), 1) AS brand_pct_of_annual,
      ROUND(100.0 * COALESCE(me.gross_pln, 0) / NULLIF((SELECT annual_gross FROM annual_media), 0), 1) AS media_pct_of_annual
    FROM brand_events be
    FULL OUTER JOIN media_events me ON be.peak_event = me.peak_event
    WHERE COALESCE(be.peak_event, me.peak_event) IS NOT NULL
    ORDER BY media_pct_of_annual DESC
    """
    return run_query(q, params(ps, pe, brand_id, cat, subcat, channel))
