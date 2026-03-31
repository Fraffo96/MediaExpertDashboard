"""Query precalc: peak events (BC e MI)."""
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery

from app.db.client import run_query


def query_peak_bc_raw_all_from_precalc(year: int, brand_id: int, competitor_id: int) -> tuple[list[dict], list[dict]]:
    """Peak BC: raw data per tutti. Ritorna (peak_rows, annual_rows) per calcolo % in Python."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("competitor", "INT64", competitor_id),
    ]
    q_peak = """
    SELECT brand_id, parent_category_id, category_id, channel, peak_event, SUM(gross_pln) AS gross_pln
    FROM mart.precalc_peak_agg
    WHERE year = @year AND brand_id IN (@brand, @competitor)
    GROUP BY 1, 2, 3, 4, 5
    """
    q_annual = """
    SELECT brand_id, parent_category_id, category_id, channel, SUM(gross_pln) AS annual_gross
    FROM mart.precalc_sales_agg
    WHERE year = @year AND brand_id IN (@brand, @competitor)
    GROUP BY 1, 2, 3, 4
    """
    peak_rows = run_query(q_peak, p)
    annual_rows = run_query(q_annual, p)
    return (list(peak_rows) if peak_rows else [], list(annual_rows) if annual_rows else [])


def query_peak_mi_raw_all_from_precalc(year: int, brand_id: int) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Peak MI: (peak_brand, annual_brand, peak_competitors, annual_competitors).

    Competitor rows includono ``brand_id`` per calcolare la media aritmetica dei % annui
    tra i brand escluso il focale (non più il pool aggregato come un solo “mercato”).
    Le quattro query BigQuery partono in parallelo.
    """
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    q_peak = """
    SELECT parent_category_id, category_id, channel, peak_event, SUM(gross_pln) AS gross_pln
    FROM mart.precalc_peak_agg
    WHERE year = @year AND brand_id = @brand
    GROUP BY 1, 2, 3, 4
    """
    q_annual = """
    SELECT parent_category_id, category_id, channel, SUM(gross_pln) AS annual_gross
    FROM mart.precalc_sales_agg
    WHERE year = @year AND brand_id = @brand
    GROUP BY 1, 2, 3
    """
    q_peak_comp = """
    SELECT brand_id, parent_category_id, category_id, channel, peak_event, SUM(gross_pln) AS gross_pln
    FROM mart.precalc_peak_agg
    WHERE year = @year AND brand_id != @brand
    GROUP BY 1, 2, 3, 4, 5
    """
    q_annual_comp = """
    SELECT brand_id, parent_category_id, category_id, channel, SUM(gross_pln) AS annual_gross
    FROM mart.precalc_sales_agg
    WHERE year = @year AND brand_id != @brand
    GROUP BY 1, 2, 3, 4
    """
    jobs = [
        ("pk", q_peak, p),
        ("an", q_annual, p),
        ("pkc", q_peak_comp, p),
        ("anc", q_annual_comp, p),
    ]
    out: dict[str, list] = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(run_query, ql, pp): k for k, ql, pp in jobs}
        for fut in as_completed(futs):
            k = futs[fut]
            rows = fut.result()
            out[k] = list(rows) if rows else []
    return (out["pk"], out["an"], out["pkc"], out["anc"])


def query_peak_events_brand_vs_competitor_from_precalc(
    year: int, brand_id: int, competitor_id: int, cat: int | None = None, subcat: int | None = None, channel: str | None = None
) -> list[dict]:
    """Peak events: brand vs competitor % annuale."""
    ch = (channel or "").strip() if channel else ""
    if ch not in ("", "web", "app", "store"):
        ch = ""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("competitor", "INT64", competitor_id),
        bigquery.ScalarQueryParameter("channel", "STRING", ch),
    ]
    where_cat = ""
    if subcat and subcat >= 100:
        where_cat = "AND category_id = @subcat"
        p.append(bigquery.ScalarQueryParameter("subcat", "INT64", subcat))
    elif cat and 1 <= cat <= 10:
        where_cat = "AND parent_category_id = @cat"
        p.append(bigquery.ScalarQueryParameter("cat", "INT64", cat))
    q = f"""
    WITH annual_brand AS (
      SELECT SUM(gross_pln) AS annual_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND channel = @channel {where_cat}
    ),
    annual_comp AS (
      SELECT SUM(gross_pln) AS annual_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @competitor AND channel = @channel {where_cat}
    ),
    brand_events AS (
      SELECT peak_event, SUM(gross_pln) AS gross_pln
      FROM mart.precalc_peak_agg
      WHERE year = @year AND brand_id = @brand AND channel = @channel {where_cat}
      GROUP BY peak_event
    ),
    comp_events AS (
      SELECT peak_event, SUM(gross_pln) AS gross_pln
      FROM mart.precalc_peak_agg
      WHERE year = @year AND brand_id = @competitor AND channel = @channel {where_cat}
      GROUP BY peak_event
    )
    SELECT COALESCE(be.peak_event, ce.peak_event) AS peak_event,
      ROUND(100.0 * COALESCE(be.gross_pln, 0) / NULLIF((SELECT annual_gross FROM annual_brand), 0), 1) AS brand_pct_of_annual,
      ROUND(100.0 * COALESCE(ce.gross_pln, 0) / NULLIF((SELECT annual_gross FROM annual_comp), 0), 1) AS media_pct_of_annual
    FROM brand_events be
    FULL OUTER JOIN comp_events ce ON be.peak_event = ce.peak_event
    WHERE COALESCE(be.peak_event, ce.peak_event) IS NOT NULL
    ORDER BY COALESCE(ce.gross_pln, 0) DESC
    """
    return run_query(q, p)


def query_peak_events_brand_vs_media_from_precalc(
    year: int, brand_id: int, cat: int | None = None, subcat: int | None = None, channel: str | None = None
) -> list[dict]:
    """Peak events: brand vs media % dell'annuale."""
    ch = (channel or "").strip() if channel else ""
    if ch not in ("", "web", "app", "store"):
        ch = ""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("channel", "STRING", ch),
    ]
    where_cat = ""
    if subcat and subcat >= 100:
        where_cat = "AND category_id = @subcat"
        p.append(bigquery.ScalarQueryParameter("subcat", "INT64", subcat))
    elif cat and 1 <= cat <= 10:
        where_cat = "AND parent_category_id = @cat"
        p.append(bigquery.ScalarQueryParameter("cat", "INT64", cat))
    q = f"""
    WITH annual_brand AS (
      SELECT SUM(gross_pln) AS annual_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND channel = @channel {where_cat}
    ),
    annual_media AS (
      SELECT SUM(gross_pln) AS annual_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND channel = @channel AND brand_id != @brand {where_cat}
    ),
    brand_events AS (
      SELECT peak_event, SUM(gross_pln) AS gross_pln
      FROM mart.precalc_peak_agg
      WHERE year = @year AND brand_id = @brand AND channel = @channel {where_cat}
      GROUP BY peak_event
    ),
    media_events AS (
      SELECT peak_event, SUM(gross_pln) AS gross_pln
      FROM mart.precalc_peak_agg
      WHERE year = @year AND channel = @channel AND brand_id != @brand {where_cat}
      GROUP BY peak_event
    )
    SELECT COALESCE(be.peak_event, me.peak_event) AS peak_event,
      ROUND(100.0 * COALESCE(be.gross_pln, 0) / NULLIF((SELECT annual_gross FROM annual_brand), 0), 1) AS brand_pct_of_annual,
      ROUND(100.0 * COALESCE(me.gross_pln, 0) / NULLIF((SELECT annual_gross FROM annual_media), 0), 1) AS media_pct_of_annual
    FROM brand_events be
    FULL OUTER JOIN media_events me ON be.peak_event = me.peak_event
    WHERE COALESCE(be.peak_event, me.peak_event) IS NOT NULL
    ORDER BY media_pct_of_annual DESC
    """
    return run_query(q, p)
