"""
Client BigQuery per la dashboard.
Legge dati dal dataset mart (progetto GCP da variabile d'ambiente o default).
"""
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
import os
from typing import Optional

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "mediaexpertdashboard")
DATASET = "mart"


def get_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def run_query(
    query: str,
    params: Optional[list] = None,
) -> list[dict]:
    """Esegue una query parametrizzata e restituisce le righe come lista di dict.
    params: lista di bigquery.ScalarQueryParameter (nome, tipo, valore).
    """
    client = get_client()
    job_config = QueryJobConfig()
    if params:
        job_config.query_parameters = params
    job = client.query(query, job_config=job_config)
    rows = job.result()
    return [dict(row) for row in rows]


def query_category_sales(period_start: str, period_end: str, category_id: Optional[str] = None) -> list[dict]:
    """Vendite per categoria (PLN) nel periodo."""
    q = """
    SELECT
      CAST(d.date AS STRING) AS date,
      d.year,
      d.quarter,
      d.month,
      c.category_id,
      c.category_name,
      SUM(f.gross_pln) AS gross_pln,
      SUM(f.net_pln) AS net_pln,
      SUM(f.units) AS units
    FROM mart.fact_sales_daily f
    JOIN mart.dim_date d ON d.date = f.date
    JOIN mart.dim_category c ON c.category_id = f.category_id
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @period_start) AND PARSE_DATE('%Y-%m-%d', @period_end)
      AND (@category_id IS NULL OR f.category_id = @category_id)
    GROUP BY d.date, d.year, d.quarter, d.month, c.category_id, c.category_name
    ORDER BY d.date, gross_pln DESC
    """
    params = [
        bigquery.ScalarQueryParameter("period_start", "STRING", period_start),
        bigquery.ScalarQueryParameter("period_end", "STRING", period_end),
        bigquery.ScalarQueryParameter("category_id", "INT64", int(category_id) if (category_id and str(category_id).strip()) else None),
    ]
    return run_query(q, params)


def query_promo_share(period_start: str, period_end: str, category_id: Optional[str] = None) -> list[dict]:
    """Quota vendite in promo sul totale."""
    q = """
    WITH totals AS (
      SELECT
        SUM(gross_pln) AS total_gross,
        SUM(CASE WHEN promo_flag THEN gross_pln ELSE 0 END) AS promo_gross
      FROM mart.fact_sales_daily
      WHERE date BETWEEN PARSE_DATE('%Y-%m-%d', @period_start) AND PARSE_DATE('%Y-%m-%d', @period_end)
        AND (@category_id IS NULL OR category_id = @category_id)
    )
    SELECT
      total_gross,
      promo_gross,
      ROUND(100.0 * promo_gross / NULLIF(total_gross, 0), 2) AS promo_share_pct
    FROM totals
    """
    params = [
        bigquery.ScalarQueryParameter("period_start", "STRING", period_start),
        bigquery.ScalarQueryParameter("period_end", "STRING", period_end),
        bigquery.ScalarQueryParameter("category_id", "INT64", int(category_id) if (category_id and str(category_id).strip()) else None),
    ]
    return run_query(q, params)


def query_yoy(period_start: str, period_end: str, category_id: Optional[str] = None) -> list[dict]:
    """Confronto anno su anno."""
    q = """
    WITH yearly AS (
      SELECT
        EXTRACT(YEAR FROM date) AS year,
        SUM(gross_pln) AS total_gross
      FROM mart.fact_sales_daily
      WHERE date BETWEEN PARSE_DATE('%Y-%m-%d', @period_start) AND PARSE_DATE('%Y-%m-%d', @period_end)
        AND (@category_id IS NULL OR category_id = @category_id)
      GROUP BY EXTRACT(YEAR FROM date)
    ),
    yoy AS (
      SELECT
        year,
        total_gross,
        LAG(total_gross) OVER (ORDER BY year) AS prior_year_gross,
        total_gross - LAG(total_gross) OVER (ORDER BY year) AS incremental_pln,
        ROUND(100.0 * (total_gross - LAG(total_gross) OVER (ORDER BY year)) / NULLIF(LAG(total_gross) OVER (ORDER BY year), 0), 2) AS yoy_pct
      FROM yearly
    )
    SELECT * FROM yoy WHERE prior_year_gross IS NOT NULL
    """
    params = [
        bigquery.ScalarQueryParameter("period_start", "STRING", period_start),
        bigquery.ScalarQueryParameter("period_end", "STRING", period_end),
        bigquery.ScalarQueryParameter("category_id", "INT64", int(category_id) if (category_id and str(category_id).strip()) else None),
    ]
    return run_query(q, params)


def query_promo_roi(period_start: str, period_end: str, promo_type: Optional[str] = None) -> list[dict]:
    """ROI per promo."""
    q = """
    SELECT
      p.promo_id,
      p.promo_name,
      p.promo_type,
      SUM(f.attributed_sales_pln) AS attributed_sales_pln,
      SUM(f.discount_cost_pln + f.media_cost_pln) AS total_cost_pln,
      ROUND((SUM(f.attributed_sales_pln) - SUM(f.discount_cost_pln + f.media_cost_pln)) / NULLIF(SUM(f.discount_cost_pln + f.media_cost_pln), 0), 4) AS roi
    FROM mart.fact_promo_performance f
    JOIN mart.dim_promo p ON p.promo_id = f.promo_id
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @period_start) AND PARSE_DATE('%Y-%m-%d', @period_end)
      AND (@promo_type IS NULL OR p.promo_type = @promo_type)
    GROUP BY p.promo_id, p.promo_name, p.promo_type
    ORDER BY roi DESC
    """
    params = [
        bigquery.ScalarQueryParameter("period_start", "STRING", period_start),
        bigquery.ScalarQueryParameter("period_end", "STRING", period_end),
        bigquery.ScalarQueryParameter("promo_type", "STRING", promo_type if promo_type else None),
    ]
    return run_query(q, params)


def query_peak_events(period_start: str, period_end: str, category_id: Optional[str] = None) -> list[dict]:
    """Vendite per periodo (Black Friday, Xmas, Back to School, Normal)."""
    q = """
    SELECT
      CASE
        WHEN d.is_black_friday_week THEN 'Black Friday'
        WHEN d.is_xmas_period THEN 'Xmas'
        WHEN d.is_back_to_school THEN 'Back to School'
        ELSE 'Normal'
      END AS peak_event,
      COUNT(DISTINCT f.date) AS days_count,
      SUM(f.gross_pln) AS gross_pln,
      SUM(f.units) AS units,
      ROUND(SUM(f.gross_pln) / NULLIF(COUNT(DISTINCT f.date), 0), 2) AS avg_daily_gross
    FROM mart.fact_sales_daily f
    JOIN mart.dim_date d ON d.date = f.date
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @period_start) AND PARSE_DATE('%Y-%m-%d', @period_end)
      AND (@category_id IS NULL OR f.category_id = @category_id)
    GROUP BY peak_event
    ORDER BY gross_pln DESC
    """
    params = [
        bigquery.ScalarQueryParameter("period_start", "STRING", period_start),
        bigquery.ScalarQueryParameter("period_end", "STRING", period_end),
        bigquery.ScalarQueryParameter("category_id", "INT64", int(category_id) if (category_id and str(category_id).strip()) else None),
    ]
    return run_query(q, params)


def query_categories() -> list[dict]:
    """Elenco categorie per filtri."""
    q = "SELECT category_id, category_name FROM mart.dim_category ORDER BY category_id"
    return run_query(q)
