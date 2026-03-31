"""Marketing: needstates by category - segment share and needstate per category."""
from google.cloud import bigquery

from app.db.client import run_query


def query_category_segment_share(ps: str, pe: str, category_id: int) -> list[dict]:
    """Segment share of sales for a category. Returns segment_id, segment_name, gross_pln, share_pct.
    Needstate is resolved from segment_profiles.json in the service layer."""
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("cat", "INT64", category_id),
    ]
    q = """
    WITH total AS (
      SELECT SUM(f.gross_pln) AS tot
      FROM mart.fact_sales_daily f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.parent_category_id = @cat AND f.gross_pln > 0
    )
    SELECT f.segment_id, s.segment_name,
      SUM(f.gross_pln) AS gross_pln,
      ROUND(100.0 * SUM(f.gross_pln) / NULLIF((SELECT tot FROM total), 0), 1) AS share_pct
    FROM mart.fact_sales_daily f
    JOIN mart.dim_segment s ON s.segment_id = f.segment_id
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND f.parent_category_id = @cat AND f.gross_pln > 0
    GROUP BY f.segment_id, s.segment_name
    ORDER BY gross_pln DESC
    """
    return run_query(q, params)
