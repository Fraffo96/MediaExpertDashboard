"""Query precalc: percentuali vendite anno precedente (prev_year)."""
from google.cloud import bigquery
from google.cloud.bigquery import ArrayQueryParameter

from app.db.client import run_query


def query_sales_pct_by_brand_prev_year_categories_all_channels_from_precalc(
    year_prev: int, category_ids: list[int]
) -> list[dict]:
    """pct_value_prev per brand per category anno precedente: tutti i canali in una query."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year_prev),
        ArrayQueryParameter("cat_ids", "INT64", category_ids),
    ]
    q = """
    SELECT channel, category_id, brand_id, pct_value_prev
    FROM mart.precalc_prev_year_pct
    WHERE year = @year AND category_id IN UNNEST(@cat_ids)
    """
    return run_query(q, p)


def query_sales_pct_by_brand_prev_year_categories_from_precalc(
    year_prev: int, category_ids: list[int], channel: str | None = None
) -> list[dict]:
    """pct_value_prev per brand per category anno precedente."""
    ch = (channel or "").strip() if channel else ""
    if ch not in ("", "web", "app", "store"):
        ch = ""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year_prev),
        ArrayQueryParameter("cat_ids", "INT64", category_ids),
        bigquery.ScalarQueryParameter("channel", "STRING", ch),
    ]
    q = """
    SELECT category_id, brand_id, pct_value_prev
    FROM mart.precalc_prev_year_pct
    WHERE year = @year AND category_id IN UNNEST(@cat_ids) AND channel = @channel
    """
    return run_query(q, p)


def query_sales_pct_by_brand_prev_year_subcategories_all_channels_from_precalc(
    year_prev: int, subcategory_ids: list[int]
) -> list[dict]:
    """pct_value_prev per brand per subcategory anno precedente: tutti i canali in una query."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year_prev),
        ArrayQueryParameter("sub_ids", "INT64", subcategory_ids),
    ]
    q = """
    SELECT channel, category_id, brand_id, pct_value_prev
    FROM mart.precalc_prev_year_pct
    WHERE year = @year AND category_id IN UNNEST(@sub_ids)
    """
    return run_query(q, p)


def query_sales_pct_by_brand_prev_year_subcategories_from_precalc(
    year_prev: int, subcategory_ids: list[int], channel: str | None = None
) -> list[dict]:
    """pct_value_prev per brand per subcategory anno precedente."""
    ch = (channel or "").strip() if channel else ""
    if ch not in ("", "web", "app", "store"):
        ch = ""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year_prev),
        ArrayQueryParameter("sub_ids", "INT64", subcategory_ids),
        bigquery.ScalarQueryParameter("channel", "STRING", ch),
    ]
    q = """
    SELECT category_id, brand_id, pct_value_prev
    FROM mart.precalc_prev_year_pct
    WHERE year = @year AND category_id IN UNNEST(@sub_ids) AND channel = @channel
    """
    return run_query(q, p)
