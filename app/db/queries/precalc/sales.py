"""Query precalc: vendite (value, volume, pie per category/subcategory)."""
from google.cloud import bigquery
from google.cloud.bigquery import ArrayQueryParameter

from app.db.client import run_query


def query_sales_value_volume_by_category_from_precalc(year: int, brand_id: int) -> list[dict]:
    """Vendite value + volume per parent category. Dati già pronti in precalc_sales_bar_category (SELECT)."""
    q = """
    SELECT category_id, category_name, brand_gross_pln, brand_units, media_gross_pln, media_units
    FROM mart.precalc_sales_bar_category
    WHERE year = @year AND brand_id = @brand
    ORDER BY brand_gross_pln DESC
    """
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    return run_query(q, p)


def query_sales_value_by_category_from_precalc(year: int, brand_id: int) -> list[dict]:
    """Vendite value per parent category: brand vs media. Da precalc_sales_agg."""
    q = """
    WITH brand_data AS (
      SELECT parent_category_id AS category_id, SUM(gross_pln) AS gross_pln
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND channel = '' AND gross_pln > 0
      GROUP BY parent_category_id
    ),
    media_data AS (
      SELECT parent_category_id AS category_id, SUM(gross_pln) AS gross_pln
      FROM mart.precalc_sales_agg
      WHERE year = @year AND channel = ''
      GROUP BY parent_category_id
    )
    SELECT c.category_id, c.category_name,
      COALESCE(b.gross_pln, 0) AS brand_gross_pln,
      COALESCE(m.gross_pln, 0) AS media_gross_pln
    FROM mart.dim_category c
    JOIN brand_data b ON b.category_id = c.category_id
    LEFT JOIN media_data m ON m.category_id = c.category_id
    WHERE c.level = 1
    ORDER BY b.gross_pln DESC
    """
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    return run_query(q, p)


def query_sales_volume_by_category_from_precalc(year: int, brand_id: int) -> list[dict]:
    """Vendite volume per parent category: brand vs media."""
    q = """
    WITH brand_data AS (
      SELECT parent_category_id AS category_id, SUM(units) AS units
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND channel = '' AND gross_pln > 0
      GROUP BY parent_category_id
    ),
    media_data AS (
      SELECT parent_category_id AS category_id, SUM(units) AS units
      FROM mart.precalc_sales_agg
      WHERE year = @year AND channel = ''
      GROUP BY parent_category_id
    )
    SELECT c.category_id, c.category_name,
      COALESCE(b.units, 0) AS brand_units,
      COALESCE(m.units, 0) AS media_units
    FROM mart.dim_category c
    JOIN brand_data b ON b.category_id = c.category_id
    LEFT JOIN media_data m ON m.category_id = c.category_id
    WHERE c.level = 1
    ORDER BY b.units DESC
    """
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    return run_query(q, p)


def query_sales_value_volume_by_subcategory_from_precalc(year: int, brand_id: int, parent_id: int) -> list[dict]:
    """Vendite value + volume per subcategoria. Dati già pronti in precalc_sales_bar_subcategory (SELECT)."""
    q = """
    SELECT category_id, category_name, brand_gross_pln, brand_units, media_gross_pln, media_units
    FROM mart.precalc_sales_bar_subcategory
    WHERE year = @year AND brand_id = @brand AND parent_category_id = @parent
    ORDER BY brand_gross_pln DESC
    """
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("parent", "INT64", parent_id),
    ]
    return run_query(q, p)


def query_sales_value_by_subcategory_from_precalc(year: int, brand_id: int, parent_id: int) -> list[dict]:
    """Vendite value per subcategoria sotto una parent: brand vs media."""
    q = """
    WITH brand_data AS (
      SELECT category_id, SUM(gross_pln) AS gross_pln
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND parent_category_id = @parent AND channel = '' AND gross_pln > 0
      GROUP BY category_id
    ),
    media_data AS (
      SELECT category_id, SUM(gross_pln) AS gross_pln
      FROM mart.precalc_sales_agg
      WHERE year = @year AND parent_category_id = @parent AND channel = ''
      GROUP BY category_id
    )
    SELECT c.category_id, c.category_name,
      COALESCE(b.gross_pln, 0) AS brand_gross_pln,
      COALESCE(m.gross_pln, 0) AS media_gross_pln
    FROM mart.dim_category c
    JOIN brand_data b ON b.category_id = c.category_id
    LEFT JOIN media_data m ON m.category_id = c.category_id
    WHERE c.level = 2 AND c.parent_category_id = @parent
    ORDER BY b.gross_pln DESC
    """
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("parent", "INT64", parent_id),
    ]
    return run_query(q, p)


def query_sales_volume_by_subcategory_from_precalc(year: int, brand_id: int, parent_id: int) -> list[dict]:
    """Vendite volume per subcategoria: brand vs media."""
    q = """
    WITH brand_data AS (
      SELECT category_id, SUM(units) AS units
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND parent_category_id = @parent AND channel = '' AND gross_pln > 0
      GROUP BY category_id
    ),
    media_data AS (
      SELECT category_id, SUM(units) AS units
      FROM mart.precalc_sales_agg
      WHERE year = @year AND parent_category_id = @parent AND channel = ''
      GROUP BY category_id
    )
    SELECT c.category_id, c.category_name,
      COALESCE(b.units, 0) AS brand_units,
      COALESCE(m.units, 0) AS media_units
    FROM mart.dim_category c
    JOIN brand_data b ON b.category_id = c.category_id
    LEFT JOIN media_data m ON m.category_id = c.category_id
    WHERE c.level = 2 AND c.parent_category_id = @parent
    ORDER BY b.units DESC
    """
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("parent", "INT64", parent_id),
    ]
    return run_query(q, p)


def query_sales_by_brand_in_all_categories_all_channels_from_precalc(
    year: int, category_ids: list[int]
) -> list[dict]:
    """Pie category: tutti i canali in una query. Ritorna channel, category_id, brand_id, brand_name, gross_pln, units, pct_value, pct_volume."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        ArrayQueryParameter("cat_ids", "INT64", category_ids),
    ]
    q = """
    SELECT channel, category_id, brand_id, brand_name, gross_pln, units, pct_value, pct_volume
    FROM mart.precalc_pie_brands_category
    WHERE year = @year AND category_id IN UNNEST(@cat_ids)
    ORDER BY channel, category_id, gross_pln DESC
    """
    return run_query(q, p)


def query_sales_by_brand_in_all_categories_from_precalc(
    year: int, category_ids: list[int], channel: str | None = None
) -> list[dict]:
    """Pie category: brand_id, brand_name, gross_pln, units, pct_value, pct_volume."""
    ch = (channel or "").strip() if channel else ""
    if ch not in ("", "web", "app", "store"):
        ch = ""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        ArrayQueryParameter("cat_ids", "INT64", category_ids),
        bigquery.ScalarQueryParameter("channel", "STRING", ch),
    ]
    q = """
    SELECT category_id, brand_id, brand_name, gross_pln, units, pct_value, pct_volume
    FROM mart.precalc_pie_brands_category
    WHERE year = @year AND category_id IN UNNEST(@cat_ids) AND channel = @channel
    ORDER BY category_id, gross_pln DESC
    """
    return run_query(q, p)


def query_sales_by_brand_in_all_subcategories_all_channels_from_precalc(
    year: int, subcategory_ids: list[int]
) -> list[dict]:
    """Pie subcategory: tutti i canali in una query. Ritorna channel, category_id, brand_id, brand_name, gross_pln, units, pct_value, pct_volume."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        ArrayQueryParameter("sub_ids", "INT64", subcategory_ids),
    ]
    q = """
    SELECT channel, category_id, brand_id, brand_name, gross_pln, units, pct_value, pct_volume
    FROM mart.precalc_pie_brands_subcategory
    WHERE year = @year AND category_id IN UNNEST(@sub_ids)
    ORDER BY channel, category_id, gross_pln DESC
    """
    return run_query(q, p)


def query_sales_by_brand_in_all_subcategories_from_precalc(
    year: int, subcategory_ids: list[int], channel: str | None = None
) -> list[dict]:
    """Pie subcategory."""
    ch = (channel or "").strip() if channel else ""
    if ch not in ("", "web", "app", "store"):
        ch = ""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        ArrayQueryParameter("sub_ids", "INT64", subcategory_ids),
        bigquery.ScalarQueryParameter("channel", "STRING", ch),
    ]
    q = """
    SELECT category_id, brand_id, brand_name, gross_pln, units, pct_value, pct_volume
    FROM mart.precalc_pie_brands_subcategory
    WHERE year = @year AND category_id IN UNNEST(@sub_ids) AND channel = @channel
    ORDER BY category_id, gross_pln DESC
    """
    return run_query(q, p)


def query_sales_pie_bc_categories_all_channels_from_precalc(
    year: int, category_ids: list[int], brand_id: int, competitor_id: int
) -> list[dict]:
    """Pie category BC: tutti i canali in una query. Ritorna channel, category_id, brand_id, brand_name, gross_pln, units, pct_value, pct_volume."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        ArrayQueryParameter("cat_ids", "INT64", category_ids),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("competitor", "INT64", competitor_id),
    ]
    q = """
    SELECT channel, category_id, brand_id, brand_name, gross_pln, units, pct_value, pct_volume
    FROM mart.precalc_pie_brands_category
    WHERE year = @year AND category_id IN UNNEST(@cat_ids) AND brand_id IN (@brand, @competitor)
    ORDER BY channel, category_id, gross_pln DESC
    """
    return run_query(q, p)


def query_sales_pie_bc_subcategories_all_channels_from_precalc(
    year: int, subcategory_ids: list[int], brand_id: int, competitor_id: int
) -> list[dict]:
    """Pie subcategory BC: tutti i canali in una query."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        ArrayQueryParameter("sub_ids", "INT64", subcategory_ids),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("competitor", "INT64", competitor_id),
    ]
    q = """
    SELECT channel, category_id, brand_id, brand_name, gross_pln, units, pct_value, pct_volume
    FROM mart.precalc_pie_brands_subcategory
    WHERE year = @year AND category_id IN UNNEST(@sub_ids) AND brand_id IN (@brand, @competitor)
    ORDER BY channel, category_id, gross_pln DESC
    """
    return run_query(q, p)


def query_sales_by_brand_in_all_categories_bc_from_precalc(
    year: int, category_ids: list[int], brand_id: int, competitor_id: int, channel: str | None = None
) -> list[dict]:
    """Pie category: solo brand + competitor. Ricalcola pct sul totale dei due brand."""
    ch = (channel or "").strip() if channel else ""
    if ch not in ("", "web", "app", "store"):
        ch = ""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        ArrayQueryParameter("cat_ids", "INT64", category_ids),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("competitor", "INT64", competitor_id),
        bigquery.ScalarQueryParameter("channel", "STRING", ch),
    ]
    q = """
    SELECT category_id, brand_id, brand_name, gross_pln, units, pct_value, pct_volume
    FROM mart.precalc_pie_brands_category
    WHERE year = @year AND category_id IN UNNEST(@cat_ids) AND channel = @channel
      AND brand_id IN (@brand, @competitor)
    ORDER BY category_id, gross_pln DESC
    """
    return run_query(q, p)


def query_sales_by_brand_in_all_subcategories_bc_from_precalc(
    year: int, subcategory_ids: list[int], brand_id: int, competitor_id: int, channel: str | None = None
) -> list[dict]:
    """Pie subcategory: solo brand + competitor. Ricalcola pct sul totale dei due brand."""
    ch = (channel or "").strip() if channel else ""
    if ch not in ("", "web", "app", "store"):
        ch = ""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        ArrayQueryParameter("sub_ids", "INT64", subcategory_ids),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("competitor", "INT64", competitor_id),
        bigquery.ScalarQueryParameter("channel", "STRING", ch),
    ]
    q = """
    SELECT category_id, brand_id, brand_name, gross_pln, units, pct_value, pct_volume
    FROM mart.precalc_pie_brands_subcategory
    WHERE year = @year AND category_id IN UNNEST(@sub_ids) AND channel = @channel
      AND brand_id IN (@brand, @competitor)
    ORDER BY category_id, gross_pln DESC
    """
    return run_query(q, p)
