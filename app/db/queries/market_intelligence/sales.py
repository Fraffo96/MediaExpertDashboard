"""Query vendite brand vs media per Market Intelligence."""
from google.cloud import bigquery
from google.cloud.bigquery import ArrayQueryParameter

from app.db.client import run_query
from .shared import params, where_cat_subcat, from_table, where_channel


def query_sales_value_by_category(ps, pe, brand_id):
    """Vendite per parent category (1-10): brand vs media. Solo categorie con prodotti brand."""
    q = """
    WITH brand_data AS (
      SELECT c.category_id, c.category_name, SUM(f.gross_pln) AS gross_pln
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.parent_category_id AND c.level = 1
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand AND f.gross_pln > 0
      GROUP BY c.category_id, c.category_name
    ),
    media_data AS (
      SELECT c.category_id, c.category_name, SUM(f.gross_pln) AS gross_pln
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.parent_category_id AND c.level = 1
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      GROUP BY c.category_id, c.category_name
    )
    SELECT b.category_id, b.category_name,
      COALESCE(b.gross_pln, 0) AS brand_gross_pln,
      COALESCE(m.gross_pln, 0) AS media_gross_pln
    FROM brand_data b
    LEFT JOIN media_data m ON b.category_id = m.category_id
    ORDER BY b.gross_pln DESC
    """
    return run_query(q, params(ps, pe, brand_id, None, None))


def query_sales_by_brand_in_category(ps, pe, category_id):
    """Vendite per brand in una parent category. Per pie chart."""
    pparams = params(ps, pe, 0, category_id, None)
    q = """
    WITH totals AS (
      SELECT SUM(f.gross_pln) AS total_gross, SUM(f.units) AS total_units
      FROM mart.fact_sales_daily f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.parent_category_id = @cat
    )
    SELECT b.brand_id, b.brand_name,
      SUM(f.gross_pln) AS gross_pln,
      SUM(f.units) AS units,
      ROUND(100.0 * SUM(f.gross_pln) / NULLIF((SELECT total_gross FROM totals), 0), 1) AS pct_value,
      ROUND(100.0 * SUM(f.units) / NULLIF((SELECT total_units FROM totals), 0), 1) AS pct_volume
    FROM mart.fact_sales_daily f
    JOIN mart.dim_brand b ON b.brand_id = f.brand_id
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND f.parent_category_id = @cat AND f.gross_pln > 0
    GROUP BY b.brand_id, b.brand_name
    ORDER BY gross_pln DESC
    """
    return run_query(q, pparams)


def query_sales_by_brand_in_all_categories(ps, pe, category_ids, channel=None):
    """Vendite per brand in più parent categories. Per category_pie_brands_map. channel: '', 'web', 'app', 'store'."""
    if not category_ids:
        return []
    ids = [int(x) for x in category_ids if x]
    if not ids:
        return []
    tbl = from_table(channel)
    wch = where_channel(channel)
    pparams = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        ArrayQueryParameter("cat_ids", "INT64", ids),
    ]
    if channel and str(channel).strip() in ("web", "app", "store"):
        pparams.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))
    q = f"""
    WITH per_cat AS (
      SELECT f.parent_category_id AS category_id,
        b.brand_id, b.brand_name,
        SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units
      FROM {tbl} f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.parent_category_id IN UNNEST(@cat_ids) AND f.gross_pln > 0
        {wch}
      GROUP BY f.parent_category_id, b.brand_id, b.brand_name
    ),
    totals AS (
      SELECT category_id, SUM(gross_pln) AS total_gross, SUM(units) AS total_units
      FROM per_cat GROUP BY category_id
    )
    SELECT p.category_id, p.brand_id, p.brand_name, p.gross_pln, p.units,
      ROUND(100.0 * p.gross_pln / NULLIF(t.total_gross, 0), 1) AS pct_value,
      ROUND(100.0 * p.units / NULLIF(t.total_units, 0), 1) AS pct_volume
    FROM per_cat p
    JOIN totals t ON p.category_id = t.category_id
    ORDER BY p.category_id, p.gross_pln DESC
    """
    return run_query(q, pparams)


def query_sales_by_brand_in_all_subcategories(ps, pe, subcategory_ids, channel=None):
    """Vendite per brand in più sottocategorie. Per subcategory_pie_brands_map. channel: '', 'web', 'app', 'store'."""
    if not subcategory_ids:
        return []
    ids = [int(x) for x in subcategory_ids if x]
    if not ids:
        return []
    tbl = from_table(channel)
    wch = where_channel(channel)
    pparams = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        ArrayQueryParameter("sub_ids", "INT64", ids),
    ]
    if channel and str(channel).strip() in ("web", "app", "store"):
        pparams.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))
    q = f"""
    WITH per_sub AS (
      SELECT f.category_id,
        b.brand_id, b.brand_name,
        SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units
      FROM {tbl} f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.category_id IN UNNEST(@sub_ids) AND f.gross_pln > 0
        {wch}
      GROUP BY f.category_id, b.brand_id, b.brand_name
    ),
    totals AS (
      SELECT category_id, SUM(gross_pln) AS total_gross, SUM(units) AS total_units
      FROM per_sub GROUP BY category_id
    )
    SELECT p.category_id, p.brand_id, p.brand_name, p.gross_pln, p.units,
      ROUND(100.0 * p.gross_pln / NULLIF(t.total_gross, 0), 1) AS pct_value,
      ROUND(100.0 * p.units / NULLIF(t.total_units, 0), 1) AS pct_volume
    FROM per_sub p
    JOIN totals t ON p.category_id = t.category_id
    ORDER BY p.category_id, p.gross_pln DESC
    """
    return run_query(q, pparams)


def query_sales_pct_by_brand_prev_year_categories(ps_prev, pe_prev, category_ids, channel=None):
    """pct_value per brand per category per anno precedente (per delta market share)."""
    if not category_ids:
        return []
    ids = [int(x) for x in category_ids if x]
    if not ids:
        return []
    tbl = from_table(channel)
    wch = where_channel(channel)
    pparams = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps_prev),
        bigquery.ScalarQueryParameter("pe", "STRING", pe_prev),
        ArrayQueryParameter("cat_ids", "INT64", ids),
    ]
    if channel and str(channel).strip() in ("web", "app", "store"):
        pparams.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))
    q = f"""
    WITH per_cat AS (
      SELECT f.parent_category_id AS category_id, b.brand_id,
        SUM(f.gross_pln) AS gross_pln
      FROM {tbl} f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.parent_category_id IN UNNEST(@cat_ids) AND f.gross_pln > 0
        {wch}
      GROUP BY f.parent_category_id, b.brand_id
    ),
    totals AS (
      SELECT category_id, SUM(gross_pln) AS total_gross FROM per_cat GROUP BY category_id
    )
    SELECT p.category_id, p.brand_id,
      ROUND(100.0 * p.gross_pln / NULLIF(t.total_gross, 0), 1) AS pct_value_prev
    FROM per_cat p
    JOIN totals t ON p.category_id = t.category_id
    """
    return run_query(q, pparams)


def query_sales_by_brand_in_all_categories_all_channels(ps, pe, category_ids):
    """Una sola query: vendite per brand in tutte le categories, per TUTTI i channel ('' + web, app, store)."""
    if not category_ids:
        return []
    ids = [int(x) for x in category_ids if x]
    if not ids:
        return []
    pparams = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        ArrayQueryParameter("cat_ids", "INT64", ids),
    ]
    q = """
    WITH all_data AS (
      SELECT CAST('' AS STRING) AS channel, f.parent_category_id AS category_id,
        b.brand_id, b.brand_name, f.gross_pln, f.units
      FROM mart.fact_sales_daily f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.parent_category_id IN UNNEST(@cat_ids) AND f.gross_pln > 0
      UNION ALL
      SELECT f.channel, f.parent_category_id AS category_id,
        b.brand_id, b.brand_name, f.gross_pln, f.units
      FROM mart.v_sales_daily_by_channel f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.parent_category_id IN UNNEST(@cat_ids) AND f.gross_pln > 0
    ),
    per_cat AS (
      SELECT channel, category_id, brand_id, brand_name,
        SUM(gross_pln) AS gross_pln, SUM(units) AS units
      FROM all_data GROUP BY channel, category_id, brand_id, brand_name
    ),
    totals AS (
      SELECT channel, category_id, SUM(gross_pln) AS total_gross, SUM(units) AS total_units
      FROM per_cat GROUP BY channel, category_id
    )
    SELECT p.channel, p.category_id, p.brand_id, p.brand_name, p.gross_pln, p.units,
      ROUND(100.0 * p.gross_pln / NULLIF(t.total_gross, 0), 1) AS pct_value,
      ROUND(100.0 * p.units / NULLIF(t.total_units, 0), 1) AS pct_volume
    FROM per_cat p
    JOIN totals t ON p.channel = t.channel AND p.category_id = t.category_id
    ORDER BY p.channel, p.category_id, p.gross_pln DESC
    """
    return run_query(q, pparams)


def query_sales_by_brand_in_all_subcategories_all_channels(ps, pe, subcategory_ids):
    """Una sola query: vendite per brand in tutte le subcategories, per TUTTI i channel."""
    if not subcategory_ids:
        return []
    ids = [int(x) for x in subcategory_ids if x]
    if not ids:
        return []
    pparams = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        ArrayQueryParameter("sub_ids", "INT64", ids),
    ]
    q = """
    WITH all_data AS (
      SELECT CAST('' AS STRING) AS channel, f.category_id,
        b.brand_id, b.brand_name, f.gross_pln, f.units
      FROM mart.fact_sales_daily f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.category_id IN UNNEST(@sub_ids) AND f.gross_pln > 0
      UNION ALL
      SELECT f.channel, f.category_id, b.brand_id, b.brand_name, f.gross_pln, f.units
      FROM mart.v_sales_daily_by_channel f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.category_id IN UNNEST(@sub_ids) AND f.gross_pln > 0
    ),
    per_sub AS (
      SELECT channel, category_id, brand_id, brand_name,
        SUM(gross_pln) AS gross_pln, SUM(units) AS units
      FROM all_data GROUP BY channel, category_id, brand_id, brand_name
    ),
    totals AS (
      SELECT channel, category_id, SUM(gross_pln) AS total_gross, SUM(units) AS total_units
      FROM per_sub GROUP BY channel, category_id
    )
    SELECT p.channel, p.category_id, p.brand_id, p.brand_name, p.gross_pln, p.units,
      ROUND(100.0 * p.gross_pln / NULLIF(t.total_gross, 0), 1) AS pct_value,
      ROUND(100.0 * p.units / NULLIF(t.total_units, 0), 1) AS pct_volume
    FROM per_sub p
    JOIN totals t ON p.channel = t.channel AND p.category_id = t.category_id
    ORDER BY p.channel, p.category_id, p.gross_pln DESC
    """
    return run_query(q, pparams)


def query_sales_pct_by_brand_prev_year_categories_all_channels(ps_prev, pe_prev, category_ids):
    """Una sola query: pct prev year per tutte le categories, per TUTTI i channel."""
    if not category_ids:
        return []
    ids = [int(x) for x in category_ids if x]
    if not ids:
        return []
    pparams = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps_prev),
        bigquery.ScalarQueryParameter("pe", "STRING", pe_prev),
        ArrayQueryParameter("cat_ids", "INT64", ids),
    ]
    q = """
    WITH per_cat AS (
      SELECT COALESCE(f.channel, '') AS channel, f.parent_category_id AS category_id, b.brand_id,
        SUM(f.gross_pln) AS gross_pln
      FROM mart.v_sales_daily_by_channel f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.parent_category_id IN UNNEST(@cat_ids) AND f.gross_pln > 0
      GROUP BY f.channel, f.parent_category_id, b.brand_id
    ),
    totals AS (
      SELECT channel, category_id, SUM(gross_pln) AS total_gross FROM per_cat GROUP BY channel, category_id
    )
    SELECT p.channel, p.category_id, p.brand_id,
      ROUND(100.0 * p.gross_pln / NULLIF(t.total_gross, 0), 1) AS pct_value_prev
    FROM per_cat p
    JOIN totals t ON p.channel = t.channel AND p.category_id = t.category_id
    """
    return run_query(q, pparams)


def query_sales_pct_by_brand_prev_year_subcategories_all_channels(ps_prev, pe_prev, subcategory_ids):
    """Una sola query: pct prev year per tutte le subcategories, per TUTTI i channel."""
    if not subcategory_ids:
        return []
    ids = [int(x) for x in subcategory_ids if x]
    if not ids:
        return []
    pparams = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps_prev),
        bigquery.ScalarQueryParameter("pe", "STRING", pe_prev),
        ArrayQueryParameter("sub_ids", "INT64", ids),
    ]
    q = """
    WITH all_data AS (
      SELECT CAST('' AS STRING) AS channel, f.category_id, b.brand_id, f.gross_pln
      FROM mart.fact_sales_daily f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.category_id IN UNNEST(@sub_ids) AND f.gross_pln > 0
      UNION ALL
      SELECT f.channel, f.category_id, b.brand_id, f.gross_pln
      FROM mart.v_sales_daily_by_channel f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.category_id IN UNNEST(@sub_ids) AND f.gross_pln > 0
    ),
    per_sub AS (
      SELECT channel, category_id, brand_id, SUM(gross_pln) AS gross_pln
      FROM all_data GROUP BY channel, category_id, brand_id
    ),
    totals AS (
      SELECT channel, category_id, SUM(gross_pln) AS total_gross FROM per_sub GROUP BY channel, category_id
    )
    SELECT p.channel, p.category_id, p.brand_id,
      ROUND(100.0 * p.gross_pln / NULLIF(t.total_gross, 0), 1) AS pct_value_prev
    FROM per_sub p
    JOIN totals t ON p.channel = t.channel AND p.category_id = t.category_id
    """
    return run_query(q, pparams)


def query_sales_pct_by_brand_prev_year_subcategories(ps_prev, pe_prev, subcategory_ids, channel=None):
    """pct_value per brand per subcategory per anno precedente."""
    if not subcategory_ids:
        return []
    ids = [int(x) for x in subcategory_ids if x]
    if not ids:
        return []
    tbl = from_table(channel)
    wch = where_channel(channel)
    pparams = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps_prev),
        bigquery.ScalarQueryParameter("pe", "STRING", pe_prev),
        ArrayQueryParameter("sub_ids", "INT64", ids),
    ]
    if channel and str(channel).strip() in ("web", "app", "store"):
        pparams.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))
    q = f"""
    WITH per_sub AS (
      SELECT f.category_id, b.brand_id,
        SUM(f.gross_pln) AS gross_pln
      FROM {tbl} f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.category_id IN UNNEST(@sub_ids) AND f.gross_pln > 0
        {wch}
      GROUP BY f.category_id, b.brand_id
    ),
    totals AS (
      SELECT category_id, SUM(gross_pln) AS total_gross FROM per_sub GROUP BY category_id
    )
    SELECT p.category_id, p.brand_id,
      ROUND(100.0 * p.gross_pln / NULLIF(t.total_gross, 0), 1) AS pct_value_prev
    FROM per_sub p
    JOIN totals t ON p.category_id = t.category_id
    """
    return run_query(q, pparams)


def query_sales_by_brand_in_subcategory(ps, pe, subcategory_id):
    """Vendite per brand in una sottocategoria. Per pie chart."""
    pparams = params(ps, pe, 0, None, subcategory_id)
    q = """
    WITH totals AS (
      SELECT SUM(f.gross_pln) AS total_gross, SUM(f.units) AS total_units
      FROM mart.fact_sales_daily f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.category_id = @subcat
    )
    SELECT b.brand_id, b.brand_name,
      SUM(f.gross_pln) AS gross_pln,
      SUM(f.units) AS units,
      ROUND(100.0 * SUM(f.gross_pln) / NULLIF((SELECT total_gross FROM totals), 0), 1) AS pct_value,
      ROUND(100.0 * SUM(f.units) / NULLIF((SELECT total_units FROM totals), 0), 1) AS pct_volume
    FROM mart.fact_sales_daily f
    JOIN mart.dim_brand b ON b.brand_id = f.brand_id
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND f.category_id = @subcat AND f.gross_pln > 0
    GROUP BY b.brand_id, b.brand_name
    ORDER BY gross_pln DESC
    """
    return run_query(q, pparams)


def query_sales_value_by_subcategory(ps, pe, brand_id, parent_id):
    """Vendite per sottocategoria per una parent: brand vs media."""
    p = int(parent_id) if parent_id else None
    pparams = params(ps, pe, brand_id, parent_id, None) + [
        bigquery.ScalarQueryParameter("parent", "INT64", p),
    ]
    q = """
    WITH brand_data AS (
      SELECT c.category_id, c.category_name, SUM(f.gross_pln) AS gross_pln
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.category_id AND c.level = 2
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand AND f.parent_category_id = @parent AND f.gross_pln > 0
      GROUP BY c.category_id, c.category_name
    ),
    media_data AS (
      SELECT c.category_id, c.category_name, SUM(f.gross_pln) AS gross_pln
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.category_id AND c.level = 2
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.parent_category_id = @parent
      GROUP BY c.category_id, c.category_name
    )
    SELECT b.category_id, b.category_name,
      COALESCE(b.gross_pln, 0) AS brand_gross_pln,
      COALESCE(m.gross_pln, 0) AS media_gross_pln
    FROM brand_data b
    LEFT JOIN media_data m ON b.category_id = m.category_id
    ORDER BY b.gross_pln DESC
    """
    return run_query(q, pparams)


def query_sales_value_brand_vs_media(ps, pe, brand_id, cat=None, subcat=None):
    """Legacy: vendite per category per value (subcategory level). Per compatibilità."""
    q = f"""
    WITH brand_data AS (
      SELECT c.category_id, c.category_name, SUM(f.gross_pln) AS gross_pln
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.category_id AND c.level = 2
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand
        {where_cat_subcat()}
      GROUP BY c.category_id, c.category_name
    ),
    media_data AS (
      SELECT c.category_id, c.category_name, SUM(f.gross_pln) AS gross_pln
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.category_id AND c.level = 2
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        {where_cat_subcat()}
      GROUP BY c.category_id, c.category_name
    )
    SELECT COALESCE(b.category_id, m.category_id) AS category_id,
      COALESCE(b.category_name, m.category_name) AS category_name,
      COALESCE(b.gross_pln, 0) AS brand_gross_pln,
      COALESCE(m.gross_pln, 0) AS media_gross_pln
    FROM brand_data b
    FULL OUTER JOIN media_data m ON b.category_id = m.category_id AND b.category_name = m.category_name
    ORDER BY media_gross_pln DESC
    """
    return run_query(q, params(ps, pe, brand_id, cat, subcat))


def query_sales_volume_by_category(ps, pe, brand_id):
    """Vendite per parent category (1-10) per volume: brand vs media."""
    q = """
    WITH brand_data AS (
      SELECT c.category_id, c.category_name, SUM(f.units) AS units
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.parent_category_id AND c.level = 1
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand AND f.gross_pln > 0
      GROUP BY c.category_id, c.category_name
    ),
    media_data AS (
      SELECT c.category_id, c.category_name, SUM(f.units) AS units
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.parent_category_id AND c.level = 1
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      GROUP BY c.category_id, c.category_name
    )
    SELECT b.category_id, b.category_name,
      COALESCE(b.units, 0) AS brand_units,
      COALESCE(m.units, 0) AS media_units
    FROM brand_data b
    LEFT JOIN media_data m ON b.category_id = m.category_id
    ORDER BY b.units DESC
    """
    return run_query(q, params(ps, pe, brand_id, None, None))


def query_sales_volume_by_subcategory(ps, pe, brand_id, parent_id):
    """Vendite per sottocategoria per una parent per volume: brand vs media."""
    p = int(parent_id) if parent_id else None
    pparams = params(ps, pe, brand_id, parent_id, None) + [
        bigquery.ScalarQueryParameter("parent", "INT64", p),
    ]
    q = """
    WITH brand_data AS (
      SELECT c.category_id, c.category_name, SUM(f.units) AS units
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.category_id AND c.level = 2
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand AND f.parent_category_id = @parent AND f.gross_pln > 0
      GROUP BY c.category_id, c.category_name
    ),
    media_data AS (
      SELECT c.category_id, c.category_name, SUM(f.units) AS units
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.category_id AND c.level = 2
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.parent_category_id = @parent
      GROUP BY c.category_id, c.category_name
    )
    SELECT b.category_id, b.category_name,
      COALESCE(b.units, 0) AS brand_units,
      COALESCE(m.units, 0) AS media_units
    FROM brand_data b
    LEFT JOIN media_data m ON b.category_id = m.category_id
    ORDER BY b.units DESC
    """
    return run_query(q, pparams)


def query_sales_volume_brand_vs_media(ps, pe, brand_id, cat=None, subcat=None):
    """Legacy: vendite per category per volume. Per compatibilità."""
    q = f"""
    WITH brand_data AS (
      SELECT c.category_id, c.category_name, SUM(f.units) AS units
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.category_id AND c.level = 2
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand
        {where_cat_subcat()}
      GROUP BY c.category_id, c.category_name
    ),
    media_data AS (
      SELECT c.category_id, c.category_name, SUM(f.units) AS units
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.category_id AND c.level = 2
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        {where_cat_subcat()}
      GROUP BY c.category_id, c.category_name
    )
    SELECT COALESCE(b.category_id, m.category_id) AS category_id,
      COALESCE(b.category_name, m.category_name) AS category_name,
      COALESCE(b.units, 0) AS brand_units,
      COALESCE(m.units, 0) AS media_units
    FROM brand_data b
    FULL OUTER JOIN media_data m ON b.category_id = m.category_id AND b.category_name = m.category_name
    ORDER BY media_units DESC
    """
    return run_query(q, params(ps, pe, brand_id, cat, subcat))
