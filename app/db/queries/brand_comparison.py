"""Brand Comparison: competitor selection, comparative metrics, product deep dive.
Stesso formato output di Market Intelligence ma Brand vs Competitor invece di Brand vs Media.
"""
from google.cloud import bigquery
from google.cloud.bigquery import ArrayQueryParameter

from app.db.client import run_query
from app.db.queries.market_intelligence.shared import (
    from_table,
    where_cat_subcat,
    where_channel,
)


def _params(ps, pe, brand_id, competitor_id=None, cat=None, subcat=None):
    return [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id) if brand_id else None),
        bigquery.ScalarQueryParameter("competitor", "INT64", int(competitor_id) if competitor_id else None),
        bigquery.ScalarQueryParameter("cat", "INT64", int(cat) if cat and 1 <= int(cat) <= 10 else None),
        bigquery.ScalarQueryParameter("subcat", "INT64", int(subcat) if subcat and int(subcat) >= 100 else None),
    ]


def query_competitors_in_scope(ps, pe, brand_id, cat=None, subcat=None):
    """List brands that sell in the same category/subcategory as user's brand."""
    where_cat = "AND (@cat IS NULL OR f.parent_category_id = @cat OR f.category_id = @cat)"
    where_subcat = "AND (@subcat IS NULL OR f.category_id = @subcat)"
    q = f"""
    SELECT DISTINCT f.brand_id, b.brand_name
    FROM mart.fact_sales_daily f
    JOIN mart.dim_brand b ON b.brand_id = f.brand_id
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND f.brand_id != @brand
      {where_cat}
      {where_subcat}
    ORDER BY b.brand_name
    """
    return run_query(q, _params(ps, pe, brand_id, None, cat, subcat))


def query_brand_vs_competitor_sales(ps, pe, brand_id, competitor_id, cat=None, subcat=None):
    """Sales comparison: my brand vs competitor."""
    where_cat = "AND (@cat IS NULL OR f.parent_category_id = @cat OR f.category_id = @cat)"
    where_subcat = "AND (@subcat IS NULL OR f.category_id = @subcat)"
    q = f"""
    SELECT
      SUM(CASE WHEN f.brand_id = @brand THEN f.gross_pln ELSE 0 END) AS my_gross_pln,
      SUM(CASE WHEN f.brand_id = @competitor THEN f.gross_pln ELSE 0 END) AS competitor_gross_pln,
      SUM(CASE WHEN f.brand_id = @brand THEN f.units ELSE 0 END) AS my_units,
      SUM(CASE WHEN f.brand_id = @competitor THEN f.units ELSE 0 END) AS competitor_units,
      ROUND(100.0 * SUM(CASE WHEN f.brand_id = @brand AND f.promo_flag THEN f.gross_pln ELSE 0 END)
            / NULLIF(SUM(CASE WHEN f.brand_id = @brand THEN f.gross_pln ELSE 0 END), 0), 1) AS my_promo_share_pct,
      ROUND(100.0 * SUM(CASE WHEN f.brand_id = @competitor AND f.promo_flag THEN f.gross_pln ELSE 0 END)
            / NULLIF(SUM(CASE WHEN f.brand_id = @competitor THEN f.gross_pln ELSE 0 END), 0), 1) AS competitor_promo_share_pct
    FROM mart.fact_sales_daily f
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND f.brand_id IN (@brand, @competitor)
      {where_cat}
      {where_subcat}
    """
    return run_query(q, _params(ps, pe, brand_id, competitor_id, cat, subcat))


def query_brand_vs_competitor_roi(ps, pe, brand_id, competitor_id, cat=None, subcat=None):
    """Promo ROI: my brand vs competitor."""
    roi_cat = int(cat) if cat and 1 <= int(cat) <= 10 else (int(subcat) // 100 if subcat and int(subcat) >= 100 else None)
    params = _params(ps, pe, brand_id, competitor_id, cat, subcat) + [
        bigquery.ScalarQueryParameter("roi_cat", "INT64", roi_cat),
    ]
    q = """
    SELECT fp.brand_id, fp.brand_name,
      ROUND(AVG(fp.roi), 2) AS avg_roi,
      SUM(fp.incremental_sales_pln) AS incremental_sales
    FROM mart.fact_promo_performance fp
    WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND fp.brand_id IN (@brand, @competitor)
      AND (@roi_cat IS NULL OR fp.category_id = @roi_cat)
    GROUP BY fp.brand_id, fp.brand_name
    """
    return run_query(q, params)


def query_product_deep_dive(ps, pe, brand_id, competitor_id, subcat):
    """Product-level comparison in same subcategory: my product vs competitor product."""
    if not subcat or int(subcat) < 100:
        return []
    q = """
    WITH my_products AS (
      SELECT p.product_id, p.product_name, SUM(oi.gross_pln) AS gross_pln, SUM(oi.quantity) AS units
      FROM mart.fact_order_items oi
      JOIN mart.fact_orders o ON o.order_id = oi.order_id
      JOIN mart.dim_product p ON p.product_id = oi.product_id
      WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND p.brand_id = @brand AND p.subcategory_id = @subcat
      GROUP BY p.product_id, p.product_name
      ORDER BY gross_pln DESC LIMIT 5
    ),
    comp_products AS (
      SELECT p.product_id, p.product_name, SUM(oi.gross_pln) AS gross_pln, SUM(oi.quantity) AS units
      FROM mart.fact_order_items oi
      JOIN mart.fact_orders o ON o.order_id = oi.order_id
      JOIN mart.dim_product p ON p.product_id = oi.product_id
      WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND p.brand_id = @competitor AND p.subcategory_id = @subcat
      GROUP BY p.product_id, p.product_name
      ORDER BY gross_pln DESC LIMIT 5
    )
    SELECT 'my' AS side, product_id, product_name, gross_pln, units FROM my_products
    UNION ALL
    SELECT 'competitor' AS side, product_id, product_name, gross_pln, units FROM comp_products
    """
    return run_query(q, _params(ps, pe, brand_id, competitor_id, None, subcat))


def _bc_params(ps, pe, brand_id, competitor_id, cat=None, subcat=None, channel=None):
    """Params per query BC con competitor e channel."""
    c = int(cat) if cat and str(cat).strip() else None
    s = int(subcat) if subcat and str(subcat).strip() else None
    roi_cat = c if (c and 1 <= c <= 10) else (s // 100 if s and s >= 100 else None)
    p = _params(ps, pe, brand_id, competitor_id, cat, subcat) + [
        bigquery.ScalarQueryParameter("roi_cat", "INT64", roi_cat),
    ]
    if channel and str(channel).strip() in ("web", "app", "store"):
        p.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))
    return p


def query_sales_by_brand_in_category_bc(ps, pe, category_id, brand_id, competitor_id, channel=None):
    """Pie category: solo user brand + competitor. Output come MI (brand_id, brand_name, gross_pln, units, pct_value, pct_volume)."""
    tbl = from_table(channel)
    wch = where_channel(channel)
    p = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("cat", "INT64", int(category_id)),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
        bigquery.ScalarQueryParameter("competitor", "INT64", int(competitor_id)),
    ]
    if channel and str(channel).strip() in ("web", "app", "store"):
        p.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))
    q = f"""
    WITH totals AS (
      SELECT SUM(f.gross_pln) AS total_gross, SUM(f.units) AS total_units
      FROM {tbl} f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.parent_category_id = @cat AND f.brand_id IN (@brand, @competitor)
        {wch}
    )
    SELECT b.brand_id, b.brand_name,
      SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units,
      ROUND(100.0 * SUM(f.gross_pln) / NULLIF((SELECT total_gross FROM totals), 0), 1) AS pct_value,
      ROUND(100.0 * SUM(f.units) / NULLIF((SELECT total_units FROM totals), 0), 1) AS pct_volume
    FROM {tbl} f
    JOIN mart.dim_brand b ON b.brand_id = f.brand_id
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND f.parent_category_id = @cat AND f.brand_id IN (@brand, @competitor) AND f.gross_pln > 0
      {wch}
    GROUP BY b.brand_id, b.brand_name
    ORDER BY gross_pln DESC
    """
    return run_query(q, p)


def query_sales_by_brand_in_all_categories_bc(ps, pe, category_ids, brand_id, competitor_id, channel=None):
    """Pie map per category: solo user + competitor. Output come MI query_sales_by_brand_in_all_categories."""
    if not category_ids:
        return []
    ids = [int(x) for x in category_ids if x]
    if not ids:
        return []
    tbl = from_table(channel)
    wch = where_channel(channel)
    p = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        ArrayQueryParameter("cat_ids", "INT64", ids),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
        bigquery.ScalarQueryParameter("competitor", "INT64", int(competitor_id)),
    ]
    if channel and str(channel).strip() in ("web", "app", "store"):
        p.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))
    q = f"""
    WITH per_cat AS (
      SELECT f.parent_category_id AS category_id,
        b.brand_id, b.brand_name,
        SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units
      FROM {tbl} f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.parent_category_id IN UNNEST(@cat_ids) AND f.brand_id IN (@brand, @competitor) AND f.gross_pln > 0
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
    return run_query(q, p)


def query_sales_by_brand_in_all_subcategories_bc(ps, pe, subcategory_ids, brand_id, competitor_id, channel=None):
    """Pie map per subcategory: solo user + competitor."""
    if not subcategory_ids:
        return []
    ids = [int(x) for x in subcategory_ids if x]
    if not ids:
        return []
    tbl = from_table(channel)
    wch = where_channel(channel)
    p = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        ArrayQueryParameter("sub_ids", "INT64", ids),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
        bigquery.ScalarQueryParameter("competitor", "INT64", int(competitor_id)),
    ]
    if channel and str(channel).strip() in ("web", "app", "store"):
        p.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))
    q = f"""
    WITH per_sub AS (
      SELECT f.category_id,
        b.brand_id, b.brand_name,
        SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units
      FROM {tbl} f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.category_id IN UNNEST(@sub_ids) AND f.brand_id IN (@brand, @competitor) AND f.gross_pln > 0
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
    return run_query(q, p)


def query_sales_by_brand_in_all_categories_bc_all_channels(ps, pe, category_ids, brand_id, competitor_id):
    """Una query: pie categorie BC per tutti i channel ('' + web, app, store). Come MI all_channels ma solo brand+competitor."""
    if not category_ids:
        return []
    ids = [int(x) for x in category_ids if x]
    if not ids:
        return []
    p = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        ArrayQueryParameter("cat_ids", "INT64", ids),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
        bigquery.ScalarQueryParameter("competitor", "INT64", int(competitor_id)),
    ]
    q = """
    WITH all_data AS (
      SELECT CAST('' AS STRING) AS channel, f.parent_category_id AS category_id,
        b.brand_id, b.brand_name, f.gross_pln, f.units
      FROM mart.fact_sales_daily f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.parent_category_id IN UNNEST(@cat_ids) AND f.brand_id IN (@brand, @competitor) AND f.gross_pln > 0
      UNION ALL
      SELECT f.channel, f.parent_category_id AS category_id,
        b.brand_id, b.brand_name, f.gross_pln, f.units
      FROM mart.v_sales_daily_by_channel f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.parent_category_id IN UNNEST(@cat_ids) AND f.brand_id IN (@brand, @competitor) AND f.gross_pln > 0
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
    return run_query(q, p)


def query_sales_by_brand_in_all_subcategories_bc_all_channels(ps, pe, subcategory_ids, brand_id, competitor_id):
    """Una query: pie sottocategorie BC per tutti i channel."""
    if not subcategory_ids:
        return []
    ids = [int(x) for x in subcategory_ids if x]
    if not ids:
        return []
    p = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        ArrayQueryParameter("sub_ids", "INT64", ids),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
        bigquery.ScalarQueryParameter("competitor", "INT64", int(competitor_id)),
    ]
    q = """
    WITH all_data AS (
      SELECT CAST('' AS STRING) AS channel, f.category_id,
        b.brand_id, b.brand_name, f.gross_pln, f.units
      FROM mart.fact_sales_daily f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.category_id IN UNNEST(@sub_ids) AND f.brand_id IN (@brand, @competitor) AND f.gross_pln > 0
      UNION ALL
      SELECT f.channel, f.category_id,
        b.brand_id, b.brand_name, f.gross_pln, f.units
      FROM mart.v_sales_daily_by_channel f
      JOIN mart.dim_brand b ON b.brand_id = f.brand_id
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.category_id IN UNNEST(@sub_ids) AND f.brand_id IN (@brand, @competitor) AND f.gross_pln > 0
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
    return run_query(q, p)


def query_promo_share_by_subcategory_brand_vs_competitor(ps, pe, brand_id, competitor_id, parent_cat_id, channel=None):
    """Promo share per subcategorie: brand vs competitor. Output come MI."""
    if not parent_cat_id:
        return []
    tbl = from_table(channel)
    wch = where_channel(channel)
    p = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
        bigquery.ScalarQueryParameter("competitor", "INT64", int(competitor_id)),
        bigquery.ScalarQueryParameter("parent_cat", "INT64", int(parent_cat_id)),
    ]
    if channel and str(channel).strip() in ("web", "app", "store"):
        p.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))
    q = f"""
    WITH brand_data AS (
      SELECT c.category_id, c.category_name,
        SUM(f.gross_pln) AS total_gross,
        SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross
      FROM {tbl} f
      JOIN mart.dim_category c ON c.category_id = f.category_id AND c.parent_category_id = @parent_cat
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand AND f.gross_pln > 0
        {wch}
      GROUP BY c.category_id, c.category_name
    ),
    comp_data AS (
      SELECT c.category_id, c.category_name,
        SUM(f.gross_pln) AS total_gross,
        SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross
      FROM {tbl} f
      JOIN mart.dim_category c ON c.category_id = f.category_id AND c.parent_category_id = @parent_cat
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @competitor
        {wch}
      GROUP BY c.category_id, c.category_name
    )
    SELECT b.category_id, b.category_name,
      ROUND(100.0 * COALESCE(b.promo_gross, 0) / NULLIF(b.total_gross, 0), 1) AS brand_promo_share_pct,
      ROUND(100.0 * COALESCE(c.promo_gross, 0) / NULLIF(c.total_gross, 0), 1) AS media_promo_share_pct
    FROM brand_data b
    LEFT JOIN comp_data c ON b.category_id = c.category_id
    ORDER BY b.promo_gross DESC
    """
    return run_query(q, p)


def query_sales_by_brand_in_subcategory_bc(ps, pe, subcategory_id, brand_id, competitor_id, channel=None):
    """Pie subcategory: solo user brand + competitor."""
    tbl = from_table(channel)
    wch = where_channel(channel)
    p = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("subcat", "INT64", int(subcategory_id)),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
        bigquery.ScalarQueryParameter("competitor", "INT64", int(competitor_id)),
    ]
    if channel and str(channel).strip() in ("web", "app", "store"):
        p.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))
    q = f"""
    WITH totals AS (
      SELECT SUM(f.gross_pln) AS total_gross, SUM(f.units) AS total_units
      FROM {tbl} f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.category_id = @subcat AND f.brand_id IN (@brand, @competitor)
        {wch}
    )
    SELECT b.brand_id, b.brand_name,
      SUM(f.gross_pln) AS gross_pln, SUM(f.units) AS units,
      ROUND(100.0 * SUM(f.gross_pln) / NULLIF((SELECT total_gross FROM totals), 0), 1) AS pct_value,
      ROUND(100.0 * SUM(f.units) / NULLIF((SELECT total_units FROM totals), 0), 1) AS pct_volume
    FROM {tbl} f
    JOIN mart.dim_brand b ON b.brand_id = f.brand_id
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND f.category_id = @subcat AND f.brand_id IN (@brand, @competitor) AND f.gross_pln > 0
      {wch}
    GROUP BY b.brand_id, b.brand_name
    ORDER BY gross_pln DESC
    """
    return run_query(q, p)


def query_promo_share_by_category_brand_vs_competitor(ps, pe, brand_id, competitor_id, cat=None, subcat=None, channel=None):
    """Promo share per categoria: brand vs competitor. Output come MI (brand_promo_share_pct, media_promo_share_pct)."""
    tbl = from_table(channel)
    wch = where_channel(channel)
    p = _bc_params(ps, pe, brand_id, competitor_id, cat, subcat, channel)
    q = f"""
    WITH brand_data AS (
      SELECT c.category_id, c.category_name,
        SUM(f.gross_pln) AS total_gross,
        SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross
      FROM {tbl} f
      JOIN mart.dim_category c ON c.category_id = f.parent_category_id AND c.level = 1
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand AND f.gross_pln > 0
        {wch}
      GROUP BY c.category_id, c.category_name
    ),
    comp_data AS (
      SELECT c.category_id, c.category_name,
        SUM(f.gross_pln) AS total_gross,
        SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross
      FROM {tbl} f
      JOIN mart.dim_category c ON c.category_id = f.parent_category_id AND c.level = 1
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @competitor
        {wch}
      GROUP BY c.category_id, c.category_name
    )
    SELECT b.category_id, b.category_name,
      ROUND(100.0 * COALESCE(b.promo_gross, 0) / NULLIF(b.total_gross, 0), 1) AS brand_promo_share_pct,
      ROUND(100.0 * COALESCE(c.promo_gross, 0) / NULLIF(c.total_gross, 0), 1) AS media_promo_share_pct
    FROM brand_data b
    LEFT JOIN comp_data c ON b.category_id = c.category_id
    ORDER BY b.promo_gross DESC
    """
    return run_query(q, p)


def query_promo_roi_brand_vs_competitor(ps, pe, brand_id, competitor_id, cat=None, subcat=None):
    """Promo ROI per tipo: brand vs competitor. Output come MI (promo_type, brand_avg_roi, media_avg_roi)."""
    roi_cat = int(cat) if cat and 1 <= int(cat) <= 10 else (int(subcat) // 100 if subcat and int(subcat) >= 100 else None)
    p = _params(ps, pe, brand_id, competitor_id, cat, subcat) + [
        bigquery.ScalarQueryParameter("roi_cat", "INT64", roi_cat),
    ]
    q = """
    WITH brand_data AS (
      SELECT p.promo_type, ROUND(AVG(fp.roi), 2) AS avg_roi
      FROM mart.fact_promo_performance fp
      JOIN mart.dim_promo p ON p.promo_id = fp.promo_id
      WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND fp.brand_id = @brand
        AND (@roi_cat IS NULL OR fp.category_id = @roi_cat)
      GROUP BY p.promo_type
    ),
    comp_data AS (
      SELECT p.promo_type, ROUND(AVG(fp.roi), 2) AS avg_roi
      FROM mart.fact_promo_performance fp
      JOIN mart.dim_promo p ON p.promo_id = fp.promo_id
      WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND fp.brand_id = @competitor
        AND (@roi_cat IS NULL OR fp.category_id = @roi_cat)
      GROUP BY p.promo_type
    )
    SELECT COALESCE(b.promo_type, c.promo_type) AS promo_type,
      COALESCE(b.avg_roi, 0) AS brand_avg_roi,
      COALESCE(c.avg_roi, 0) AS media_avg_roi
    FROM brand_data b
    FULL OUTER JOIN comp_data c ON b.promo_type = c.promo_type
    ORDER BY COALESCE(c.avg_roi, 0) DESC
    """
    return run_query(q, p)


def query_peak_events_brand_vs_competitor(ps, pe, brand_id, competitor_id, cat=None, subcat=None, channel=None):
    """Peak events: brand vs competitor % annuale. Output come MI (peak_event, brand_pct_of_annual, media_pct_of_annual)."""
    tbl = from_table(channel)
    wch = where_channel(channel)
    p = _bc_params(ps, pe, brand_id, competitor_id, cat, subcat, channel)
    q = f"""
    WITH annual_brand AS (
      SELECT SUM(f.gross_pln) AS annual_gross
      FROM {tbl} f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand
        {where_cat_subcat()}
        {wch}
    ),
    annual_comp AS (
      SELECT SUM(f.gross_pln) AS annual_gross
      FROM {tbl} f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @competitor
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
    comp_events AS (
      SELECT d.peak_event, SUM(f.gross_pln) AS gross_pln
      FROM {tbl} f
      JOIN mart.dim_date d ON d.date = f.date
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @competitor
        {where_cat_subcat()}
        {wch}
      GROUP BY d.peak_event
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


def query_discount_depth_brand_vs_competitor(ps, pe, brand_id, competitor_id, cat=None, subcat=None):
    """Discount depth: brand vs competitor. Output come MI (brand_avg_discount_depth, media_avg_discount_depth)."""
    p = _params(ps, pe, brand_id, competitor_id, cat, subcat)
    q = f"""
    WITH brand_data AS (
      SELECT ROUND(COALESCE(
        SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
        / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0)
      , 0), 1) AS avg_discount_depth
      FROM mart.fact_sales_daily f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand
        {where_cat_subcat()}
    ),
    comp_data AS (
      SELECT ROUND(COALESCE(
        SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
        / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0)
      , 0), 1) AS avg_discount_depth
      FROM mart.fact_sales_daily f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @competitor
        {where_cat_subcat()}
    )
    SELECT b.avg_discount_depth AS brand_avg_discount_depth, c.avg_discount_depth AS media_avg_discount_depth
    FROM brand_data b, comp_data c
    """
    return run_query(q, p)


def query_discount_depth_brand_vs_competitor_all_categories(ps, pe, brand_id, competitor_id):
    """Discount depth per categoria: brand vs competitor. Come MI query_discount_depth_brand_vs_media."""
    p = _params(ps, pe, brand_id, competitor_id, None, None)
    q = """
    WITH brand_data AS (
      SELECT c.category_id, c.category_name,
        ROUND(COALESCE(
          SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
          / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0)
        , 0), 1) AS avg_discount_depth
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.parent_category_id AND c.level = 1
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand AND f.gross_pln > 0
      GROUP BY c.category_id, c.category_name
    ),
    comp_data AS (
      SELECT c.category_id, c.category_name,
        ROUND(COALESCE(
          SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
          / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0)
        , 0), 1) AS avg_discount_depth
      FROM mart.fact_sales_daily f
      JOIN mart.dim_category c ON c.category_id = f.parent_category_id AND c.level = 1
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @competitor
      GROUP BY c.category_id, c.category_name
    )
    SELECT b.category_id, b.category_name,
      COALESCE(b.avg_discount_depth, 0) AS brand_avg_discount_depth,
      COALESCE(c.avg_discount_depth, 0) AS media_avg_discount_depth
    FROM brand_data b
    LEFT JOIN comp_data c ON b.category_id = c.category_id
    ORDER BY b.avg_discount_depth DESC
    """
    return run_query(q, p)


def query_discount_depth_for_all_subcategories_bc(ps, pe, brand_id, competitor_id, subcategory_ids):
    """Discount depth per subcategoria: brand vs competitor."""
    if not subcategory_ids:
        return []
    ids = [int(x) for x in subcategory_ids if x]
    if not ids:
        return []
    p = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
        bigquery.ScalarQueryParameter("competitor", "INT64", int(competitor_id)),
        ArrayQueryParameter("sub_ids", "INT64", ids),
    ]
    q = """
    WITH brand_data AS (
      SELECT f.category_id,
        ROUND(COALESCE(
          SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
          / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0)
        , 0), 1) AS brand_avg_discount_depth
      FROM mart.fact_sales_daily f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand AND f.category_id IN UNNEST(@sub_ids) AND f.gross_pln > 0
      GROUP BY f.category_id
    ),
    comp_data AS (
      SELECT f.category_id,
        ROUND(COALESCE(
          SUM(CASE WHEN f.promo_flag THEN f.discount_depth_pct * f.gross_pln ELSE 0 END)
          / NULLIF(SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END), 0)
        , 0), 1) AS media_avg_discount_depth
      FROM mart.fact_sales_daily f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @competitor AND f.category_id IN UNNEST(@sub_ids)
      GROUP BY f.category_id
    )
    SELECT COALESCE(b.category_id, c.category_id) AS category_id,
      COALESCE(b.brand_avg_discount_depth, 0) AS brand_avg_discount_depth,
      COALESCE(c.media_avg_discount_depth, 0) AS media_avg_discount_depth
    FROM brand_data b
    FULL OUTER JOIN comp_data c ON b.category_id = c.category_id
    ORDER BY COALESCE(b.category_id, c.category_id)
    """
    return run_query(q, p)
