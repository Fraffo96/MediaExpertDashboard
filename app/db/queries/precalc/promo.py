"""Query precalc: promo_share e promo_roi (BC e MI, consolidate e granulari)."""
from google.cloud import bigquery

from app.db.client import run_query


def query_promo_share_bc_all_channels_from_precalc(year: int, brand_id: int, competitor_id: int) -> list[dict]:
    """Promo share BC: tutte le categorie e canali in una query. Ritorna channel, category_id, parent_category_id, brand_pct, media_pct."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("competitor", "INT64", competitor_id),
    ]
    q = """
    WITH brand_data AS (
      SELECT channel, parent_category_id AS category_id, parent_category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND gross_pln > 0 AND parent_category_id BETWEEN 1 AND 10
      GROUP BY channel, parent_category_id
    ),
    comp_data AS (
      SELECT channel, parent_category_id AS category_id, parent_category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @competitor AND parent_category_id BETWEEN 1 AND 10
      GROUP BY channel, parent_category_id
    )
    SELECT COALESCE(b.channel, c.channel) AS channel, COALESCE(b.category_id, c.category_id) AS category_id,
      COALESCE(b.category_id, c.category_id) AS parent_category_id,
      dc.category_name,
      ROUND(100.0 * COALESCE(b.promo_gross, 0) / NULLIF(b.total_gross, 0), 1) AS brand_promo_share_pct,
      ROUND(100.0 * COALESCE(c.promo_gross, 0) / NULLIF(c.total_gross, 0), 1) AS media_promo_share_pct
    FROM brand_data b
    FULL OUTER JOIN comp_data c ON b.channel = c.channel AND b.category_id = c.category_id
    LEFT JOIN mart.dim_category dc ON dc.category_id = COALESCE(b.category_id, c.category_id) AND dc.level = 1
    ORDER BY COALESCE(b.channel, c.channel), COALESCE(b.promo_gross, 0) DESC
    """
    return run_query(q, p)


def query_promo_share_sub_bc_all_channels_from_precalc(year: int, brand_id: int, competitor_id: int) -> list[dict]:
    """Promo share BC subcategory: tutte le subcategorie e canali in una query."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("competitor", "INT64", competitor_id),
    ]
    q = """
    WITH brand_data AS (
      SELECT channel, category_id, parent_category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND category_id >= 100 AND gross_pln > 0
      GROUP BY channel, category_id, parent_category_id
    ),
    comp_data AS (
      SELECT channel, category_id, parent_category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @competitor AND category_id >= 100
      GROUP BY channel, category_id, parent_category_id
    )
    SELECT COALESCE(b.channel, c.channel) AS channel, COALESCE(b.category_id, c.category_id) AS category_id,
      COALESCE(b.parent_category_id, c.parent_category_id) AS parent_category_id,
      dc.category_name,
      ROUND(100.0 * COALESCE(b.promo_gross, 0) / NULLIF(b.total_gross, 0), 1) AS brand_promo_share_pct,
      ROUND(100.0 * COALESCE(c.promo_gross, 0) / NULLIF(c.total_gross, 0), 1) AS media_promo_share_pct
    FROM brand_data b
    FULL OUTER JOIN comp_data c ON b.channel = c.channel AND b.category_id = c.category_id
    LEFT JOIN mart.dim_category dc ON dc.category_id = COALESCE(b.category_id, c.category_id) AND dc.level = 2
    ORDER BY COALESCE(b.channel, c.channel), COALESCE(b.parent_category_id, c.parent_category_id), COALESCE(b.promo_gross, 0) DESC
    """
    return run_query(q, p)


def query_promo_roi_bc_all_categories_from_precalc(year: int, brand_id: int, competitor_id: int) -> list[dict]:
    """ROI BC: tutte le categorie in una query. Ritorna category_id, promo_type, brand_avg_roi, media_avg_roi."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("competitor", "INT64", competitor_id),
    ]
    q = """
    WITH brand_data AS (
      SELECT category_id, promo_type, AVG(avg_roi) AS avg_roi
      FROM mart.precalc_roi_agg
      WHERE year = @year AND brand_id = @brand AND category_id BETWEEN 1 AND 10
      GROUP BY category_id, promo_type
    ),
    comp_data AS (
      SELECT category_id, promo_type, AVG(avg_roi) AS avg_roi
      FROM mart.precalc_roi_agg
      WHERE year = @year AND brand_id = @competitor AND category_id BETWEEN 1 AND 10
      GROUP BY category_id, promo_type
    )
    SELECT COALESCE(b.category_id, c.category_id) AS category_id, COALESCE(b.promo_type, c.promo_type) AS promo_type,
      COALESCE(b.avg_roi, 0) AS brand_avg_roi, COALESCE(c.avg_roi, 0) AS media_avg_roi
    FROM brand_data b
    FULL OUTER JOIN comp_data c ON b.category_id = c.category_id AND b.promo_type = c.promo_type
    ORDER BY b.category_id, COALESCE(c.avg_roi, 0) DESC
    """
    return run_query(q, p)


def query_promo_share_mi_all_channels_from_precalc(year: int, brand_id: int) -> list[dict]:
    """Promo share MI: tutte le categorie parent e canali in una query. Ritorna channel, category_id, category_name, brand_pct, media_pct."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    q = """
    WITH brand_data AS (
      SELECT channel, parent_category_id AS category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND gross_pln > 0 AND parent_category_id BETWEEN 1 AND 10
      GROUP BY channel, parent_category_id
    ),
    media_data AS (
      SELECT channel, parent_category_id AS category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND parent_category_id BETWEEN 1 AND 10
      GROUP BY channel, parent_category_id
    )
    SELECT b.channel, b.category_id, dc.category_name,
      ROUND(100.0 * b.promo_gross / NULLIF(b.total_gross, 0), 1) AS brand_promo_share_pct,
      ROUND(100.0 * COALESCE(m.promo_gross, 0) / NULLIF(m.total_gross, 0), 1) AS media_promo_share_pct
    FROM brand_data b
    INNER JOIN media_data m ON b.channel = m.channel AND b.category_id = m.category_id
    LEFT JOIN mart.dim_category dc ON dc.category_id = b.category_id AND dc.level = 1
    ORDER BY b.channel, b.promo_gross DESC
    """
    return run_query(q, p)


def query_promo_share_sub_mi_all_channels_from_precalc(year: int, brand_id: int) -> list[dict]:
    """Promo share MI subcategory: tutte le subcategorie e canali in una query."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    q = """
    WITH brand_data AS (
      SELECT channel, category_id, parent_category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND category_id >= 100 AND gross_pln > 0
      GROUP BY channel, category_id, parent_category_id
    ),
    media_data AS (
      SELECT channel, category_id, parent_category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND category_id >= 100
      GROUP BY channel, category_id, parent_category_id
    )
    SELECT b.channel, b.category_id, b.parent_category_id, dc.category_name,
      ROUND(100.0 * b.promo_gross / NULLIF(b.total_gross, 0), 1) AS brand_promo_share_pct,
      ROUND(100.0 * COALESCE(m.promo_gross, 0) / NULLIF(m.total_gross, 0), 1) AS media_promo_share_pct
    FROM brand_data b
    INNER JOIN media_data m ON b.channel = m.channel AND b.category_id = m.category_id
    LEFT JOIN mart.dim_category dc ON dc.category_id = b.category_id AND dc.level = 2
    ORDER BY b.channel, b.parent_category_id, b.promo_gross DESC
    """
    return run_query(q, p)


def query_promo_roi_mi_all_from_precalc(year: int, brand_id: int) -> list[dict]:
    """ROI MI: tutte le categorie (parent + subcategory) in una query. Ritorna category_id, promo_type, brand_avg_roi, media_avg_roi."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    q = """
    WITH brand_data AS (
      SELECT category_id, promo_type,
        SAFE_DIVIDE(
          SUM(avg_roi * incremental_sales_pln),
          NULLIF(SUM(incremental_sales_pln), 0)
        ) AS avg_roi
      FROM mart.precalc_roi_agg
      WHERE year = @year AND brand_id = @brand
      GROUP BY category_id, promo_type
    ),
    media_data AS (
      SELECT category_id, promo_type,
        SAFE_DIVIDE(
          SUM(avg_roi * incremental_sales_pln),
          NULLIF(SUM(incremental_sales_pln), 0)
        ) AS avg_roi
      FROM mart.precalc_roi_agg
      WHERE year = @year AND brand_id != @brand
      GROUP BY category_id, promo_type
    )
    SELECT COALESCE(b.category_id, m.category_id) AS category_id, COALESCE(b.promo_type, m.promo_type) AS promo_type,
      COALESCE(b.avg_roi, 0) AS brand_avg_roi,
      COALESCE(m.avg_roi, 0) AS media_avg_roi
    FROM brand_data b
    FULL OUTER JOIN media_data m ON b.category_id = m.category_id AND b.promo_type = m.promo_type
    ORDER BY COALESCE(b.category_id, m.category_id), COALESCE(m.avg_roi, 0) DESC
    """
    return run_query(q, p)


def query_promo_share_by_category_brand_vs_competitor_from_precalc(
    year: int, brand_id: int, competitor_id: int, cat: int | None = None, subcat: int | None = None, channel: str | None = None
) -> list[dict]:
    """Promo share per categoria: brand vs competitor. Output come BC live."""
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
    WITH brand_data AS (
      SELECT parent_category_id AS category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND channel = @channel AND gross_pln > 0 {where_cat}
      GROUP BY parent_category_id
    ),
    comp_data AS (
      SELECT parent_category_id AS category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @competitor AND channel = @channel {where_cat}
      GROUP BY parent_category_id
    )
    SELECT COALESCE(b.category_id, c.category_id) AS category_id,
      dc.category_name,
      ROUND(100.0 * COALESCE(b.promo_gross, 0) / NULLIF(b.total_gross, 0), 1) AS brand_promo_share_pct,
      ROUND(100.0 * COALESCE(c.promo_gross, 0) / NULLIF(c.total_gross, 0), 1) AS media_promo_share_pct
    FROM brand_data b
    FULL OUTER JOIN comp_data c ON b.category_id = c.category_id
    LEFT JOIN mart.dim_category dc ON dc.category_id = COALESCE(b.category_id, c.category_id) AND dc.level = 1
    ORDER BY COALESCE(b.promo_gross, 0) DESC
    """
    return run_query(q, p)


def query_promo_share_by_subcategory_brand_vs_competitor_from_precalc(
    year: int, brand_id: int, competitor_id: int, parent_cat_id: int, channel: str | None = None
) -> list[dict]:
    """Promo share per subcategorie: brand vs competitor."""
    ch = (channel or "").strip() if channel else ""
    if ch not in ("", "web", "app", "store"):
        ch = ""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("competitor", "INT64", competitor_id),
        bigquery.ScalarQueryParameter("parent", "INT64", parent_cat_id),
        bigquery.ScalarQueryParameter("channel", "STRING", ch),
    ]
    q = """
    WITH brand_data AS (
      SELECT category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND channel = @channel AND parent_category_id = @parent AND category_id >= 100 AND gross_pln > 0
      GROUP BY category_id
    ),
    comp_data AS (
      SELECT category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @competitor AND channel = @channel AND parent_category_id = @parent AND category_id >= 100
      GROUP BY category_id
    )
    SELECT COALESCE(b.category_id, c.category_id) AS category_id,
      dc.category_name,
      ROUND(100.0 * COALESCE(b.promo_gross, 0) / NULLIF(b.total_gross, 0), 1) AS brand_promo_share_pct,
      ROUND(100.0 * COALESCE(c.promo_gross, 0) / NULLIF(c.total_gross, 0), 1) AS media_promo_share_pct
    FROM brand_data b
    FULL OUTER JOIN comp_data c ON b.category_id = c.category_id
    LEFT JOIN mart.dim_category dc ON dc.category_id = COALESCE(b.category_id, c.category_id) AND dc.level = 2
    ORDER BY COALESCE(b.promo_gross, 0) DESC
    """
    return run_query(q, p)


def query_promo_roi_brand_vs_competitor_from_precalc(
    year: int, brand_id: int, competitor_id: int, roi_cat: int | None = None
) -> list[dict]:
    """ROI per promo_type: brand vs competitor."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("competitor", "INT64", competitor_id),
    ]
    where_cat = "AND (@roi_cat IS NULL OR category_id = @roi_cat)"
    if roi_cat is not None:
        p.append(bigquery.ScalarQueryParameter("roi_cat", "INT64", roi_cat))
    else:
        where_cat = ""
    q = f"""
    WITH brand_data AS (
      SELECT promo_type, AVG(avg_roi) AS avg_roi
      FROM mart.precalc_roi_agg
      WHERE year = @year AND brand_id = @brand {where_cat}
      GROUP BY promo_type
    ),
    comp_data AS (
      SELECT promo_type, AVG(avg_roi) AS avg_roi
      FROM mart.precalc_roi_agg
      WHERE year = @year AND brand_id = @competitor {where_cat}
      GROUP BY promo_type
    )
    SELECT COALESCE(b.promo_type, c.promo_type) AS promo_type,
      COALESCE(b.avg_roi, 0) AS brand_avg_roi,
      COALESCE(c.avg_roi, 0) AS media_avg_roi
    FROM brand_data b
    FULL OUTER JOIN comp_data c ON b.promo_type = c.promo_type
    ORDER BY COALESCE(c.avg_roi, 0) DESC
    """
    return run_query(q, p)


def query_promo_share_by_category_brand_vs_media_from_precalc(
    year: int, brand_id: int, channel: str | None = None
) -> list[dict]:
    """Promo share per parent category: brand vs media."""
    ch = (channel or "").strip() if channel else ""
    if ch not in ("", "web", "app", "store"):
        ch = ""
    q = """
    WITH brand_data AS (
      SELECT parent_category_id AS category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND channel = @channel AND gross_pln > 0
      GROUP BY parent_category_id
    ),
    media_data AS (
      SELECT parent_category_id AS category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND channel = @channel
      GROUP BY parent_category_id
    )
    SELECT c.category_id, c.category_name,
      ROUND(100.0 * COALESCE(b.promo_gross, 0) / NULLIF(b.total_gross, 0), 1) AS brand_promo_share_pct,
      ROUND(100.0 * COALESCE(m.promo_gross, 0) / NULLIF(m.total_gross, 0), 1) AS media_promo_share_pct
    FROM mart.dim_category c
    JOIN brand_data b ON b.category_id = c.category_id
    LEFT JOIN media_data m ON m.category_id = c.category_id
    WHERE c.level = 1
    ORDER BY b.promo_gross DESC
    """
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("channel", "STRING", ch),
    ]
    return run_query(q, p)


def query_promo_share_by_subcategory_brand_vs_media_from_precalc(
    year: int, brand_id: int, parent_cat_id: int, channel: str | None = None
) -> list[dict]:
    """Promo share per subcategoria: brand vs media."""
    ch = (channel or "").strip() if channel else ""
    if ch not in ("", "web", "app", "store"):
        ch = ""
    q = """
    WITH brand_data AS (
      SELECT category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND brand_id = @brand AND parent_category_id = @parent AND channel = @channel AND gross_pln > 0
      GROUP BY category_id
    ),
    media_data AS (
      SELECT category_id, SUM(gross_pln) AS total_gross, SUM(promo_gross) AS promo_gross
      FROM mart.precalc_sales_agg
      WHERE year = @year AND parent_category_id = @parent AND channel = @channel
      GROUP BY category_id
    )
    SELECT c.category_id, c.category_name,
      ROUND(100.0 * COALESCE(b.promo_gross, 0) / NULLIF(b.total_gross, 0), 1) AS brand_promo_share_pct,
      ROUND(100.0 * COALESCE(m.promo_gross, 0) / NULLIF(m.total_gross, 0), 1) AS media_promo_share_pct
    FROM mart.dim_category c
    JOIN brand_data b ON b.category_id = c.category_id
    LEFT JOIN media_data m ON m.category_id = c.category_id
    WHERE c.level = 2 AND c.parent_category_id = @parent
    ORDER BY b.promo_gross DESC
    """
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
        bigquery.ScalarQueryParameter("parent", "INT64", parent_cat_id),
        bigquery.ScalarQueryParameter("channel", "STRING", ch),
    ]
    return run_query(q, p)


def query_promo_roi_brand_vs_media_from_precalc(year: int, brand_id: int, roi_cat: int | None = None) -> list[dict]:
    """ROI per promo_type: brand vs media. roi_cat = parent category (1-10) o None per all."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    where_cat = "AND (@roi_cat IS NULL OR category_id = @roi_cat)"
    if roi_cat is not None:
        p.append(bigquery.ScalarQueryParameter("roi_cat", "INT64", roi_cat))
    else:
        where_cat = ""
    q = f"""
    WITH brand_data AS (
      SELECT promo_type, AVG(avg_roi) AS avg_roi
      FROM mart.precalc_roi_agg
      WHERE year = @year AND brand_id = @brand {where_cat}
      GROUP BY promo_type
    ),
    media_data AS (
      SELECT promo_type, AVG(avg_roi) AS avg_roi
      FROM mart.precalc_roi_agg
      WHERE year = @year AND brand_id != @brand {where_cat}
      GROUP BY promo_type
    )
    SELECT COALESCE(b.promo_type, m.promo_type) AS promo_type,
      COALESCE(b.avg_roi, 0) AS brand_avg_roi,
      COALESCE(m.avg_roi, 0) AS media_avg_roi
    FROM brand_data b
    FULL OUTER JOIN media_data m ON b.promo_type = m.promo_type
    ORDER BY media_avg_roi DESC
    """
    return run_query(q, p)
