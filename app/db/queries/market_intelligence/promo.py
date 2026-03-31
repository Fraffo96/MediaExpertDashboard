"""Query promo share e ROI brand vs media per Market Intelligence."""
from app.db.client import run_query
from .shared import params, where_cat_subcat, from_table, where_channel


def query_promo_share_brand_vs_media(ps, pe, brand_id, cat=None, subcat=None, channel=None):
    """Promo share delle vendite: brand vs media."""
    tbl = from_table(channel)
    wch = where_channel(channel)
    q = f"""
    WITH brand_data AS (
      SELECT
        SUM(f.gross_pln) AS total_gross,
        SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross
      FROM {tbl} f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND f.brand_id = @brand
        {where_cat_subcat()}
        {wch}
    ),
    media_data AS (
      SELECT
        SUM(f.gross_pln) AS total_gross,
        SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross
      FROM {tbl} f
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        {where_cat_subcat()}
        {wch}
    )
    SELECT
      ROUND(100.0 * b.promo_gross / NULLIF(b.total_gross, 0), 1) AS brand_promo_share_pct,
      ROUND(100.0 * m.promo_gross / NULLIF(m.total_gross, 0), 1) AS media_promo_share_pct
    FROM brand_data b, media_data m
    """
    return run_query(q, params(ps, pe, brand_id, cat, subcat, channel))


def query_promo_share_by_category_brand_vs_media(ps, pe, brand_id, cat=None, subcat=None, channel=None):
    """Promo share per parent category: brand vs media. Solo categorie con prodotti brand."""
    tbl = from_table(channel)
    wch = where_channel(channel)
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
    media_data AS (
      SELECT c.category_id, c.category_name,
        SUM(f.gross_pln) AS total_gross,
        SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross
      FROM {tbl} f
      JOIN mart.dim_category c ON c.category_id = f.parent_category_id AND c.level = 1
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        {wch}
      GROUP BY c.category_id, c.category_name
    )
    SELECT b.category_id, b.category_name,
      ROUND(100.0 * COALESCE(b.promo_gross, 0) / NULLIF(b.total_gross, 0), 1) AS brand_promo_share_pct,
      ROUND(100.0 * COALESCE(m.promo_gross, 0) / NULLIF(m.total_gross, 0), 1) AS media_promo_share_pct
    FROM brand_data b
    LEFT JOIN media_data m ON b.category_id = m.category_id
    ORDER BY b.promo_gross DESC
    """
    return run_query(q, params(ps, pe, brand_id, cat, subcat, channel))


def query_promo_share_by_subcategory_brand_vs_media(ps, pe, brand_id, parent_cat_id, channel=None):
    """Promo share per subcategorie sotto una parent category: brand vs media."""
    if not parent_cat_id:
        return []
    tbl = from_table(channel)
    wch = where_channel(channel)
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
    media_data AS (
      SELECT c.category_id, c.category_name,
        SUM(f.gross_pln) AS total_gross,
        SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross
      FROM {tbl} f
      JOIN mart.dim_category c ON c.category_id = f.category_id AND c.parent_category_id = @parent_cat
      WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        {wch}
      GROUP BY c.category_id, c.category_name
    )
    SELECT b.category_id, b.category_name,
      ROUND(100.0 * COALESCE(b.promo_gross, 0) / NULLIF(b.total_gross, 0), 1) AS brand_promo_share_pct,
      ROUND(100.0 * COALESCE(m.promo_gross, 0) / NULLIF(m.total_gross, 0), 1) AS media_promo_share_pct
    FROM brand_data b
    LEFT JOIN media_data m ON b.category_id = m.category_id
    ORDER BY b.promo_gross DESC
    """
    from google.cloud import bigquery
    p = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
        bigquery.ScalarQueryParameter("parent_cat", "INT64", int(parent_cat_id)),
    ]
    if channel and str(channel).strip() in ("web", "app", "store"):
        p.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))
    return run_query(q, p)


def query_incremental_yoy_vendite(ps, pe, brand_id, cat=None, subcat=None, channel=None):
    """Vendite e vendite promo per anno (YoY). Per chart Average Incremental YoY.
    Ritorna year, total_gross, promo_gross per il brand, con filtri cat/subcat/channel."""
    tbl = from_table(channel)
    wch = where_channel(channel)
    q = f"""
    SELECT EXTRACT(YEAR FROM f.date) AS year,
      SUM(f.gross_pln) AS total_gross,
      SUM(CASE WHEN f.promo_flag THEN f.gross_pln ELSE 0 END) AS promo_gross
    FROM {tbl} f
    WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND f.brand_id = @brand
      {where_cat_subcat()}
      {wch}
    GROUP BY 1
    ORDER BY 1
    """
    return run_query(q, params(ps, pe, brand_id, cat, subcat, channel))


def query_incremental_yoy_brand_vs_media(ps, pe, brand_id, cat=None, subcat=None):
    """Media incremental YoY: brand vs media."""
    q = f"""
    WITH brand_inc AS (
      SELECT ROUND(AVG(100.0 * fp.incremental_sales_pln / NULLIF(yt.total_gross, 0)), 1) AS incremental_pct
      FROM (
        SELECT EXTRACT(YEAR FROM fp.date) AS year, SUM(fp.incremental_sales_pln) AS inc
        FROM mart.fact_promo_performance fp
        WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
          AND fp.brand_id = @brand
          AND (@roi_cat IS NULL OR fp.category_id = @roi_cat)
        GROUP BY 1
      ) yp
      JOIN (
        SELECT EXTRACT(YEAR FROM f.date) AS year, SUM(f.gross_pln) AS total_gross
        FROM mart.fact_sales_daily f
        WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
          AND f.brand_id = @brand
          {where_cat_subcat()}
        GROUP BY 1
      ) yt ON yp.year = yt.year
    ),
    media_inc AS (
      SELECT ROUND(AVG(100.0 * fp.incremental_sales_pln / NULLIF(yt.total_gross, 0)), 1) AS incremental_pct
      FROM (
        SELECT EXTRACT(YEAR FROM fp.date) AS year, SUM(fp.incremental_sales_pln) AS inc
        FROM mart.fact_promo_performance fp
        WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
          AND (@roi_cat IS NULL OR fp.category_id = @roi_cat)
        GROUP BY 1
      ) yp
      JOIN (
        SELECT EXTRACT(YEAR FROM f.date) AS year, SUM(f.gross_pln) AS total_gross
        FROM mart.fact_sales_daily f
        WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
          {where_cat_subcat()}
        GROUP BY 1
      ) yt ON yp.year = yt.year
    )
    SELECT b.incremental_pct AS brand_incremental_pct, m.incremental_pct AS media_incremental_pct
    FROM brand_inc b, media_inc m
    """
    return run_query(q, params(ps, pe, brand_id, cat, subcat))


def query_promo_roi_brand_vs_media(ps, pe, brand_id, cat=None, subcat=None):
    """Promo ROI: brand vs media per tipo promo.
    fact_promo_performance ha solo parent (1-10). Per subcategorie usiamo fact_sales_daily.
    """
    from google.cloud import bigquery

    # Se subcat: calcola ROI da fact_sales_daily (granularità subcategoria)
    if subcat and int(subcat) >= 100:
        p = [
            bigquery.ScalarQueryParameter("ps", "STRING", ps),
            bigquery.ScalarQueryParameter("pe", "STRING", pe),
            bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
            bigquery.ScalarQueryParameter("subcat", "INT64", int(subcat)),
        ]
        q = """
        WITH pcfg AS (
          SELECT 1 AS pid, 1.80 AS br UNION ALL SELECT 2, 1.30 UNION ALL SELECT 3, 0.85
          UNION ALL SELECT 4, 1.60 UNION ALL SELECT 5, 1.40 UNION ALL SELECT 6, 2.10
          UNION ALL SELECT 7, 1.70 UNION ALL SELECT 8, 1.20 UNION ALL SELECT 9, 1.10 UNION ALL SELECT 10, 1.00
        ),
        yadj AS (SELECT 2023 AS yr, 1.15 AS ra UNION ALL SELECT 2024, 1.00 UNION ALL SELECT 2025, 0.85),
        agg AS (
          SELECT f.promo_id, f.brand_id, f.category_id, f.date, SUM(f.gross_pln) AS att
          FROM mart.fact_sales_daily f
          WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
            AND f.promo_flag AND f.promo_id IS NOT NULL AND f.category_id = @subcat
          GROUP BY f.promo_id, f.brand_id, f.category_id, f.date
        ),
        non_promo AS (
          SELECT brand_id, category_id, date, SUM(gross_pln) AS gross
          FROM mart.fact_sales_daily WHERE NOT promo_flag AND category_id = @subcat
          GROUP BY brand_id, category_id, date
        ),
        baseline AS (
          SELECT a.promo_id, a.brand_id, a.date, a.att,
            AVG(np.gross) AS bl
          FROM agg a
          LEFT JOIN non_promo np ON np.brand_id = a.brand_id
            AND np.date BETWEEN DATE_SUB(a.date, INTERVAL 28 DAY) AND DATE_SUB(a.date, INTERVAL 1 DAY)
          GROUP BY a.promo_id, a.brand_id, a.date, a.att
        ),
        roi_computed AS (
          SELECT b.promo_id, b.brand_id,
            ROUND(
              (p.br * y.ra + 0.04 * (MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(b.date AS STRING), CAST(b.promo_id AS STRING), CAST(b.brand_id AS STRING)))), 21) - 10) / 10.0)
              * (0.84 + 0.32 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('bmul', CAST(b.brand_id AS STRING)))), 1000) / 1000.0))
              + 0.20 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('padj', CAST(b.brand_id AS STRING), '|', CAST(b.promo_id AS STRING)))), 21) - 10) / 10.0,
              4) AS roi
          FROM baseline b
          JOIN pcfg p ON p.pid = b.promo_id
          JOIN yadj y ON y.yr = EXTRACT(YEAR FROM b.date)
        ),
        brand_data AS (
          SELECT p.promo_type, ROUND(AVG(r.roi), 2) AS avg_roi
          FROM roi_computed r
          JOIN mart.dim_promo p ON p.promo_id = r.promo_id
          WHERE r.brand_id = @brand
          GROUP BY p.promo_type
        ),
        media_data AS (
          SELECT p.promo_type, ROUND(AVG(r.roi), 2) AS avg_roi
          FROM roi_computed r
          JOIN mart.dim_promo p ON p.promo_id = r.promo_id
          GROUP BY p.promo_type
        )
        SELECT COALESCE(b.promo_type, m.promo_type) AS promo_type,
          COALESCE(b.avg_roi, 0) AS brand_avg_roi,
          COALESCE(m.avg_roi, 0) AS media_avg_roi
        FROM brand_data b
        FULL OUTER JOIN media_data m ON b.promo_type = m.promo_type
        ORDER BY media_avg_roi DESC
        """
        return run_query(q, p)

    # Categoria parent o all: usa fact_promo_performance (category_id = parent 1-10)
    q = f"""
    WITH brand_data AS (
      SELECT p.promo_type,
        ROUND(AVG(fp.roi), 2) AS avg_roi
      FROM mart.fact_promo_performance fp
      JOIN mart.dim_promo p ON p.promo_id = fp.promo_id
      WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND fp.brand_id = @brand
        AND (@roi_cat IS NULL OR fp.category_id = @roi_cat)
      GROUP BY p.promo_type
    ),
    media_data AS (
      SELECT p.promo_type,
        ROUND(AVG(fp.roi), 2) AS avg_roi
      FROM mart.fact_promo_performance fp
      JOIN mart.dim_promo p ON p.promo_id = fp.promo_id
      WHERE fp.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        AND (@roi_cat IS NULL OR fp.category_id = @roi_cat)
      GROUP BY p.promo_type
    )
    SELECT COALESCE(b.promo_type, m.promo_type) AS promo_type,
      COALESCE(b.avg_roi, 0) AS brand_avg_roi,
      COALESCE(m.avg_roi, 0) AS media_avg_roi
    FROM brand_data b
    FULL OUTER JOIN media_data m ON b.promo_type = m.promo_type
    ORDER BY media_avg_roi DESC
    """
    return run_query(q, params(ps, pe, brand_id, cat, subcat))
