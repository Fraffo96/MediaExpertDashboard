"""Query precalc: ROI benchmark, category discount benchmark, incremental YoY."""
from google.cloud import bigquery

from app.db.client import run_query


def query_roi_benchmark_by_type_from_precalc(
    year: int, promo_type: str | None = None, roi_cat: int | None = None, brand_id: int | None = None
) -> list[dict]:
    """ROI benchmark per promo_type in categoria. Per Promo Creator."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
    ]
    where_pt = "AND (@pt IS NULL OR promo_type = @pt)"
    if promo_type:
        p.append(bigquery.ScalarQueryParameter("pt", "STRING", promo_type))
    else:
        where_pt = ""
    where_cat = "AND (@roi_cat IS NULL OR category_id = @roi_cat)"
    if roi_cat is not None:
        p.append(bigquery.ScalarQueryParameter("roi_cat", "INT64", roi_cat))
    else:
        where_cat = ""
    where_brand = "AND (@brand IS NULL OR brand_id = @brand)"
    if brand_id is not None:
        p.append(bigquery.ScalarQueryParameter("brand", "INT64", brand_id))
    else:
        where_brand = ""
    q = f"""
    SELECT ROUND(AVG(avg_roi), 2) AS avg_roi, COUNT(*) AS n_promos
    FROM mart.precalc_roi_agg
    WHERE year = @year {where_pt} {where_cat} {where_brand}
    """
    return run_query(q, p, log_label="pc_precalc_roi_agg")


def query_top_competitor_roi_from_precalc(
    year: int, category_id: int, promo_type: str | None, exclude_brand_id: int
) -> list[dict]:
    """Promo Creator: top competitor per ROI in (year, category, promo_type). Esclude brand utente."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("cat", "INT64", category_id),
        bigquery.ScalarQueryParameter("exclude", "INT64", exclude_brand_id),
    ]
    where_pt = "AND (@pt IS NULL OR r.promo_type = @pt)"
    if promo_type:
        p.append(bigquery.ScalarQueryParameter("pt", "STRING", promo_type))
    else:
        where_pt = ""
    q = f"""
    SELECT b.brand_name, ROUND(AVG(r.avg_roi), 2) AS avg_roi
    FROM mart.precalc_roi_agg r
    JOIN mart.dim_brand b ON b.brand_id = r.brand_id
    WHERE r.year = @year AND r.category_id = @cat AND r.brand_id != @exclude {where_pt}
    GROUP BY r.brand_id, b.brand_name
    ORDER BY AVG(r.avg_roi) DESC
    LIMIT 1
    """
    return run_query(q, p, log_label="pc_precalc_top_comp_parent")


def query_category_discount_benchmark_from_precalc(year: int, brand_id: int, category_id: int | None = None) -> list[dict]:
    """Promo Creator: discount benchmark media vs brand. category_id = parent (1-10) o None per all."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    where_cat = ""
    if category_id and 1 <= category_id <= 10:
        where_cat = "AND category_id = @cat"
        p.append(bigquery.ScalarQueryParameter("cat", "INT64", category_id))
    q = f"""
    SELECT
      ROUND(AVG(media_avg_discount), 1) AS media_avg_discount,
      ROUND(AVG(brand_avg_discount), 1) AS brand_avg_discount
    FROM mart.precalc_promo_creator_benchmark
    WHERE year = @year AND brand_id = @brand {where_cat}
    """
    return run_query(q, p, log_label="pc_precalc_discount_bench")


def query_promo_creator_subcat_from_precalc(
    year: int,
    parent_category_id: int,
    subcategory_id: int,
    promo_type: str,
    user_discount: float,
) -> list[dict]:
    """Slice Promo Creator subcategory: ROI, top_brands (ARRAY), top_segments (ARRAY). Match discount_target ±7."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("p", "INT64", int(parent_category_id)),
        bigquery.ScalarQueryParameter("s", "INT64", int(subcategory_id)),
        bigquery.ScalarQueryParameter("pt", "STRING", promo_type),
        bigquery.ScalarQueryParameter("ud", "FLOAT64", float(user_discount)),
    ]
    q = """
    SELECT
      year,
      parent_category_id,
      category_id,
      promo_type,
      discount_target,
      avg_roi,
      n_promos,
      media_avg_discount,
      top_brands,
      top_segments
    FROM mart.precalc_promo_creator_subcat
    WHERE year = @year
      AND parent_category_id = @p
      AND category_id = @s
      AND promo_type = @pt
      AND ABS(discount_target - @ud) <= 7
    ORDER BY ABS(discount_target - @ud), discount_target
    LIMIT 1
    """
    return run_query(q, p, timeout_sec=60, log_label="pc_precalc_subcat_bucket")


def query_promo_creator_subcat_type_from_precalc(
    year: int,
    parent_category_id: int,
    subcategory_id: int,
    promo_type: str,
) -> list[dict]:
    """Fallback subcat+promo_type senza bucket sconto: usa solo precalc aggregando sui target disponibili."""
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", int(year)),
        bigquery.ScalarQueryParameter("p", "INT64", int(parent_category_id)),
        bigquery.ScalarQueryParameter("s", "INT64", int(subcategory_id)),
        bigquery.ScalarQueryParameter("pt", "STRING", promo_type),
    ]
    q = """
    SELECT
      year,
      parent_category_id,
      category_id,
      promo_type,
      CAST(NULL AS INT64) AS discount_target,
      ROUND(AVG(avg_roi), 2) AS avg_roi,
      SUM(n_promos) AS n_promos,
      ROUND(AVG(media_avg_discount), 1) AS media_avg_discount,
      ARRAY_AGG(top_brands IGNORE NULLS LIMIT 1)[OFFSET(0)] AS top_brands,
      ARRAY_AGG(top_segments IGNORE NULLS LIMIT 1)[OFFSET(0)] AS top_segments
    FROM mart.precalc_promo_creator_subcat
    WHERE year = @year
      AND parent_category_id = @p
      AND category_id = @s
      AND promo_type = @pt
      AND n_promos > 0
    GROUP BY year, parent_category_id, category_id, promo_type
    LIMIT 1
    """
    return run_query(q, p, timeout_sec=60, log_label="pc_precalc_subcat_type_agg")


def query_incremental_yoy_vendite_from_precalc(
    year: int, brand_id: int, cat: int | None = None, subcat: int | None = None, channel: str | None = None
) -> list[dict]:
    """year, total_gross, promo_gross, incremental_sales_pln. Da precalc_incremental_yoy + precalc_sales_agg.

    Nota: `precalc_incremental_yoy` non è per channel; le subquery usano channel '' su precalc_sales_agg.
    Il parametro `channel` è riservato per estensioni future: oggi non modifica il risultato.
    """
    p = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    where_cat = ""
    if subcat and subcat >= 100:
        where_cat = "AND iy.category_id = @subcat"
        p.append(bigquery.ScalarQueryParameter("subcat", "INT64", subcat))
    elif cat and 1 <= cat <= 10:
        where_cat = "AND iy.parent_category_id = @cat"
        p.append(bigquery.ScalarQueryParameter("cat", "INT64", cat))

    if subcat and subcat >= 100:
        q = f"""
        SELECT iy.year, SUM(iy.total_gross) AS total_gross,
          (SELECT COALESCE(SUM(promo_gross), 0) FROM mart.precalc_sales_agg
           WHERE year = @year AND brand_id = @brand AND channel = '' AND category_id = @subcat) AS promo_gross,
          SUM(iy.incremental_sales_pln) AS incremental_sales_pln
        FROM mart.precalc_incremental_yoy iy
        WHERE iy.year = @year AND iy.brand_id = @brand AND iy.category_id = @subcat
        GROUP BY iy.year
        """
    elif cat and 1 <= cat <= 10:
        q = f"""
        SELECT iy.year, SUM(iy.total_gross) AS total_gross,
          (SELECT COALESCE(SUM(promo_gross), 0) FROM mart.precalc_sales_agg
           WHERE year = @year AND brand_id = @brand AND channel = '' AND parent_category_id = @cat) AS promo_gross,
          SUM(iy.incremental_sales_pln) AS incremental_sales_pln
        FROM mart.precalc_incremental_yoy iy
        WHERE iy.year = @year AND iy.brand_id = @brand AND iy.parent_category_id = @cat
        GROUP BY iy.year
        """
    else:
        q = """
        WITH by_parent AS (
          SELECT iy.year, iy.parent_category_id,
            SUM(iy.total_gross) AS total_gross,
            SUM(iy.incremental_sales_pln) AS incr_pln
          FROM mart.precalc_incremental_yoy iy
          WHERE iy.year = @year AND iy.brand_id = @brand
          GROUP BY 1, 2
        ),
        promo_by_parent AS (
          SELECT year, parent_category_id, SUM(promo_gross) AS promo_gross
          FROM mart.precalc_sales_agg
          WHERE year = @year AND brand_id = @brand AND channel = ''
          GROUP BY 1, 2
        )
        SELECT bp.year, SUM(bp.total_gross) AS total_gross,
          COALESCE(SUM(pp.promo_gross), 0) AS promo_gross,
          SUM(bp.incr_pln) AS incremental_sales_pln
        FROM by_parent bp
        LEFT JOIN promo_by_parent pp ON pp.year = bp.year AND pp.parent_category_id = bp.parent_category_id
        GROUP BY bp.year
        """
    return run_query(q, p)


def query_incremental_yoy_vendite_multi_year_from_precalc(
    years: list[int],
    brand_id: int,
    cat: int | None = None,
    subcat: int | None = None,
    channel: str | None = None,
) -> list[dict]:
    """Come query_incremental_yoy_vendite_from_precalc ma per più anni in una sola query BigQuery."""
    _ = channel  # stesso comportamento della variante single-year
    if not years:
        return []
    years_sorted = sorted({int(y) for y in years})
    p = [
        bigquery.ArrayQueryParameter("years", "INT64", years_sorted),
        bigquery.ScalarQueryParameter("brand", "INT64", brand_id),
    ]
    if subcat and subcat >= 100:
        p.append(bigquery.ScalarQueryParameter("subcat", "INT64", subcat))
        q = """
        SELECT iy.year,
          SUM(iy.total_gross) AS total_gross,
          (SELECT COALESCE(SUM(p.promo_gross), 0) FROM mart.precalc_sales_agg p
           WHERE p.year = iy.year AND p.brand_id = @brand AND p.channel = '' AND p.category_id = @subcat) AS promo_gross,
          SUM(iy.incremental_sales_pln) AS incremental_sales_pln
        FROM mart.precalc_incremental_yoy iy
        WHERE iy.year IN UNNEST(@years) AND iy.brand_id = @brand AND iy.category_id = @subcat
        GROUP BY iy.year
        ORDER BY iy.year
        """
    elif cat and 1 <= cat <= 10:
        p.append(bigquery.ScalarQueryParameter("cat", "INT64", cat))
        q = """
        SELECT iy.year,
          SUM(iy.total_gross) AS total_gross,
          (SELECT COALESCE(SUM(p.promo_gross), 0) FROM mart.precalc_sales_agg p
           WHERE p.year = iy.year AND p.brand_id = @brand AND p.channel = '' AND p.parent_category_id = @cat) AS promo_gross,
          SUM(iy.incremental_sales_pln) AS incremental_sales_pln
        FROM mart.precalc_incremental_yoy iy
        WHERE iy.year IN UNNEST(@years) AND iy.brand_id = @brand AND iy.parent_category_id = @cat
        GROUP BY iy.year
        ORDER BY iy.year
        """
    else:
        q = """
        WITH by_parent AS (
          SELECT iy.year, iy.parent_category_id,
            SUM(iy.total_gross) AS total_gross,
            SUM(iy.incremental_sales_pln) AS incr_pln
          FROM mart.precalc_incremental_yoy iy
          WHERE iy.year IN UNNEST(@years) AND iy.brand_id = @brand
          GROUP BY 1, 2
        ),
        promo_by_parent AS (
          SELECT year, parent_category_id, SUM(promo_gross) AS promo_gross
          FROM mart.precalc_sales_agg
          WHERE year IN UNNEST(@years) AND brand_id = @brand AND channel = ''
          GROUP BY 1, 2
        )
        SELECT bp.year, SUM(bp.total_gross) AS total_gross,
          COALESCE(SUM(pp.promo_gross), 0) AS promo_gross,
          SUM(bp.incr_pln) AS incremental_sales_pln
        FROM by_parent bp
        LEFT JOIN promo_by_parent pp ON pp.year = bp.year AND pp.parent_category_id = bp.parent_category_id
        GROUP BY bp.year
        ORDER BY bp.year
        """
    return run_query(q, p)
