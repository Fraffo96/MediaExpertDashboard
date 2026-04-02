"""Query condivise: filtri, dropdown, dimensioni."""
import time

from google.cloud import bigquery

from app.db.client import run_query

# Evita ripetere DISTINCT anni su ogni get_mi_base / get_mi_all_years (stesso valore per minuti).
_AVAILABLE_YEARS_TTL_SEC = 120.0
_available_years_cached: list[int] | None = None
_available_years_cached_at: float = 0.0


def clear_available_years_cache() -> None:
    """Invalida cache locale anni disponibili (non Redis)."""
    global _available_years_cached, _available_years_cached_at
    _available_years_cached = None
    _available_years_cached_at = 0.0


def query_categories():
    return run_query("""
        SELECT category_id, category_name, level, parent_category_id, category_path
        FROM mart.dim_category
        ORDER BY COALESCE(parent_category_id, category_id), level, category_id
    """)


def query_subcategories():
    """Subcategories only (level=2) for filter bar."""
    return run_query("""
        SELECT category_id, category_name, parent_category_id
        FROM mart.dim_category
        WHERE level = 2
        ORDER BY parent_category_id, category_id
    """)


def query_segments():
    return run_query("SELECT segment_id, segment_name, segment_description, age_range, income_level, gender_skew, top_driver FROM mart.dim_segment ORDER BY segment_id")


def query_brands():
    return run_query("SELECT brand_id, brand_name FROM mart.dim_brand ORDER BY brand_name")


def query_promo_types():
    """Una riga per tipo promo (evita duplicati nel dropdown se mechanic varia tra promoid)."""
    return run_query(
        """
        SELECT promo_type, ANY_VALUE(promo_mechanic) AS promo_mechanic
        FROM mart.dim_promo
        WHERE promo_type IS NOT NULL AND TRIM(CAST(promo_type AS STRING)) != ''
        GROUP BY promo_type
        ORDER BY promo_type
        """
    )


def query_promos():
    return run_query("SELECT promo_id, promo_name, promo_type, promo_mechanic, funding_type, CAST(start_date AS STRING) AS start_date, CAST(end_date AS STRING) AS end_date FROM mart.dim_promo ORDER BY promo_id")


def query_genders():
    """Restituisce opzioni genere da fact_sales_daily."""
    try:
        return run_query("SELECT DISTINCT gender FROM mart.fact_sales_daily WHERE gender IS NOT NULL ORDER BY gender")
    except Exception:
        return [{"gender": "M"}, {"gender": "F"}]


def query_categories_by_brand(brand_id: int) -> list[dict]:
    """Parent categories (level=1) where the brand has products. For marketing filter dropdown."""
    return run_query(
        """
        SELECT DISTINCT c.category_id, c.category_name, 1 AS level, NULL AS parent_category_id, c.category_path
        FROM mart.dim_product p
        JOIN mart.dim_category c ON c.category_id = p.category_id AND c.level = 1
        WHERE p.brand_id = @brand
        ORDER BY c.category_id
        """,
        [bigquery.ScalarQueryParameter("brand", "INT64", brand_id)],
    )


def query_subcategories_by_brand(brand_id: int) -> list[dict]:
    """Subcategories (level=2) where the brand has products. For marketing filter dropdown."""
    return run_query(
        """
        SELECT DISTINCT c.category_id, c.category_name, c.parent_category_id
        FROM mart.dim_product p
        JOIN mart.dim_category c ON c.category_id = p.subcategory_id AND c.level = 2
        WHERE p.brand_id = @brand
        ORDER BY c.parent_category_id, c.category_id
        """,
        [bigquery.ScalarQueryParameter("brand", "INT64", brand_id)],
    )


def query_available_years():
    """Anni con dati in fact_sales_daily, ordinati asc."""
    global _available_years_cached, _available_years_cached_at
    now = time.monotonic()
    if _available_years_cached is not None and (now - _available_years_cached_at) < _AVAILABLE_YEARS_TTL_SEC:
        return list(_available_years_cached)
    try:
        rows = run_query("""
            SELECT DISTINCT EXTRACT(YEAR FROM date) AS year
            FROM mart.fact_sales_daily
            WHERE date IS NOT NULL
            ORDER BY year
        """)
        out = [int(r["year"]) for r in rows if r.get("year") is not None]
        _available_years_cached = out
        _available_years_cached_at = now
        return list(out)
    except Exception:
        return []
