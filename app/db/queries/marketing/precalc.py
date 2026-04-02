"""Marketing precalc: read from precalc tables when period is full year."""
from google.cloud import bigquery

from app.db.client import run_query


def _is_full_year(ps: str, pe: str) -> bool:
    """True if ps..pe is a full calendar year."""
    if not ps or not pe or len(ps) < 10 or len(pe) < 10:
        return False
    return ps.endswith("-01-01") and pe.endswith("-12-31") and ps[:4] == pe[:4]


def query_precalc_segment_top_categories(
    year: int,
    segment_id: int | None = None,
    category_id: int | None = None,
    subcategory_id: int | None = None,
    limit: int = 10,
) -> list[dict]:
    """Top categories from precalc. category_id=parent filter; subcategory_id filter not used for precalc."""
    params = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("lim", "INT64", limit),
    ]
    where = ["year = @year"]
    if segment_id is not None:
        where.append("segment_id = @seg")
        params.append(bigquery.ScalarQueryParameter("seg", "INT64", segment_id))

    if subcategory_id is not None and subcategory_id >= 100:
        where.append("level = 2 AND parent_category_id = (SELECT parent_category_id FROM mart.dim_category WHERE category_id = @subcat AND level = 2 LIMIT 1)")
        params.append(bigquery.ScalarQueryParameter("subcat", "INT64", subcategory_id))
    elif category_id is not None and 1 <= category_id <= 10:
        where.append("level = 2 AND parent_category_id = @cat")
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", category_id))
    else:
        where.append("level = 1")

    where_clause = " AND ".join(where)
    q = f"""
    WITH ranked AS (
      SELECT segment_id, category_id, parent_category_id, category_name, gross_pln,
        ROW_NUMBER() OVER (PARTITION BY segment_id ORDER BY gross_pln DESC) AS rn
      FROM mart.precalc_mkt_segment_categories
      WHERE {where_clause}
    )
    SELECT segment_id, category_id AS parent_category_id, category_name, gross_pln
    FROM ranked WHERE rn <= @lim
    ORDER BY segment_id, rn
    """
    return run_query(q, params)


def query_precalc_segment_top_skus(
    year: int,
    segment_id: int | None = None,
    category_id: int | None = None,
    subcategory_id: int | None = None,
    limit: int = 10,
) -> list[dict]:
    """Top SKUs from precalc."""
    params = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
        bigquery.ScalarQueryParameter("lim", "INT64", limit),
    ]
    where = ["year = @year"]
    if segment_id is not None:
        where.append("segment_id = @seg")
        params.append(bigquery.ScalarQueryParameter("seg", "INT64", segment_id))
    if subcategory_id is not None and subcategory_id >= 100:
        where.append("parent_category_id = (SELECT parent_category_id FROM mart.dim_category WHERE category_id = @subcat AND level = 2 LIMIT 1)")
        params.append(bigquery.ScalarQueryParameter("subcat", "INT64", subcategory_id))
    elif category_id is not None and 1 <= category_id <= 10:
        where.append("parent_category_id = @cat")
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", category_id))

    where_clause = " AND ".join(where)
    q = f"""
    WITH ranked AS (
      SELECT segment_id, product_id, product_name, brand_name, gross_pln, units,
        ROW_NUMBER() OVER (PARTITION BY segment_id ORDER BY gross_pln DESC) AS rn
      FROM mart.precalc_mkt_segment_skus
      WHERE {where_clause}
    )
    SELECT segment_id, product_id, product_name, brand_name, gross_pln, units
    FROM ranked WHERE rn <= @lim
    ORDER BY segment_id, rn
    """
    return run_query(q, params)


def query_precalc_purchasing_channel(
    year: int, segment_id: int | None = None, parent_category_id: int | None = None,
) -> list[dict]:
    """Channel mix from precalc. parent_category_id None = righe aggregate (tutte le categorie)."""
    params = [bigquery.ScalarQueryParameter("year", "INT64", year)]
    where = ["year = @year"]
    if segment_id is not None:
        where.append("segment_id = @seg")
        params.append(bigquery.ScalarQueryParameter("seg", "INT64", segment_id))
    if parent_category_id is not None:
        where.append("parent_category_id = @pcat")
        params.append(bigquery.ScalarQueryParameter("pcat", "INT64", int(parent_category_id)))
    else:
        where.append("parent_category_id IS NULL")
    q = f"""
    SELECT segment_id, segment_name, channel, gross_pln
    FROM mart.precalc_mkt_purchasing_channel
    WHERE {" AND ".join(where)}
    ORDER BY segment_id, gross_pln DESC
    """
    return run_query(q, params)


def query_precalc_purchasing_peak(
    year: int, segment_id: int | None = None, parent_category_id: int | None = None,
) -> list[dict]:
    """Peak events from precalc. parent_category_id None = righe aggregate."""
    params = [bigquery.ScalarQueryParameter("year", "INT64", year)]
    where = ["year = @year"]
    if segment_id is not None:
        where.append("segment_id = @seg")
        params.append(bigquery.ScalarQueryParameter("seg", "INT64", segment_id))
    if parent_category_id is not None:
        where.append("parent_category_id = @pcat")
        params.append(bigquery.ScalarQueryParameter("pcat", "INT64", int(parent_category_id)))
    else:
        where.append("parent_category_id IS NULL")
    q = f"""
    SELECT segment_id, segment_name, peak_event, orders_pct AS orders_pct, gross_pln
    FROM mart.precalc_mkt_purchasing_peak
    WHERE {" AND ".join(where)}
    ORDER BY segment_id, gross_pln DESC
    """
    rows = run_query(q, params)
    return [{"segment_id": r["segment_id"], "segment_name": r["segment_name"], "peak_event": r["peak_event"], "orders": 0, "orders_pct": float(r.get("orders_pct") or 0), "gross_pln": r["gross_pln"]} for r in rows]
