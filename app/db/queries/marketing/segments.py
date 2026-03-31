"""Marketing: segment summary - top categories and SKUs per segment."""
from google.cloud import bigquery

from app.db.client import run_query


def query_segment_top_categories(
    ps: str,
    pe: str,
    segment_id: int | None = None,
    category_id: int | None = None,
    subcategory_id: int | None = None,
    brand_id: int | None = None,
    limit: int = 5,
) -> list[dict]:
    """Top categories/subcategories by sales per segment. Filter by category (1-10), subcategory (101+), brand."""
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("lim", "INT64", limit),
    ]
    where_parts = ["f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)", "f.gross_pln > 0"]
    if segment_id is not None:
        where_parts.append("f.segment_id = @seg")
        params.append(bigquery.ScalarQueryParameter("seg", "INT64", segment_id))
    if brand_id is not None:
        where_parts.append("f.brand_id = @brand")
        params.append(bigquery.ScalarQueryParameter("brand", "INT64", brand_id))

    if subcategory_id is not None and subcategory_id >= 100:
        where_parts.append("f.parent_category_id = (SELECT parent_category_id FROM mart.dim_category WHERE category_id = @subcat AND level = 2 LIMIT 1)")
        params.append(bigquery.ScalarQueryParameter("subcat", "INT64", subcategory_id))
    elif category_id is not None and 1 <= category_id <= 10:
        where_parts.append("f.parent_category_id = @cat")
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", category_id))

    where_clause = " AND ".join(where_parts)

    if category_id is not None and 1 <= category_id <= 10:
        q = f"""
        WITH ranked AS (
          SELECT f.segment_id, f.category_id AS parent_category_id, c.category_name,
            SUM(f.gross_pln) AS gross_pln,
            ROW_NUMBER() OVER (PARTITION BY f.segment_id ORDER BY SUM(f.gross_pln) DESC) AS rn
          FROM mart.fact_sales_daily f
          JOIN mart.dim_category c ON c.category_id = f.category_id AND c.level = 2 AND c.parent_category_id = @cat
          WHERE {where_clause}
          GROUP BY f.segment_id, f.category_id, c.category_name
        )
        SELECT segment_id, parent_category_id, category_name, gross_pln
        FROM ranked WHERE rn <= @lim
        ORDER BY segment_id, rn
        """
    elif subcategory_id is not None and subcategory_id >= 100:
        q = f"""
        WITH ranked AS (
          SELECT f.segment_id, f.category_id AS parent_category_id, c.category_name,
            SUM(f.gross_pln) AS gross_pln,
            ROW_NUMBER() OVER (PARTITION BY f.segment_id ORDER BY SUM(f.gross_pln) DESC) AS rn
          FROM mart.fact_sales_daily f
          JOIN mart.dim_category c ON c.category_id = f.category_id AND c.level = 2
          WHERE {where_clause}
          GROUP BY f.segment_id, f.category_id, c.category_name
        )
        SELECT segment_id, parent_category_id, category_name, gross_pln
        FROM ranked WHERE rn <= @lim
        ORDER BY segment_id, rn
        """
    else:
        q = f"""
        WITH ranked AS (
          SELECT f.segment_id, f.parent_category_id, c.category_name,
            SUM(f.gross_pln) AS gross_pln,
            ROW_NUMBER() OVER (PARTITION BY f.segment_id ORDER BY SUM(f.gross_pln) DESC) AS rn
          FROM mart.fact_sales_daily f
          JOIN mart.dim_category c ON c.category_id = f.parent_category_id AND c.level = 1
          WHERE {where_clause}
          GROUP BY f.segment_id, f.parent_category_id, c.category_name
        )
        SELECT segment_id, parent_category_id, category_name, gross_pln
        FROM ranked WHERE rn <= @lim
        ORDER BY segment_id, rn
        """
    return run_query(q, params)


def query_segment_top_skus(
    ps: str,
    pe: str,
    segment_id: int | None = None,
    category_id: int | None = None,
    subcategory_id: int | None = None,
    brand_id: int | None = None,
    limit: int = 5,
) -> list[dict]:
    """Top SKUs (products) by sales per segment. Filter by category, subcategory, brand."""
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("lim", "INT64", limit),
    ]
    where_parts = ["o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)"]
    if segment_id is not None:
        where_parts.append("c.segment_id = @seg")
        params.append(bigquery.ScalarQueryParameter("seg", "INT64", segment_id))
    if brand_id is not None:
        where_parts.append("p.brand_id = @brand")
        params.append(bigquery.ScalarQueryParameter("brand", "INT64", brand_id))
    if subcategory_id is not None and subcategory_id >= 100:
        where_parts.append("p.subcategory_id = @subcat")
        params.append(bigquery.ScalarQueryParameter("subcat", "INT64", subcategory_id))
    elif category_id is not None and 1 <= category_id <= 10:
        where_parts.append("p.category_id = @cat")
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", category_id))

    where_clause = " AND ".join(where_parts)
    q = f"""
    WITH ranked AS (
      SELECT c.segment_id, p.product_id, p.product_name, b.brand_name,
        SUM(oi.gross_pln) AS gross_pln, SUM(oi.quantity) AS units,
        ROW_NUMBER() OVER (PARTITION BY c.segment_id ORDER BY SUM(oi.gross_pln) DESC) AS rn
      FROM mart.fact_order_items oi
      JOIN mart.fact_orders o ON o.order_id = oi.order_id
      JOIN mart.dim_customer c ON c.customer_id = o.customer_id
      JOIN mart.dim_product p ON p.product_id = oi.product_id
      JOIN mart.dim_brand b ON b.brand_id = p.brand_id
      WHERE {where_clause}
      GROUP BY c.segment_id, p.product_id, p.product_name, b.brand_name
    )
    SELECT segment_id, product_id, product_name, brand_name, gross_pln, units
    FROM ranked WHERE rn <= @lim
    ORDER BY segment_id, rn
    """
    return run_query(q, params)
