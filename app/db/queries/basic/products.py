"""Top prodotti e vendite per prodotto (fact_order_items)."""
import re

from google.cloud import bigquery

from app.db.client import run_query


def query_top_products(ps, pe, limit=20, cat=None, brand=None):
    """Top prodotti per vendite (gross_pln). Include channel per filtro client-side."""
    cat_clause = " AND (p.category_id = @cat OR p.subcategory_id = @cat)" if (cat and str(cat).strip()) else ""
    brand_clause = " AND p.brand_id = @brand" if (brand and str(brand).strip()) else ""
    q = f"""
    SELECT p.product_id, p.product_name, p.brand_id, b.brand_name,
      p.category_id, c.category_name, o.channel,
      SUM(oi.gross_pln) AS gross_pln, SUM(oi.quantity) AS units
    FROM mart.fact_order_items oi
    JOIN mart.fact_orders o ON o.order_id = oi.order_id
    JOIN mart.dim_product p ON p.product_id = oi.product_id
    JOIN mart.dim_brand b ON b.brand_id = p.brand_id
    JOIN mart.dim_category c ON c.category_id = p.category_id AND c.level = 1
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe){cat_clause}{brand_clause}
    GROUP BY p.product_id, p.product_name, p.brand_id, b.brand_name, p.category_id, c.category_name, o.channel
    ORDER BY gross_pln DESC
    LIMIT {int(limit)}
    """
    params = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
    ]
    if cat and str(cat).strip():
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(cat)))
    if brand and str(brand).strip():
        params.append(bigquery.ScalarQueryParameter("brand", "INT64", int(brand)))
    return run_query(q, params)


def query_products_by_category(ps, pe, cat=None):
    """Vendite per prodotto raggruppate per categoria. Per grafici prodotti."""
    q = """
    SELECT p.product_id, p.product_name, b.brand_name, c.category_name,
      SUM(oi.gross_pln) AS gross_pln, SUM(oi.quantity) AS units
    FROM mart.fact_order_items oi
    JOIN mart.fact_orders o ON o.order_id = oi.order_id
    JOIN mart.dim_product p ON p.product_id = oi.product_id
    JOIN mart.dim_brand b ON b.brand_id = p.brand_id
    JOIN mart.dim_category c ON c.category_id = p.category_id AND c.level = 1
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND (@cat IS NULL OR p.category_id = @cat OR p.subcategory_id = @cat)
    GROUP BY p.product_id, p.product_name, b.brand_name, c.category_name
    ORDER BY gross_pln DESC
    """
    return run_query(q, [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("cat", "INT64", int(cat) if cat else None),
    ])


def query_underperforming_products(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int | None = None,
    bottom_pct: float = 0.10,
    limit: int = 80,
    top_n: int | None = None,
) -> list[dict]:
    """
    Brand SKUs with sales in the window, ranked by gross_pln ascending.

    - **Percentile mode** (default): rows with PERCENT_RANK <= ``bottom_pct`` (0 = lowest sales).
    - **Top-N mode**: when ``top_n`` > 0, returns exactly the N lowest-grossing SKUs (ignores ``bottom_pct``).
 Optional parent (1–10) or subcategory (>=100) filter, same semantics as query_top_products.
    """
    cat_clause = (
        " AND (p.category_id = @cat OR p.subcategory_id = @cat)"
        if (parent_category_id is not None and str(parent_category_id).strip())
        else ""
    )
    lim_cap = max(1, min(int(limit or 80), 80))
    use_top_n = top_n is not None and int(top_n) > 0
    if use_top_n:
        n = max(1, min(int(top_n), 80))
        q = f"""
        WITH agg AS (
          SELECT p.product_id, p.product_name,
            p.category_id, c.category_name,
            SUM(oi.gross_pln) AS gross_pln,
            SUM(oi.quantity) AS units
          FROM mart.fact_order_items oi
          JOIN mart.fact_orders o ON o.order_id = oi.order_id
          JOIN mart.dim_product p ON p.product_id = oi.product_id
          JOIN mart.dim_category c ON c.category_id = p.category_id AND c.level = 1
          WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
            AND p.brand_id = @brand
            {cat_clause}
          GROUP BY p.product_id, p.product_name, p.category_id, c.category_name
        ),
        ranked AS (
          SELECT *,
            PERCENT_RANK() OVER (ORDER BY gross_pln ASC) AS pct_rank
          FROM agg
        )
        SELECT product_id, product_name, category_id, category_name, gross_pln, units, pct_rank
        FROM ranked
        ORDER BY gross_pln ASC, product_id ASC
        LIMIT @lim
        """
        params: list = [
            bigquery.ScalarQueryParameter("ps", "STRING", ps),
            bigquery.ScalarQueryParameter("pe", "STRING", pe),
            bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
            bigquery.ScalarQueryParameter("lim", "INT64", n),
        ]
    else:
        pct = max(0.01, min(float(bottom_pct or 0.10), 0.50))
        q = f"""
        WITH agg AS (
          SELECT p.product_id, p.product_name,
            p.category_id, c.category_name,
            SUM(oi.gross_pln) AS gross_pln,
            SUM(oi.quantity) AS units
          FROM mart.fact_order_items oi
          JOIN mart.fact_orders o ON o.order_id = oi.order_id
          JOIN mart.dim_product p ON p.product_id = oi.product_id
          JOIN mart.dim_category c ON c.category_id = p.category_id AND c.level = 1
          WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
            AND p.brand_id = @brand
            {cat_clause}
          GROUP BY p.product_id, p.product_name, p.category_id, c.category_name
        ),
        ranked AS (
          SELECT *,
            PERCENT_RANK() OVER (ORDER BY gross_pln ASC) AS pct_rank
          FROM agg
        )
        SELECT product_id, product_name, category_id, category_name, gross_pln, units, pct_rank
        FROM ranked
        WHERE pct_rank <= @pct_threshold
        ORDER BY gross_pln ASC, product_id ASC
        LIMIT @lim
        """
        params = [
            bigquery.ScalarQueryParameter("ps", "STRING", ps),
            bigquery.ScalarQueryParameter("pe", "STRING", pe),
            bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
            bigquery.ScalarQueryParameter("pct_threshold", "FLOAT64", pct),
            bigquery.ScalarQueryParameter("lim", "INT64", lim_cap),
        ]
    if parent_category_id is not None and str(parent_category_id).strip():
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(parent_category_id)))
    return run_query(q, params)


def query_products_by_price(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
    order: str = "desc",
    limit: int = 15,
) -> list[dict]:
    """Products with sales in the period, ordered by list price (price_pln). order='desc' = most expensive first."""
    cat_clause = ""
    params: list = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
        bigquery.ScalarQueryParameter("lim", "INT64", max(1, min(int(limit), 50))),
    ]
    if subcategory_id and int(subcategory_id) >= 100:
        cat_clause = " AND p.subcategory_id = @cat"
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(subcategory_id)))
    elif parent_category_id and 1 <= int(parent_category_id) <= 10:
        cat_clause = " AND p.category_id = @cat"
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(parent_category_id)))
    order_dir = "ASC" if (order or "").lower().startswith("asc") else "DESC"
    q = f"""
    SELECT p.product_id, p.product_name, p.price_pln, p.premium_flag, p.launch_year,
           c_parent.category_name AS parent_category_name,
           c_sub.category_name AS subcategory_name,
           SUM(oi.quantity) AS units_sold, SUM(oi.gross_pln) AS gross_pln
    FROM mart.fact_order_items oi
    JOIN mart.fact_orders o ON o.order_id = oi.order_id
    JOIN mart.dim_product p ON p.product_id = oi.product_id
    LEFT JOIN mart.dim_category c_parent ON c_parent.category_id = p.category_id AND c_parent.level = 1
    LEFT JOIN mart.dim_category c_sub ON c_sub.category_id = p.subcategory_id AND c_sub.level = 2
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND p.brand_id = @brand
      {cat_clause}
    GROUP BY p.product_id, p.product_name, p.price_pln, p.premium_flag, p.launch_year,
             c_parent.category_name, c_sub.category_name
    ORDER BY p.price_pln {order_dir}
    LIMIT @lim
    """
    return run_query(q, params)


def query_products_in_price_range(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    price_min: float | None = None,
    price_max: float | None = None,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
    limit: int = 20,
) -> list[dict]:
    """Products in a PLN price bracket, with sales in the period. price_min/max are inclusive."""
    params: list = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
        bigquery.ScalarQueryParameter("lim", "INT64", max(1, min(int(limit), 80))),
    ]
    price_clause = ""
    if price_min is not None:
        price_clause += " AND p.price_pln >= @price_min"
        params.append(bigquery.ScalarQueryParameter("price_min", "FLOAT64", float(price_min)))
    if price_max is not None:
        price_clause += " AND p.price_pln <= @price_max"
        params.append(bigquery.ScalarQueryParameter("price_max", "FLOAT64", float(price_max)))
    cat_clause = ""
    if subcategory_id and int(subcategory_id) >= 100:
        cat_clause = " AND p.subcategory_id = @cat"
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(subcategory_id)))
    elif parent_category_id and 1 <= int(parent_category_id) <= 10:
        cat_clause = " AND p.category_id = @cat"
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(parent_category_id)))
    q = f"""
    SELECT p.product_id, p.product_name, p.price_pln, p.premium_flag, p.launch_year,
           c_parent.category_name AS parent_category_name,
           c_sub.category_name AS subcategory_name,
           SUM(oi.quantity) AS units_sold, SUM(oi.gross_pln) AS gross_pln
    FROM mart.fact_order_items oi
    JOIN mart.fact_orders o ON o.order_id = oi.order_id
    JOIN mart.dim_product p ON p.product_id = oi.product_id
    LEFT JOIN mart.dim_category c_parent ON c_parent.category_id = p.category_id AND c_parent.level = 1
    LEFT JOIN mart.dim_category c_sub ON c_sub.category_id = p.subcategory_id AND c_sub.level = 2
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND p.brand_id = @brand
      {price_clause}
      {cat_clause}
    GROUP BY p.product_id, p.product_name, p.price_pln, p.premium_flag, p.launch_year,
             c_parent.category_name, c_sub.category_name
    ORDER BY gross_pln DESC
    LIMIT @lim
    """
    return run_query(q, params)


def query_new_launches(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    min_launch_year: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
    limit: int = 20,
) -> list[dict]:
    """Products launched in min_launch_year or later, with sales in the period, ranked by revenue."""
    params: list = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
        bigquery.ScalarQueryParameter("min_year", "INT64", int(min_launch_year)),
        bigquery.ScalarQueryParameter("lim", "INT64", max(1, min(int(limit), 50))),
    ]
    cat_clause = ""
    if subcategory_id and int(subcategory_id) >= 100:
        cat_clause = " AND p.subcategory_id = @cat"
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(subcategory_id)))
    elif parent_category_id and 1 <= int(parent_category_id) <= 10:
        cat_clause = " AND p.category_id = @cat"
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(parent_category_id)))
    q = f"""
    SELECT p.product_id, p.product_name, p.price_pln, p.premium_flag, p.launch_year,
           c_parent.category_name AS parent_category_name,
           c_sub.category_name AS subcategory_name,
           SUM(oi.quantity) AS units_sold, SUM(oi.gross_pln) AS gross_pln
    FROM mart.fact_order_items oi
    JOIN mart.fact_orders o ON o.order_id = oi.order_id
    JOIN mart.dim_product p ON p.product_id = oi.product_id
    LEFT JOIN mart.dim_category c_parent ON c_parent.category_id = p.category_id AND c_parent.level = 1
    LEFT JOIN mart.dim_category c_sub ON c_sub.category_id = p.subcategory_id AND c_sub.level = 2
    WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND p.brand_id = @brand
      AND p.launch_year >= @min_year
      {cat_clause}
    GROUP BY p.product_id, p.product_name, p.price_pln, p.premium_flag, p.launch_year,
             c_parent.category_name, c_sub.category_name
    ORDER BY gross_pln DESC
    LIMIT @lim
    """
    return run_query(q, params)


def query_sales_trend_by_month(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
) -> list[dict]:
    """Monthly sales trend (gross_pln, units) for a brand, optionally scoped to category/subcategory."""
    params: list = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
    ]
    cat_clause = ""
    if subcategory_id and int(subcategory_id) >= 100:
        cat_clause = " AND category_id = @cat"
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(subcategory_id)))
    elif parent_category_id and 1 <= int(parent_category_id) <= 10:
        cat_clause = " AND parent_category_id = @cat"
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(parent_category_id)))
    q = f"""
    SELECT
      EXTRACT(YEAR FROM date) AS year,
      EXTRACT(MONTH FROM date) AS month,
      FORMAT_DATE('%Y-%m', date) AS year_month,
      SUM(gross_pln) AS gross_pln,
      SUM(units) AS units
    FROM mart.fact_sales_daily
    WHERE date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND brand_id = @brand
      {cat_clause}
    GROUP BY year, month, year_month
    ORDER BY year, month
    """
    return run_query(q, params)


def query_sales_by_gender_breakdown(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
) -> list[dict]:
    """Sales breakdown by gender (M/F) for a brand, optionally scoped to category/subcategory."""
    params: list = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)),
    ]
    cat_clause = ""
    if subcategory_id and int(subcategory_id) >= 100:
        cat_clause = " AND category_id = @cat"
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(subcategory_id)))
    elif parent_category_id and 1 <= int(parent_category_id) <= 10:
        cat_clause = " AND parent_category_id = @cat"
        params.append(bigquery.ScalarQueryParameter("cat", "INT64", int(parent_category_id)))
    q = f"""
    SELECT gender, SUM(gross_pln) AS gross_pln, SUM(units) AS units,
           ROUND(100.0 * SUM(gross_pln) / NULLIF(SUM(SUM(gross_pln)) OVER (), 0), 1) AS pct_of_total
    FROM mart.fact_sales_daily
    WHERE date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
      AND brand_id = @brand
      AND gender IS NOT NULL
      {cat_clause}
    GROUP BY gender
    ORDER BY gross_pln DESC
    """
    return run_query(q, params)


def query_products_any_token_match(
    ps: str,
    pe: str,
    *,
    tokens: list[str],
    brand_id: int | None = None,
    candidate_limit: int = 800,
) -> list[dict]:
    """
    Products (with sales in window) whose name matches at least one token (substring).
    Used as a broad candidate set for natural-language SKU resolution; refine in Python.
    """
    clean: list[str] = []
    for t in tokens:
        s = re.sub(r"[^a-z0-9]", "", (t or "").lower())
        if 2 <= len(s) <= 48:
            clean.append(s)
    if not clean:
        return []
    lim = max(50, min(int(candidate_limit), 2000))
    brand_clause = ""
    params: list = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ArrayQueryParameter("tokens", "STRING", clean),
        bigquery.ScalarQueryParameter("lim", "INT64", lim),
    ]
    if brand_id is not None:
        brand_clause = " AND p.brand_id = @brand"
        params.append(bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id)))
    q = f"""
    WITH products AS (
      SELECT p.product_id, p.product_name,
        SUM(oi.gross_pln) AS gross_pln, SUM(oi.quantity) AS units
      FROM mart.fact_order_items oi
      JOIN mart.fact_orders o ON o.order_id = oi.order_id
      JOIN mart.dim_product p ON p.product_id = oi.product_id
      WHERE o.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
        {brand_clause}
      GROUP BY p.product_id, p.product_name
    )
    SELECT product_id, product_name, gross_pln, units
    FROM products p
    WHERE (
      SELECT COUNT(1) FROM UNNEST(@tokens) AS tok
      WHERE CONTAINS_SUBSTR(LOWER(p.product_name), tok)
    ) >= 1
    ORDER BY gross_pln DESC
    LIMIT @lim
    """
    return run_query(q, params)
