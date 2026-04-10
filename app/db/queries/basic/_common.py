"""Parametri e filtri WHERE comuni per query dashboard Basic."""
from google.cloud import bigquery

_P = lambda ps, pe, cat, seg, gender=None, brand=None: [
    bigquery.ScalarQueryParameter("ps", "STRING", ps),
    bigquery.ScalarQueryParameter("pe", "STRING", pe),
    bigquery.ScalarQueryParameter("cat", "INT64", int(cat) if cat else None),
    bigquery.ScalarQueryParameter("seg", "INT64", int(seg) if seg else None),
    bigquery.ScalarQueryParameter("gender", "STRING", gender if gender else None),
    bigquery.ScalarQueryParameter("brand", "INT64", int(brand) if brand else None),
]

_WHERE = """
  WHERE f.date BETWEEN PARSE_DATE('%Y-%m-%d', @ps) AND PARSE_DATE('%Y-%m-%d', @pe)
    AND (@cat IS NULL OR f.parent_category_id = @cat OR f.category_id = @cat)
    AND (@seg IS NULL OR f.segment_id = @seg)
    AND (@brand IS NULL OR f.brand_id = @brand)
    AND (@gender IS NULL OR f.gender = @gender)
"""


def _is_parent_cat(cat):
    """Parent categories have IDs 1-10, subcategories 101+."""
    return cat is not None and 1 <= int(cat) <= 10
