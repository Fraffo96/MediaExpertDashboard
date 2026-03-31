"""Parametri e filtri condivisi per le query Market Intelligence."""
from google.cloud import bigquery

CHANNELS = ["", "web", "app", "store"]


def params(ps, pe, brand_id, cat=None, subcat=None, channel=None):
    """cat=parent 1-10, subcat=subcategory 101+. Per fact_promo_performance, roi_cat = parent."""
    c = int(cat) if cat and str(cat).strip() else None
    s = int(subcat) if subcat and str(subcat).strip() else None
    roi_cat = c if (c and 1 <= c <= 10) else (s // 100 if s and s >= 100 else None)
    p = [
        bigquery.ScalarQueryParameter("ps", "STRING", ps),
        bigquery.ScalarQueryParameter("pe", "STRING", pe),
        bigquery.ScalarQueryParameter("brand", "INT64", int(brand_id) if brand_id else None),
        bigquery.ScalarQueryParameter("cat", "INT64", c if (c and 1 <= c <= 10) else None),
        bigquery.ScalarQueryParameter("subcat", "INT64", s if (s and s >= 100) else None),
        bigquery.ScalarQueryParameter("roi_cat", "INT64", roi_cat),
    ]
    if channel and str(channel).strip() in ("web", "app", "store"):
        p.append(bigquery.ScalarQueryParameter("channel", "STRING", str(channel).strip()))
    return p


def where_cat_subcat():
    """Clausola WHERE per filtrare per category/subcategory."""
    return """
      AND (@cat IS NULL OR f.parent_category_id = @cat OR f.category_id = @cat)
      AND (@subcat IS NULL OR f.category_id = @subcat)
    """


def from_table(channel=None):
    """Tabella/vista da usare: fact_sales_daily o v_sales_daily_by_channel se channel."""
    if channel and str(channel).strip() in ("web", "app", "store"):
        return "mart.v_sales_daily_by_channel"
    return "mart.fact_sales_daily"


def where_channel(channel=None):
    """Clausola WHERE per channel (solo se v_sales_daily_by_channel)."""
    if channel and str(channel).strip() in ("web", "app", "store"):
        return " AND f.channel = @channel"
    return ""
