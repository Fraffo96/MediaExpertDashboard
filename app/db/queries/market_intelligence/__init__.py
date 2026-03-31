"""
Market Intelligence: query brand vs media per category/subcategory.
Moduli: categories, sales, promo, discount, peak.
"""
from .categories import query_brand_categories, query_brand_subcategories
from .sales import (
    query_sales_value_by_category,
    query_sales_volume_by_category,
    query_sales_value_by_subcategory,
    query_sales_volume_by_subcategory,
    query_sales_by_brand_in_category,
    query_sales_by_brand_in_all_categories,
    query_sales_by_brand_in_subcategory,
    query_sales_by_brand_in_all_subcategories,
    query_sales_by_brand_in_all_categories_all_channels,
    query_sales_by_brand_in_all_subcategories_all_channels,
    query_sales_pct_by_brand_prev_year_categories,
    query_sales_pct_by_brand_prev_year_subcategories,
    query_sales_pct_by_brand_prev_year_categories_all_channels,
    query_sales_pct_by_brand_prev_year_subcategories_all_channels,
    query_sales_value_brand_vs_media,
    query_sales_volume_brand_vs_media,
)
from .promo import (
    query_promo_share_brand_vs_media,
    query_promo_share_by_category_brand_vs_media,
    query_promo_share_by_subcategory_brand_vs_media,
    query_incremental_yoy_vendite,
    query_incremental_yoy_brand_vs_media,
    query_promo_roi_brand_vs_media,
)
from .discount import (
    query_discount_depth_brand_vs_media,
    query_discount_depth_single,
    query_discount_depth_for_all_subcategories,
)
from .peak import query_peak_events_brand_vs_media

__all__ = [
    "query_brand_categories",
    "query_brand_subcategories",
    "query_sales_value_by_category",
    "query_sales_volume_by_category",
    "query_sales_value_by_subcategory",
    "query_sales_volume_by_subcategory",
    "query_sales_by_brand_in_category",
    "query_sales_by_brand_in_all_categories",
    "query_sales_by_brand_in_subcategory",
    "query_sales_by_brand_in_all_subcategories",
    "query_sales_pct_by_brand_prev_year_categories",
    "query_sales_pct_by_brand_prev_year_subcategories",
    "query_sales_value_brand_vs_media",
    "query_sales_volume_brand_vs_media",
    "query_promo_share_brand_vs_media",
    "query_promo_share_by_category_brand_vs_media",
    "query_promo_share_by_subcategory_brand_vs_media",
    "query_incremental_yoy_vendite",
    "query_incremental_yoy_brand_vs_media",
    "query_promo_roi_brand_vs_media",
    "query_discount_depth_brand_vs_media",
    "query_discount_depth_single",
    "query_discount_depth_for_all_subcategories",
    "query_peak_events_brand_vs_media",
]
