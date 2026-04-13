"""
Query per dashboard BASIC – package con re-export pubblico.

Supporta parent_category_id per raggruppamento top-level
e drill-down a subcategoria quando si filtra una parent category.
"""
from .buyers import (
    query_buyer_demographics,
    query_buyer_demographics_by_channel,
    query_buyer_segments,
    query_buyer_segments_by_channel,
    query_channel_by_segment,
    query_channel_mix,
    query_loyalty_breakdown,
    query_loyalty_breakdown_by_channel,
    query_repeat_rate,
    query_repeat_rate_by_channel,
)
from .incremental import query_incremental_yoy, query_incremental_yoy_by_promo
from .kpi_and_category import (
    query_discount_depth_detail,
    query_kpi,
    query_promo_share,
    query_promo_share_by_category,
    query_promo_share_by_subcategory,
    query_promo_share_detail,
    query_sales_by_category,
    query_sales_by_subcategory,
)
from .products import (
    query_products_any_token_match,
    query_products_by_category,
    query_top_products,
    query_underperforming_products,
)
from .promo_roi import (
    query_discount_depth_by_category,
    query_promo_roi_by_brand,
    query_promo_roi_by_category,
    query_promo_roi_by_type,
    query_promo_roi_detail,
)
from .sales_breakdown import (
    query_sales_brand_category_crosstab,
    query_sales_by_brand,
    query_sales_by_brand_detail,
    query_sales_by_category_by_gender,
    query_sales_by_category_by_segment,
    query_sales_detail,
)
from .yoy_peak import query_peak_events, query_peak_events_detail, query_yoy, query_yoy_detail

__all__ = [
    "query_buyer_demographics",
    "query_buyer_demographics_by_channel",
    "query_buyer_segments",
    "query_buyer_segments_by_channel",
    "query_channel_by_segment",
    "query_channel_mix",
    "query_discount_depth_by_category",
    "query_discount_depth_detail",
    "query_incremental_yoy",
    "query_incremental_yoy_by_promo",
    "query_kpi",
    "query_loyalty_breakdown",
    "query_loyalty_breakdown_by_channel",
    "query_peak_events",
    "query_peak_events_detail",
    "query_products_by_category",
    "query_promo_roi_by_brand",
    "query_promo_roi_by_category",
    "query_promo_roi_by_type",
    "query_promo_roi_detail",
    "query_promo_share",
    "query_promo_share_by_category",
    "query_promo_share_by_subcategory",
    "query_promo_share_detail",
    "query_repeat_rate",
    "query_repeat_rate_by_channel",
    "query_sales_brand_category_crosstab",
    "query_sales_by_brand",
    "query_sales_by_brand_detail",
    "query_sales_by_category",
    "query_sales_by_category_by_gender",
    "query_sales_by_category_by_segment",
    "query_sales_by_subcategory",
    "query_sales_detail",
    "query_products_any_token_match",
    "query_top_products",
    "query_underperforming_products",
    "query_yoy",
    "query_yoy_detail",
]
