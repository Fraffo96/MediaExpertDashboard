"""Query BigQuery per tutte le dashboard."""
from .shared import query_categories, query_segments, query_brands, query_promo_types, query_promos
from .basic import (
    query_kpi, query_sales_by_category, query_promo_share,
    query_promo_share_by_category, query_yoy, query_incremental_yoy,
    query_peak_events, query_promo_roi_by_type,
    query_promo_roi_by_category, query_promo_roi_by_brand,
    query_discount_depth_by_category, query_sales_detail,
    query_sales_by_brand, query_sales_brand_category_crosstab,
    query_sales_by_category_by_segment, query_sales_by_category_by_gender,
)
from .promo import query_promo_kpi, query_performance_by_type, query_uplift_by_category, query_promo_timeline, query_promo_ranking
from .customer import (
    query_segment_overview, query_seasonality, query_spend_distribution,
    query_channel_mix, query_loyalty_penetration, query_repeat_rate,
)
from .simulation import query_historical_baseline, query_uplift_by_promo_type, query_segment_response
from .why_buy import query_category_by_segment, query_category_growth, query_segment_radar
from . import market_intelligence, brand_comparison, promo_creator
