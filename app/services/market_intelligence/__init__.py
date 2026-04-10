"""Market Intelligence: brand vs media (precalc + live). Package modulare."""
from ._config import PRECALC_ONLY_ERR
from .all_years import get_mi_all_years
from .base import get_mi_available_years_payload, get_mi_base
from .batch import get_mi_all
from .extra import get_mi_segment_by_sku, get_mi_top_products
from .incremental import get_mi_incremental_yoy, get_mi_incremental_yoy_api
from .sales import _use_precalc_sales, get_mi_sales
from .discount import get_mi_discount
from .peak import get_mi_peak
from .promo import get_mi_promo

__all__ = [
    "PRECALC_ONLY_ERR",
    "_use_precalc_sales",
    "get_mi_all",
    "get_mi_all_years",
    "get_mi_available_years_payload",
    "get_mi_base",
    "get_mi_discount",
    "get_mi_incremental_yoy",
    "get_mi_incremental_yoy_api",
    "get_mi_peak",
    "get_mi_promo",
    "get_mi_sales",
    "get_mi_segment_by_sku",
    "get_mi_top_products",
]
