"""Market Intelligence: batch get_mi_all."""
import asyncio

from .base import get_mi_base
from .discount import get_mi_discount
from .peak import get_mi_peak
from .promo import get_mi_promo
from .sales import get_mi_sales


async def get_mi_all(ps, pe, brand_id, discount_cat=None, discount_subcat=None):
    """Batch: base + sales + promo + peak + discount in una sola chiamata. Riduce round-trip per caricamento iniziale."""
    if not brand_id or not str(brand_id).strip():
        return {"error": "Brand required"}
    base = await get_mi_base(ps, pe, brand_id)
    if base.get("error"):
        return base
    brand_cats = base.get("brand_categories") or []
    brand_subcats = base.get("brand_subcategories") or {}
    cat_ids = base.get("cat_ids") or []
    sub_ids = base.get("sub_ids") or []
    sub_cat_id = base.get("subcategory_category_id") or (cat_ids[0] if cat_ids else None)
    disc_cat = discount_cat or (str(brand_cats[0]["category_id"]) if brand_cats else None)
    disc_sub = discount_subcat

    sales, promo, peak, discount = await asyncio.gather(
        get_mi_sales(ps, pe, brand_id, cat_ids, sub_ids, sub_cat_id),
        get_mi_promo(ps, pe, brand_id, brand_cats, brand_subcats),
        get_mi_peak(ps, pe, brand_id, brand_cats, brand_subcats),
        get_mi_discount(ps, pe, brand_id, brand_cats, sub_ids, disc_cat, disc_sub),
    )
    out = {**base}
    if not sales.get("error"):
        out.update(sales)
    if not promo.get("error"):
        out.update(promo)
    if not peak.get("error"):
        out.update(peak)
    if not discount.get("error"):
        out.update(discount)
    return out
