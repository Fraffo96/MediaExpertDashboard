"""Scope categorie/sottocategorie per brand: usato in creazione/aggiornamento utenti."""
import logging

from app.constants import ADMIN_CATEGORIES, ADMIN_SUBCATEGORIES
from app.db.queries.shared import query_categories_by_brand, query_subcategories_by_brand

logger = logging.getLogger(__name__)


def full_scope_for_brand(brand_id: int) -> tuple[list[int], list[int]]:
    """Parent e subcategory dove il brand ha prodotti; fallback su costanti admin se BQ non disponibile."""
    try:
        cats = query_categories_by_brand(brand_id) or []
        if cats:
            subs = query_subcategories_by_brand(brand_id) or []
            parent_ids = [int(c["category_id"]) for c in cats]
            sub_ids = [int(s["category_id"]) for s in subs]
            return parent_ids, sub_ids
    except Exception as e:
        logger.warning("full_scope_for_brand BQ fallback: %s", e)
    parent_ids = [int(c["category_id"]) for c in ADMIN_CATEGORIES]
    sub_ids = [int(s["category_id"]) for s in ADMIN_SUBCATEGORIES]
    return parent_ids, sub_ids
