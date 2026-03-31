"""Scope categorie/sottocategorie per brand: utenti admin, dropdown dashboard, API."""
from __future__ import annotations

import logging
from typing import Optional

from app.auth.firestore_store import StoredUser
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


def brand_category_scope_ids(user: StoredUser) -> tuple[list[int], list[int]]:
    """Parent e subcategory ID per il brand utente, rispettando allowed_* da Firestore."""
    if not user.brand_id:
        return [], []
    parent_ids, sub_ids = full_scope_for_brand(int(user.brand_id))
    if user.category_ids_list:
        allow = set(user.category_ids_list)
        parent_ids = [p for p in parent_ids if p in allow]
    if user.subcategory_ids_list:
        allow_s = set(user.subcategory_ids_list)
        sub_ids = [s for s in sub_ids if s in allow_s]
    return parent_ids, sub_ids


def scoped_category_dropdowns(user: Optional[StoredUser], f: dict) -> tuple[list, list]:
    """Categorie (level=1) e subcategorie da dim_category filtrate al brand utente."""
    cats_src = f.get("categories") or []
    subs_src = f.get("subcategories") or []
    if not user or not user.brand_id:
        cats = [c for c in cats_src if c.get("level") == 1] or list(ADMIN_CATEGORIES)
        subs = subs_src or list(ADMIN_SUBCATEGORIES)
        return cats, subs
    parent_ids, sub_ids = brand_category_scope_ids(user)
    pset, sset = set(parent_ids), set(sub_ids)
    cats = [
        c
        for c in cats_src
        if c.get("level") == 1 and int(c.get("category_id", -1)) in pset
    ]
    subs = [s for s in subs_src if int(s.get("category_id", -1)) in sset]
    if not cats:
        cats = [c for c in ADMIN_CATEGORIES if int(c["category_id"]) in pset]
    if not subs:
        subs = [s for s in ADMIN_SUBCATEGORIES if int(s["category_id"]) in sset]
    return cats, subs


def scoped_brands_dropdown(
    user: Optional[StoredUser], f: dict, restrict_to_brand_ids: Optional[list[int]] = None
) -> list:
    """Dropdown brand: intersezione con ecosystem (se presente); utente non-admin solo il proprio brand."""
    brands = list(f.get("brands") or [])
    if restrict_to_brand_ids is not None:
        bset = {int(x) for x in restrict_to_brand_ids}
        brands = [
            b
            for b in brands
            if b.get("brand_id") is not None and int(b["brand_id"]) in bset
        ]
    if user and user.brand_id and not user.is_admin:
        bid = int(user.brand_id)
        brands = [
            b
            for b in brands
            if b.get("brand_id") is not None and int(b["brand_id"]) == bid
        ]
    return brands
