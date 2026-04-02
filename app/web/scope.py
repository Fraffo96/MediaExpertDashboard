"""Validazione parametri categoria/sottocategoria/brand rispetto allo scope utente."""
from __future__ import annotations

from fastapi.responses import JSONResponse

from app.auth.brand_scope import brand_category_scope_ids
from app.auth.firestore_store import StoredUser


def reject_if_category_out_of_scope(user: StoredUser, category_id: str | None):
    if not category_id or not str(category_id).strip():
        return None
    try:
        cid = int(category_id)
    except ValueError:
        return JSONResponse({"error": "Invalid category_id"}, status_code=400)
    parents, subs = brand_category_scope_ids(user)
    if cid in set(parents) or cid in set(subs):
        return None
    return JSONResponse({"error": "Category not in your brand scope"}, status_code=400)


def reject_if_parent_category_out_of_scope(user: StoredUser, category_id: str | None):
    """Solo macro-categorie (parent), es. filtro needstates o subcategory_category_id MI."""
    if not category_id or not str(category_id).strip():
        return None
    try:
        cid = int(category_id)
    except ValueError:
        return JSONResponse({"error": "Invalid category_id"}, status_code=400)
    parents, _ = brand_category_scope_ids(user)
    if cid in set(parents):
        return None
    return JSONResponse({"error": "Category not in your brand scope"}, status_code=400)


def reject_if_cat_sub_out_of_scope(
    user: StoredUser,
    category_id: str | None,
    subcategory_id: str | None,
) -> JSONResponse | None:
    parents, subs = brand_category_scope_ids(user)
    pset, sset = set(parents), set(subs)
    if category_id and str(category_id).strip():
        try:
            cid = int(category_id)
        except ValueError:
            return JSONResponse({"error": "Invalid category_id"}, status_code=400)
        if cid not in pset:
            return JSONResponse({"error": "Category not in your brand scope"}, status_code=400)
    if subcategory_id and str(subcategory_id).strip():
        try:
            sid = int(subcategory_id)
        except ValueError:
            return JSONResponse({"error": "Invalid subcategory_id"}, status_code=400)
        if sid not in sset:
            return JSONResponse({"error": "Subcategory not in your brand scope"}, status_code=400)
    return None


def reject_if_cat_sub_id_lists_out_of_scope(
    user: StoredUser, cat_ids_csv: str, sub_ids_csv: str
) -> JSONResponse | None:
    cat_list = [x.strip() for x in cat_ids_csv.split(",") if x.strip()] if cat_ids_csv else []
    sub_list = [x.strip() for x in sub_ids_csv.split(",") if x.strip()] if sub_ids_csv else []
    parents, subs = brand_category_scope_ids(user)
    pset, sset = set(parents), set(subs)
    for x in cat_list:
        try:
            cid = int(x)
        except ValueError:
            return JSONResponse({"error": "Invalid category id in cat_ids"}, status_code=400)
        if cid not in pset:
            return JSONResponse({"error": "Category not in your brand scope"}, status_code=400)
    for x in sub_list:
        try:
            sid = int(x)
        except ValueError:
            return JSONResponse({"error": "Invalid subcategory id in sub_ids"}, status_code=400)
        if sid not in sset:
            return JSONResponse({"error": "Subcategory not in your brand scope"}, status_code=400)
    return None


def reject_if_brand_param_not_allowed(user: StoredUser, brand_id: str | None) -> JSONResponse | None:
    if not brand_id or not str(brand_id).strip():
        return None
    try:
        bid = int(brand_id)
    except ValueError:
        return JSONResponse({"error": "Invalid brand_id"}, status_code=400)
    if user.is_admin:
        return None
    if not user.brand_id or bid != int(user.brand_id):
        return JSONResponse({"error": "Brand not allowed"}, status_code=403)
    return None
