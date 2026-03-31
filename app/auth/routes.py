"""Auth routes: login/logout + admin CRUD for users, ecosystems, permissions."""
import json
import logging
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Form, Request, Response, Cookie
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from .brand_scope import full_scope_for_brand
from .firestore_store import (
    create_ecosystem_record,
    create_user_record,
    delete_ecosystem_record,
    delete_user_record,
    get_ecosystem_by_id,
    get_user_by_id,
    get_user_by_username,
    list_ecosystems,
    list_users_non_admin,
    update_ecosystem_record,
    update_user_record,
)
from .models import ALL_ACCESS_TYPES
from .security import verify_password, hash_password, create_access_token, require_admin, get_current_user

logger = logging.getLogger(__name__)


async def _prewarm_cache_safe():
    """Dopo CRUD utente/brand: ricalcola cache Redis per tutti i brand con utenti attivi."""
    try:
        from app.services.prewarm import prewarm_cache

        await prewarm_cache()
    except Exception as e:
        logger.warning("Prewarm dopo modifica utente: %s", e)

templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))

router = APIRouter()


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, access_token: Optional[str] = Cookie(None)):
    user = get_current_user(access_token)
    if user:
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse(request, "login.html", {"error": None})


@router.post("/login")
async def login_submit(
    request: Request,
    response: Response,
    username: str = Form(...),
    password: str = Form(...),
):
    user = get_user_by_username(username)
    if not user or not user.is_active or not verify_password(password, user.hashed_password):
        return templates.TemplateResponse(
            request,
            "login.html",
            {"error": "Invalid username or password."},
            status_code=401,
        )
    token = create_access_token({"sub": user.username, "role": user.role})
    resp = RedirectResponse("/admin" if user.role == "admin" else "/", status_code=302)
    resp.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        samesite="lax",
        max_age=60 * 480,
    )
    return resp


@router.get("/logout")
async def logout():
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("access_token")
    return resp


@router.post("/api/me/password")
async def change_own_password(request: Request, access_token: Optional[str] = Cookie(None)):
    user = get_current_user(access_token)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    try:
        body = await request.json()
        current_pw = body.get("current_password", "")
        new_pw = body.get("new_password", "")
        if not verify_password(current_pw, user.hashed_password):
            return JSONResponse({"error": "Password attuale errata"}, status_code=400)
        if not new_pw:
            return JSONResponse({"error": "Nuova password richiesta"}, status_code=400)
        if new_pw != body.get("confirm_password", ""):
            return JSONResponse({"error": "Le password non coincidono"}, status_code=400)
        u = update_user_record(user.id, {"hashed_password": hash_password(new_pw)})
        if not u:
            return JSONResponse({"error": "Aggiornamento fallito"}, status_code=400)
        return {"ok": True}
    except Exception as e:
        logger.error("change_own_password: %s", e)
        return JSONResponse({"error": str(e)}, status_code=400)


@router.get("/api/admin/users")
async def list_users(access_token: Optional[str] = Cookie(None)):
    require_admin(access_token)
    return [u.to_dict() for u in list_users_non_admin()]


@router.get("/api/admin/users/{user_id}")
async def get_user(user_id: int, access_token: Optional[str] = Cookie(None)):
    require_admin(access_token)
    u = get_user_by_id(user_id)
    if not u or u.role == "admin":
        return JSONResponse({"error": "User not found"}, status_code=404)
    return u.to_dict()


@router.post("/api/admin/users")
async def create_user(
    request: Request,
    background_tasks: BackgroundTasks,
    access_token: Optional[str] = Cookie(None),
):
    require_admin(access_token)
    try:
        body = await request.json()
        if get_user_by_username(body["username"]):
            return JSONResponse({"error": "Username already exists"}, status_code=409)
        brand_id = body.get("brand_id")
        if brand_id is None or brand_id == "":
            return JSONResponse({"error": "Brand is required"}, status_code=400)
        brand_id = int(brand_id)
        access_types = body.get("access_types") or []
        if not access_types:
            access_types = list(ALL_ACCESS_TYPES)
        cat_ids, sub_ids = full_scope_for_brand(brand_id)
        u = create_user_record(
            username=body["username"],
            hashed_password=hash_password(body["password"]),
            display_name=body.get("display_name", ""),
            role="user",
            is_active=body.get("is_active", True),
            brand_id=brand_id,
            access_types=access_types,
            allowed_category_ids=cat_ids,
            allowed_subcategory_ids=sub_ids,
            allowed_filters=body.get("allowed_filters", []),
            allowed_tabs=body.get("allowed_tabs", ["basic"]),
        )
        if u.is_active and u.brand_id:
            background_tasks.add_task(_prewarm_cache_safe)
        return u.to_dict()
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=409)
    except Exception as e:
        logger.error("create_user: %s", e)
        return JSONResponse({"error": str(e)}, status_code=400)


@router.put("/api/admin/users/{user_id}")
async def update_user(
    user_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    access_token: Optional[str] = Cookie(None),
):
    require_admin(access_token)
    u = get_user_by_id(user_id)
    if not u:
        return JSONResponse({"error": "User not found"}, status_code=404)
    body = await request.json()
    updates: dict = {}
    if "display_name" in body:
        updates["display_name"] = body["display_name"]
    if "is_active" in body:
        updates["is_active"] = body["is_active"]
    if "password" in body and body["password"]:
        updates["hashed_password"] = hash_password(body["password"])
    if "brand_id" in body:
        bid = int(body["brand_id"]) if body["brand_id"] else None
        updates["brand_id"] = bid
        if bid:
            c, s = full_scope_for_brand(bid)
            updates["allowed_category_ids"] = c
            updates["allowed_subcategory_ids"] = s
        else:
            updates["allowed_category_ids"] = []
            updates["allowed_subcategory_ids"] = []
    if "access_types" in body:
        at = body["access_types"] or []
        if not at:
            at = list(ALL_ACCESS_TYPES)
        updates["access_types"] = at
    if "allowed_filters" in body:
        updates["allowed_filters"] = body["allowed_filters"]
    if "allowed_tabs" in body:
        updates["allowed_tabs"] = body["allowed_tabs"]
    out = update_user_record(user_id, updates)
    if not out:
        return JSONResponse({"error": "User not found"}, status_code=404)
    if ("brand_id" in body or "is_active" in body) and out.is_active and out.brand_id:
        background_tasks.add_task(_prewarm_cache_safe)
    return out.to_dict()


@router.delete("/api/admin/users/{user_id}")
async def delete_user(user_id: int, access_token: Optional[str] = Cookie(None)):
    admin = require_admin(access_token)
    u = get_user_by_id(user_id)
    if not u:
        return JSONResponse({"error": "User not found"}, status_code=404)
    if u.id == admin.id:
        return JSONResponse({"error": "Cannot delete yourself"}, status_code=400)
    if u.role == "admin":
        return JSONResponse({"error": "Cannot delete admin users"}, status_code=400)
    delete_user_record(user_id)
    return {"ok": True}


@router.get("/api/admin/ecosystems")
async def list_ecosystems_api(access_token: Optional[str] = Cookie(None)):
    require_admin(access_token)
    return [e.to_dict() for e in list_ecosystems()]


@router.post("/api/admin/ecosystems")
async def create_ecosystem(request: Request, access_token: Optional[str] = Cookie(None)):
    require_admin(access_token)
    try:
        body = await request.json()
        eco = create_ecosystem_record(
            name=body["name"],
            description=body.get("description", ""),
            icon=body.get("icon", ""),
            is_active=body.get("is_active", True),
            category_ids=body.get("category_ids", []),
            brand_ids=body.get("brand_ids", []),
        )
        return eco.to_dict()
    except Exception as e:
        logger.error("create_ecosystem: %s", e)
        return JSONResponse({"error": str(e)}, status_code=400)


@router.put("/api/admin/ecosystems/{eco_id}")
async def update_ecosystem(eco_id: int, request: Request, access_token: Optional[str] = Cookie(None)):
    require_admin(access_token)
    body = await request.json()
    updates = {}
    if "name" in body:
        updates["name"] = body["name"]
    if "description" in body:
        updates["description"] = body["description"]
    if "icon" in body:
        updates["icon"] = body["icon"]
    if "is_active" in body:
        updates["is_active"] = body["is_active"]
    if "category_ids" in body:
        updates["category_ids"] = body["category_ids"]
    if "brand_ids" in body:
        updates["brand_ids"] = body["brand_ids"]
    eco = update_ecosystem_record(eco_id, updates)
    if not eco:
        return JSONResponse({"error": "Ecosystem not found"}, status_code=404)
    return eco.to_dict()


@router.delete("/api/admin/ecosystems/{eco_id}")
async def delete_ecosystem(eco_id: int, access_token: Optional[str] = Cookie(None)):
    require_admin(access_token)
    if not get_ecosystem_by_id(eco_id):
        return JSONResponse({"error": "Ecosystem not found"}, status_code=404)
    delete_ecosystem_record(eco_id)
    return {"ok": True}
