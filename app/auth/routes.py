"""Auth routes: login/logout + admin CRUD for users, ecosystems, permissions."""
import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, Form, Request, Response, Cookie
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from sqlalchemy.orm import Session

from .brand_scope import full_scope_for_brand
from .database import SessionLocal
from .models import ALL_ACCESS_TYPES, User, Ecosystem, EcosystemCategory, EcosystemBrand, UserEcosystem
from .security import verify_password, hash_password, create_access_token, require_user, require_admin, get_current_user

logger = logging.getLogger(__name__)

templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))

router = APIRouter()

# ---------------------------------------------------------------------------
# Login / Logout
# ---------------------------------------------------------------------------

@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, access_token: Optional[str] = Cookie(None)):
    db = SessionLocal()
    try:
        user = get_current_user(db, access_token)
        if user:
            return RedirectResponse("/", status_code=302)
    finally:
        db.close()
    return templates.TemplateResponse(request, "login.html", {"error": None})


@router.post("/login")
async def login_submit(
    request: Request,
    response: Response,
    username: str = Form(...),
    password: str = Form(...),
):
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.username == username, User.is_active == True).first()
        if not user or not verify_password(password, user.hashed_password):
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
    finally:
        db.close()


@router.get("/logout")
async def logout():
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("access_token")
    return resp


# ---------------------------------------------------------------------------
# Cambio password (utente corrente)
# ---------------------------------------------------------------------------

@router.post("/api/me/password")
async def change_own_password(request: Request, access_token: Optional[str] = Cookie(None)):
    db = SessionLocal()
    try:
        user = get_current_user(db, access_token)
        if not user:
            return JSONResponse({"error": "Not authenticated"}, status_code=401)
        body = await request.json()
        current_pw = body.get("current_password", "")
        new_pw = body.get("new_password", "")
        if not verify_password(current_pw, user.hashed_password):
            return JSONResponse({"error": "Password attuale errata"}, status_code=400)
        if not new_pw:
            return JSONResponse({"error": "Nuova password richiesta"}, status_code=400)
        if new_pw != body.get("confirm_password", ""):
            return JSONResponse({"error": "Le password non coincidono"}, status_code=400)
        user.hashed_password = hash_password(new_pw)
        db.commit()
        return {"ok": True}
    except Exception as e:
        db.rollback()
        logger.error("change_own_password: %s", e)
        return JSONResponse({"error": str(e)}, status_code=400)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Admin API — Users
# ---------------------------------------------------------------------------

@router.get("/api/admin/users")
async def list_users(access_token: Optional[str] = Cookie(None)):
    db = SessionLocal()
    try:
        require_admin(db, access_token)
        users = db.query(User).filter(User.role != "admin").order_by(User.id).all()
        return [u.to_dict() for u in users]
    finally:
        db.close()


@router.get("/api/admin/users/{user_id}")
async def get_user(user_id: int, access_token: Optional[str] = Cookie(None)):
    db = SessionLocal()
    try:
        require_admin(db, access_token)
        u = db.query(User).filter(User.id == user_id, User.role != "admin").first()
        if not u:
            return JSONResponse({"error": "User not found"}, status_code=404)
        return u.to_dict()
    finally:
        db.close()


@router.post("/api/admin/users")
async def create_user(request: Request, access_token: Optional[str] = Cookie(None)):
    db = SessionLocal()
    try:
        require_admin(db, access_token)
        body = await request.json()
        if db.query(User).filter(User.username == body["username"]).first():
            return JSONResponse({"error": "Username already exists"}, status_code=409)
        brand_id = body.get("brand_id")
        if brand_id is None or brand_id == "":
            return JSONResponse({"error": "Brand is required"}, status_code=400)
        brand_id = int(brand_id)
        access_types = body.get("access_types") or []
        if not access_types:
            access_types = list(ALL_ACCESS_TYPES)
        cat_ids, sub_ids = full_scope_for_brand(brand_id)
        u = User(
            username=body["username"],
            hashed_password=hash_password(body["password"]),
            display_name=body.get("display_name", ""),
            role="user",
            is_active=body.get("is_active", True),
            brand_id=brand_id,
            access_types=json.dumps(access_types),
            allowed_category_ids=json.dumps(cat_ids),
            allowed_subcategory_ids=json.dumps(sub_ids),
            allowed_filters=json.dumps(body.get("allowed_filters", [])),
            allowed_tabs=json.dumps(body.get("allowed_tabs", ["basic"])),
        )
        db.add(u)
        db.commit()
        db.refresh(u)
        return u.to_dict()
    except Exception as e:
        db.rollback()
        logger.error("create_user: %s", e)
        return JSONResponse({"error": str(e)}, status_code=400)
    finally:
        db.close()


@router.put("/api/admin/users/{user_id}")
async def update_user(user_id: int, request: Request, access_token: Optional[str] = Cookie(None)):
    db = SessionLocal()
    try:
        require_admin(db, access_token)
        u = db.query(User).get(user_id)
        if not u:
            return JSONResponse({"error": "User not found"}, status_code=404)
        body = await request.json()
        if "display_name" in body:
            u.display_name = body["display_name"]
        if "is_active" in body:
            u.is_active = body["is_active"]
        if "password" in body and body["password"]:
            u.hashed_password = hash_password(body["password"])
        if "brand_id" in body:
            u.brand_id = int(body["brand_id"]) if body["brand_id"] else None
            if u.brand_id:
                c, s = full_scope_for_brand(u.brand_id)
                u.allowed_category_ids = json.dumps(c)
                u.allowed_subcategory_ids = json.dumps(s)
            else:
                u.allowed_category_ids = "[]"
                u.allowed_subcategory_ids = "[]"
        if "access_types" in body:
            at = body["access_types"] or []
            if not at:
                at = list(ALL_ACCESS_TYPES)
            u.access_types = json.dumps(at)
        if "allowed_filters" in body:
            u.allowed_filters = json.dumps(body["allowed_filters"])
        if "allowed_tabs" in body:
            u.allowed_tabs = json.dumps(body["allowed_tabs"])
        db.commit()
        db.refresh(u)
        return u.to_dict()
    except Exception as e:
        db.rollback()
        logger.error("update_user: %s", e)
        return JSONResponse({"error": str(e)}, status_code=400)
    finally:
        db.close()


@router.delete("/api/admin/users/{user_id}")
async def delete_user(user_id: int, access_token: Optional[str] = Cookie(None)):
    db = SessionLocal()
    try:
        admin = require_admin(db, access_token)
        u = db.query(User).get(user_id)
        if not u:
            return JSONResponse({"error": "User not found"}, status_code=404)
        if u.id == admin.id:
            return JSONResponse({"error": "Cannot delete yourself"}, status_code=400)
        if u.role == "admin":
            return JSONResponse({"error": "Cannot delete admin users"}, status_code=400)
        db.delete(u)
        db.commit()
        return {"ok": True}
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Admin API — Ecosystems
# ---------------------------------------------------------------------------

@router.get("/api/admin/ecosystems")
async def list_ecosystems(access_token: Optional[str] = Cookie(None)):
    db = SessionLocal()
    try:
        require_admin(db, access_token)
        ecos = db.query(Ecosystem).order_by(Ecosystem.id).all()
        return [e.to_dict() for e in ecos]
    finally:
        db.close()


@router.post("/api/admin/ecosystems")
async def create_ecosystem(request: Request, access_token: Optional[str] = Cookie(None)):
    db = SessionLocal()
    try:
        require_admin(db, access_token)
        body = await request.json()
        eco = Ecosystem(
            name=body["name"],
            description=body.get("description", ""),
            icon=body.get("icon", ""),
            is_active=body.get("is_active", True),
        )
        db.add(eco)
        db.flush()
        for cid in body.get("category_ids", []):
            db.add(EcosystemCategory(ecosystem_id=eco.id, category_id=cid))
        for bid in body.get("brand_ids", []):
            db.add(EcosystemBrand(ecosystem_id=eco.id, brand_id=bid))
        db.commit()
        db.refresh(eco)
        return eco.to_dict()
    except Exception as e:
        db.rollback()
        logger.error("create_ecosystem: %s", e)
        return JSONResponse({"error": str(e)}, status_code=400)
    finally:
        db.close()


@router.put("/api/admin/ecosystems/{eco_id}")
async def update_ecosystem(eco_id: int, request: Request, access_token: Optional[str] = Cookie(None)):
    db = SessionLocal()
    try:
        require_admin(db, access_token)
        eco = db.query(Ecosystem).get(eco_id)
        if not eco:
            return JSONResponse({"error": "Ecosystem not found"}, status_code=404)
        body = await request.json()
        if "name" in body:
            eco.name = body["name"]
        if "description" in body:
            eco.description = body["description"]
        if "icon" in body:
            eco.icon = body["icon"]
        if "is_active" in body:
            eco.is_active = body["is_active"]
        if "category_ids" in body:
            db.query(EcosystemCategory).filter(EcosystemCategory.ecosystem_id == eco.id).delete()
            for cid in body["category_ids"]:
                db.add(EcosystemCategory(ecosystem_id=eco.id, category_id=cid))
        if "brand_ids" in body:
            db.query(EcosystemBrand).filter(EcosystemBrand.ecosystem_id == eco.id).delete()
            for bid in body["brand_ids"]:
                db.add(EcosystemBrand(ecosystem_id=eco.id, brand_id=bid))
        db.commit()
        db.refresh(eco)
        return eco.to_dict()
    except Exception as e:
        db.rollback()
        logger.error("update_ecosystem: %s", e)
        return JSONResponse({"error": str(e)}, status_code=400)
    finally:
        db.close()


@router.delete("/api/admin/ecosystems/{eco_id}")
async def delete_ecosystem(eco_id: int, access_token: Optional[str] = Cookie(None)):
    db = SessionLocal()
    try:
        require_admin(db, access_token)
        eco = db.query(Ecosystem).get(eco_id)
        if not eco:
            return JSONResponse({"error": "Ecosystem not found"}, status_code=404)
        db.delete(eco)
        db.commit()
        return {"ok": True}
    finally:
        db.close()
