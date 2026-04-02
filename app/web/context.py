"""Contesto Jinja, sessione utente, filtri BigQuery lazy."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi.responses import RedirectResponse

from app.auth.firestore_store import StoredUser, get_ecosystem_by_id, list_ecosystems
from app.auth.security import get_current_user
from app.constants import CLP_DATA_MAX_DATE, DP
from app.web.brand_logo import (
    brand_logo_upstream_url,
    brand_logo_url_for_user,
    brand_logos_force_same_origin_img,
    effective_brand_id_for_logo,
)

logger = logging.getLogger(__name__)

_GLOSSARY: dict = {}


def _svc():
    from app import services

    return services


def _bc():
    from app.services import brand_comparison

    return brand_comparison


def load_glossary() -> dict:
    global _GLOSSARY
    if not _GLOSSARY:
        from app.jinja_env import BASE_DIR

        p = BASE_DIR / "static" / "data" / "glossary.json"
        if p.exists():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    _GLOSSARY = json.load(f)
            except Exception as e:
                logger.warning("Glossary load failed: %s", e)
    return _GLOSSARY


def get_user(access_token: Optional[str] = None) -> Optional[StoredUser]:
    if not access_token:
        return None
    return get_current_user(access_token)


def user_ecosystems(user: Optional[StoredUser]) -> list[dict]:
    if not user:
        return []
    if user.is_admin:
        active = [e.to_dict() for e in list_ecosystems() if e.is_active]
        active.sort(key=lambda x: (x.get("name") or "").lower())
        return active
    out: list[dict] = []
    for eid in user.ecosystem_ids:
        e = get_ecosystem_by_id(eid)
        if e and e.is_active:
            out.append(e.to_dict())
    out.sort(key=lambda x: (x.get("name") or "").lower())
    return out


async def filters_payload():
    try:
        return await _svc().get_filters()
    except Exception as e:
        logger.warning("Filters load failed: %s", e)
        return {
            "categories": [],
            "subcategories": [],
            "segments": [],
            "brands": [],
            "promo_types": [],
            "promos": [],
            "genders": [],
            "available_years": [],
        }


def page_ctx(f: dict, user: Optional[StoredUser] = None) -> dict:
    effective_bid = effective_brand_id_for_logo(user)
    gcs_logo = brand_logo_upstream_url(effective_bid) if effective_bid is not None else ""
    logo_url = brand_logo_url_for_user(
        user, _precomputed_effective_brand_id=effective_bid
    )
    force_proxy_img = brand_logos_force_same_origin_img()
    # brand_logo_url è già HTTPS GCS di default (stesso schema Cloud Run), salvo FORCE_SAME_ORIGIN.
    brand_logo_img_src = logo_url or ""
    topbar_logo_use_gcs_direct = bool(logo_url and bool(gcs_logo) and not force_proxy_img)
    if user and not logo_url:
        logger.warning(
            "[brand_logo] Contesto pagina: topbar senza immagine logo | user=%s role=%s | "
            "Vedi log [brand_logo] TOPBAR_SENZA_LOGO sopra per causa e azione.",
            getattr(user, "username", "?"),
            getattr(user, "role", "?"),
        )
    return {
        **f,
        "period_start": DP[0],
        "period_end": DP[1],
        "glossary": load_glossary(),
        "user": user,
        "user_ecosystems": user_ecosystems(user),
        "allowed_filters": user.filter_list if user else [],
        "allowed_tabs": user.tab_list if user else ["basic"],
        "brand_logo_effective_brand_id": effective_bid,
        "brand_logo_url": logo_url,
        "brand_logo_gcs_url": gcs_logo,
        "brand_logo_img_src": brand_logo_img_src,
        "brand_logo_topbar_use_gcs_direct": topbar_logo_use_gcs_direct,
    }


def brand_name_for_user(user: Optional[StoredUser], f: dict) -> str:
    """Resolve brand name for greeting on landing page."""
    if not user or not user.brand_id:
        return "your brand"
    for b in f.get("brands") or []:
        if b.get("brand_id") == user.brand_id:
            return b.get("brand_name", "your brand")
    return "your brand"


def require_login(access_token: Optional[str]):
    user = get_user(access_token)
    if not user:
        return None, RedirectResponse("/login", status_code=302)
    return user, None


def check_tab(user: StoredUser, tab: str):
    """Return redirect if user can't access this tab."""
    if not user.can_access_tab(tab):
        tabs = user.tab_list
        first = tabs[0] if tabs else None
        url = (
            "/market-intelligence"
            if first == "market_intelligence"
            else "/brand-comparison"
            if first == "brand_comparison"
            else "/promo-creator"
            if first == "promo_creator"
            else "/check-live-promo"
            if first == "check_live_promo"
            else "/marketing/overview"
            if first == "marketing"
            else "/"
        )
        return RedirectResponse(url, status_code=302)
    return None


def check_live_anchor_end() -> str:
    """End date for presets: min(today, last date in demo/production feed)."""
    from datetime import date

    max_d = date.fromisoformat(CLP_DATA_MAX_DATE)
    today = date.today()
    return min(today, max_d).isoformat()


def default_check_live_dates():
    """Default: last 7d inclusive ending at anchor."""
    end = datetime.strptime(check_live_anchor_end(), "%Y-%m-%d").date()
    start = end - timedelta(days=6)
    return start.isoformat(), end.isoformat()


def require_mi_user(access_token: Optional[str]):
    """Return (user, error_response). If error_response is not None, return it."""
    from fastapi.responses import JSONResponse

    user = get_user(access_token)
    if not user:
        return None, JSONResponse({"error": "Not authenticated"}, status_code=401)
    if not user.brand_id:
        return None, JSONResponse({"error": "Brand required for Market Intelligence"}, status_code=400)
    return user, None
