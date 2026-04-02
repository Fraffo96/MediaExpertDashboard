"""URL loghi brand (GCS) e proxy same-origin."""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import httpx
from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from app.constants import DEFAULT_GCS_BRAND_LOGOS_BASE
from app.jinja_env import BASE_DIR

if TYPE_CHECKING:
    from app.auth.firestore_store import StoredUser

logger = logging.getLogger(__name__)

router = APIRouter()


def _env_flag(name: str) -> bool:
    v = (os.environ.get(name) or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def brand_logos_public_base() -> str:
    """Base URL loghi su GCS. Ordine: .env, poi bucket da PROJECT_ID, infine costante nel codice (mai vuoto)."""
    base = (os.environ.get("BRAND_LOGOS_PUBLIC_BASE") or "").strip().rstrip("/")
    if base:
        return base
    try:
        from app.db.client import PROJECT_ID

        pid = (PROJECT_ID or "").strip()
        if pid:
            return f"https://storage.googleapis.com/{pid}-brand-logos/brands"
    except Exception as ex:
        logger.warning("[brand_logo] base_url: lettura PROJECT_ID fallita: %s", ex)
    return DEFAULT_GCS_BRAND_LOGOS_BASE.rstrip("/")


def brand_logo_upstream_url(brand_id: int) -> str:
    """URL pubblico GCS per il PNG (usato dal proxy lato server)."""
    base = brand_logos_public_base()
    url = f"{base}/{int(brand_id)}.png"
    bust = (os.environ.get("BRAND_LOGOS_CACHE_VERSION") or "").strip()
    if bust:
        url = f"{url}?v={bust}"
    return url


def effective_brand_id_for_logo(user: Optional["StoredUser"]) -> Optional[int]:
    """brand_id per la topbar: profilo utente, oppure fallback per admin (spesso senza brand_id in Firestore)."""
    if not user:
        return None
    ub = getattr(user, "brand_id", None)
    if ub is not None:
        return int(ub)
    if getattr(user, "role", None) == "admin":
        raw = (os.environ.get("BRAND_LOGO_FALLBACK_BRAND_ID") or "1").strip()
        if raw.isdigit():
            n = int(raw)
            if n > 0:
                return n
        return None
    return None


def brand_logo_url_for_user(
    user: Optional["StoredUser"],
    *,
    _precomputed_effective_brand_id: Optional[int] = None,
) -> str | None:
    """URL pubblico del PNG per la topbar.

    **Default = come Cloud Run:** HTTPS verso GCS (``brand_logo_upstream_url``). Il browser carica il bucket
    direttamente; **non** servono file in ``static/`` né Firestore per i byte dell'immagine.

    Solo se ``BRAND_LOGOS_FORCE_SAME_ORIGIN_IMG=1``: path same-origin ``/brand-logo/<id>.png`` (proxy httpx).

    ``_precomputed_effective_brand_id``: se passato (es. da ``page_ctx``), evita doppio calcolo e doppi log.
    """
    if not user:
        logger.warning(
            "[brand_logo] TOPBAR_SENZA_LOGO | causa=nessun utente (cookie/sessione assenti o pagina pubblica) "
            "| AZIONE=effettua login"
        )
        return None
    bid = (
        _precomputed_effective_brand_id
        if _precomputed_effective_brand_id is not None
        else effective_brand_id_for_logo(user)
    )
    if bid is None:
        logger.warning(
            "[brand_logo] TOPBAR_SENZA_LOGO | user=%s id=%s role=%s brand_id_profilo=None "
            "| causa=profilo senza brand_id e nessun fallback (solo admin: BRAND_LOGO_FALLBACK_BRAND_ID, default 1; 0=disattiva) "
            "| AZIONE=imposta brand_id su Firestore/Admin oppure per admin controlla BRAND_LOGO_FALLBACK_BRAND_ID nel .env",
            getattr(user, "username", "?"),
            getattr(user, "id", "?"),
            getattr(user, "role", "?"),
        )
        return None
    if getattr(user, "brand_id", None) is None and getattr(user, "role", None) == "admin":
        logger.warning(
            "[brand_logo] TOPBAR_LOGO_FALLBACK_ADMIN | user=%s | uso brand_id=%s (profilo senza brand_id). "
            "AZIONE=imposta brand_id sull'utente admin per il logo corretto, o BRAND_LOGO_FALLBACK_BRAND_ID=N",
            getattr(user, "username", "?"),
            bid,
        )
    bust = (os.environ.get("BRAND_LOGOS_CACHE_VERSION") or "").strip()
    if brand_logos_force_same_origin_img():
        path = f"/brand-logo/{bid}.png"
        url = f"{path}?v={bust}" if bust else path
        logger.info(
            "[brand_logo] brand_logo_url=PROXY same-origin | %s | (BRAND_LOGOS_FORCE_SAME_ORIGIN_IMG)",
            path,
        )
        return url
    url = brand_logo_upstream_url(bid)
    logger.info(
        "[brand_logo] brand_logo_url=HTTPS_GCS | brand_id=%s | come produzione Cloud Run "
        "(loghi restano su GCS; Firestore tiene solo brand_id utente)",
        bid,
    )
    return url


def brand_logo_skip_proxy_enabled() -> bool:
    """True se l'URL del logo non usa /brand-logo/ (default: sempre True perché URL = GCS)."""
    return not brand_logos_force_same_origin_img()


def brand_logos_force_same_origin_img() -> bool:
    """Se True, l'attributo src dell'img topbar usa solo /brand-logo/ (proxy), non HTTPS GCS."""
    return _env_flag("BRAND_LOGOS_FORCE_SAME_ORIGIN_IMG")


def local_brand_logo_path(brand_id: int) -> Path:
    """File PNG opzionale in repo per fallback dev se GCS non risponde."""
    return BASE_DIR / "static" / "img" / "brands" / f"{int(brand_id)}.png"


def _png_response(content: bytes, *, content_type: str, cache_control: str) -> Response:
    return Response(
        content=content,
        media_type=content_type,
        headers={"Cache-Control": cache_control},
    )


@router.get("/brand-logo/{brand_id}.png", include_in_schema=False)
async def brand_logo_png_proxy(brand_id: int):
    """Scarica il logo da GCS lato server e lo serve come same-origin (evita blocchi browser su storage.googleapis.com).

    Se GCS non è raggiungibile (rete) o risponde errore, e esiste ``static/img/brands/<id>.png``,
    serve quel file (TTL corto) così in locale non resta solo 502/404.
    """
    bid = int(brand_id)
    upstream = brand_logo_upstream_url(bid)
    local_path = local_brand_logo_path(bid)
    has_proxy_env = bool(
        (os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY") or "").strip()
    )
    logger.info(
        "[brand_logo] proxy IN | brand_id=%s | upstream=%s | httpx trust_env=True (usa HTTP_PROXY se impostato) | proxy_env=%s",
        bid,
        upstream,
        has_proxy_env,
    )

    r: httpx.Response | None = None
    req_err: httpx.RequestError | None = None
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True, trust_env=True) as client:
            r = await client.get(upstream, headers={"Accept": "image/*,*/*;q=0.8"})
    except httpx.RequestError as e:
        req_err = e

    if req_err is not None:
        if local_path.is_file():
            data = local_path.read_bytes()
            logger.warning(
                "[brand_logo] proxy OK fonte=STATICO_LOCALE | CAUSA=GCS irraggiungibile da Python | "
                "brand_id=%s bytes=%s path=%s | eccezione=%s",
                bid,
                len(data),
                local_path,
                req_err,
            )
            return _png_response(
                data,
                content_type="image/png",
                cache_control="public, max-age=120",
            )
        logger.error(
            "[brand_logo] proxy FALLITO fonte=nessuna | CAUSA=rete/httpx verso GCS | "
            "HTTP al client=502 | brand_id=%s upstream=%s err=%s | "
            "AZIONE=BRAND_LOGOS_SKIP_PROXY=1 nel .env (browser carica GCS diretto) oppure HTTP_PROXY/HTTPS_PROXY; "
            "oppure aggiungi PNG in %s",
            bid,
            upstream,
            req_err,
            local_path,
        )
        raise HTTPException(status_code=502, detail="Logo upstream unreachable") from req_err

    assert r is not None
    if r.status_code == 200:
        ct = (r.headers.get("content-type") or "image/png").split(";")[0].strip()
        if "image" not in ct.lower():
            ct = "image/png"
        logger.info(
            "[brand_logo] proxy OK fonte=GCS | brand_id=%s http=200 bytes=%s content_type=%s",
            bid,
            len(r.content),
            ct,
        )
        return _png_response(
            r.content,
            content_type=ct,
            cache_control="public, max-age=300",
        )

    _preview = r.content[:80] if r.content else b""
    logger.warning(
        "[brand_logo] proxy GCS http=%s (non 200) | brand_id=%s upstream=%s body_preview=%r | "
        "CAUSA=su bucket manca l'oggetto, URL base errato, o oggetto non pubblico",
        r.status_code,
        bid,
        upstream,
        _preview,
    )
    if local_path.is_file():
        data = local_path.read_bytes()
        logger.warning(
            "[brand_logo] proxy OK fonte=STATICO_LOCALE | CAUSA=GCS http=%s | brand_id=%s bytes=%s path=%s",
            r.status_code,
            bid,
            len(data),
            local_path,
        )
        return _png_response(
            data,
            content_type="image/png",
            cache_control="public, max-age=120",
        )
    logger.error(
        "[brand_logo] proxy FALLITO fonte=nessuna | HTTP al client=404 | brand_id=%s | "
        "CAUSA=GCS non ha %s.png (verifica path) e file locale assente | AZIONE=carica su gs://.../brands/%s.png o aggiungi static",
        bid,
        bid,
        bid,
    )
    raise HTTPException(status_code=404, detail="Logo not found")
