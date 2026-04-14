#!/usr/bin/env python3
"""Svuota cache Redis + prewarm sul Cloud Run.

Percorsi (in ordine di priorità se non forzato):

1. **PREWARM_TOKEN** → ``POST /internal/clear-cache`` (header ``X-Prewarm-Token``).
2. **Login** → ``REMOTE_ADMIN_USERNAME`` + ``REMOTE_ADMIN_PASSWORD`` → ``/login`` → cookie.
3. **Mint JWT** (nessuna password): credenziali GCP (es. ``credentials/bigquery-sa.json``) leggono
   Firestore ``dashboard_users``, si sceglie un admin attivo, si firma un JWT con la stessa chiave
   dell'app (``JWT_SECRET_KEY`` o default in codice). Utile quando Cloud Run non imposta
   ``JWT_SECRET_KEY`` e il default coincide con ``app/auth/security.py``.

   **Attenzione:** se in produzione imposti ``JWT_SECRET_KEY`` su Cloud Run senza lo stesso valore
   in locale, il mint fallisce (comportamento voluto).

Opzionale ``--flush`` → body ``{"flush_redis_db": true}`` (serve ``ENABLE_ADMIN_REDIS_FLUSHDB=1``).

URL: ``DASHBOARD_BASE_URL`` / ``REMOTE_DASHBOARD_URL``, oppure ``gcloud run services describe dashboard``.

Esempi::

  python scripts/remote_admin_flush_cache.py --flush
  python scripts/remote_admin_flush_cache.py --prewarm-only
  python scripts/remote_admin_flush_cache.py --mint-only --mint-user expert --flush
  python scripts/remote_admin_flush_cache.py --use-login -u admin -p '...'
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent

DEFAULT_CLOUD_RUN_SERVICE = "dashboard"
DEFAULT_REGION = "europe-west1"
DEFAULT_PROJECT = "mediaexpertdashboard"
SA_CANDIDATES = (
    ROOT / "credentials" / "bigquery-sa.json",
    ROOT / "credentials" / "dashboard-sa.json",
)


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(ROOT / ".env")


def _ensure_project_and_sa() -> None:
    os.environ.setdefault("GCP_PROJECT_ID", DEFAULT_PROJECT)
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return
    for p in SA_CANDIDATES:
        if p.is_file():
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(p)
            return


def _resolve_base_url(explicit: str, project: str, region: str, service: str) -> str:
    for raw in (explicit, os.environ.get("REMOTE_DASHBOARD_URL"), os.environ.get("DASHBOARD_BASE_URL")):
        b = (raw or "").strip().rstrip("/")
        if b:
            return b
    try:
        r = subprocess.run(
            [
                "gcloud",
                "run",
                "services",
                "describe",
                service,
                f"--region={region}",
                f"--project={project}",
                "--format=value(status.url)",
            ],
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
    except FileNotFoundError:
        print("gcloud non trovato: imposta DASHBOARD_BASE_URL o REMOTE_DASHBOARD_URL", file=sys.stderr)
        return ""
    url = (r.stdout or "").strip().rstrip("/")
    if not url:
        print(
            "URL Cloud Run vuoto (gcloud). Imposta DASHBOARD_BASE_URL o controlla service/region.",
            file=sys.stderr,
        )
        if r.stderr:
            print(r.stderr.strip(), file=sys.stderr)
        return ""
    return url


def _find_mint_subject(mint_user: str) -> tuple[str, str]:
    """Ritorna (username, role) per il claim JWT."""
    from app.auth.firestore_store import COL_USERS, _user_from_doc, get_firestore_client

    db = get_firestore_client()
    admins: list[tuple[str, str, str]] = []
    for snap in db.collection(COL_USERS).stream():
        u = _user_from_doc(snap.id, snap.to_dict() or {})
        if u.is_admin and u.is_active:
            admins.append((u.username.lower(), u.username, u.role))
    if not admins:
        raise RuntimeError("Nessun utente admin attivo in Firestore (dashboard_users).")
    if mint_user.strip():
        want = mint_user.strip().lower()
        for _low, uname, role in admins:
            if _low == want:
                return (uname, role)
        raise RuntimeError(f"Admin attivo '{mint_user}' non trovato in dashboard_users.")
    admins.sort(key=lambda t: t[1])
    _low, uname, role = admins[0]
    return (uname, role)


def _mint_access_token(username: str, role: str) -> str:
    from app.auth.security import create_access_token

    return create_access_token({"sub": username, "role": role})


def _print_response(label: str, r: httpx.Response) -> dict:
    try:
        d = r.json()
    except Exception:
        d = {"raw": r.text[:800]}
    print(f"{label}:", r.status_code, d)
    return d if isinstance(d, dict) else {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Clear cache + prewarm su Cloud Run")
    parser.add_argument("--flush", action="store_true", help="Invia flush_redis_db (ENABLE_ADMIN_REDIS_FLUSHDB=1)")
    parser.add_argument("--no-prewarm", action="store_true")
    parser.add_argument(
        "--prewarm-only",
        action="store_true",
        help="Solo prewarm (nessun clear-cache). Utile dopo uno svuotamento già fatto.",
    )
    parser.add_argument("--base", default="", help="URL servizio (override)")
    parser.add_argument(
        "--use-login",
        action="store_true",
        help="Forza login form anche se PREWARM_TOKEN o mint sono disponibili",
    )
    parser.add_argument("--user", "-u", default="", help="REMOTE_ADMIN_USERNAME")
    parser.add_argument("--password", "-p", default="", help="REMOTE_ADMIN_PASSWORD")
    parser.add_argument("--prewarm-token", default="", help="PREWARM_TOKEN (internal API)")
    parser.add_argument(
        "--mint-only",
        action="store_true",
        help="Usa solo mint JWT (Firestore + SA), ignora PREWARM_TOKEN e login",
    )
    parser.add_argument(
        "--mint-user",
        default="",
        help="Username admin in Firestore per il mint (default: primo admin attivo per nome)",
    )
    parser.add_argument(
        "--no-mint-fallback",
        action="store_true",
        help="Non provare il mint se mancano password e PREWARM_TOKEN",
    )
    parser.add_argument("--project", default="", help="GCP project (default: GCP_PROJECT_ID o mediaexpertdashboard)")
    parser.add_argument("--region", default="", help=f"Cloud Run region (default: {DEFAULT_REGION})")
    parser.add_argument("--service", default="", help=f"Nome servizio Cloud Run (default: {DEFAULT_CLOUD_RUN_SERVICE})")
    parser.add_argument(
        "--prewarm-timeout",
        type=float,
        default=600.0,
        help="Timeout lettura secondi per POST /api/admin/prewarm (default 600)",
    )
    args = parser.parse_args()

    if args.prewarm_only and args.no_prewarm:
        print("Non usare insieme --prewarm-only e --no-prewarm.", file=sys.stderr)
        return 1

    _load_dotenv()
    project = (args.project or os.environ.get("GCP_PROJECT_ID") or DEFAULT_PROJECT).strip()
    os.environ["GCP_PROJECT_ID"] = project
    region = (args.region or os.environ.get("GCP_REGION") or DEFAULT_REGION).strip()
    service = (args.service or os.environ.get("CLOUD_RUN_SERVICE") or DEFAULT_CLOUD_RUN_SERVICE).strip()

    base = _resolve_base_url(args.base, project, region, service)
    if not base:
        return 1

    prewarm_token = (args.prewarm_token or os.environ.get("PREWARM_TOKEN") or "").strip()
    user = (args.user or os.environ.get("REMOTE_ADMIN_USERNAME") or "").strip()
    password = (args.password or os.environ.get("REMOTE_ADMIN_PASSWORD") or "").strip()
    body = {"flush_redis_db": True} if args.flush else {}

    if args.use_login and (not user or not password):
        print("--use-login richiede --user e --password (o REMOTE_* nel .env).", file=sys.stderr)
        return 1

    if args.mint_only:
        mode = "mint"
    elif args.use_login:
        mode = "login"
    elif prewarm_token:
        mode = "internal"
    elif user and password:
        mode = "login"
    elif not args.no_mint_fallback:
        mode = "mint"
    else:
        print(
            "Nessun metodo: imposta PREWARM_TOKEN, oppure REMOTE_ADMIN_USERNAME/PASSWORD, "
            "oppure rimuovi --no-mint-fallback e configura ADC/SA per Firestore.",
            file=sys.stderr,
        )
        return 1

    sys.path.insert(0, str(ROOT))
    t_clear = httpx.Timeout(120.0)
    t_prewarm = httpx.Timeout(30.0, read=max(30.0, float(args.prewarm_timeout)))

    with httpx.Client(follow_redirects=True, timeout=t_clear) as client:
        if mode == "internal":
            hdr = {"X-Prewarm-Token": prewarm_token, "Content-Type": "application/json"}
            if not args.prewarm_only:
                r1 = client.post(f"{base}/internal/clear-cache", json=body, headers=hdr)
                _print_response("internal/clear-cache", r1)
                if not r1.is_success:
                    return 1
                if args.no_prewarm:
                    return 0
            else:
                print("prewarm-only: skip internal/clear-cache")
            with httpx.Client(follow_redirects=True, timeout=t_prewarm) as pre_client:
                r2 = pre_client.get(
                    f"{base}/internal/prewarm",
                    headers={"X-Prewarm-Token": prewarm_token},
                )
            _print_response("internal/prewarm", r2)
            if r2.status_code == 504:
                print(
                    "prewarm: 504 — Cloud Run --timeout troppo basso; vedi AGENTS.md (cache / prewarm).",
                    file=sys.stderr,
                )
            return 0 if r2.is_success else 1

        if mode == "login":
            r0 = client.post(f"{base}/login", data={"username": user, "password": password})
            if r0.status_code != 200 or "/admin" not in str(r0.url):
                print(f"Login fallito: status={r0.status_code} url={r0.url}", file=sys.stderr)
                if r0.status_code == 200:
                    print(r0.text[:800], file=sys.stderr)
                return 1
            cookies = dict(client.cookies)
        else:
            _ensure_project_and_sa()
            try:
                uname, role = _find_mint_subject(args.mint_user)
                jwt_val = _mint_access_token(uname, role)
            except Exception as e:
                print(f"Mint JWT: {e}", file=sys.stderr)
                return 1
            print(f"mint-jwt: utente={uname!r} role={role!r}")
            cookies = {"access_token": jwt_val}

        if not args.prewarm_only:
            r1 = client.post(f"{base}/api/admin/clear-cache", json=body, cookies=cookies)
            _print_response("clear-cache", r1)
            if not r1.is_success:
                return 1
        else:
            print("prewarm-only: skip /api/admin/clear-cache")

        if args.no_prewarm:
            return 0

        with httpx.Client(follow_redirects=True, timeout=t_prewarm) as pre_client:
            r2 = pre_client.post(f"{base}/api/admin/prewarm", cookies=cookies)
        _print_response("prewarm", r2)
        if r2.status_code == 504:
            print(
                "prewarm: 504 — il servizio Cloud Run ha probabilmente --timeout=120s; "
                "la cache si ricostruisce ai prossimi accessi, oppure alza il timeout su Cloud Run.",
                file=sys.stderr,
            )
        return 0 if r2.is_success else 1


if __name__ == "__main__":
    raise SystemExit(main())
