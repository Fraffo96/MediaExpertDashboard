#!/usr/bin/env python3
"""Verifica risoluzione URL loghi (GCS vs static) con lo stesso codice dell'app.

Esegui dalla root del repo: python scripts/verify_brand_logo_env.py
Esci con codice 1 se l'URL per brand_id=1 punta a /static (processo che non esegue questo main.py).
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")

    from app.db.client import PROJECT_ID
    from app.main import _brand_logos_public_base, _brand_logo_url
    from unittest.mock import MagicMock

    base = _brand_logos_public_base()
    u = MagicMock()
    u.brand_id = 1
    url = _brand_logo_url(u)

    os_mod = __import__("os")
    print("PROJECT_ID (resolved):", PROJECT_ID)
    print("BRAND_LOGOS_PUBLIC_BASE env:", (os_mod.environ.get("BRAND_LOGOS_PUBLIC_BASE") or "(unset)").strip() or "(unset)")
    print("BRAND_LOGOS_SKIP_PROXY env:", (os_mod.environ.get("BRAND_LOGOS_SKIP_PROXY") or "(unset)").strip() or "(unset)")
    print("_brand_logos_public_base():", base)
    print("_brand_logo_url(brand_id=1):", url)

    if url and url.startswith("/static/"):
        print("\n[ERR] L'URL usa ancora /static/ — stai eseguendo un altro albero di codice o un vecchio processo.")
        return 1
    if url and not (url.startswith("/brand-logo/") or url.startswith("https://")):
        print("\n[ERR] URL inatteso (atteso HTTPS GCS o /brand-logo/ se BRAND_LOGOS_FORCE_SAME_ORIGIN_IMG).")
        return 1
    if url and url.startswith("https://"):
        print("\n[OK] URL logo = HTTPS GCS (default, come Cloud Run). File su bucket, non su Firestore.")
    else:
        print("\n[OK] URL logo = /brand-logo/ (BRAND_LOGOS_FORCE_SAME_ORIGIN_IMG attivo → proxy lato server).")
        print("    Se 502: HTTP_PROXY/HTTPS_PROXY per Python verso GCS.")

    print("\n--- Diagnostica rapida ---")
    print("  Topbar: Network → richiesta a storage.googleapis.com/.../brands/<id>.png (default)")
    print("  Utente: il logo in topbar richiede brand_id sul documento utente in Firestore.")
    print("  Admin: GET /api/admin/brand-logo-debug?brand_id=1 (cookie sessione admin)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
