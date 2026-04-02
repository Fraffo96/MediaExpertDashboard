"""Verifica che l'HTML della home (/) contenga il logo via proxy /brand-logo/, non /static/."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.auth.firestore_store import StoredUser
from app.auth.models import ACCESS_MARKETING_INSIGHTS, ACCESS_SALES_INTELLIGENCE


def main() -> int:
    from fastapi.testclient import TestClient

    import app.web.context as ctx_mod

    user = StoredUser(
        id=99,
        username="verify_logo",
        hashed_password="x",
        display_name="Francesco",
        role="user",
        is_active=True,
        brand_id=1,
        _access_types=[ACCESS_SALES_INTELLIGENCE, ACCESS_MARKETING_INSIGHTS],
        _allowed_category_ids=[],
        _allowed_subcategory_ids=[],
        _allowed_filters=[],
        _allowed_tabs=[],
        ecosystem_ids=[],
    )

    def fake_get_user(_token):
        return user

    import app.main as main_mod

    with patch.object(ctx_mod, "get_user", fake_get_user):
        client = TestClient(main_mod.app)
        r = client.get("/", cookies={"access_token": "dummy"})
    if r.status_code != 200:
        print("[ERR] GET / status", r.status_code)
        return 1
    html = r.text
    bad = 'src="/static/img/brands/'
    if bad in html:
        print("[ERR] HTML contiene ancora logo static locale:", bad)
        return 1
    if "topbar-brand-logo" not in html:
        print("[ERR] Manca classe topbar-brand-logo nell'HTML")
        return 1
    if "/brand-logo/" not in html and "storage.googleapis.com" not in html:
        print("[ERR] Logo topbar: atteso /brand-logo/ oppure storage.googleapis.com nell'HTML")
        return 1
    print("[OK] Home HTML: logo topbar da GCS HTTPS e/o /brand-logo/ (mai src=/static/img/brands/).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
