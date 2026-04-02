"""Risoluzione URL loghi brand (GCS vs static)."""
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi.testclient import TestClient


def test_brand_logo_url_default_is_https_gcs_like_cloud_run(monkeypatch):
    monkeypatch.delenv("BRAND_LOGOS_FORCE_SAME_ORIGIN_IMG", raising=False)
    from app.main import _brand_logo_url

    u = MagicMock()
    u.brand_id = 1
    u.role = "user"
    url = _brand_logo_url(u)
    assert url.startswith("https://")
    assert "storage.googleapis.com" in url or "googleapis.com" in url
    assert "/1.png" in url
    assert not url.startswith("/static/")


def test_brand_logo_url_ignores_old_static_env(monkeypatch):
    """BRAND_LOGOS_USE_STATIC non ha più effetto: evita placeholder grigi per env globale Windows."""
    monkeypatch.delenv("BRAND_LOGOS_FORCE_SAME_ORIGIN_IMG", raising=False)
    from app.main import _brand_logo_url

    monkeypatch.setenv("BRAND_LOGOS_USE_STATIC", "1")
    u = MagicMock()
    u.brand_id = 2
    u.role = "user"
    url = _brand_logo_url(u)
    assert url.startswith("https://")
    assert "/2.png" in url


def test_brand_logo_url_force_same_origin_uses_proxy_path(monkeypatch):
    monkeypatch.setenv("BRAND_LOGOS_FORCE_SAME_ORIGIN_IMG", "1")
    from app.web.brand_logo import brand_logo_url_for_user

    u = MagicMock()
    u.brand_id = 1
    u.role = "user"
    url = brand_logo_url_for_user(u)
    assert url.startswith("/brand-logo/")
    assert "/1.png" in url


def test_brand_logo_url_skip_proxy_redundant_still_https(monkeypatch):
    """BRAND_LOGOS_SKIP_PROXY è ridondante: il default è già HTTPS GCS."""
    monkeypatch.delenv("BRAND_LOGOS_FORCE_SAME_ORIGIN_IMG", raising=False)
    monkeypatch.setenv("BRAND_LOGOS_SKIP_PROXY", "1")
    from app.web.brand_logo import brand_logo_url_for_user

    u = MagicMock()
    u.brand_id = 1
    u.role = "user"
    url = brand_logo_url_for_user(u)
    assert url.startswith("https://")
    assert "storage.googleapis.com" in url or "googleapis.com" in url
    assert "/1.png" in url


def _patch_httpx_client(monkeypatch, *, status_code: int | None = None, raise_connect: bool = False):
    """status_code: risposta sintetica; raise_connect: eccezione di rete."""
    from app.web import brand_logo as bl_mod

    class FakeCtx:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, *_a, **_k):
            if raise_connect:
                raise httpx.ConnectError("test unreachable", request=MagicMock())
            resp = MagicMock()
            resp.status_code = status_code
            resp.content = b"err"
            resp.headers = {}
            return resp

    def _factory(*_a, **_k):
        return FakeCtx()

    monkeypatch.setattr(bl_mod.httpx, "AsyncClient", _factory)


def test_brand_logo_proxy_fallback_static_when_gcs_404(monkeypatch):
    """GCS 404 + PNG in static/img/brands → 200 dal file locale (diag rete / bucket)."""
    from app.main import app

    _patch_httpx_client(monkeypatch, status_code=404)
    client = TestClient(app)
    r = client.get("/brand-logo/1.png")
    assert r.status_code == 200
    assert r.content[:8] == b"\x89PNG\r\n\x1a\n"
    assert "max-age=120" in (r.headers.get("cache-control") or "")


def test_brand_logo_proxy_404_when_gcs_404_and_no_local_file(monkeypatch):
    _patch_httpx_client(monkeypatch, status_code=404)
    from app.main import app

    client = TestClient(app)
    r = client.get("/brand-logo/999999.png")
    assert r.status_code == 404


def test_brand_logo_proxy_fallback_static_when_gcs_unreachable(monkeypatch):
    """httpx fallisce: se esiste static locale per id, 200 invece di 502."""
    from app.main import app

    _patch_httpx_client(monkeypatch, raise_connect=True)
    client = TestClient(app)
    r = client.get("/brand-logo/1.png")
    assert r.status_code == 200
    assert r.content[:8] == b"\x89PNG\r\n\x1a\n"


def test_brand_logo_proxy_502_when_gcs_unreachable_and_no_local(monkeypatch):
    _patch_httpx_client(monkeypatch, raise_connect=True)
    from app.main import app

    client = TestClient(app)
    r = client.get("/brand-logo/999999.png")
    assert r.status_code == 502


def test_user_without_brand_id_gets_no_logo():
    from app.web.brand_logo import brand_logo_url_for_user

    u = MagicMock()
    u.brand_id = None
    u.role = "user"
    u.username = "nike"
    u.id = 42
    assert brand_logo_url_for_user(u) is None


def test_admin_without_brand_id_gets_fallback_logo(monkeypatch):
    from app.web.brand_logo import brand_logo_url_for_user

    monkeypatch.delenv("BRAND_LOGO_FALLBACK_BRAND_ID", raising=False)
    monkeypatch.delenv("BRAND_LOGOS_FORCE_SAME_ORIGIN_IMG", raising=False)
    u = MagicMock()
    u.brand_id = None
    u.role = "admin"
    u.username = "root"
    u.id = 1
    url = brand_logo_url_for_user(u)
    assert url is not None
    assert url.startswith("https://")
    assert "/1.png" in url


def test_admin_fallback_disabled_with_zero(monkeypatch):
    from app.web.brand_logo import brand_logo_url_for_user

    monkeypatch.setenv("BRAND_LOGO_FALLBACK_BRAND_ID", "0")
    u = MagicMock()
    u.brand_id = None
    u.role = "admin"
    u.username = "root"
    assert brand_logo_url_for_user(u) is None
