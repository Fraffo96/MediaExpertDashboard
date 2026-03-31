"""Test API e pagine MediaExpert Insights."""
import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_basic_page():
    r = client.get("/basic")
    assert r.status_code == 200
    assert "chartCat" in r.text or "Basic" in r.text


def test_promo_page():
    r = client.get("/promo")
    assert r.status_code == 200
    assert "Promo" in r.text


def test_customer_page():
    r = client.get("/customer")
    assert r.status_code == 200
    assert "Customer" in r.text


def test_simulation_page():
    r = client.get("/simulation")
    assert r.status_code == 200
    assert "Simulation" in r.text


def test_why_buy_page():
    r = client.get("/why-buy")
    assert r.status_code == 200
    assert "Why" in r.text


def test_root_redirects_to_basic():
    r = client.get("/")
    assert r.status_code == 200


def test_api_basic():
    r = client.get("/api/basic?period_start=2025-01-01&period_end=2025-12-31")
    assert r.status_code == 200
    data = r.json()
    assert "kpi" in data
    assert "sales_by_category" in data


def test_api_promo():
    r = client.get("/api/promo?period_start=2025-01-01&period_end=2025-12-31")
    assert r.status_code == 200
    assert "kpi" in r.json()


def test_api_customer():
    r = client.get("/api/customer?period_start=2025-01-01&period_end=2025-12-31")
    assert r.status_code == 200
    assert "overview" in r.json()


def test_api_simulation():
    r = client.get("/api/simulation?period_start=2025-01-01&period_end=2025-12-31")
    assert r.status_code == 200
    assert "baseline" in r.json()


def test_api_why_buy():
    r = client.get("/api/why-buy?period_start=2025-01-01&period_end=2025-12-31")
    assert r.status_code == 200
    assert "by_segment" in r.json()


def test_api_filters():
    r = client.get("/api/filters")
    assert r.status_code == 200
    data = r.json()
    assert "categories" in data
    assert "segments" in data
