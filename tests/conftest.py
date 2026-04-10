"""Fixture comuni: utente admin fittizio per pagine che richiedono login."""
import pytest

from app.auth.firestore_store import StoredUser
from app.auth.models import ACCESS_MARKETING_INSIGHTS, ACCESS_SALES_INTELLIGENCE, ALL_FILTERS


@pytest.fixture(autouse=True)
def _authed_user_for_html_pages(monkeypatch):
    """Le route HTML usano require_login → get_user; senza cookie i test vedevano solo /login."""

    user = StoredUser(
        id=1,
        username="pytest-user",
        hashed_password="",
        display_name="Pytest",
        role="admin",
        brand_id=1,
        _access_types=[ACCESS_SALES_INTELLIGENCE, ACCESS_MARKETING_INSIGHTS],
        _allowed_filters=list(ALL_FILTERS),
    )

    def _get_user(access_token=None):
        return user

    monkeypatch.setattr("app.web.context.get_user", _get_user)
