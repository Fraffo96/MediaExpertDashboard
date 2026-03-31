"""Persistenza utenti ed ecosistemi su Firestore (Cloud Run e locale con emulatore)."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Optional

from google.cloud import firestore

from app.auth.models import (
    ACCESS_MARKETING_INSIGHTS,
    ACCESS_SALES_INTELLIGENCE,
    TAB_BRAND_COMPARISON,
    TAB_CHECK_LIVE_PROMO,
    TAB_MARKETING,
    TAB_MARKET_INTELLIGENCE,
    TAB_PROMO_CREATOR,
)

logger = logging.getLogger(__name__)

COL_USERS = "dashboard_users"
COL_ECOS = "dashboard_ecosystems"
COL_COUNTERS = "dashboard_counters"
COUNTER_DOC = "meta"


def _project_id() -> str:
    return os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT") or ""


def _database_id() -> Optional[str]:
    d = os.environ.get("FIRESTORE_DATABASE", "").strip()
    return d if d else None


_client: Optional[firestore.Client] = None


def get_firestore_client() -> firestore.Client:
    global _client
    if _client is None:
        if os.environ.get("FIRESTORE_EMULATOR_HOST"):
            _client = firestore.Client()
            return _client
        pid = _project_id()
        if not pid:
            raise RuntimeError("GCP_PROJECT_ID or GOOGLE_CLOUD_PROJECT required (or FIRESTORE_EMULATOR_HOST for local)")
        kwargs: dict = {"project": pid}
        dbname = _database_id()
        if dbname:
            kwargs["database"] = dbname
        _client = firestore.Client(**kwargs)
    return _client


def _parse_int_list(v: Any) -> list[int]:
    if v is None:
        return []
    if isinstance(v, list):
        return [int(x) for x in v]
    if isinstance(v, str):
        try:
            return [int(x) for x in json.loads(v or "[]")]
        except Exception:
            return []
    return []


def _parse_str_list(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str):
        try:
            obj = json.loads(v or "[]")
            return [str(x) for x in obj] if isinstance(obj, list) else []
        except Exception:
            return []
    return []


@dataclass
class StoredUser:
    """Stesso contratto usato dai template/API dell'ex ORM User."""

    id: int
    username: str
    hashed_password: str
    display_name: str = ""
    role: str = "user"
    is_active: bool = True
    brand_id: Optional[int] = None
    _access_types: list[str] = field(default_factory=list)
    _allowed_category_ids: list[int] = field(default_factory=list)
    _allowed_subcategory_ids: list[int] = field(default_factory=list)
    _allowed_filters: list[str] = field(default_factory=list)
    _allowed_tabs: list[str] = field(default_factory=list)
    ecosystem_ids: list[int] = field(default_factory=list)

    @property
    def access_types(self) -> str:
        return json.dumps(self._access_types)

    @property
    def allowed_category_ids(self) -> str:
        return json.dumps(self._allowed_category_ids)

    @property
    def allowed_subcategory_ids(self) -> str:
        return json.dumps(self._allowed_subcategory_ids)

    @property
    def allowed_filters(self) -> str:
        return json.dumps(self._allowed_filters)

    @property
    def allowed_tabs(self) -> str:
        return json.dumps(self._allowed_tabs)

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    @property
    def can_recalculate(self) -> bool:
        return self.is_admin

    @property
    def access_type_list(self) -> list[str]:
        return list(self._access_types)

    @property
    def filter_list(self) -> list[str]:
        return list(self._allowed_filters)

    @property
    def tab_list(self) -> list[str]:
        acc = self._access_types
        if not acc:
            return []
        tabs = []
        if ACCESS_SALES_INTELLIGENCE in acc:
            tabs.extend([TAB_MARKET_INTELLIGENCE, TAB_BRAND_COMPARISON, TAB_PROMO_CREATOR, TAB_CHECK_LIVE_PROMO])
        if ACCESS_MARKETING_INSIGHTS in acc:
            tabs.append(TAB_MARKETING)
        return list(dict.fromkeys(tabs))

    @property
    def category_ids_list(self) -> list[int]:
        return list(self._allowed_category_ids)

    @property
    def subcategory_ids_list(self) -> list[int]:
        return list(self._allowed_subcategory_ids)

    def can_access_tab(self, tab: str) -> bool:
        if self.is_admin:
            return True
        return tab in self.tab_list

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "username": self.username,
            "display_name": self.display_name,
            "role": self.role,
            "is_active": self.is_active,
            "brand_id": self.brand_id,
            "access_types": self.access_type_list,
            "allowed_category_ids": self.category_ids_list,
            "allowed_subcategory_ids": self.subcategory_ids_list,
            "allowed_filters": self.filter_list,
            "allowed_tabs": self.tab_list,
            "ecosystems": list(self.ecosystem_ids),
        }


def _str_list_field(val: Any) -> list[str]:
    if isinstance(val, list):
        return [str(x) for x in val]
    return _parse_str_list(val)


def _user_from_doc(doc_id: str, d: dict[str, Any]) -> StoredUser:
    return StoredUser(
        id=int(d.get("id", 0)),
        username=d.get("username", doc_id),
        hashed_password=d.get("hashed_password", ""),
        display_name=d.get("display_name", "") or "",
        role=d.get("role", "user") or "user",
        is_active=bool(d.get("is_active", True)),
        brand_id=d.get("brand_id") if d.get("brand_id") is not None else None,
        _access_types=_str_list_field(d.get("access_types")),
        _allowed_category_ids=_parse_int_list(d.get("allowed_category_ids")),
        _allowed_subcategory_ids=_parse_int_list(d.get("allowed_subcategory_ids")),
        _allowed_filters=_str_list_field(d.get("allowed_filters")),
        _allowed_tabs=_str_list_field(d.get("allowed_tabs")),
        ecosystem_ids=_parse_int_list(d.get("ecosystem_ids")),
    )


@dataclass
class StoredEcosystem:
    id: int
    name: str
    description: str = ""
    icon: str = ""
    is_active: bool = True
    category_ids: list[int] = field(default_factory=list)
    brand_ids: list[int] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "icon": self.icon,
            "is_active": self.is_active,
            "category_ids": list(self.category_ids),
            "brand_ids": list(self.brand_ids),
        }


def _eco_from_doc(d: dict[str, Any], doc_id: str) -> StoredEcosystem:
    return StoredEcosystem(
        id=int(d.get("id", doc_id)),
        name=d.get("name", ""),
        description=d.get("description", "") or "",
        icon=d.get("icon", "") or "",
        is_active=bool(d.get("is_active", True)),
        category_ids=_parse_int_list(d.get("category_ids")),
        brand_ids=_parse_int_list(d.get("brand_ids")),
    )


def get_user_by_username(username: str) -> Optional[StoredUser]:
    if not username:
        return None
    db = get_firestore_client()
    snap = db.collection(COL_USERS).document(username).get()
    if not snap.exists:
        return None
    return _user_from_doc(snap.id, snap.to_dict() or {})


def get_user_by_id(user_id: int) -> Optional[StoredUser]:
    db = get_firestore_client()
    for snap in db.collection(COL_USERS).stream():
        d = snap.to_dict() or {}
        if int(d.get("id", -1)) == int(user_id):
            return _user_from_doc(snap.id, d)
    return None


def get_user_doc_id(user_id: int) -> Optional[str]:
    db = get_firestore_client()
    for snap in db.collection(COL_USERS).stream():
        d = snap.to_dict() or {}
        if int(d.get("id", -1)) == int(user_id):
            return snap.id
    return None


def list_users_non_admin() -> list[StoredUser]:
    db = get_firestore_client()
    users: list[StoredUser] = []
    for snap in db.collection(COL_USERS).stream():
        d = snap.to_dict() or {}
        if (d.get("role") or "") == "admin":
            continue
        users.append(_user_from_doc(snap.id, d))
    users.sort(key=lambda u: u.id)
    return users


def list_users_active_with_brand() -> list[StoredUser]:
    out: list[StoredUser] = []
    for u in _iter_all_users():
        if u.role == "admin":
            continue
        if u.is_active and u.brand_id:
            out.append(u)
    return out


def _iter_all_users() -> list[StoredUser]:
    db = get_firestore_client()
    return [_user_from_doc(snap.id, snap.to_dict() or {}) for snap in db.collection(COL_USERS).stream()]


def _ensure_counters() -> None:
    db = get_firestore_client()
    ref = db.collection(COL_COUNTERS).document(COUNTER_DOC)
    if not ref.get().exists:
        ref.set({"next_user_id": 2, "next_ecosystem_id": 1})


def allocate_user_id() -> int:
    db = get_firestore_client()
    cref = db.collection(COL_COUNTERS).document(COUNTER_DOC)
    _ensure_counters()

    @firestore.transactional
    def _alloc(transaction, ref):
        snap = ref.get(transaction=transaction)
        data = snap.to_dict() or {}
        n = int(data.get("next_user_id", 1))
        transaction.update(ref, {"next_user_id": n + 1})
        return n

    return _alloc(db.transaction(), cref)


def allocate_ecosystem_id() -> int:
    db = get_firestore_client()
    cref = db.collection(COL_COUNTERS).document(COUNTER_DOC)
    _ensure_counters()

    @firestore.transactional
    def _alloc(transaction, ref):
        snap = ref.get(transaction=transaction)
        data = snap.to_dict() or {}
        n = int(data.get("next_ecosystem_id", 1))
        transaction.update(ref, {"next_ecosystem_id": n + 1})
        return n

    return _alloc(db.transaction(), cref)


def create_user_record(
    username: str,
    hashed_password: str,
    display_name: str,
    role: str,
    is_active: bool,
    brand_id: Optional[int],
    access_types: list[str],
    allowed_category_ids: list[int],
    allowed_subcategory_ids: list[int],
    allowed_filters: list[str],
    allowed_tabs: list[str],
) -> StoredUser:
    db = get_firestore_client()
    ref = db.collection(COL_USERS).document(username)
    if ref.get().exists:
        raise ValueError("Username already exists")
    uid = allocate_user_id()
    payload = {
        "id": uid,
        "username": username,
        "hashed_password": hashed_password,
        "display_name": display_name,
        "role": role,
        "is_active": is_active,
        "brand_id": brand_id,
        "access_types": access_types,
        "allowed_category_ids": allowed_category_ids,
        "allowed_subcategory_ids": allowed_subcategory_ids,
        "allowed_filters": allowed_filters,
        "allowed_tabs": allowed_tabs,
        "ecosystem_ids": [],
    }
    ref.set(payload)
    return _user_from_doc(username, payload)


def update_user_record(user_id: int, updates: dict[str, Any]) -> Optional[StoredUser]:
    doc_id = get_user_doc_id(user_id)
    if not doc_id:
        return None
    db = get_firestore_client()
    ref = db.collection(COL_USERS).document(doc_id)
    ref2 = ref.get()
    if not ref2.exists:
        return None
    cur = ref2.to_dict() or {}

    patch: dict[str, Any] = {}
    if "display_name" in updates:
        patch["display_name"] = updates["display_name"]
    if "is_active" in updates:
        patch["is_active"] = bool(updates["is_active"])
    if "hashed_password" in updates and updates["hashed_password"]:
        patch["hashed_password"] = updates["hashed_password"]
    if "brand_id" in updates:
        patch["brand_id"] = updates["brand_id"]
    if "access_types" in updates:
        patch["access_types"] = updates["access_types"]
    if "allowed_category_ids" in updates:
        patch["allowed_category_ids"] = updates["allowed_category_ids"]
    if "allowed_subcategory_ids" in updates:
        patch["allowed_subcategory_ids"] = updates["allowed_subcategory_ids"]
    if "allowed_filters" in updates:
        patch["allowed_filters"] = updates["allowed_filters"]
    if "allowed_tabs" in updates:
        patch["allowed_tabs"] = updates["allowed_tabs"]
    if "ecosystem_ids" in updates:
        patch["ecosystem_ids"] = updates["ecosystem_ids"]

    merged = {**cur, **patch}
    ref.set(merged)
    return _user_from_doc(doc_id, merged)


def delete_user_record(user_id: int) -> bool:
    doc_id = get_user_doc_id(user_id)
    if not doc_id:
        return False
    get_firestore_client().collection(COL_USERS).document(doc_id).delete()
    return True


def list_ecosystems() -> list[StoredEcosystem]:
    db = get_firestore_client()
    ecos: list[StoredEcosystem] = []
    for snap in db.collection(COL_ECOS).stream():
        ecos.append(_eco_from_doc(snap.to_dict() or {}, snap.id))
    ecos.sort(key=lambda e: e.id)
    return ecos


def get_ecosystem_by_id(eco_id: int) -> Optional[StoredEcosystem]:
    db = get_firestore_client()
    snap = db.collection(COL_ECOS).document(str(int(eco_id))).get()
    if not snap.exists:
        return None
    return _eco_from_doc(snap.to_dict() or {}, snap.id)


def create_ecosystem_record(
    name: str,
    description: str,
    icon: str,
    is_active: bool,
    category_ids: list[int],
    brand_ids: list[int],
) -> StoredEcosystem:
    eid = allocate_ecosystem_id()
    payload = {
        "id": eid,
        "name": name,
        "description": description,
        "icon": icon,
        "is_active": is_active,
        "category_ids": category_ids,
        "brand_ids": brand_ids,
    }
    get_firestore_client().collection(COL_ECOS).document(str(eid)).set(payload)
    return _eco_from_doc(payload, str(eid))


def update_ecosystem_record(eco_id: int, updates: dict[str, Any]) -> Optional[StoredEcosystem]:
    ref = get_firestore_client().collection(COL_ECOS).document(str(int(eco_id)))
    snap = ref.get()
    if not snap.exists:
        return None
    cur = snap.to_dict() or {}
    patch: dict[str, Any] = {}
    for k in ("name", "description", "icon", "is_active", "category_ids", "brand_ids"):
        if k in updates:
            patch[k] = updates[k]
    merged = {**cur, **patch}
    ref.set(merged)
    return _eco_from_doc(merged, str(eco_id))


def delete_ecosystem_record(eco_id: int) -> bool:
    db = get_firestore_client()
    ref = db.collection(COL_ECOS).document(str(int(eco_id)))
    if not ref.get().exists:
        return False
    ref.delete()
    # Rimuovi da tutti gli utenti
    for snap in db.collection(COL_USERS).stream():
        d = snap.to_dict() or {}
        eids = _parse_int_list(d.get("ecosystem_ids"))
        if eco_id in eids:
            d["ecosystem_ids"] = [x for x in eids if x != eco_id]
            db.collection(COL_USERS).document(snap.id).set(d)
    return True


def ensure_default_admin() -> None:
    """Crea expert/test se assente; inizializza contatori."""
    from app.auth.security import hash_password

    db = get_firestore_client()
    cref = db.collection(COL_COUNTERS).document(COUNTER_DOC)
    if not cref.get().exists:
        cref.set({"next_user_id": 2, "next_ecosystem_id": 1})

    ref = db.collection(COL_USERS).document("expert")
    if ref.get().exists:
        logger.info("Admin user 'expert' already in Firestore.")
        return
    ref.set(
        {
            "id": 1,
            "username": "expert",
            "hashed_password": hash_password("test"),
            "display_name": "Admin",
            "role": "admin",
            "is_active": True,
            "brand_id": None,
            "access_types": [],
            "allowed_category_ids": [],
            "allowed_subcategory_ids": [],
            "allowed_filters": [],
            "allowed_tabs": [],
            "ecosystem_ids": [],
        }
    )
    logger.info("Default admin user 'expert' created in Firestore.")
