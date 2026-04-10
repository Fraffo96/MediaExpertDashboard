"""Job asincroni pipeline dati (Firestore: dashboard_data_jobs, dashboard_seed_profiles)."""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

COL_JOBS = "dashboard_data_jobs"
COL_PROFILES = "dashboard_seed_profiles"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fs():
    from app.auth.firestore_store import get_firestore_client

    return get_firestore_client()


def create_data_job(job_type: str, seed_profile_id: Optional[str] = None, payload: Optional[dict] = None) -> dict:
    """Crea documento job in stato queued. Ritorna dict serializzabile (include id)."""
    jid = uuid.uuid4().hex
    now = _utc_now()
    to_set: dict[str, Any] = {
        "status": "queued",
        "job_type": job_type,
        "seed_profile_id": seed_profile_id,
        "payload": payload or {},
        "message": "",
        "error_snippet": "",
        "created_at": now,
        "updated_at": now,
        "finished_at": None,
    }
    _fs().collection(COL_JOBS).document(jid).set(to_set)
    return {
        "id": jid,
        **{k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in to_set.items()},
    }


def update_data_job(
    jid: str,
    status: str,
    message: str = "",
    error_snippet: str = "",
) -> None:
    ref = _fs().collection(COL_JOBS).document(jid)
    patch: dict[str, Any] = {
        "status": status,
        "message": message or "",
        "error_snippet": error_snippet or "",
        "updated_at": _utc_now(),
    }
    if status in ("ok", "error"):
        patch["finished_at"] = _utc_now()
    ref.update(patch)


def get_data_job(jid: str) -> Optional[dict]:
    snap = _fs().collection(COL_JOBS).document(jid).get()
    if not snap.exists:
        return None
    d = snap.to_dict() or {}
    d["id"] = snap.id
    return _deserialize_doc(d)


def list_seed_profiles(limit: int = 50) -> list[dict]:
    q = _fs().collection(COL_PROFILES).limit(limit)
    out = []
    for snap in q.stream():
        d = snap.to_dict() or {}
        d["id"] = snap.id
        out.append(_deserialize_doc(d))
    return out


def get_seed_profile(profile_id: str) -> Optional[dict]:
    snap = _fs().collection(COL_PROFILES).document(profile_id).get()
    if not snap.exists:
        return None
    d = snap.to_dict() or {}
    d["id"] = snap.id
    return _deserialize_doc(d)


def save_seed_profile(
    profile_id: str,
    name: str,
    num_orders: int,
    num_customers: int,
    product_count: int,
) -> dict:
    ref = _fs().collection(COL_PROFILES).document(profile_id)
    doc = {
        "name": name,
        "num_orders": int(num_orders),
        "num_customers": int(num_customers),
        "product_count": int(product_count),
        "updated_at": _utc_now(),
    }
    ref.set(doc, merge=True)
    d = ref.get().to_dict() or {}
    d["id"] = profile_id
    return _deserialize_doc(d)


def delete_seed_profile(profile_id: str) -> None:
    _fs().collection(COL_PROFILES).document(profile_id).delete()


def profile_to_env(profile: Optional[dict]) -> dict[str, str]:
    """Variabili d'ambiente per worker / run_bigquery_schema / generate_seed_data."""
    if not profile:
        return {}
    return {
        "SEED_NUM_ORDERS": str(int(profile.get("num_orders") or 380000)),
        "SEED_NUM_CUSTOMERS": str(int(profile.get("num_customers") or 24000)),
        "SEED_NUM_PRODUCTS": str(int(profile.get("product_count") or 1200)),
    }


def _deserialize_doc(d: dict) -> dict:
    out = dict(d)
    for k in ("payload",):
        if k in out and isinstance(out[k], str):
            try:
                out[k] = json.loads(out[k])
            except json.JSONDecodeError:
                pass
    for k in ("created_at", "updated_at", "finished_at"):
        if out.get(k) is not None and hasattr(out[k], "isoformat"):
            out[k] = out[k].isoformat()
    return out


def trigger_cloud_run_job(
    job_id: str,
    job_type: str,
    seed_profile_json: Optional[str] = None,
) -> bool:
    """Esegue RunJob con override env. Ritorna True se invocato."""
    name = (os.environ.get("CLOUD_RUN_DATA_JOB_NAME") or "").strip()
    region = (os.environ.get("GCP_REGION") or os.environ.get("CLOUD_RUN_REGION") or "europe-west1").strip()
    project = (os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT") or "").strip()
    if not name or not project:
        return False
    try:
        from google.cloud.run_v2 import JobsClient
        from google.cloud.run_v2.types import RunJobRequest
        from google.cloud.run_v2.types.k8s_min import EnvVar

        client = JobsClient()
        job_resource = f"projects/{project}/locations/{region}/jobs/{name}"
        env = [
            EnvVar(name="DATA_JOB_ID", value=job_id),
            EnvVar(name="DATA_JOB_TYPE", value=job_type),
        ]
        if seed_profile_json:
            env.append(EnvVar(name="SEED_PROFILE_JSON", value=seed_profile_json))
        overrides = RunJobRequest.Overrides(
            container_overrides=[
                RunJobRequest.Overrides.ContainerOverride(env=env),
            ],
        )
        req = RunJobRequest(name=job_resource, overrides=overrides)
        op = client.run_job(request=req)
        logger.info("Cloud Run Job avviato: %s op=%s", job_resource, getattr(op, "operation", op))
        return True
    except Exception as e:
        logger.exception("RunJob fallito: %s", e)
        return False


def spawn_local_worker(job_id: str, job_type: str, seed_profile: Optional[dict]) -> bool:
    """Avvia worker in subprocess (sviluppo / VM con script nel path)."""
    import subprocess
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    worker = root / "scripts" / "data_pipeline_worker.py"
    if not worker.is_file():
        return False
    env = {**os.environ, "DATA_JOB_ID": job_id, "DATA_JOB_TYPE": job_type}
    if seed_profile:
        env.update(profile_to_env(seed_profile))
        env["SEED_PROFILE_JSON"] = json.dumps(seed_profile)
    try:
        subprocess.Popen(
            [sys.executable, str(worker)],
            env=env,
            cwd=str(root),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return True
    except Exception as e:
        logger.exception("spawn worker: %s", e)
        return False
