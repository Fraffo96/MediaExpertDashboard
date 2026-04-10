"""Admin: osservabilità BigQuery, cache/prewarm, job pipeline asincroni, profili seed."""
from __future__ import annotations

import json
import os
import urllib.parse
from typing import Any, Optional

from fastapi import APIRouter, Body, Cookie, HTTPException
from fastapi.responses import JSONResponse

from app.db.client import DATASET, PROJECT_ID, run_query_admin
from app.services.data_jobs import (
    create_data_job,
    delete_seed_profile,
    get_data_job,
    get_seed_profile,
    list_seed_profiles,
    save_seed_profile,
    spawn_local_worker,
    trigger_cloud_run_job,
)
from app.services.prewarm import prewarm_cache
from app.web.context import get_user

router = APIRouter(tags=["Admin"])


def _admin_user(access_token: Optional[str]):
    user = get_user(access_token)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin only")
    return user


def _fmt_bytes(n: Any) -> str:
    if n is None:
        return ""
    try:
        x = float(n)
    except (TypeError, ValueError):
        return str(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if x < 1024 or unit == "TiB":
            return f"{x:.2f} {unit}"
        x /= 1024
    return f"{x:.2f} TiB"


@router.get("/api/admin/bq/tables")
async def api_admin_bq_tables(access_token: Optional[str] = Cookie(None)):
    """Righe e dimensioni tabelle dataset mart (__TABLES__)."""
    _admin_user(access_token)
    q = f"""
SELECT table_id, row_count, size_bytes, creation_time, last_modified_time
FROM `{PROJECT_ID}.{DATASET}.__TABLES__`
ORDER BY table_id
"""
    try:
        rows = run_query_admin(q, timeout_sec=120)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"BigQuery: {e}") from e
    out = []
    for r in rows:
        sz = r.get("size_bytes")
        out.append(
            {
                **r,
                "size_human": _fmt_bytes(sz),
                "creation_time": str(r.get("creation_time") or ""),
                "last_modified_time": str(r.get("last_modified_time") or ""),
            }
        )
    return {"project_id": PROJECT_ID, "dataset": DATASET, "tables": out}


@router.get("/api/admin/bq/schema")
async def api_admin_bq_schema(access_token: Optional[str] = Cookie(None)):
    """INFORMATION_SCHEMA.COLUMNS per mart, raggruppato per table_name."""
    _admin_user(access_token)
    q = f"""
SELECT table_name, column_name, ordinal_position, data_type, is_nullable
FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
ORDER BY table_name, ordinal_position
"""
    try:
        rows = run_query_admin(q, timeout_sec=120)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"BigQuery: {e}") from e
    by_table: dict[str, list[dict]] = {}
    for r in rows:
        t = str(r.get("table_name") or "")
        by_table.setdefault(t, []).append(
            {
                "column_name": r.get("column_name"),
                "ordinal_position": r.get("ordinal_position"),
                "data_type": r.get("data_type"),
                "is_nullable": r.get("is_nullable"),
            }
        )
    return {"project_id": PROJECT_ID, "dataset": DATASET, "schema_by_table": by_table}


@router.post("/api/admin/prewarm")
async def api_admin_prewarm(access_token: Optional[str] = Cookie(None)):
    """Pre-warm cache dashboard (stesso stack di /internal/prewarm ma cookie admin)."""
    _admin_user(access_token)
    return await prewarm_cache()


@router.get("/api/admin/data-ops-links")
async def api_admin_data_ops_links(access_token: Optional[str] = Cookie(None)):
    """URL Console GCP (BigQuery, Logging Cloud Run / Job)."""
    _admin_user(access_token)
    region = (os.environ.get("GCP_REGION") or os.environ.get("CLOUD_RUN_REGION") or "europe-west1").strip()
    job_name = (os.environ.get("CLOUD_RUN_DATA_JOB_NAME") or "").strip()
    base = "https://console.cloud.google.com"
    log_query_job = urllib.parse.quote(
        f'resource.type="cloud_run_job"\nlabels."run.googleapis.com/job_name"="{job_name}"' if job_name else 'resource.type="cloud_run_revision"',
        safe="",
    )
    return {
        "project_id": PROJECT_ID,
        "region": region,
        "links": {
            "bigquery_dataset": f"{base}/bigquery?project={PROJECT_ID}&ws=!1m5!1m4!4m3!1s{PROJECT_ID}!2s{DATASET}!3s",
            "logging_job": f"{base}/logs/query;query={log_query_job}?project={PROJECT_ID}",
            "run_jobs": f"{base}/run/jobs?project={PROJECT_ID}&region={region}",
        },
    }


@router.post("/api/admin/data-jobs")
async def api_admin_start_data_job(
    access_token: Optional[str] = Cookie(None),
    body: dict = Body(...),
):
    """Avvia pipeline lunga (precalc o full_seed) via Cloud Run Job o subprocess locale."""
    user = _admin_user(access_token)
    job_type = str(body.get("job_type") or "").strip()
    if job_type not in ("precalc", "full_seed"):
        raise HTTPException(status_code=400, detail="job_type deve essere precalc o full_seed")
    seed_profile_id = body.get("seed_profile_id")
    seed_profile_id = str(seed_profile_id).strip() if seed_profile_id else None
    profile = None
    if seed_profile_id:
        profile = get_seed_profile(seed_profile_id)
        if not profile:
            raise HTTPException(status_code=404, detail="Profilo seed non trovato")
    try:
        rec = create_data_job(job_type, seed_profile_id=seed_profile_id, payload={"started_by": user.username})
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Firestore: {e}") from e
    jid = rec["id"]
    prof_json = json.dumps(profile) if profile else None
    triggered = trigger_cloud_run_job(jid, job_type, seed_profile_json=prof_json)
    runner = "cloud_run_job"
    if not triggered:
        if spawn_local_worker(jid, job_type, profile):
            runner = "subprocess"
        else:
            return JSONResponse(
                {
                    "error": "Impossibile avviare il worker (configura CLOUD_RUN_DATA_JOB_NAME o esegui in ambiente con scripts/).",
                    "id": jid,
                },
                status_code=503,
            )
    return {"id": jid, "status": "queued", "job_type": job_type, "runner": runner}


@router.get("/api/admin/data-jobs/{job_id}")
async def api_admin_get_data_job(job_id: str, access_token: Optional[str] = Cookie(None)):
    _admin_user(access_token)
    job = get_data_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job non trovato")
    return job


@router.get("/api/admin/seed-profiles")
async def api_admin_list_seed_profiles(access_token: Optional[str] = Cookie(None)):
    _admin_user(access_token)
    return {"profiles": list_seed_profiles()}


@router.post("/api/admin/seed-profiles")
async def api_admin_upsert_seed_profile(
    access_token: Optional[str] = Cookie(None),
    body: dict = Body(...),
):
    _admin_user(access_token)
    pid = str(body.get("id") or "").strip()
    if not pid:
        raise HTTPException(status_code=400, detail="id profilo obbligatorio")
    name = str(body.get("name") or pid).strip()
    try:
        num_orders = int(body.get("num_orders") or 380000)
        num_customers = int(body.get("num_customers") or 24000)
        product_count = int(body.get("product_count") or 1200)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="numeri non validi")
    return save_seed_profile(pid, name, num_orders, num_customers, product_count)


@router.delete("/api/admin/seed-profiles/{profile_id}")
async def api_admin_delete_seed_profile(profile_id: str, access_token: Optional[str] = Cookie(None)):
    _admin_user(access_token)
    delete_seed_profile(profile_id)
    return {"ok": True}
