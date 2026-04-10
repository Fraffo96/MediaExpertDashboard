"""Admin: osservabilità BigQuery, cache/prewarm, job pipeline asincroni, profili seed."""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Body, Cookie, HTTPException
from fastapi.responses import JSONResponse

from app.db.client import DATASET, PROJECT_ID, run_ddl_admin, run_query_admin

_TABLE_ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,1022}$")
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
from app.services.seed_profile_v2 import preview_profile_v2, validate_profile_v2
from app.web.context import get_user

router = APIRouter(tags=["Admin"])

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS = _REPO_ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))


def _compile_seed_profile_dict(profile: dict) -> dict:
    from seed_planner.compiler import compile_seed_profile  # type: ignore[import-not-found]

    return compile_seed_profile(profile)


def _bq_drop_allowed(table_id: str, *, force: bool) -> tuple[bool, str]:
    if not _TABLE_ID_RE.match(table_id):
        return False, "Nome tabella non valido."
    if table_id.startswith("precalc_") or table_id.startswith("mv_"):
        return True, ""
    if force:
        if os.getenv("ENABLE_ADMIN_BQ_DROP_ANY", "").strip().lower() not in ("1", "true", "yes"):
            return (
                False,
                "Eliminare dim_/fact_/altre tabelle richiede ENABLE_ADMIN_BQ_DROP_ANY=1 sul servizio e body force=true.",
            )
        return True, ""
    return (
        False,
        "Da questa UI puoi eliminare senza flag solo tabelle precalc_* e mv_*. "
        "Per dim_/fact_/altro: abilita ENABLE_ADMIN_BQ_DROP_ANY=1 e invia force=true.",
    )


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


@router.get("/api/admin/bq/drop-rules")
async def api_admin_bq_drop_rules(access_token: Optional[str] = Cookie(None)):
    """Regole drop tabella (solo admin)."""
    _admin_user(access_token)
    force_env = os.getenv("ENABLE_ADMIN_BQ_DROP_ANY", "").strip().lower() in ("1", "true", "yes")
    return {
        "project_id": PROJECT_ID,
        "dataset": DATASET,
        "safe_prefixes": ["precalc_", "mv_"],
        "force_any_enabled": force_env,
        "hint": "DROP è irreversibile. Usa refresh precalc / full seed per ricreare.",
    }


@router.post("/api/admin/bq/drop-table")
async def api_admin_bq_drop_table(access_token: Optional[str] = Cookie(None), body: dict = Body(...)):
    """Elimina una tabella nel dataset mart (DROP TABLE IF EXISTS)."""
    _admin_user(access_token)
    tid = str(body.get("table_id") or "").strip()
    confirm = str(body.get("confirm") or "").strip()
    force = bool(body.get("force"))
    if not tid or tid != confirm:
        raise HTTPException(status_code=400, detail="Il campo confirm deve essere identico a table_id.")
    ok, msg = _bq_drop_allowed(tid, force=force)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    fq = f"`{PROJECT_ID}.{DATASET}.{tid}`"
    try:
        run_ddl_admin(f"DROP TABLE IF EXISTS {fq}", timeout_sec=600)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"BigQuery: {e}") from e
    return {"ok": True, "dropped": tid}


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

@router.get("/api/admin/bq/summary")
async def api_admin_bq_summary(access_token: Optional[str] = Cookie(None)):
    """Riepilogo dataset: totals + aggregati per famiglia (dim/fact/precalc/mv/other)."""
    _admin_user(access_token)
    q = f"""
WITH t AS (
  SELECT
    table_id,
    row_count,
    size_bytes,
    last_modified_time
  FROM `{PROJECT_ID}.{DATASET}.__TABLES__`
),
fam AS (
  SELECT
    table_id,
    row_count,
    size_bytes,
    last_modified_time,
    CASE
      WHEN STARTS_WITH(table_id, 'dim_') THEN 'dim'
      WHEN STARTS_WITH(table_id, 'fact_') THEN 'fact'
      WHEN STARTS_WITH(table_id, 'precalc_') THEN 'precalc'
      WHEN STARTS_WITH(table_id, 'mv_') THEN 'mv'
      ELSE 'other'
    END AS family
  FROM t
)
SELECT
  family,
  COUNT(*) AS table_count,
  SUM(row_count) AS row_count,
  SUM(size_bytes) AS size_bytes,
  MAX(last_modified_time) AS last_modified_time
FROM fam
GROUP BY family
ORDER BY size_bytes DESC
"""
    try:
        fam_rows = run_query_admin(q, timeout_sec=120)
        top_rows = run_query_admin(
            f"""
SELECT table_id, row_count, size_bytes, last_modified_time
FROM `{PROJECT_ID}.{DATASET}.__TABLES__`
ORDER BY size_bytes DESC
LIMIT 20
""",
            timeout_sec=120,
        )
        totals = run_query_admin(
            f"""
SELECT
  SUM(row_count) AS row_count,
  SUM(size_bytes) AS size_bytes,
  COUNT(*) AS table_count,
  MAX(last_modified_time) AS last_modified_time
FROM `{PROJECT_ID}.{DATASET}.__TABLES__`
""",
            timeout_sec=120,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"BigQuery: {e}") from e

    return {
        "project_id": PROJECT_ID,
        "dataset": DATASET,
        "totals": (totals[0] if totals else {}),
        "families": fam_rows,
        "top_tables": top_rows,
    }


@router.get("/api/admin/bq/quality")
async def api_admin_bq_quality(access_token: Optional[str] = Cookie(None)):
    """Controlli qualità minimi: null ratio chiavi, cardinalità principali, promo rate."""
    _admin_user(access_token)
    # NB: query leggere e robuste (se tabelle mancano -> errore chiaro)
    q = f"""
WITH base AS (
  SELECT
    (SELECT COUNT(*) FROM `mart.dim_customer`) AS customers,
    (SELECT COUNTIF(segment_id IS NULL) FROM `mart.dim_customer`) AS customers_null_segment,
    (SELECT COUNT(DISTINCT segment_id) FROM `mart.dim_customer`) AS segments_distinct,
    (SELECT COUNT(*) FROM `mart.fact_orders`) AS orders,
    (SELECT COUNTIF(promo_flag) FROM `mart.fact_orders`) AS orders_promo,
    (SELECT COUNTIF(channel IS NULL OR channel = '') FROM `mart.fact_orders`) AS orders_null_channel,
    (SELECT COUNT(*) FROM `mart.fact_order_items`) AS order_items,
    (SELECT COUNTIF(product_id IS NULL) FROM `mart.fact_order_items`) AS order_items_null_product,
    (SELECT COUNT(DISTINCT product_id) FROM `mart.fact_order_items`) AS order_items_distinct_products
)
SELECT
  customers,
  customers_null_segment,
  SAFE_DIVIDE(customers_null_segment, NULLIF(customers, 0)) AS customers_null_segment_rate,
  segments_distinct,
  orders,
  orders_promo,
  SAFE_DIVIDE(orders_promo, NULLIF(orders, 0)) AS orders_promo_rate,
  orders_null_channel,
  SAFE_DIVIDE(orders_null_channel, NULLIF(orders, 0)) AS orders_null_channel_rate,
  order_items,
  order_items_null_product,
  SAFE_DIVIDE(order_items_null_product, NULLIF(order_items, 0)) AS order_items_null_product_rate,
  order_items_distinct_products
FROM base
"""
    try:
        rows = run_query_admin(q, timeout_sec=120)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"BigQuery: {e}") from e
    return {"project_id": PROJECT_ID, "dataset": DATASET, "quality": (rows[0] if rows else {})}


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
    profile_v2 = body.get("profile_v2")
    profile_for_job: Optional[dict] = None
    if isinstance(profile_v2, dict) and profile_v2:
        normalized, verr = validate_profile_v2(profile_v2)
        if verr:
            raise HTTPException(
                status_code=400,
                detail={"errors": [e.to_dict() for e in verr]},
            )
        profile_for_job = normalized
        seed_profile_id = None
    elif seed_profile_id:
        profile_for_job = get_seed_profile(seed_profile_id)
        if not profile_for_job:
            raise HTTPException(status_code=404, detail="Profilo seed non trovato")
    try:
        rec = create_data_job(
            job_type,
            seed_profile_id=seed_profile_id,
            payload={"started_by": user.username},
            profile_inline=profile_for_job,
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Firestore: {e}") from e
    jid = rec["id"]
    prof_json = json.dumps(profile_for_job) if profile_for_job else None
    triggered = trigger_cloud_run_job(jid, job_type, seed_profile_json=prof_json)
    runner = "cloud_run_job"
    if not triggered:
        if spawn_local_worker(jid, job_type, profile_for_job):
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


@router.post("/api/admin/seed/validate")
async def api_admin_seed_validate(access_token: Optional[str] = Cookie(None), body: dict = Body(...)):
    """Valida un profilo seed v2 (senza eseguire la pipeline)."""
    _admin_user(access_token)
    raw = dict(body)
    nested = raw.pop("profile", None)
    to_validate: dict = nested if isinstance(nested, dict) else raw
    normalized, errors = validate_profile_v2(to_validate)
    return {
        "ok": len(errors) == 0,
        "normalized": normalized,
        "errors": [e.to_dict() for e in errors],
    }


@router.post("/api/admin/seed/preview")
async def api_admin_seed_preview(access_token: Optional[str] = Cookie(None), body: dict = Body(...)):
    """Anteprima profilo v2 + (default) distribuzioni attese dal compiler."""
    _admin_user(access_token)
    do_compile = body.get("compile", True)
    raw = dict(body)
    raw.pop("compile", None)
    nested = raw.pop("profile", None)
    to_validate: dict = nested if isinstance(nested, dict) else raw
    normalized, errors = validate_profile_v2(to_validate)
    if errors:
        return JSONResponse(
            {"ok": False, "errors": [e.to_dict() for e in errors]},
            status_code=400,
        )
    compiled = _compile_seed_profile_dict(normalized) if do_compile else None
    return {"ok": True, "preview": preview_profile_v2(normalized, compiled)}


@router.post("/api/admin/seed/compile")
async def api_admin_seed_compile(access_token: Optional[str] = Cookie(None), body: dict = Body(...)):
    """Compila profilo v2 in struttura per patch SQL + preview (senza eseguire job)."""
    _admin_user(access_token)
    raw = dict(body)
    include_sql = bool(raw.pop("include_sql", False))
    nested = raw.pop("profile", None)
    to_validate: dict = nested if isinstance(nested, dict) else raw
    normalized, errors = validate_profile_v2(to_validate)
    if errors:
        return JSONResponse(
            {"ok": False, "errors": [e.to_dict() for e in errors]},
            status_code=400,
        )
    compiled = _compile_seed_profile_dict(normalized)
    slim = dict(compiled)
    if not include_sql:
        slim.pop("sql", None)
    return {"ok": True, "normalized": normalized, "compiled": slim}
