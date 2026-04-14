"""
Client BigQuery generico: connessione e esecuzione query parametrizzate.
Legge dati dal dataset mart (progetto GCP da variabile d'ambiente o default).
Carica .env dalla root del repo (non dalla cwd) per GOOGLE_APPLICATION_CREDENTIALS.
"""
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

import logging
import os
import threading
import time
from typing import Optional

from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig


def _resolve_gcp_project_id() -> str:
    """GCP_PROJECT_ID vuoto nel launch (VS Code / shell) non deve mascherare il default."""
    v = (os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT") or "").strip()
    return v or "mediaexpertdashboard"


PROJECT_ID = _resolve_gcp_project_id()
DATASET = "mart"

logger = logging.getLogger(__name__)

_bq_client: Optional[bigquery.Client] = None
_bq_client_lock = threading.Lock()


def get_client() -> bigquery.Client:
    """Un solo Client per processo: evita overhead TLS/handshake su centinaia di query (dashboard MI)."""
    global _bq_client
    if _bq_client is not None:
        return _bq_client
    with _bq_client_lock:
        if _bq_client is None:
            _bq_client = bigquery.Client(project=PROJECT_ID)
        return _bq_client


def _env_flag(name: str) -> bool:
    return (os.getenv(name, "") or "").strip().lower() in ("1", "true", "yes", "on")


def run_query(
    query: str,
    params: Optional[list] = None,
    *,
    timeout_sec: float = 30,
    log_label: str | None = None,
) -> list[dict]:
    """
    Esegue una query parametrizzata e restituisce le righe come lista di dict.
    params: lista di bigquery.ScalarQueryParameter (nome, tipo, valore).
    timeout_sec: timeout job BigQuery (Promo Creator / CTE pesanti usano 120s in app/db/queries/promo_creator.py).
    """
    client = get_client()
    job_config = QueryJobConfig()
    if params:
        job_config.query_parameters = params
    t0 = time.perf_counter()
    job = client.query(query, job_config=job_config, timeout=timeout_sec)
    rows = job.result(timeout=timeout_sec)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    bytes_proc = getattr(job, "total_bytes_processed", None) or getattr(job, "total_bytes_billed", None)
    slot_ms = getattr(job, "slot_millis", None)
    label = log_label or ""
    slow_thr = float((os.getenv("PROMO_CREATOR_SLOW_QUERY_MS") or "12000").strip() or "12000")
    verbose_bq = _env_flag("PROMO_CREATOR_VERBOSE_BQ")
    if label and verbose_bq:
        logger.info(
            "promo_creator_bq label=%s ms=%.0f bytes=%s slot_ms=%s job_id=%s",
            label,
            elapsed_ms,
            bytes_proc,
            slot_ms,
            getattr(job, "job_id", None),
        )
    elif elapsed_ms >= slow_thr:
        logger.warning(
            "promo_creator_bq_slow label=%s ms=%.0f bytes=%s slot_ms=%s job_id=%s",
            label or "unknown",
            elapsed_ms,
            bytes_proc,
            slot_ms,
            getattr(job, "job_id", None),
        )
    # Log general slow queries (>2s) per tutte le query non-promo per diagnostica
    if elapsed_ms >= 2000 and elapsed_ms < slow_thr:
        logger.warning(
            "bq_slow ms=%.0f label=%s bytes=%s slot_ms=%s job_id=%s",
            elapsed_ms,
            label or "unknown",
            bytes_proc,
            slot_ms,
            getattr(job, "job_id", None),
        )
    return [dict(row) for row in rows]


def run_query_admin(query: str, timeout_sec: int = 120) -> list[dict]:
    """Query di sola lettura per pannelli admin (INFORMATION_SCHEMA, __TABLES__)."""
    client = get_client()
    job = client.query(query, timeout=timeout_sec)
    rows = job.result(timeout=timeout_sec)
    return [dict(row) for row in rows]


def run_ddl_admin(ddl: str, timeout_sec: int = 300) -> None:
    """Esegue DDL admin (es. DROP TABLE). Usare solo da route protette admin."""
    client = get_client()
    job = client.query(ddl, timeout=timeout_sec)
    job.result(timeout=timeout_sec)
