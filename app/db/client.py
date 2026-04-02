"""
Client BigQuery generico: connessione e esecuzione query parametrizzate.
Legge dati dal dataset mart (progetto GCP da variabile d'ambiente o default).
Carica .env dalla root del repo (non dalla cwd) per GOOGLE_APPLICATION_CREDENTIALS.
"""
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

import os
import threading
from typing import Optional

from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig


def _resolve_gcp_project_id() -> str:
    """GCP_PROJECT_ID vuoto nel launch (VS Code / shell) non deve mascherare il default."""
    v = (os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT") or "").strip()
    return v or "mediaexpertdashboard"


PROJECT_ID = _resolve_gcp_project_id()
DATASET = "mart"

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


def run_query(
    query: str,
    params: Optional[list] = None,
) -> list[dict]:
    """
    Esegue una query parametrizzata e restituisce le righe come lista di dict.
    params: lista di bigquery.ScalarQueryParameter (nome, tipo, valore).
    """
    client = get_client()
    job_config = QueryJobConfig()
    if params:
        job_config.query_parameters = params
    job = client.query(query, job_config=job_config, timeout=30)
    rows = job.result(timeout=30)
    return [dict(row) for row in rows]
