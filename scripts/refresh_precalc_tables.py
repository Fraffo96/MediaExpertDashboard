"""
Refresh tabelle precalcolate su BigQuery.
Implementazione modulare: scripts/precalc_refresh/ (sql_steps + runner).

Uso: python scripts/refresh_precalc_tables.py
Richiede: gcloud auth application-default login, GCP_PROJECT_ID (default: mediaexpertdashboard)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("GCP_PROJECT_ID", "mediaexpertdashboard")

_scripts = Path(__file__).resolve().parent
if str(_scripts) not in sys.path:
    sys.path.insert(0, str(_scripts))

from precalc_refresh.runner import main

if __name__ == "__main__":
    main()
