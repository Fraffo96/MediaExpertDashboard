"""Esecuzione script refresh tabelle precalcolate (admin)."""
from __future__ import annotations

import subprocess
import sys
import time

from app.jinja_env import BASE_DIR


def run_refresh_precalc() -> tuple[bool, str, float]:
    """Esegue scripts/refresh_precalc_tables.py. Ritorna (ok, message, duration_sec)."""
    from app.db.client import PROJECT_ID as gcp_for_script

    script = BASE_DIR.parent / "scripts" / "refresh_precalc_tables.py"
    if not script.exists():
        return False, "Script refresh_precalc_tables.py non trovato", 0.0
    import os

    t0 = time.time()
    try:
        proc = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            timeout=600,
            cwd=str(BASE_DIR.parent),
            env={**os.environ, "GCP_PROJECT_ID": gcp_for_script},
        )
        elapsed = time.time() - t0
        if proc.returncode != 0:
            return False, proc.stderr or proc.stdout or f"Exit code {proc.returncode}", elapsed
        return True, proc.stdout or "OK", elapsed
    except subprocess.TimeoutExpired:
        return False, "Timeout (600s)", time.time() - t0
    except Exception as e:
        return False, str(e), time.time() - t0
