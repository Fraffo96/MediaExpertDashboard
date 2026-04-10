#!/usr/bin/env python3
"""Worker pipeline dati: aggiorna Firestore dashboard_data_jobs; esegue precalc o full_seed."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    sys.path.insert(0, str(REPO_ROOT))
    os.chdir(REPO_ROOT)

    from dotenv import load_dotenv

    load_dotenv(REPO_ROOT / ".env")

    job_id = os.environ.get("DATA_JOB_ID", "").strip()
    job_type = os.environ.get("DATA_JOB_TYPE", "precalc").strip()
    if not job_id:
        print("DATA_JOB_ID mancante", file=sys.stderr)
        sys.exit(2)

    profile: dict = {}
    raw_prof = os.environ.get("SEED_PROFILE_JSON", "").strip()
    if raw_prof:
        try:
            profile = json.loads(raw_prof)
        except json.JSONDecodeError:
            profile = {}

    from app.services.data_jobs import profile_to_env, update_data_job

    update_data_job(job_id, "running", message="started")
    env = {**os.environ}
    if profile:
        env.update(profile_to_env(profile))
    try:
        if job_type == "precalc":
            r = subprocess.run(
                [sys.executable, str(REPO_ROOT / "scripts" / "refresh_precalc_tables.py")],
                cwd=str(REPO_ROOT),
                env=env,
                capture_output=True,
                text=True,
                timeout=7200,
            )
            if r.returncode != 0:
                raise RuntimeError(r.stderr or r.stdout or f"exit {r.returncode}")
        elif job_type == "full_seed":
            r1 = subprocess.run(
                [sys.executable, str(REPO_ROOT / "scripts" / "generate_seed_data.py")],
                cwd=str(REPO_ROOT),
                env=env,
                capture_output=True,
                text=True,
                timeout=3600,
            )
            if r1.returncode != 0:
                raise RuntimeError(r1.stderr or r1.stdout or "generate_seed_data failed")
            r2 = subprocess.run(
                [sys.executable, str(REPO_ROOT / "scripts" / "run_bigquery_schema.py")],
                cwd=str(REPO_ROOT),
                env=env,
                capture_output=True,
                text=True,
                timeout=7200,
            )
            if r2.returncode != 0:
                raise RuntimeError(r2.stderr or r2.stdout or "run_bigquery_schema failed")
            r3 = subprocess.run(
                [sys.executable, str(REPO_ROOT / "scripts" / "refresh_precalc_tables.py")],
                cwd=str(REPO_ROOT),
                env=env,
                capture_output=True,
                text=True,
                timeout=7200,
            )
            if r3.returncode != 0:
                raise RuntimeError(r3.stderr or r3.stdout or "refresh_precalc failed")
        else:
            raise ValueError(f"job_type sconosciuto: {job_type}")
        update_data_job(job_id, "ok", message="completato")
    except Exception as e:
        err = (str(e) or type(e).__name__)[:2000]
        update_data_job(job_id, "error", message="fallito", error_snippet=err)
        print(traceback.format_exc(), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
