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

    from app.services.data_jobs import get_data_job, profile_volumes_to_env, update_data_job

    job_doc = get_data_job(job_id) or {}
    profile: dict = {}
    inline = job_doc.get("profile_inline")
    if isinstance(inline, dict) and inline:
        profile = inline
    else:
        raw_prof = os.environ.get("SEED_PROFILE_JSON", "").strip()
        if raw_prof:
            try:
                profile = json.loads(raw_prof)
            except json.JSONDecodeError:
                profile = {}

    update_data_job(job_id, "running", message="started")
    env = {**os.environ}
    env.update(profile_volumes_to_env(profile))

    compiled_path: Path | None = None
    if isinstance(profile, dict) and profile.get("profile_version") == 2:
        sys.path.insert(0, str(REPO_ROOT / "scripts"))
        from seed_planner.compiler import compile_seed_profile

        compiled = compile_seed_profile(profile)
        cache = REPO_ROOT / ".seed_cache"
        cache.mkdir(exist_ok=True)
        compiled_path = cache / f"{job_id}_compiled.json"
        compiled_path.write_text(json.dumps(compiled, indent=2), encoding="utf-8")
        env["SEED_COMPILED_PATH"] = str(compiled_path)
        g = compiled.get("global") or {}
        env["SEED_NUM_CUSTOMERS"] = str(int(g.get("num_customers", 24000)))
        env["SEED_NUM_ORDERS"] = str(int(g.get("num_orders", 380000)))
        env["SEED_NUM_PRODUCTS"] = str(int(g.get("num_products", 1200)))
        for k, v in (compiled.get("env") or {}).items():
            if v:
                env[str(k)] = str(v)

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

        verify_report = None
        if job_type == "full_seed":
            try:
                from app.services.seed_verify import bq_seed_verify_report

                verify_report = bq_seed_verify_report()
            except Exception as ve:
                verify_report = {"error": str(ve)[:500]}

        update_data_job(job_id, "ok", message="completato", verify_report=verify_report)
    except Exception as e:
        err = (str(e) or type(e).__name__)[:2000]
        update_data_job(job_id, "error", message="fallito", error_snippet=err)
        print(traceback.format_exc(), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
