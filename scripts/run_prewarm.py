#!/usr/bin/env python3
"""Riscalda la cache applicativa (MI all-years, BC, CLP, marketing, filters) senza login browser.

Richiede lo stesso .env della root del repo (GCP / Redis se usi Memorystore in locale tramite tunnel).

Da root repository:
  set PREWARM_BRAND_IDS=1,2,8   (opzionale; altrimenti legge brand da Firestore o usa 1)
  $env:PYTHONPATH = (Get-Location).Path
  python scripts/run_prewarm.py

In cloud (dopo deploy con PREWARM_TOKEN):
  curl -sS "https://<host>/internal/prewarm" -H "X-Prewarm-Token: <token>"
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    sys.path.insert(0, str(ROOT))
    os.chdir(ROOT)
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")

    async def _run():
        from app.services.prewarm import prewarm_cache

        return await prewarm_cache()

    out = asyncio.run(_run())
    print(out)


if __name__ == "__main__":
    main()
