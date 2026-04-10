"""
Verifica: primo caricamento (cold) vs secondo (warm cache).
Esegui: python scripts/diagnostics/test_cache_timing.py

Se il secondo è <1s, la cache funziona. Il primo sarà lento per le query BigQuery.
"""
import asyncio
import os
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
os.chdir(_ROOT)
sys.path.insert(0, str(_ROOT))


def main():
    from app.services._cache import _CACHE
    from app.services.market_intelligence import get_mi_all_years

    # Svuota cache per simulare cold start
    _CACHE.clear()
    brand_id = 1

    print("=" * 50)
    print("TEST CACHE: cold vs warm")
    print("=" * 50)

    async def run():
        # 1. Cold (prima chiamata)
        print("\n1. COLD (prima chiamata, cache vuota)...")
        t0 = time.perf_counter()
        r1 = await get_mi_all_years(brand_id)
        t_cold = time.perf_counter() - t0
        err = r1.get("error")
        years = list((r1.get("by_year") or {}).keys())
        print(f"   Tempo: {t_cold:.2f}s | Anni: {years} | Error: {err}")

        # 2. Warm (seconda chiamata, da cache)
        print("\n2. WARM (seconda chiamata, da cache)...")
        t0 = time.perf_counter()
        r2 = await get_mi_all_years(brand_id)
        t_warm = time.perf_counter() - t0
        print(f"   Tempo: {t_warm:.2f}s | Cache hit: {t_warm < 0.5}")

        print("\n" + "=" * 50)
        if t_warm < 0.5:
            print("OK: Cache funziona. Secondo caricamento istantaneo.")
        else:
            print("ATTENZIONE: Secondo caricamento ancora lento - cache potrebbe non funzionare.")
        print("=" * 50)

    asyncio.run(run())


if __name__ == "__main__":
    main()
