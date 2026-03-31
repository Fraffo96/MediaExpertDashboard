"""
Benchmark API e BigQuery – misura tempi reali di caricamento dashboard.
Uso: python -u scripts/benchmark_api.py
Richiede: .env con GCP, database mart popolato.
"""
import asyncio
import functools
_print = functools.partial(print, flush=True)
import os
import sys
import time

# Setup path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("GCP_PROJECT_ID", "mediaexpertdashboard")

# Clear cache prima del benchmark (cold start)
def clear_cache():
    try:
        from app.services._cache import _CACHE
        _CACHE.clear()
        _print("[OK] Cache in-memory svuotata")
    except Exception as e:
        _print(f"[WARN] Cache clear: {e}")


async def benchmark_mi_all_years(brand_id=8):
    """Market Intelligence: get_mi_all_years (endpoint principale)."""
    from app.services.market_intelligence import get_mi_all_years

    clear_cache()
    t0 = time.perf_counter()
    out = await get_mi_all_years(brand_id)
    elapsed = time.perf_counter() - t0

    if out.get("error"):
        _print(f"[ERR] MI all-years: {out['error']}")
        return None
    years = out.get("available_years", [])
    by_year = out.get("by_year", {})
    _print(f"[MI all-years] brand={brand_id} | anni={len(years)} | by_year keys={len(by_year)} | {elapsed:.2f}s")
    return elapsed


async def benchmark_bc_all_years(brand_id=8, competitor_id=1):
    """Brand Comparison: get_bc_all_years."""
    from app.services.brand_comparison import get_bc_all_years

    clear_cache()
    t0 = time.perf_counter()
    out = await get_bc_all_years(brand_id, competitor_id)
    elapsed = time.perf_counter() - t0

    if out.get("error"):
        _print(f"[ERR] BC all-years: {out['error']}")
        return None
    years = out.get("available_years", [])
    by_year = out.get("by_year", {})
    _print(f"[BC all-years] brand={brand_id} comp={competitor_id} | anni={len(years)} | {elapsed:.2f}s")
    return elapsed


async def benchmark_mi_cold_vs_cached(brand_id=8, runs=2):
    """Confronta cold (no cache) vs cached."""
    from app.services.market_intelligence import get_mi_all_years

    _print("\n--- Market Intelligence: cold vs cached ---")
    times = []
    for i in range(runs):
        if i == 0:
            clear_cache()
        t0 = time.perf_counter()
        out = await get_mi_all_years(brand_id)
        elapsed = time.perf_counter() - t0
        if not out.get("error"):
            times.append(elapsed)
            print(f"  Run {i+1}: {elapsed:.2f}s" + (" (cold)" if i == 0 else " (cached)"))
    if len(times) >= 2:
        speedup = times[0] / times[1] if times[1] > 0 else 0
        _print(f"  Speedup cache: {speedup:.1f}x")
    return times


async def benchmark_raw_bigquery():
    """Query BigQuery dirette per baseline."""
    from app.db.client import run_query

    _print("\n--- BigQuery raw (baseline) ---")
    t0 = time.perf_counter()
    years = run_query("SELECT DISTINCT EXTRACT(YEAR FROM date) AS y FROM mart.fact_sales_daily ORDER BY y")
    elapsed = time.perf_counter() - t0
    _print(f"  anni disponibili: {elapsed:.2f}s | rows={len(years)}")
    return elapsed


async def main():
    _print("=" * 50)
    _print("BENCHMARK API / BigQuery")
    _print("=" * 50)

    # 1. Raw BigQuery
    try:
        await benchmark_raw_bigquery()
    except Exception as e:
        _print(f"[ERR] BigQuery: {e}")

    # 2. MI all-years (cold)
    try:
        t_mi = await benchmark_mi_all_years(brand_id=8)
    except Exception as e:
        print(f"[ERR] MI: {e}")
        t_mi = None

    # 3. BC all-years (cold)
    try:
        t_bc = await benchmark_bc_all_years(brand_id=8, competitor_id=1)
    except Exception as e:
        _print(f"[ERR] BC: {e}")
        t_bc = None

    # 4. MI cold vs cached
    try:
        await benchmark_mi_cold_vs_cached(brand_id=8)
    except Exception as e:
        _print(f"[ERR] MI cold/cached: {e}")

    _print("\n" + "=" * 50)
    if t_mi:
        _print(f"MI all-years (cold): {t_mi:.2f}s")
    if t_bc:
        _print(f"BC all-years (cold): {t_bc:.2f}s")
    _print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
