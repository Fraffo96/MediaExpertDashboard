"""
Diagnostica: verifica se le API usano le tabelle precalcolate e misura i tempi.
Esegui: python scripts/diagnostics/diagnose_precalc.py

Richiede: utente loggato con brand_id (usa brand_id=1 per test).
"""
import asyncio
import os
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
os.chdir(_ROOT)
sys.path.insert(0, str(_ROOT))

# Patch per loggare precalc vs live
_precalc_log = []


def _log_precalc(service: str, scope: str, used: bool, detail: str = ""):
    msg = f"{'PRECALC' if used else 'LIVE'}: {service}.{scope}" + (f" ({detail})" if detail else "")
    _precalc_log.append(msg)
    print(f"  [{msg}]")


# Monkey-patch is_full_year_period per loggare
from app.db.queries import precalc as precalc_mod

_orig_is_full = precalc_mod.is_full_year_period


def _patched_is_full(ps, pe):
    r = _orig_is_full(ps, pe)
    return r


# Patch dei servizi per loggare
from app.services import market_intelligence as mi_svc
from app.services import brand_comparison as bc_svc

# Wrapper che logga precalc
def _wrap_mi_sales():
    orig = mi_svc.get_mi_sales
    async def wrapped(ps, pe, brand_id, cat_ids, sub_ids, sub_cat_id=None):
        use = mi_svc._use_precalc_sales(ps, pe, brand_id, cat_ids, sub_ids, sub_cat_id)
        _log_precalc("MI", "sales", use, f"ps={ps} pe={pe}")
        return await orig(ps, pe, brand_id, cat_ids, sub_ids, sub_cat_id)
    return wrapped

# Meglio: aggiungiamo logging direttamente nel modulo precalc
# Creiamo uno script che chiama le funzioni e verifica is_full_year_period
# e get_multi_year_full_years per i parametri usati


def main():
    from app.db.queries.precalc import is_full_year_period, get_multi_year_full_years

    print("=" * 60)
    print("DIAGNOSTICA PRECALC - Verifica parametri e condizioni")
    print("=" * 60)

    tests = [
        ("2024-01-01", "2024-12-31", "Anno singolo (MI/BC all)"),
        ("2023-01-01", "2025-12-31", "Range multi-anno (MI all-years incremental_yoy)"),
        ("2023-01-01", "2025-12-31", "Competitors (BC loadBase)"),
        ("2024-06-01", "2024-08-31", "Trimestre (dovrebbe essere LIVE)"),
    ]

    for ps, pe, desc in tests:
        full = is_full_year_period(ps, pe)
        multi = get_multi_year_full_years(ps, pe)
        print(f"\n{desc}")
        print(f"  ps={ps} pe={pe}")
        print(f"  is_full_year_period -> {full} {'<- PRECALC per anno singolo' if full else '<- LIVE'}")
        print(f"  get_multi_year_full_years -> {multi} {'<- PRECALC incremental_yoy/competitors' if multi else ''}")

    print("\n" + "=" * 60)
    print("CHIAMATE API REALI (misura tempi)")
    print("=" * 60)

    async def run_tests():
        from app.services.market_intelligence import get_mi_all_years
        from app.services.brand_comparison import get_bc_all_years, get_bc_competitors

        brand_id = 1
        competitor_id = 2

        # 1. MI all-years
        print("\n1. GET /api/market-intelligence/all-years (brand_id=1)")
        t0 = time.perf_counter()
        try:
            r = await get_mi_all_years(brand_id)
            elapsed = time.perf_counter() - t0
            err = r.get("error")
            years = list((r.get("by_year") or {}).keys())
            print(f"   Tempo: {elapsed:.2f}s | Anni: {years} | Error: {err}")
        except Exception as e:
            print(f"   ERRORE: {e}")
            import traceback
            traceback.print_exc()

        # 2. BC competitors (usa ps=2023-01-01 pe=2025-12-31)
        print("\n2. GET /api/brand-comparison/competitors (ps=2023-01-01 pe=2025-12-31)")
        t0 = time.perf_counter()
        try:
            r = await get_bc_competitors("2023-01-01", "2025-12-31", brand_id)
            elapsed = time.perf_counter() - t0
            comps = len(r.get("competitors") or [])
            print(f"   Tempo: {elapsed:.2f}s | Competitors: {comps} | Error: {r.get('error')}")
        except Exception as e:
            print(f"   ERRORE: {e}")

        # 3. BC all-years
        print("\n3. GET /api/brand-comparison/all-years (brand_id=1 competitor_id=2)")
        t0 = time.perf_counter()
        try:
            r = await get_bc_all_years(brand_id, competitor_id)
            elapsed = time.perf_counter() - t0
            err = r.get("error")
            years = list((r.get("by_year") or {}).keys())
            print(f"   Tempo: {elapsed:.2f}s | Anni: {years} | Error: {err}")
        except Exception as e:
            print(f"   ERRORE: {e}")
            import traceback
            traceback.print_exc()

        # 4. Query BigQuery diretta per confronto
        print("\n4. Verifica: query precalc_sales_agg vs fact_sales_daily")
        from app.db.queries.precalc import query_sales_value_by_category_from_precalc
        from app.db.queries import market_intelligence as mi_q

        year = 2024
        t0 = time.perf_counter()
        precalc_rows = query_sales_value_by_category_from_precalc(year, brand_id)
        t_precalc = time.perf_counter() - t0

        t0 = time.perf_counter()
        live_rows = mi_q.query_sales_value_by_category("2024-01-01", "2024-12-31", brand_id)
        t_live = time.perf_counter() - t0

        print(f"   PRECALC: {t_precalc:.2f}s ({len(precalc_rows)} righe)")
        print(f"   LIVE:    {t_live:.2f}s ({len(live_rows)} righe)")
        print(f"   Speedup: {t_live / t_precalc:.1f}x" if t_precalc > 0 else "")

    asyncio.run(run_tests())

    print("\n" + "=" * 60)
    print("Fine diagnostica")
    print("=" * 60)


if __name__ == "__main__":
    main()
