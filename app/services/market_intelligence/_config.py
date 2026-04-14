"""Costanti e semafori condivisi per Market Intelligence."""
import asyncio
import logging

logger = logging.getLogger(__name__)

PRECALC_ONLY_ERR = "Precalc tables only. Use full year period (YYYY-01-01 to YYYY-12-31)."

# Limiti di concorrenza verso BigQuery durante il caricamento multi-anno.
# Valori ridotti per evitare saturazione BQ quando prewarm e richieste utente
# girano in parallelo (in precedenza 4 anni × ~10 query + 28 scope = 68 query concorrenti).
_MI_YEAR_LOAD_SEM = asyncio.Semaphore(2)
_MI_INCR_SCOPE_SEM = asyncio.Semaphore(8)
