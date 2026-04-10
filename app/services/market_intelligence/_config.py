"""Costanti e semafori condivisi per Market Intelligence."""
import asyncio
import logging

logger = logging.getLogger(__name__)

PRECALC_ONLY_ERR = "Precalc tables only. Use full year period (YYYY-01-01 to YYYY-12-31)."

_MI_YEAR_LOAD_SEM = asyncio.Semaphore(4)
_MI_INCR_SCOPE_SEM = asyncio.Semaphore(28)
