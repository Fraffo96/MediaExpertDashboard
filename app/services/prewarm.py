"""Pre-warming cache per Cloud Run. Chiamato da Cloud Scheduler per tenere la cache calda."""
import asyncio
import logging

from app.auth.database import SessionLocal
from app.auth.models import User

logger = logging.getLogger(__name__)


async def prewarm_cache():
    """Riscalda la cache per tutti i brand con utenti attivi. MI: solo ultimo anno disponibile + filters."""
    from app.db.queries.shared import query_available_years
    from app.services.market_intelligence import get_mi_all
    from app.services.filters import get_filters

    db = SessionLocal()
    try:
        users = db.query(User).filter(User.is_active == True, User.brand_id.isnot(None)).all()
        brand_ids = list({u.brand_id for u in users if u.brand_id})
        if not brand_ids:
            brand_ids = [1]  # fallback per dev locale
            logger.info("Prewarm: no brand users, warming brand_id=1")

        years = await asyncio.to_thread(query_available_years)
        ys = list(years) if years else [2024]
        y_last = ys[-1]
        ps, pe = f"{y_last}-01-01", f"{y_last}-12-31"

        tasks = []
        for bid in brand_ids:
            tasks.append(get_mi_all(ps, pe, bid))
        tasks.append(get_filters())  # filters è globale, non per brand

        results = await asyncio.gather(*tasks, return_exceptions=True)
        ok = sum(1 for r in results if not isinstance(r, Exception) and (not isinstance(r, dict) or not r.get("error")))
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            logger.warning("Prewarm: %d errors", len(errors))
        logger.info("Prewarm: warmed %d/%d (brands: %s)", ok, len(tasks), brand_ids)
        return {"warmed": ok, "brands": brand_ids, "errors": len(errors)}
    finally:
        db.close()
