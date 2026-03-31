"""Inizializzazione auth: Firestore (utenti persistenti su Cloud Run)."""
import logging

logger = logging.getLogger(__name__)


def get_db():
    """Rimosso: l'auth non usa più SQLAlchemy. Tenuto per compatibilità import legacy."""
    raise RuntimeError("get_db non supportato: usare Firestore (app.auth.firestore_store)")


def init_db():
    """Assicura admin predefinito e contatori su Firestore."""
    try:
        from app.auth.firestore_store import ensure_default_admin

        ensure_default_admin()
    except Exception as e:
        logger.error("init_db (Firestore): %s", e)
