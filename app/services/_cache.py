"""Cache e helper condivisi per i servizi dashboard.
Redis (Memorystore su GCP): i valori persistono per tutto il TTL anche a giorni di distanza
e tra più istanze Cloud Run. Senza REDIS_URL la cache è solo RAM (persa a restart / scale-to-zero).

Env opzionali:
- CACHE_TTL_SECONDS — default 900 senza Redis; 86400 (24h) se REDIS_URL è impostato.
- CACHE_TTL_LONG_SECONDS — default 3600 senza Redis; 604800 (7g) se REDIS_URL è impostato.
"""
import json
import logging
import os
import time

logger = logging.getLogger(__name__)

_REDIS_URL_SET = bool(os.getenv("REDIS_URL", "").strip())


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
        return v if v > 0 else default
    except ValueError:
        return default


_TTL = _env_int("CACHE_TTL_SECONDS", 86400 if _REDIS_URL_SET else 900)
_TTL_LONG = _env_int("CACHE_TTL_LONG_SECONDS", 604800 if _REDIS_URL_SET else 3600)

# Redis client lazy-init
_redis_client = None
_redis_available = None


def _get_redis():
    """Lazy init Redis. Ritorna None se non configurato o errore."""
    global _redis_client, _redis_available
    if _redis_available is False:
        return None
    if _redis_client is not None:
        return _redis_client
    url = os.getenv("REDIS_URL", "").strip()
    if not url:
        _redis_available = False
        return None
    try:
        import redis
        _redis_client = redis.from_url(url, decode_responses=True)
        _redis_client.ping()
        _redis_available = True
        logger.info(
            "Redis cache enabled: %s (TTL default=%ss long=%ss)",
            url.split("@")[-1] if "@" in url else "configured",
            _TTL,
            _TTL_LONG,
        )
        return _redis_client
    except Exception as e:
        logger.warning("Redis unavailable, using in-memory cache: %s", e)
        _redis_available = False
        return None


# In-memory fallback
_CACHE: dict[str, tuple[dict, float]] = {}


def cache_key(prefix: str, **kwargs) -> str:
    parts = [str(kwargs.get(k, "")) for k in sorted(kwargs)]
    return prefix + ":" + "|".join(parts)


def get_cached(key: str, ttl: int | None = None):
    """Ritorna dato cached se presente e non scaduto. ttl: secondi (default _TTL)."""
    effective_ttl = ttl if ttl is not None else _TTL
    r = _get_redis()
    if r:
        try:
            raw = r.get(key)
            if raw:
                return json.loads(raw)
        except Exception as e:
            logger.warning("Redis get failed for %s: %s", key[:50], e)
    # Fallback in-memory
    now = time.monotonic()
    if key in _CACHE:
        data, ts = _CACHE[key]
        if now - ts < effective_ttl:
            return data
    return None


def _json_default(obj):
    """Serializza Decimal, datetime, ecc. per Redis."""
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return float(obj) if hasattr(obj, "__float__") else str(obj)


def set_cached(key: str, data: dict, ttl: int | None = None):
    """Salva in cache. ttl: secondi (default _TTL)."""
    effective_ttl = ttl if ttl is not None else _TTL
    r = _get_redis()
    if r:
        try:
            r.setex(key, effective_ttl, json.dumps(data, default=_json_default))
        except Exception as e:
            logger.warning("Redis set failed for %s: %s", key[:50], e)
    # Sempre aggiorna anche in-memory come fallback
    _CACHE[key] = (data, time.monotonic())


# Esporta per uso nei servizi
TTL_LONG = _TTL_LONG


def safe(fn, *a, **kw):
    try:
        return fn(*a, **kw)
    except Exception as e:
        logger.warning("Query failed: %s %s", fn.__name__, e)
        return []
