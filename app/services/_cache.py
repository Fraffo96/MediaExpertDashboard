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

# Prefissi cache_key(...) usati dall'app (per SCAN/delete su Redis senza FLUSHDB).
# Inclusi prefissi legacy (vecchie revisioni) per evitare chiavi orfane.
_CACHE_KEY_PREFIXES: tuple[str, ...] = (
    "bc_all_years",
    "bc_all_years_v2_duel_prev",
    "bc_base",
    "bc_competitors",
    "bc_discount",
    "bc_peak",
    "bc_promo",
    "bc_sales",
    "bc_sales_v3_duel_prev",
    "basic",
    "basic_granular",
    "clp_active",
    "clp_seg",
    "clp_sku",
    "customer",
    "mi_all_years",
    "mi_all_years_v2",
    "mi_all_years_v3",
    "mi_all_years_v4",
    "mi_base",
    "mi_discount_v2_excl_brand",
    "mi_incr_yoy_v2_dedup_ch",
    "mi_peak_v3_comp_avg",
    "mi_promo_v4_weighted_scope",
    "mi_sales",
    "mi_seg_sku_v3_mix46",
    "mi_top_prod_v2",
    "mkt_need",
    "mkt_purch_v2",
    "mkt_seg_cat",
    "mkt_seg_v2",
    "pc_v3_fast_ui",
    "products",
    "promo",
    "simulation",
)


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


def clear_service_cache(*, flush_redis_db: bool = False) -> dict:
    """Svuota cache RAM dell'app e, se Redis è configurato, elimina le chiavi dashboard.

    - Default: SCAN + DELETE per ogni prefisso in ``_CACHE_KEY_PREFIXES`` (senza FLUSHDB).
    - ``flush_redis_db=True``: esegue FLUSHDB sull'istanza (solo Redis dedicato alla dashboard).
    """
    global _CACHE
    n_mem = len(_CACHE)
    _CACHE.clear()
    redis_deleted = 0
    redis_flushed = False
    r = _get_redis()
    if r:
        try:
            if flush_redis_db:
                r.flushdb()
                redis_flushed = True
                logger.warning("clear_service_cache: Redis FLUSHDB eseguito (istanza intera).")
            else:
                for prefix in _CACHE_KEY_PREFIXES:
                    pattern = f"{prefix}:*"
                    cursor = 0
                    while True:
                        cursor, keys = r.scan(cursor=cursor, match=pattern, count=500)
                        if keys:
                            redis_deleted += int(r.delete(*keys))
                        if cursor == 0:
                            break
        except Exception as e:
            logger.warning("Redis cache clear failed: %s", e)
    try:
        from app.db.queries.shared import clear_available_years_cache

        clear_available_years_cache()
    except Exception as e:
        logger.warning("clear_available_years_cache: %s", e)
    logger.info(
        "clear_service_cache: memory=%s redis_keys_deleted=%s redis_flushed_db=%s",
        n_mem,
        redis_deleted,
        redis_flushed,
    )
    return {
        "memory_entries_cleared": n_mem,
        "redis_keys_deleted": redis_deleted,
        "redis_flushed_db": redis_flushed,
    }


def safe(fn, *a, **kw):
    try:
        return fn(*a, **kw)
    except Exception as e:
        logger.warning("Query failed: %s %s", fn.__name__, e)
        return []
