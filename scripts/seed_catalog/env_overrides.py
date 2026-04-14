"""Override da variabili d'ambiente per il catalogo seed."""

from __future__ import annotations

import json
import os

from .constants import BRAND_FOCUS


def effective_brand_focus() -> dict[int, list[int]]:
    raw = (os.environ.get("SEED_BRAND_FOCUS_JSON") or "").strip()
    if not raw:
        return BRAND_FOCUS
    try:
        d = json.loads(raw)
        out = dict(BRAND_FOCUS)
        for k, v in d.items():
            bid = int(k)
            if isinstance(v, list):
                out[bid] = [int(x) for x in v]
        return out
    except (TypeError, ValueError, json.JSONDecodeError):
        return BRAND_FOCUS


def effective_brand_promo_affinity() -> dict[int, float]:
    """Moltiplicatore sul mix premium per brand (1 = default). Da SEED_BRAND_PROMO_AFFINITY_JSON."""
    raw = (os.environ.get("SEED_BRAND_PROMO_AFFINITY_JSON") or "").strip()
    if not raw:
        return {}
    try:
        d = json.loads(raw)
        if not isinstance(d, dict):
            return {}
        out: dict[int, float] = {}
        for k, v in d.items():
            try:
                bid = int(k)
                fv = float(v)
                if fv > 0:
                    out[bid] = fv
            except (TypeError, ValueError):
                continue
        return out
    except json.JSONDecodeError:
        return {}
