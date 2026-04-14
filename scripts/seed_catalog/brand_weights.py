"""Caricamento pesi brand × parent category da JSON (proxy revenue mix)."""

from __future__ import annotations

import json
from pathlib import Path

_WEIGHTS_FILE = Path(__file__).resolve().parent / "brand_parent_revenue_weights.json"


def load_brand_parent_weights() -> dict[int, dict[int, float]]:
    if not _WEIGHTS_FILE.exists():
        return {}
    try:
        raw = json.loads(_WEIGHTS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    brands = raw.get("brands")
    if not isinstance(brands, dict):
        return {}
    out: dict[int, dict[int, float]] = {}
    for k, v in brands.items():
        try:
            bid = int(k)
        except (TypeError, ValueError):
            continue
        if not isinstance(v, dict):
            continue
        wmap: dict[int, float] = {}
        for pk, wv in v.items():
            try:
                pid = int(pk)
                wmap[pid] = float(wv)
            except (TypeError, ValueError):
                continue
        if wmap:
            out[bid] = wmap
    return out


def load_catalog_share_multipliers() -> dict[int, float]:
    if not _WEIGHTS_FILE.exists():
        return {}
    try:
        raw = json.loads(_WEIGHTS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    mul = raw.get("catalog_share_multiplier")
    if not isinstance(mul, dict):
        return {}
    out: dict[int, float] = {}
    for k, v in mul.items():
        try:
            out[int(k)] = max(0.1, float(v))
        except (TypeError, ValueError):
            continue
    return out


def normalized_parent_weights(
    brand_id: int,
    parents: list[int],
    wdb: dict[int, dict[int, float]],
) -> dict[int, float]:
    """Pesi su `parents` che sommano a 1; se assenti nel JSON → uniforme."""
    raw = wdb.get(brand_id, {})
    vals: dict[int, float] = {}
    for p in parents:
        vals[p] = float(raw.get(p, raw.get(str(p), 0.0)))
    s = sum(vals.values())
    if s < 1e-12:
        u = 1.0 / len(parents) if parents else 0.0
        return {p: u for p in parents}
    return {p: vals[p] / s for p in parents}


def allocate_integers_proportional(weights: dict[int, float], total: int) -> dict[int, int]:
    """Largest remainder: chiavi = stesso insieme di weights, somma = total."""
    if total <= 0 or not weights:
        return {k: 0 for k in weights}
    keys = list(weights.keys())
    s = sum(max(0.0, float(weights[k])) for k in keys)
    if s < 1e-12:
        u = total // len(keys)
        r = total - u * len(keys)
        return {k: u + (1 if i < r else 0) for i, k in enumerate(keys)}
    raw_fracs = {k: total * max(0.0, float(weights[k])) / s for k in keys}
    floors = {k: int(raw_fracs[k]) for k in keys}
    rem = total - sum(floors.values())
    order = sorted(keys, key=lambda k: (raw_fracs[k] - floors[k]), reverse=True)
    for i in range(rem):
        floors[order[i]] += 1
    return floors


def brand_pair_mass(brand_id: int, parents: list[int], parent_w: dict[int, float]) -> float:
    """Massa per ripartire `n` tra i brand: somma peso(parent) × numero subcat effettive (allowlist)."""
    from .constants import subcategories_for_brand_parent

    total = 0.0
    for p in parents:
        n = len(subcategories_for_brand_parent(brand_id, p))
        if n > 0:
            total += float(parent_w.get(p, 0.0)) * n
    return total
