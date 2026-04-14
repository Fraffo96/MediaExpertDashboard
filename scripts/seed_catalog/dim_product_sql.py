"""Generazione SQL INSERT per mart.dim_product (mix calibrato su proxy di mercato PL/EU)."""

from __future__ import annotations

from .brand_weights import (
    allocate_integers_proportional,
    brand_pair_mass,
    load_brand_parent_weights,
    load_catalog_share_multipliers,
)
from .constants import (
    BRAND_NAMES,
    MASS_BRANDS,
    PARENT_TO_SUB,
    SPECIALIST_BRANDS,
    SUBCAT_NAMES,
    SUBCAT_PRICE,
    VARIANTS,
    subcategories_for_brand_parent,
)
from .env_overrides import effective_brand_focus, effective_brand_promo_affinity
from .market_reality import (
    brand_phone_mass_multiplier,
    brand_wearables_mass_multiplier,
    merge_parent_weights_json_and_market,
)


def _json_normalized_parent_weights(
    brand_id: int,
    parents: list[int],
    wdb: dict[int, dict[int, float]],
) -> dict[int, float]:
    raw = wdb.get(brand_id, {})
    if not raw:
        u = 1.0 / len(parents) if parents else 0.0
        return {p: u for p in parents}
    vals = {p: float(raw.get(p, raw.get(str(p), 0.0))) for p in parents}
    s = sum(max(0.0, vals[p]) for p in parents)
    if s < 1e-12:
        u = 1.0 / len(parents)
        return {p: u for p in parents}
    return {p: max(0.0, vals[p]) / s for p in parents}


def _allowed_subcat_union(brand_id: int, parents: list[int]) -> set[int]:
    out: set[int] = set()
    for p in parents:
        out.update(subcategories_for_brand_parent(brand_id, p))
    return out


def build_brand_subcat_pairs() -> list[tuple[int, int]]:
    pairs = []
    for bid, parents in effective_brand_focus().items():
        for p in parents:
            for sid in subcategories_for_brand_parent(bid, p):
                pairs.append((bid, sid))
    return pairs


def _build_weighted_slots(n: int, focus: dict[int, list[int]], wdb: dict[int, dict[int, float]]) -> list[tuple[int, int]]:
    """Lista (brand_id, subcategory_id) di lunghezza n; pesi = JSON × prior mercato × share catalogo × mobile PL."""
    if n <= 0:
        return []
    brands = sorted(focus.keys())
    shares = load_catalog_share_multipliers()
    masses: dict[int, float] = {}
    for bid in brands:
        parents = focus[bid]
        jw = _json_normalized_parent_weights(bid, parents, wdb)
        pw = merge_parent_weights_json_and_market(jw, parents)
        allowed_union = _allowed_subcat_union(bid, parents)
        masses[bid] = (
            brand_pair_mass(bid, parents, pw)
            * shares.get(bid, 1.0)
            * brand_phone_mass_multiplier(bid, allowed_union)
            * brand_wearables_mass_multiplier(bid, allowed_union)
        )
    total_mass = sum(masses.values())
    if total_mass < 1e-12:
        pairs = build_brand_subcat_pairs()
        return [pairs[i % len(pairs)] for i in range(n)]
    n_brand = allocate_integers_proportional(masses, n)
    slots: list[tuple[int, int]] = []
    for bid in brands:
        n_b = n_brand.get(bid, 0)
        if n_b <= 0:
            continue
        parents = focus[bid]
        jw = _json_normalized_parent_weights(bid, parents, wdb)
        pw = merge_parent_weights_json_and_market(jw, parents)
        n_parent = allocate_integers_proportional(pw, n_b)
        for p in parents:
            n_p = n_parent.get(p, 0)
            if n_p <= 0:
                continue
            subs = subcategories_for_brand_parent(bid, p)
            if not subs:
                continue
            k = len(subs)
            base = n_p // k
            extra = n_p % k
            for j, sid in enumerate(subs):
                cnt = base + (1 if j < extra else 0)
                for _ in range(cnt):
                    slots.append((bid, sid))
    if len(slots) != n:
        pairs = build_brand_subcat_pairs()
        while len(slots) < n:
            slots.append(pairs[len(slots) % len(pairs)])
        slots = slots[:n]
    mixed = list(enumerate(slots))
    mixed.sort(
        key=lambda it: (it[0] * 2654435761 + it[1][0] * 2246822519 + it[1][1] * 3266489917) & 0x7FFFFFFF
    )
    return [t for _, t in mixed]


def generate_products(n: int = 1200) -> str:
    focus = effective_brand_focus()
    wdb = load_brand_parent_weights()
    slots = _build_weighted_slots(n, focus, wdb)
    promo_aff = effective_brand_promo_affinity()
    products = []
    pid = 10001
    for i, (bid, sid) in enumerate(slots):
        parent = next((p for p, subs in PARENT_TO_SUB.items() if sid in subs), 1)
        avg, spread, prem_share = SUBCAT_PRICE.get(sid, (1000, 0.4, 0.3))
        if bid in MASS_BRANDS:
            avg = int(avg * 0.7)
            prem_share *= 0.5
        if bid in SPECIALIST_BRANDS:
            avg = int(avg * 1.2)
            prem_share = min(0.9, prem_share * 1.3)
        aff = promo_aff.get(bid)
        if aff is not None:
            prem_share = max(0.02, min(0.95, prem_share * aff))
        var = 1 + (spread * ((i * 17 + 31) % 100 - 50) / 100)
        price = max(99, int(avg * var / 50) * 50)
        premium = (i * 13 + 7) % 100 < int(prem_share * 100)
        variant = VARIANTS[(i * 11) % len(VARIANTS)]
        name = f"{BRAND_NAMES[bid]} {SUBCAT_NAMES[sid]}{variant}".strip().replace("'", "''")
        launch = 2020 + (i % 5)
        products.append(f"  ({pid},'{name}',{bid},{parent},{sid},{price},{launch},{str(premium).upper()})")
        pid += 1
    return (
        "INSERT mart.dim_product (product_id, product_name, brand_id, category_id, subcategory_id, "
        "price_pln, launch_year, premium_flag) VALUES\n" + ",\n".join(products)
    )
