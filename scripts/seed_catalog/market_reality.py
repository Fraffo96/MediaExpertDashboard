"""
Parametri di mercato (proxy PL / EU CE) per la generazione seed.

Fonti e caveat: docs/SEED_MARKET_RESEARCH.md
"""

from __future__ import annotations

# Prior valore CE/proxy retailer RTV-AGD (somma 1). Ricalibrato vs mix troppo “IT-heavy”.
MARKET_PARENT_VALUE_PRIOR: dict[int, float] = {
    1: 0.16,  # TV
    2: 0.20,  # smartphones / tablets / wearables
    3: 0.22,  # computers
    4: 0.08,  # gaming
    5: 0.17,  # large appliances
    6: 0.10,  # small appliances
    7: 0.04,  # audio
    8: 0.02,  # smart home
    9: 0.01,  # health / beauty tech
    10: 0.01,  # photo
}

# Statcounter mobile vendor Poland (proxy installato / traffico, non spedizioni).
_PHONE_VENDOR_SCORE: dict[int, float] = {
    8: 30.41,  # Apple
    1: 26.37,  # Samsung
    39: 12.88,  # Google
    9: 10.03,  # Xiaomi
    13: 5.06,  # Motorola
    10: 2.0,
    11: 1.8,
    12: 1.6,
    14: 1.5,
    56: 3.8,  # Honor
    57: 3.4,  # Vivo
    58: 2.9,  # Nothing
    59: 4.2,  # POCO
}

_PHONE_BASELINE = 14.0  # ordine di grandezza % “tier1” per rapporto moltiplicatore


def merge_parent_weights_json_and_market(
    json_weights: dict[int, float],
    parents: list[int],
    market_prior: dict[int, float] | None = None,
) -> dict[int, float]:
    """Combina pesi JSON con prior mercato: blend70/30 così il JSON non viene schiacciato sulle macro basse (es. bianchi)."""
    prior = market_prior or MARKET_PARENT_VALUE_PRIOR
    avg_mp = sum(float(prior.get(p, 0.01)) for p in parents) / max(len(parents), 1)
    out: dict[int, float] = {}
    for p in parents:
        jw = float(json_weights.get(p, json_weights.get(str(p), 0.0)))
        mp = float(prior.get(p, 0.01))
        rel = mp / max(avg_mp, 1e-6)
        blend = 0.78 + 0.22 * min(1.65, max(0.45, rel))
        out[p] = max(1e-6, jw) * blend
    s = sum(out.values())
    if s < 1e-12:
        u = 1.0 / len(parents)
        return {p: u for p in parents}
    return {p: out[p] / s for p in parents}


_PHONE_SUBCATEGORIES = frozenset({201, 202, 203, 204})
_WEARABLE_SUBCATEGORIES = frozenset({206, 207})


def brand_phone_mass_multiplier(brand_id: int, allowed_subcategory_ids: set[int]) -> float:
    """Moltiplicatore catalogo solo se il brand ha subcat smartphone (201–204), non tutto parent 2."""
    if not (allowed_subcategory_ids & _PHONE_SUBCATEGORIES):
        return 1.0
    sc = _PHONE_VENDOR_SCORE.get(brand_id, 6.0)
    return max(0.55, min(2.6, sc / _PHONE_BASELINE))


def brand_wearables_mass_multiplier(brand_id: int, allowed_subcategory_ids: set[int]) -> float:
    """Leggero boost per brand con smartwatch/fitness (206–207); indipendente dallo score phone."""
    if not (allowed_subcategory_ids & _WEARABLE_SUBCATEGORIES):
        return 1.0
    w = {
        15: 1.28,
        55: 1.22,
        8: 1.18,
        12: 1.08,
        1: 1.06,
        9: 1.05,
    }
    return w.get(brand_id, 1.0)
