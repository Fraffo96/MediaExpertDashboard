"""Seed profile v2: schema minimo + validazione (Fase 1).

Questo modulo definisce un contratto versionato e un validatore server-side che:
- non dipende da BigQuery (solo coerenza del profilo)
- produce un error model standard: {code, message, field_path}
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class ValidationErrorItem:
    code: str
    message: str
    field_path: str

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message, "field_path": self.field_path}


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _path(parent: str, child: str) -> str:
    return f"{parent}.{child}" if parent else child


def validate_profile_v2(profile: dict) -> tuple[dict, list[ValidationErrorItem]]:
    """Valida e normalizza un profilo seed v2 (minimo per sbloccare UI e pipeline).

    Ritorna (normalized_profile, errors). Se errors è vuoto, normalized_profile è pronto
    per le fasi successive (planner/compiler).
    """
    errors: list[ValidationErrorItem] = []
    if not isinstance(profile, dict):
        return {}, [ValidationErrorItem("type_error", "Il profilo deve essere un oggetto JSON", "$")]

    normalized: dict[str, Any] = dict(profile)

    pv = normalized.get("profile_version")
    if pv is None:
        errors.append(ValidationErrorItem("missing", "Campo obbligatorio", "profile_version"))
    elif not _is_int(pv):
        errors.append(ValidationErrorItem("type_error", "Deve essere un intero", "profile_version"))
    elif pv != 2:
        errors.append(ValidationErrorItem("invalid_version", "Versione profilo non supportata (atteso 2)", "profile_version"))

    g = normalized.get("global")
    if g is None:
        errors.append(ValidationErrorItem("missing", "Campo obbligatorio", "global"))
        g = {}
        normalized["global"] = g
    elif not isinstance(g, dict):
        errors.append(ValidationErrorItem("type_error", "Deve essere un oggetto", "global"))
        g = {}
        normalized["global"] = g

    def req_pos_int(key: str, default: Optional[int] = None):
        fp = _path("global", key)
        v = g.get(key)
        if v is None:
            if default is None:
                errors.append(ValidationErrorItem("missing", "Campo obbligatorio", fp))
                return
            g[key] = default
            return
        if not _is_int(v):
            errors.append(ValidationErrorItem("type_error", "Deve essere un intero", fp))
            return
        if v <= 0:
            errors.append(ValidationErrorItem("range_error", "Deve essere > 0", fp))

    # volumi principali (minimo)
    req_pos_int("num_orders", default=None)
    req_pos_int("num_customers", default=None)
    req_pos_int("num_products", default=None)

    # opzionali: channels, date_range
    ch = g.get("channels")
    if ch is not None:
        if not isinstance(ch, list) or not all(isinstance(x, str) and x.strip() for x in ch):
            errors.append(ValidationErrorItem("type_error", "channels deve essere lista di stringhe non vuote", "global.channels"))
        else:
            # normalizza
            g["channels"] = [x.strip() for x in ch]

    dr = g.get("date_range")
    if dr is not None:
        if not isinstance(dr, dict):
            errors.append(ValidationErrorItem("type_error", "date_range deve essere un oggetto", "global.date_range"))
        else:
            s = dr.get("start")
            e = dr.get("end")
            if s is None or e is None:
                errors.append(ValidationErrorItem("missing", "date_range.start e date_range.end sono richiesti insieme", "global.date_range"))
            elif not (isinstance(s, str) and isinstance(e, str)):
                errors.append(ValidationErrorItem("type_error", "start/end devono essere stringhe YYYY-MM-DD", "global.date_range"))

    for k in ("segment_rules", "brand_rules", "promo_rules", "weights", "guards", "segment_behavior"):
        v = normalized.get(k)
        if v is not None and not isinstance(v, dict):
            errors.append(ValidationErrorItem("type_error", f"{k} deve essere un oggetto", k))

    sr = normalized.get("segment_rules")
    if isinstance(sr, dict):
        shares = sr.get("customer_share_by_segment")
        if shares is not None:
            if not isinstance(shares, dict):
                errors.append(ValidationErrorItem("type_error", "customer_share_by_segment deve essere un oggetto", "segment_rules.customer_share_by_segment"))
            else:
                tot = 0.0
                for k, v in shares.items():
                    try:
                        seg = int(str(k))
                        if not (1 <= seg <= 6):
                            errors.append(ValidationErrorItem("range_error", "segmento deve essere 1–6", f"segment_rules.customer_share_by_segment.{k}"))
                        fv = float(v)
                        if fv < 0:
                            errors.append(ValidationErrorItem("range_error", "quota non negativa", f"segment_rules.customer_share_by_segment.{k}"))
                        tot += fv
                    except (TypeError, ValueError):
                        errors.append(ValidationErrorItem("type_error", "valore numerico atteso", f"segment_rules.customer_share_by_segment.{k}"))
                if not errors and tot <= 0:
                    errors.append(ValidationErrorItem("range_error", "somma quote > 0 richiesta", "segment_rules.customer_share_by_segment"))
        pc = sr.get("parent_categories")
        if pc is not None and isinstance(pc, dict):
            for k, v in pc.items():
                try:
                    seg = int(str(k))
                    if not (1 <= seg <= 6):
                        errors.append(ValidationErrorItem("range_error", "segmento 1–6", f"segment_rules.parent_categories.{k}"))
                    elif not isinstance(v, list):
                        errors.append(ValidationErrorItem("type_error", "lista category id attesa", f"segment_rules.parent_categories.{k}"))
                    else:
                        for x in v:
                            xi = int(x)
                            if not (1 <= xi <= 10):
                                errors.append(ValidationErrorItem("range_error", "parent category 1–10", f"segment_rules.parent_categories.{k}"))
                except (TypeError, ValueError):
                    errors.append(ValidationErrorItem("type_error", "chiavi/valori non validi", f"segment_rules.parent_categories.{k}"))

    sb = normalized.get("segment_behavior")
    if isinstance(sb, dict):
        for k, v in sb.items():
            try:
                seg = int(str(k))
                if not (1 <= seg <= 6):
                    errors.append(ValidationErrorItem("range_error", "segmento 1–6", f"segment_behavior.{k}"))
                elif not isinstance(v, dict):
                    errors.append(ValidationErrorItem("type_error", "oggetto atteso", f"segment_behavior.{k}"))
                else:
                    for fld, lo, hi in (("promo_sens", 0.0, 1.0), ("churn", 0.0, 1.0), ("loyalty_prob", 0.0, 1.0)):
                        if fld in v:
                            try:
                                fv = float(v[fld])
                                if fv < lo or fv > hi:
                                    errors.append(ValidationErrorItem("range_error", f"{fld} in [{lo},{hi}]", f"segment_behavior.{k}.{fld}"))
                            except (TypeError, ValueError):
                                errors.append(ValidationErrorItem("type_error", "numero atteso", f"segment_behavior.{k}.{fld}"))
            except (TypeError, ValueError):
                errors.append(ValidationErrorItem("type_error", "chiave segmento non valida", f"segment_behavior.{k}"))

    pr = normalized.get("promo_rules")
    if isinstance(pr, dict):
        curve = pr.get("promo_curve")
        if curve is not None:
            if not isinstance(curve, dict):
                errors.append(ValidationErrorItem("type_error", "promo_curve deve essere oggetto", "promo_rules.promo_curve"))
            else:
                for fld in ("slope", "intercept"):
                    if fld in curve:
                        try:
                            float(curve[fld])
                        except (TypeError, ValueError):
                            errors.append(ValidationErrorItem("type_error", "numero atteso", f"promo_rules.promo_curve.{fld}"))
        db = pr.get("segment_discount_bias")
        if db is not None and isinstance(db, dict):
            for k, v in db.items():
                try:
                    seg = int(str(k))
                    if not (1 <= seg <= 6):
                        errors.append(ValidationErrorItem("range_error", "segmento 1–6", f"promo_rules.segment_discount_bias.{k}"))
                    float(v)
                except (TypeError, ValueError):
                    errors.append(ValidationErrorItem("type_error", "valore numerico", f"promo_rules.segment_discount_bias.{k}"))

    br = normalized.get("brand_rules")
    if isinstance(br, dict):
        fo = br.get("focus_override")
        if fo is not None and not isinstance(fo, dict):
            errors.append(ValidationErrorItem("type_error", "focus_override deve essere oggetto brand_id -> categorie", "brand_rules.focus_override"))
        pa = br.get("promo_affinity")
        if pa is not None and not isinstance(pa, dict):
            errors.append(ValidationErrorItem("type_error", "promo_affinity deve essere oggetto", "brand_rules.promo_affinity"))

    guards = normalized.get("guards")
    if isinstance(guards, dict):
        for key, mx in (("max_num_orders", 5_000_000), ("max_num_customers", 500_000)):
            if key in guards:
                v = guards[key]
                if not _is_int(v) or v <= 0:
                    errors.append(ValidationErrorItem("type_error", f"{key} intero > 0", f"guards.{key}"))
                elif v > mx:
                    errors.append(ValidationErrorItem("range_error", f"limite amministrativo {mx}", f"guards.{key}"))
        if not errors and isinstance(g.get("num_orders"), int) and isinstance(guards.get("max_num_orders"), int):
            if g["num_orders"] > guards["max_num_orders"]:
                errors.append(ValidationErrorItem("guard_violation", "num_orders supera guards.max_num_orders", "global.num_orders"))
        if not errors and isinstance(g.get("num_customers"), int) and isinstance(guards.get("max_num_customers"), int):
            if g["num_customers"] > guards["max_num_customers"]:
                errors.append(ValidationErrorItem("guard_violation", "num_customers supera limite", "global.num_customers"))

    return normalized, errors


def preview_profile_v2(profile: dict, compiled: Optional[dict] = None) -> dict:
    """Anteprima parametri + (opzionale) output compiler (quote segmento, soglia promo approssimata)."""
    g = (profile or {}).get("global") if isinstance(profile, dict) else {}
    if not isinstance(g, dict):
        g = {}
    out: dict = {
        "global": {
            "num_orders": g.get("num_orders"),
            "num_customers": g.get("num_customers"),
            "num_products": g.get("num_products"),
            "channels": g.get("channels") or ["web", "app", "store"],
            "date_range": g.get("date_range"),
        },
    }
    if compiled:
        out["compiled_preview"] = compiled.get("preview")
        out["compiled_global"] = compiled.get("global")
    else:
        out["note"] = "Passa compile=true lato API per distribuzioni attese."
    return out

