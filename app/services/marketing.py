"""Marketing: segment summary, needstates by category, purchasing process."""
import copy
import json
from pathlib import Path

from app.db.queries.marketing import (
    _is_full_year,
    query_category_segment_share,
    query_segment_breakdown_for_category_sales,
    query_precalc_purchasing_channel,
    query_precalc_purchasing_peak,
    query_precalc_segment_top_categories,
    query_precalc_segment_top_skus,
    query_purchasing_channel_mix,
    query_purchasing_peak_events,
    query_segment_top_categories,
    query_segment_top_skus,
)
from app.constants import ADMIN_CATEGORIES
from app.services._cache import cache_key, get_cached, set_cached, safe

_SEGMENT_PROFILES_PATH = Path(__file__).resolve().parent.parent / "static" / "data" / "segment_profiles.json"
_NEEDSTATES_HCG_PATH = Path(__file__).resolve().parent.parent / "static" / "data" / "needstates_hcg.json"
_MEDIA_PREFS_PATH = Path(__file__).resolve().parent.parent / "static" / "data" / "segment_media_preferences.json"
_MEDIA_PREFS_CACHE: dict | None = None
_DEFAULT_PERIOD = ("2024-01-01", "2024-12-31")
# Needstates spider: tutte le 10×6 combinazioni precompute da JSON (O(1) lato server e opzionale embed in pagina)
_NEEDSTATES_SPIDER_PRECALC: dict[str, dict] | None = None


def _spider_payload_from_hcg(
    cat_id: int, seg_id: int, segments_map: dict, needstates: list,
) -> dict:
    """Spider payload from HCG JSON (scores_raw 0–100 per axis for radar; scores = mix shares legacy)."""
    seg_name = segments_map.get(str(seg_id), f"Segment {seg_id}")
    if not needstates:
        return {
            "category_id": cat_id,
            "segment_id": seg_id,
            "segment_name": seg_name,
            "dimensions": [],
            "scores": [],
            "scores_category_avg": [],
            "scores_raw": [],
            "scores_category_avg_raw": [],
        }
    seg_idx = max(0, min(5, seg_id - 1))
    dimensions = [n["label"] for n in needstates]
    scores_raw = [int(n["scores"][seg_idx]) for n in needstates]

    def segment_distribution(s: int) -> list[float]:
        w = [float(n["scores"][s]) for n in needstates]
        t = sum(w)
        if t <= 0:
            return [0.0] * len(w)
        return [100.0 * x / t for x in w]

    seg_pcts = [segment_distribution(s) for s in range(6)]
    n_dim = len(dimensions)
    category_avg: list[float] = []
    for i in range(n_dim):
        col = [seg_pcts[s][i] for s in range(6)]
        category_avg.append(round(sum(col) / 6.0, 1))

    selected = seg_pcts[seg_idx]
    segment_pct = [round(x, 1) for x in selected]

    category_avg_raw: list[float] = []
    for i in range(n_dim):
        col_raw = [float(needstates[i]["scores"][s]) for s in range(6)]
        category_avg_raw.append(round(sum(col_raw) / 6.0, 1))

    return {
        "category_id": cat_id,
        "segment_id": seg_id,
        "segment_name": seg_name,
        "dimensions": dimensions,
        "scores": segment_pct,
        "scores_category_avg": category_avg,
        "scores_raw": scores_raw,
        "scores_category_avg_raw": category_avg_raw,
    }


def _ensure_needstates_spider_precalc() -> dict[str, dict]:
    global _NEEDSTATES_SPIDER_PRECALC
    if _NEEDSTATES_SPIDER_PRECALC is not None:
        return _NEEDSTATES_SPIDER_PRECALC
    data = _load_needstates_hcg()
    segments_map = data.get("segments", {})
    categories = data.get("categories", {})
    precalc: dict[str, dict] = {}
    for cat_id in range(1, 11):
        needstates = categories.get(str(cat_id), [])
        for seg_id in range(1, 7):
            precalc[f"{cat_id}:{seg_id}"] = _spider_payload_from_hcg(cat_id, seg_id, segments_map, needstates)
    _NEEDSTATES_SPIDER_PRECALC = precalc
    return precalc


def get_needstates_spider_precalc_for_template() -> dict[str, dict]:
    """Dict serializzabile per Jinja (`needstates_precalc`): nessuna query, solo lookup in memoria."""
    return copy.deepcopy(_ensure_needstates_spider_precalc())


def warm_needstates_spider_precalc() -> None:
    """Startup: costruisce la mappa spider così la prima richiesta è a costo zero."""
    _ensure_needstates_spider_precalc()


def get_segment_by_category(
    brand_id: int | None,
    year: int,
    category_id: int | None,
    subcategory_id: int | None = None,
    channel: str | None = None,
) -> dict:
    """Marketing Overview: segment mix for brand sales in a parent category / optional subcategory (full year)."""
    if not brand_id:
        return {"error": "Brand required", "rows": []}
    if not category_id or category_id < 1 or category_id > 10:
        return {"error": "Parent category required", "rows": []}
    y = int(year)
    date_start = f"{y}-01-01"
    date_end = f"{y}-12-31"
    sub = int(subcategory_id) if subcategory_id and int(subcategory_id) >= 100 else None
    key = cache_key(
        "mkt_seg_cat",
        brand=int(brand_id),
        year=y,
        cat=int(category_id),
        sub=sub or 0,
        ch=(channel or "").strip() or "all",
    )
    cached = get_cached(key)
    if cached is not None:
        return cached

    rows = safe(
        query_segment_breakdown_for_category_sales,
        int(brand_id),
        date_start,
        date_end,
        int(category_id),
        sub,
        channel if channel and str(channel).strip() in ("web", "app", "store") else None,
    ) or []
    out = {"rows": list(rows)}
    set_cached(key, out)
    return out

_NEEDSTATES_HCG_CACHE: dict | None = None


def _load_needstates_hcg() -> dict:
    """Load HCG needstates (7 per category, scores 0-100 per segment). Cached at module level."""
    global _NEEDSTATES_HCG_CACHE
    if _NEEDSTATES_HCG_CACHE is not None:
        return _NEEDSTATES_HCG_CACHE
    try:
        with open(_NEEDSTATES_HCG_PATH, encoding="utf-8") as f:
            _NEEDSTATES_HCG_CACHE = json.load(f)
        return _NEEDSTATES_HCG_CACHE
    except Exception:
        return {}


def _parent_category_name(cat_id: int | None) -> str:
    if not cat_id:
        return ""
    for c in ADMIN_CATEGORIES:
        if int(c["category_id"]) == int(cat_id):
            return str(c.get("category_name") or "")
    return ""


def _normalize_pct_rows(rows: list) -> list:
    s = sum(float(r.get("pct") or 0) for r in (rows or []))
    if s <= 0:
        return list(rows or [])
    return [{**r, "pct": round(100.0 * float(r.get("pct") or 0) / s, 1)} for r in rows]


def _nudge_media_block_for_category(block: dict | None, segment_id: int, category_id: int) -> dict | None:
    if not block or not category_id:
        return block
    out = copy.deepcopy(block)
    delta = ((category_id * 5 + segment_id * 3) % 11) - 5
    cn = _parent_category_name(category_id)
    if cn:
        base = (out.get("summary") or "").strip()
        out["summary"] = f"{base} In {cn}, media choices tilt slightly vs the all-category baseline."
    for key in ("social", "comparison_sites", "ai_touchpoints", "other_channels"):
        rows = out.get(key)
        if not rows:
            continue
        adj = []
        for i, r in enumerate(rows):
            w = int(r.get("pct") or 0) + (delta if (key == "social" and i % 2 == 0) else delta // 2)
            adj.append({**r, "pct": max(3, min(52, w))})
        out[key] = _normalize_pct_rows(adj)
    inf = out.get("influence") or {}
    g, io = int(inf.get("get_influenced") or 0), int(inf.get("influencing_others") or 0)
    g2 = max(5, min(80, g + delta))
    io2 = max(5, min(80, io - delta // 2))
    s = g2 + io2
    if s > 0:
        out["influence"] = {
            "get_influenced": round(100.0 * g2 / s, 1),
            "influencing_others": round(100.0 * io2 / s, 1),
        }
    return out


def _load_media_preferences() -> dict:
    """Static media mix per segment (Polish market touchpoints)."""
    global _MEDIA_PREFS_CACHE
    if _MEDIA_PREFS_CACHE is not None:
        return _MEDIA_PREFS_CACHE
    try:
        with open(_MEDIA_PREFS_PATH, encoding="utf-8") as f:
            _MEDIA_PREFS_CACHE = json.load(f)
        return _MEDIA_PREFS_CACHE
    except Exception:
        return {}


def get_media_preferences(segment_id: int | None = None, parent_category_id: int | None = None) -> dict:
    """Static mix per segment; con parent_category_id applica variazione illustrativa per categoria."""
    data = _load_media_preferences()
    if segment_id and 1 <= segment_id <= 6:
        key = str(segment_id)
        block = data.get(key)
        if block and parent_category_id and 1 <= parent_category_id <= 10:
            block = _nudge_media_block_for_category(block, segment_id, parent_category_id)
        return {"segment_id": segment_id, "segment": block, "parent_category_id": parent_category_id} if block else {"segment_id": segment_id, "segment": None, "parent_category_id": parent_category_id}
    return {"segments": data, "parent_category_id": parent_category_id}


def _load_segment_profiles() -> dict:
    """Load segment profiles from static JSON."""
    try:
        with open(_SEGMENT_PROFILES_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def get_segment_summary(
    ps: str | None = None,
    pe: str | None = None,
    segment_id: int | None = None,
    category_id: int | None = None,
    subcategory_id: int | None = None,
    brand_id: int | None = None,
) -> dict:
    """Segment summary: profiles + top categories + top SKUs. Filters: segment, category, subcategory, brand."""
    ps = ps or _DEFAULT_PERIOD[0]
    pe = pe or _DEFAULT_PERIOD[1]
    key = cache_key("mkt_seg_v2", ps=ps, pe=pe, seg=segment_id or 0, cat=category_id or 0, sub=subcategory_id or 0, brand=brand_id or 0)
    cached = get_cached(key)
    if cached is not None:
        return cached

    profiles = _load_segment_profiles()
    seg_ids = [segment_id] if segment_id else list(range(1, 7))
    year = int(ps[:4]) if ps and len(ps) >= 4 else None
    use_precalc = year and _is_full_year(ps, pe) and not brand_id
    if use_precalc:
        top_cats = safe(query_precalc_segment_top_categories, year, segment_id, category_id, subcategory_id, limit=10) or []
        top_skus = safe(query_precalc_segment_top_skus, year, segment_id, category_id, subcategory_id, limit=10) or []
    else:
        top_cats = safe(query_segment_top_categories, ps, pe, segment_id, category_id, subcategory_id, brand_id, limit=10) or []
        top_skus = safe(query_segment_top_skus, ps, pe, segment_id, category_id, subcategory_id, brand_id, limit=10) or []

    by_segment: dict[int, dict] = {}
    for seg_id in seg_ids:
        prof = profiles.get(str(seg_id), {})
        by_segment[seg_id] = {
            "segment_id": seg_id,
            "name": prof.get("name", f"Segment {seg_id}"),
            "pain_points": prof.get("pain_points", []),
            "needstates": prof.get("needstates", []),
            "top_categories_note": prof.get("top_categories_note", ""),
            "top_categories": [c for c in top_cats if c.get("segment_id") == seg_id],
            "top_skus": [s for s in top_skus if s.get("segment_id") == seg_id],
        }

    out = {"segments": list(by_segment.values()), "period": {"ps": ps, "pe": pe}}
    set_cached(key, out)
    return out


def get_needstates_spider(category_id: int | None = None, segment_id: int | None = None) -> dict:
    """Spider chart: 7 needstates per category. Tutto da tabella precalcolata in RAM (JSON HCG)."""
    cat_id = category_id or 1
    seg_id = segment_id or 1
    row = _ensure_needstates_spider_precalc().get(f"{cat_id}:{seg_id}")
    if row is not None:
        return copy.deepcopy(row)
    data = _load_needstates_hcg()
    return _spider_payload_from_hcg(
        cat_id, seg_id, data.get("segments", {}), data.get("categories", {}).get(str(cat_id), []),
    )


def get_needstates(ps: str | None = None, pe: str | None = None, category_id: int | None = None, segment_id: int | None = None) -> dict:
    """Needstates: spider chart when segment_id given, else legacy segment share."""
    if segment_id is not None and segment_id > 0:
        return get_needstates_spider(category_id or 1, segment_id)
    # Legacy: segment share for category (kept for compatibility)
    ps = ps or _DEFAULT_PERIOD[0]
    pe = pe or _DEFAULT_PERIOD[1]
    cat_id = category_id or 1
    key = cache_key("mkt_need", ps=ps, pe=pe, cat=cat_id)
    cached = get_cached(key)
    if cached is not None:
        return cached

    profiles = _load_segment_profiles()
    hcg = _load_needstates_hcg()
    cat_needstates = profiles.get("category_needstates", {})
    rows = safe(query_category_segment_share, ps, pe, cat_id) or []

    needstates_list = hcg.get("categories", {}).get(str(cat_id), [])

    segments_data = []
    for r in rows:
        seg_id = r.get("segment_id")
        needstate = ""
        if needstates_list:
            seg_idx = max(0, min(5, seg_id - 1))
            top = max(needstates_list, key=lambda n: n["scores"][seg_idx])
            needstate = top.get("label", "")
        elif str(cat_id) in cat_needstates and str(seg_id) in cat_needstates[str(cat_id)]:
            needstate = cat_needstates[str(cat_id)][str(seg_id)]
        else:
            prof = profiles.get(str(seg_id), {})
            needstate = prof.get("top_driver", "")
        segments_data.append({
            "segment_id": seg_id,
            "segment_name": r.get("segment_name", ""),
            "gross_pln": float(r.get("gross_pln") or 0),
            "share_pct": float(r.get("share_pct") or 0),
            "needstate": needstate,
        })

    out = {
        "category_id": cat_id,
        "segments": segments_data,
        "period": {"ps": ps, "pe": pe},
    }
    set_cached(key, out)
    return out


def _synthetic_channel_mix(segment_id: int | None, parent_category_id: int | None = None) -> list[dict]:
    """Synthetic channel mix when real data is empty (web/app/store)."""
    profiles = {
        1: {"web": 45, "app": 25, "store": 30},   # Liberals – digital
        2: {"web": 40, "app": 35, "store": 25},   # Optimistic Doers – social
        3: {"web": 55, "app": 30, "store": 15},   # Go-Getters – web-first
        4: {"web": 50, "app": 40, "store": 10},   # Outcasts – app-heavy
        5: {"web": 30, "app": 15, "store": 55},   # Contributors – store
        6: {"web": 20, "app": 10, "store": 70},   # Floaters – store
    }
    if segment_id:
        mix = dict(profiles.get(segment_id, profiles[1]))
    else:
        mix = {k: sum(p.get(k, 0) for p in profiles.values()) / 6 for k in profiles[1]}
    if parent_category_id:
        off = (int(parent_category_id) % 5) * 0.03
        mix["web"] = float(mix.get("web", 40)) * (1.0 + off)
        mix["app"] = float(mix.get("app", 30)) * (1.0 - off * 0.4)
        mix["store"] = float(mix.get("store", 30)) * (1.0 - off * 0.2)
    return [{"channel": k, "gross_pln": round(float(v) * 1000, 0)} for k, v in mix.items()]


def _synthetic_peak_events(segment_id: int | None, parent_category_id: int | None = None) -> list[dict]:
    """Synthetic peak events when real data is empty."""
    events = [
        ("Black Friday", 22),
        ("Christmas", 18),
        ("Regular", 35),
        ("Back to School", 8),
        ("Summer Sales", 10),
        ("Cyber Monday", 7),
    ]
    seg_name = "All" if not segment_id else {1: "Liberals", 2: "Optimistic Doers", 3: "Go-Getters", 4: "Outcasts", 5: "Contributors", 6: "Floaters"}.get(segment_id, "Segment")
    skew = (int(parent_category_id) % 7) if parent_category_id else 0
    rows = []
    for i, e in enumerate(events):
        pct = max(3, min(45, int(e[1]) + skew if i % 2 == 0 else int(e[1]) - skew // 2))
        rows.append({"segment_id": segment_id, "segment_name": seg_name, "peak_event": e[0], "orders": 0, "orders_pct": pct, "gross_pln": 0})
    tot = sum(r["orders_pct"] for r in rows)
    if tot > 0:
        for r in rows:
            r["orders_pct"] = round(100.0 * r["orders_pct"] / tot, 1)
    return rows


def _synthetic_source_mix(segment_id: int | None) -> list[dict]:
    """Synthetic traffic source mix per segment (Google, Social, Direct, Paid, Referral)."""
    # Segment-specific profiles: younger/digital = more social; older = more direct
    profiles = {
        1: {"google": 32, "social": 18, "direct": 28, "paid": 12, "referral": 10},  # Liberals
        2: {"google": 25, "social": 35, "direct": 22, "paid": 10, "referral": 8},   # Optimistic Doers
        3: {"google": 38, "social": 15, "direct": 30, "paid": 12, "referral": 5},   # Go-Getters
        4: {"google": 28, "social": 42, "direct": 15, "paid": 8, "referral": 7},    # Outcasts
        5: {"google": 22, "social": 20, "direct": 40, "paid": 6, "referral": 12},   # Contributors
        6: {"google": 18, "social": 12, "direct": 52, "paid": 5, "referral": 13},   # Floaters
    }
    if segment_id:
        mix = profiles.get(segment_id, profiles[1])
    else:
        mix = {k: sum(p.get(k, 0) for p in profiles.values()) / 6 for k in profiles[1]}
    return [{"source": k, "pct": round(v, 1)} for k, v in mix.items()]


def _synthetic_pre_purchase_searches(segment_id: int | None) -> list[dict]:
    """Synthetic pre-purchase search intent per segment."""
    # Search types: product comparison, price, reviews, specs, brand
    profiles = {
        1: {"Product comparison": 28, "Price": 22, "Reviews": 24, "Specs": 18, "Brand": 8},   # Liberals
        2: {"Product comparison": 32, "Price": 15, "Reviews": 30, "Specs": 18, "Brand": 5},   # Optimistic Doers
        3: {"Product comparison": 35, "Price": 18, "Reviews": 22, "Specs": 20, "Brand": 5},   # Go-Getters
        4: {"Product comparison": 25, "Price": 38, "Reviews": 22, "Specs": 10, "Brand": 5},   # Outcasts
        5: {"Product comparison": 30, "Price": 25, "Reviews": 28, "Specs": 12, "Brand": 5},   # Contributors
        6: {"Product comparison": 28, "Price": 35, "Reviews": 20, "Specs": 10, "Brand": 7},   # Floaters
    }
    if segment_id:
        mix = profiles.get(segment_id, profiles[1])
    else:
        mix = {k: sum(p.get(k, 0) for p in profiles.values()) / 6 for k in profiles[1]}
    return [{"search_type": k, "pct": round(v, 1)} for k, v in mix.items()]


def get_purchasing(
    ps: str | None = None,
    pe: str | None = None,
    segment_id: int | None = None,
    parent_category_id: int | None = None,
) -> dict:
    """Purchasing process: channel mix, peak events, source + searches (synthetic)."""
    ps = ps or _DEFAULT_PERIOD[0]
    pe = pe or _DEFAULT_PERIOD[1]
    pc = int(parent_category_id) if parent_category_id and 1 <= int(parent_category_id) <= 10 else None
    key = cache_key("mkt_purch_v2", ps=ps, pe=pe, seg=segment_id or 0, cat=pc or 0)
    cached = get_cached(key)
    if cached is not None:
        return cached

    year = int(ps[:4]) if ps and len(ps) >= 4 else None
    use_precalc = bool(year and _is_full_year(ps, pe))
    if use_precalc:
        channel_mix = safe(query_precalc_purchasing_channel, year, segment_id, pc) or []
        peak_events = safe(query_precalc_purchasing_peak, year, segment_id, pc) or []
    else:
        channel_mix = safe(query_purchasing_channel_mix, ps, pe, segment_id, pc) or []
        peak_events = safe(query_purchasing_peak_events, ps, pe, segment_id, pc) or []
    if not channel_mix:
        channel_mix = _synthetic_channel_mix(segment_id, pc)
    if not peak_events:
        peak_events = _synthetic_peak_events(segment_id, pc)
    source_mix = _synthetic_source_mix(segment_id)
    pre_purchase_searches = _synthetic_pre_purchase_searches(segment_id)

    out = {
        "channel_mix": list(channel_mix),
        "peak_events": list(peak_events),
        "source_mix": source_mix,
        "pre_purchase_searches": pre_purchase_searches,
        "segment_id": segment_id,
        "parent_category_id": pc,
        "period": {"ps": ps, "pe": pe},
    }
    set_cached(key, out)
    return out
