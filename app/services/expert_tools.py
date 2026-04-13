"""BigQuery-backed tools for the Gemini-first expert chatbot (tool-only; no free-form SQL)."""

from __future__ import annotations

import json
import re
from typing import Any

from google.genai import types

from app.constants import ADMIN_CATEGORIES, ADMIN_SUBCATEGORIES
from app.db.queries.basic import products as basic_products
from app.db.queries.basic import promo_roi as basic_promo_roi
from app.db.queries import brand_comparison as bc
from app.db.queries.market_intelligence import sales as mi_sales
from app.db.queries.market_intelligence import segment_sku as mi_segment_sku
from app.db.queries.marketing import segment_by_category as mkt_seg_cat
from app.db.queries.marketing import purchasing as mkt_purchasing
from app.db.queries import promo_creator as promo_q
from app.services import marketing as marketing_svc

_QUERY_STOPWORDS = frozenset(
    {
        "my",
        "the",
        "a",
        "an",
        "for",
        "and",
        "or",
        "to",
        "of",
        "in",
        "on",
        "at",
        "sku",
        "skus",
        "our",
        "your",
        "me",
        "we",
        "i",
        "it",
        "is",
        "be",
        "as",
        "by",
        "from",
        "with",
        "this",
        "that",
        "product",
        "item",
    }
)


def _tokenize_product_query(raw: str) -> list[str]:
    s = re.sub(r"[^a-z0-9\s-]", " ", (raw or "").lower())
    out: list[str] = []
    for w in s.split():
        w = w.strip("-")
        if len(w) < 2 or w in _QUERY_STOPWORDS:
            continue
        out.append(w)
    return out


def _bq_substring_tokens(tokens: list[str]) -> list[str]:
    """Expand spoken words into substrings that often appear in product_name (for CONTAINS_SUBSTR)."""
    seen: set[str] = set()
    out: list[str] = []
    for t in tokens:
        if t in ("foldable", "folder"):
            for x in ("fold", "flip", "zfold"):
                if x not in seen:
                    seen.add(x)
                    out.append(x)
            continue
        if t == "premium":
            for x in ("premium", "pro", "ultra", "plus"):
                if x not in seen:
                    seen.add(x)
                    out.append(x)
            continue
        if t not in seen:
            s = re.sub(r"[^a-z0-9]", "", t)
            if len(s) >= 2:
                seen.add(s)
                out.append(s)
    return out


def _token_hits_in_name(name_lower: str, token: str) -> bool:
    if token in ("foldable", "fold", "folder"):
        return bool(re.search(r"fold|flip|zfold|z fold|galaxy z", name_lower))
    if token == "flip":
        return "flip" in name_lower
    if token == "premium":
        return any(p in name_lower for p in ("premium", "pro", "ultra", "plus", "max"))
    return token in name_lower


def _score_name_against_tokens(product_name: str, tokens: list[str]) -> float:
    if not tokens:
        return 0.0
    name_lower = (product_name or "").lower()
    hits = sum(1 for t in tokens if _token_hits_in_name(name_lower, t))
    return hits / float(len(tokens))


# User-facing status lines when a tool runs (SSE / UI)
TOOL_STATUS_LABELS: dict[str, str] = {
    "list_categories": "Loading category taxonomy...",
    "list_segments": "Loading customer segments...",
    "get_top_products": "Checking your current top-selling products...",
    "get_underperforming_products": "Scanning your portfolio for underperformers...",
    "get_sales_by_category_for_brand": "Pulling your brand sales by category...",
    "get_segment_breakdown_for_category": "Analyzing which customer segments buy in this category...",
    "get_purchasing_channel_mix": "Looking at sales channel performance...",
    "get_segment_promo_responsiveness": "Checking segment promo responsiveness...",
    "get_product_segment_breakdown": "Analyzing segment breakdown for this SKU...",
    "get_brand_vs_market_subcategory_sales": "Comparing your brand vs the market...",
    "get_promo_roi_by_type_for_brand": "Evaluating promo ROI benchmarks...",
    "search_products_by_query": "Searching product catalog...",
    "list_competitors_in_category": "Identifying competitors in this space...",
    "get_segment_marketing_summary": "Reviewing segment pain points and needstates...",
    "get_category_needstate_landscape": "Scanning the needstate landscape...",
    "get_needstate_dimensions_for_segment": "Diving into needstate dimensions...",
    "get_media_touchpoints": "Checking how target segments discover products...",
    "get_purchasing_journey": "Mapping the customer purchasing journey...",
}

# HCG segments (aligned with mart.dim_segment seed)
STATIC_SEGMENTS: list[dict[str, Any]] = [
    {
        "segment_id": 1,
        "segment_name": "Liberals",
        "segment_description": "Wellness, knowledge, sustainability, quality of life",
        "age_range": "45-64",
        "income_level": "high",
        "gender_skew": "57% male",
        "top_driver": "wellness",
    },
    {
        "segment_id": 2,
        "segment_name": "Optimistic Doers",
        "segment_description": "Status, image, optimism, work-life balance",
        "age_range": "35-54",
        "income_level": "high",
        "gender_skew": "balanced",
        "top_driver": "status",
    },
    {
        "segment_id": 3,
        "segment_name": "Go-Getters",
        "segment_description": "Performance, productivity, career oriented",
        "age_range": "25-44",
        "income_level": "very_high",
        "gender_skew": "balanced",
        "top_driver": "performance",
    },
    {
        "segment_id": 4,
        "segment_name": "Outcasts",
        "segment_description": "Entertainment, escapism, price sensitive, young",
        "age_range": "18-24",
        "income_level": "low",
        "gender_skew": "58% male",
        "top_driver": "entertainment",
    },
    {
        "segment_id": 5,
        "segment_name": "Contributors",
        "segment_description": "Family, community, practicality, home management",
        "age_range": "45-54",
        "income_level": "low",
        "gender_skew": "70% female",
        "top_driver": "family",
    },
    {
        "segment_id": 6,
        "segment_name": "Floaters",
        "segment_description": "Necessity, stability, low openness to change",
        "age_range": "45-54",
        "income_level": "low",
        "gender_skew": "balanced",
        "top_driver": "necessity",
    },
]

_MAX_ROWS = 40


def _json_safe(obj: Any) -> Any:
    """Make tool payloads JSON-serializable for Gemini."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(x) for x in obj]
    if isinstance(obj, (str, int, float, bool)):
        return obj
    try:
        if hasattr(obj, "item"):
            return obj.item()
    except Exception:
        pass
    try:
        return float(obj)
    except Exception:
        return str(obj)


def _truncate_rows(rows: list[dict] | None, limit: int = _MAX_ROWS) -> list[dict]:
    if not rows:
        return []
    return [_json_safe(r) for r in rows[:limit]]


def tool_list_categories() -> dict[str, Any]:
    """Return parent categories (1–10) and all subcategories with ids and names."""
    return {
        "parent_categories": list(ADMIN_CATEGORIES),
        "subcategories": list(ADMIN_SUBCATEGORIES),
    }


def tool_list_segments() -> dict[str, Any]:
    """Return HCG segment definitions."""
    return {"segments": STATIC_SEGMENTS}


def tool_get_top_products(
    ps: str,
    pe: str,
    *,
    brand_id: int | None,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
    limit: int = 15,
) -> dict[str, Any]:
    """Top SKUs by gross_pln. Filter by parent (1–10) and/or subcategory (>=100)."""
    cat: str | None = None
    if subcategory_id and int(subcategory_id) >= 100:
        cat = str(int(subcategory_id))
    elif parent_category_id and 1 <= int(parent_category_id) <= 10:
        cat = str(int(parent_category_id))
    lim = max(1, min(int(limit or 15), 50))
    rows = basic_products.query_top_products(
        ps, pe, limit=lim * 3, cat=cat, brand=str(brand_id) if brand_id else None
    )
    # Aggregate duplicate product_id+name across channels for ranking
    merged: dict[tuple[Any, Any], dict] = {}
    for r in rows or []:
        key = (r.get("product_id"), r.get("product_name"))
        if key not in merged:
            merged[key] = dict(r)
            merged[key]["gross_pln"] = float(r.get("gross_pln") or 0)
            merged[key]["units"] = float(r.get("units") or 0)
        else:
            merged[key]["gross_pln"] += float(r.get("gross_pln") or 0)
            merged[key]["units"] += float(r.get("units") or 0)
    out = sorted(merged.values(), key=lambda x: float(x.get("gross_pln") or 0), reverse=True)[:lim]
    return {"products": _truncate_rows(out, lim)}


def tool_get_underperforming_products(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
    bottom_pct: float = 0.10,
    limit: int = 30,
) -> dict[str, Any]:
    """Bottom fraction of brand SKUs by gross_pln (PERCENT_RANK ascending) for portfolio / delist analysis."""
    cat_filter: int | None = None
    if subcategory_id and int(subcategory_id) >= 100:
        cat_filter = int(subcategory_id)
    elif parent_category_id and 1 <= int(parent_category_id) <= 10:
        cat_filter = int(parent_category_id)
    bp = float(bottom_pct) if bottom_pct is not None else 0.10
    lim = max(1, min(int(limit or 30), 80))
    rows = basic_products.query_underperforming_products(
        ps,
        pe,
        brand_id=int(brand_id),
        parent_category_id=cat_filter,
        bottom_pct=bp,
        limit=lim,
    )
    return {
        "underperformers": _truncate_rows(rows or [], lim),
        "bottom_pct": bp,
        "scope_note": (
            "pct_rank is PERCENT_RANK over gross_pln ascending within the brand (and optional category filter); "
            "0 = lowest sales among products with sales in the period."
        ),
    }


def tool_get_sales_by_category_for_brand(ps: str, pe: str, *, brand_id: int) -> dict[str, Any]:
    """Brand vs market gross PLN per parent category (only categories where brand sells)."""
    rows = mi_sales.query_sales_value_by_category(ps, pe, int(brand_id))
    return {"sales_by_parent_category": _truncate_rows(rows or [])}


def tool_get_segment_breakdown_for_category(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int,
    subcategory_id: int | None = None,
) -> dict[str, Any]:
    """Share of brand sales in a category by HCG segment (orders / fact_order_items)."""
    if not (1 <= int(parent_category_id) <= 10):
        return {"error": "parent_category_id must be 1–10", "rows": []}
    rows = mkt_seg_cat.query_segment_breakdown_for_category_sales(
        int(brand_id),
        ps,
        pe,
        int(parent_category_id),
        int(subcategory_id) if subcategory_id and int(subcategory_id) >= 100 else None,
        channel=None,
    )
    return {"segment_breakdown": _truncate_rows(rows or [])}


def tool_get_purchasing_channel_mix(
    ps: str,
    pe: str,
    *,
    segment_id: int | None = None,
    parent_category_id: int | None = None,
) -> dict[str, Any]:
    """Channel mix (web/app/store) with buyers, orders, gross_pln; optional segment and macro category filter."""
    rows = mkt_purchasing.query_purchasing_channel_mix(
        ps, pe, segment_id=segment_id, parent_category_id=parent_category_id
    )
    return {"channel_mix": _truncate_rows(rows or [])}


def tool_get_segment_promo_responsiveness(
    ps: str,
    pe: str,
    *,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
    promo_type: str | None = None,
) -> dict[str, Any]:
    """Top segments by promo share % in category/subcategory (requires parent or sub)."""
    cat = int(parent_category_id) if parent_category_id and 1 <= int(parent_category_id) <= 10 else None
    sub = int(subcategory_id) if subcategory_id and int(subcategory_id) >= 100 else None
    if cat is None and sub is None:
        return {
            "error": "Provide parent_category_id (1–10) or subcategory_id (>=100)",
            "segments": [],
        }
    rows = promo_q.query_segment_promo_responsiveness(ps, pe, cat=cat, subcat=sub, promo_type=promo_type)
    return {"promo_responsive_segments": _truncate_rows(rows or [])}


def tool_get_product_segment_breakdown(
    ps: str,
    pe: str,
    *,
    product_id: int,
    brand_id: int,
    category_filter: int | None = None,
) -> dict[str, Any]:
    """All-sales segment breakdown for one SKU (remove-SKU / cannibalization scenarios)."""
    cid = str(int(category_filter)) if category_filter is not None else None
    rows = mi_segment_sku.query_segment_breakdown_for_product_all_sales(
        int(product_id), int(brand_id), ps, pe, category_id=cid, channel=None
    )
    total_gross = sum(float(r.get("gross_pln") or 0) for r in rows or [])
    total_units = sum(float(r.get("units") or 0) for r in rows or [])
    return {
        "product_id": int(product_id),
        "segments": _truncate_rows(rows or []),
        "totals": {"gross_pln": round(total_gross, 2), "units": round(total_units, 2)},
    }


def tool_get_brand_vs_market_subcategory_sales(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
) -> dict[str, Any]:
    """Subcategory-level brand vs market (media) gross PLN."""
    cat = int(parent_category_id) if parent_category_id and 1 <= int(parent_category_id) <= 10 else None
    sub = int(subcategory_id) if subcategory_id and int(subcategory_id) >= 100 else None
    rows = mi_sales.query_sales_value_brand_vs_media(ps, pe, int(brand_id), cat=cat, subcat=sub)
    return {"subcategory_brand_vs_market": _truncate_rows(rows or [])}


def tool_get_promo_roi_by_type_for_brand(
    ps: str,
    pe: str,
    *,
    brand_id: int | None,
    parent_category_id: int | None = None,
) -> dict[str, Any]:
    """Average promo ROI and attributed sales by promo type/name (optional parent category filter)."""
    cat = int(parent_category_id) if parent_category_id and 1 <= int(parent_category_id) <= 10 else None
    rows = basic_promo_roi.query_promo_roi_by_type(ps, pe, cat=cat, seg=None, gender=None, brand=brand_id)
    return {"promo_roi_by_type": _truncate_rows(rows or [])}


def tool_search_products_by_query(
    ps: str,
    pe: str,
    *,
    query: str,
    brand_id: int | None,
    limit: int = 12,
) -> dict[str, Any]:
    """
    Resolve natural-language product descriptions to catalog SKUs (token + synonym scoring).
    Searches beyond top sellers via BigQuery when needed; returns best matches with relevance scores.
    """
    raw = (query or "").strip()
    if not raw:
        return {"matches": [], "hint": "empty_query"}
    lim = max(1, min(int(limit or 12), 30))
    tokens = _tokenize_product_query(raw)
    if not tokens:
        return {"matches": [], "hint": "no_search_tokens", "query_normalized": raw}

    bq_tokens = _bq_substring_tokens(tokens)
    pool: list[dict[str, Any]] = []

    def _row_key(r: dict[str, Any]) -> Any:
        return r.get("product_id")

    if brand_id is not None:
        pool = basic_products.query_products_any_token_match(
            ps, pe, tokens=bq_tokens, brand_id=int(brand_id), candidate_limit=900
        ) or []
    if not pool and brand_id is not None:
        longish = sorted((t for t in bq_tokens if len(t) >= 4), key=len, reverse=True)[:2]
        if longish:
            pool = basic_products.query_products_any_token_match(
                ps, pe, tokens=longish, brand_id=int(brand_id), candidate_limit=900
            ) or []
    if not pool:
        pool = basic_products.query_top_products(
            ps, pe, limit=2500, cat=None, brand=str(brand_id) if brand_id else None
        ) or []

    min_rel = 1.0 / float(len(tokens))
    scored: list[tuple[float, float, dict[str, Any]]] = []
    for r in pool:
        pn = str(r.get("product_name") or "")
        rel = _score_name_against_tokens(pn, tokens)
        if rel < min_rel:
            continue
        g = float(r.get("gross_pln") or 0)
        scored.append((rel, g, r))

    scored.sort(key=lambda x: (-x[0], -x[1]))

    seen: set[Any] = set()
    out: list[dict[str, Any]] = []
    for rel, _g, r in scored:
        pid = r.get("product_id")
        if pid in seen:
            continue
        seen.add(pid)
        out.append(
            {
                "product_id": r.get("product_id"),
                "product_name": r.get("product_name"),
                "gross_pln": r.get("gross_pln"),
                "units": r.get("units"),
                "relevance": round(rel, 3),
            }
        )
        if len(out) >= lim:
            break

    if not out and brand_id is not None and len(bq_tokens) > 1:
        pool2 = basic_products.query_products_any_token_match(
            ps, pe, tokens=bq_tokens[:1], brand_id=int(brand_id), candidate_limit=400
        ) or []
        scored2: list[tuple[float, float, dict[str, Any]]] = []
        for r in pool2:
            pn = str(r.get("product_name") or "")
            rel = max(_score_name_against_tokens(pn, tokens), 0.25)
            g = float(r.get("gross_pln") or 0)
            scored2.append((rel, g, r))
        scored2.sort(key=lambda x: (-x[0], -x[1]))
        for rel, _g, r in scored2:
            pid = r.get("product_id")
            if pid in seen:
                continue
            seen.add(pid)
            out.append(
                {
                    "product_id": r.get("product_id"),
                    "product_name": r.get("product_name"),
                    "gross_pln": r.get("gross_pln"),
                    "units": r.get("units"),
                    "relevance": round(rel, 3),
                }
            )
            if len(out) >= lim:
                break

    hint = None if out else "no_matches_try_broader_terms"
    if out and out[0].get("relevance", 1) < 0.55:
        hint = "low_confidence_confirm_with_user"

    return {
        "matches": _json_safe(out),
        "query_tokens": tokens,
        "hint": hint,
    }


def tool_list_competitors_in_category(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
) -> dict[str, Any]:
    """Brands that sell in the same macro or subcategory as the user's brand."""
    cat = int(parent_category_id) if parent_category_id and 1 <= int(parent_category_id) <= 10 else None
    sub = int(subcategory_id) if subcategory_id and int(subcategory_id) >= 100 else None
    rows = bc.query_competitors_in_scope(ps, pe, int(brand_id), cat=cat, subcat=sub)
    return {"competitors": _truncate_rows(rows or [], 50)}


def _trim_segment_summary_payload(raw: dict[str, Any]) -> dict[str, Any]:
    """Keep segment summary small for Gemini (pain_points, needstates, top SKUs/categories)."""
    segs = raw.get("segments") or []
    out_segs: list[dict[str, Any]] = []
    for s in segs[:6]:
        out_segs.append(
            {
                "segment_id": s.get("segment_id"),
                "name": s.get("name"),
                "pain_points": s.get("pain_points"),
                "needstates": s.get("needstates"),
                "top_categories_note": s.get("top_categories_note"),
                "top_categories": _truncate_rows(s.get("top_categories") or [], 5),
                "top_skus": _truncate_rows(s.get("top_skus") or [], 5),
            }
        )
    return {"segments": out_segs, "period": raw.get("period")}


def tool_get_segment_marketing_summary(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    segment_id: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
) -> dict[str, Any]:
    """Pain points, needstate tags, top categories/SKUs for one HCG segment (optionally scoped to category/subcategory)."""
    sid = int(segment_id)
    if not (1 <= sid <= 6):
        return {"error": "segment_id must be 1–6"}
    pc = int(parent_category_id) if parent_category_id and 1 <= int(parent_category_id) <= 10 else None
    sub = int(subcategory_id) if subcategory_id and int(subcategory_id) >= 100 else None
    raw = marketing_svc.get_segment_summary(ps, pe, sid, pc, sub, int(brand_id))
    return _trim_segment_summary_payload(raw if isinstance(raw, dict) else {})


def tool_get_category_needstate_landscape(
    ps: str,
    pe: str,
    *,
    parent_category_id: int,
) -> dict[str, Any]:
    """All segments: revenue share in parent category + dominant needstate label (market-wide, not brand-only)."""
    pc = int(parent_category_id)
    if not (1 <= pc <= 10):
        return {"error": "parent_category_id must be 1–10"}
    raw = marketing_svc.get_needstates(ps, pe, pc, None)
    segs = raw.get("segments") or []
    return {
        "category_id": raw.get("category_id"),
        "period": raw.get("period"),
        "segments": _truncate_rows(segs, 12),
    }


def tool_get_needstate_dimensions_for_segment(
    ps: str,
    pe: str,
    *,
    parent_category_id: int,
    segment_id: int,
) -> dict[str, Any]:
    """Seven needstate dimensions with affinity scores for one segment in a parent category (spider / prioritisation)."""
    pc = int(parent_category_id)
    sid = int(segment_id)
    if not (1 <= pc <= 10) or not (1 <= sid <= 6):
        return {"error": "parent_category_id 1–10 and segment_id 1–6 required"}
    return marketing_svc.get_needstates(ps, pe, pc, sid)


def tool_get_media_touchpoints(
    *,
    segment_id: int,
    parent_category_id: int | None = None,
) -> dict[str, Any]:
    """How the segment uses media / touchpoints (social, TV-style blocks, etc.) — static model tuned by category."""
    sid = int(segment_id)
    if not (1 <= sid <= 6):
        return {"error": "segment_id must be 1–6"}
    pc = int(parent_category_id) if parent_category_id and 1 <= int(parent_category_id) <= 10 else None
    return marketing_svc.get_media_preferences(sid, pc)


def tool_get_purchasing_journey(
    ps: str,
    pe: str,
    *,
    segment_id: int | None = None,
    parent_category_id: int | None = None,
) -> dict[str, Any]:
    """Purchase channels, peak events, traffic source mix, pre-purchase search intent (for 'how they inform themselves')."""
    sid = _opt_int(segment_id)
    pc = int(parent_category_id) if parent_category_id and 1 <= int(parent_category_id) <= 10 else None
    raw = marketing_svc.get_purchasing(ps, pe, sid, pc)
    if not isinstance(raw, dict):
        return {"error": "no data"}
    return {
        "channel_mix": _truncate_rows(raw.get("channel_mix") or [], 20),
        "peak_events": _truncate_rows(raw.get("peak_events") or [], 12),
        "source_mix": raw.get("source_mix"),
        "pre_purchase_searches": raw.get("pre_purchase_searches"),
        "segment_id": raw.get("segment_id"),
        "parent_category_id": raw.get("parent_category_id"),
        "period": raw.get("period"),
    }


_TOOL_IMPL = {
    "list_categories": lambda ps, pe, bid, a: tool_list_categories(),
    "list_segments": lambda ps, pe, bid, a: tool_list_segments(),
    "get_top_products": lambda ps, pe, bid, a: tool_get_top_products(
        ps,
        pe,
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        parent_category_id=_opt_int(a.get("parent_category_id")),
        subcategory_id=_opt_int(a.get("subcategory_id")),
        limit=int(a.get("limit") or 15),
    ),
    "get_underperforming_products": lambda ps, pe, bid, a: tool_get_underperforming_products(
        ps,
        pe,
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        parent_category_id=_opt_int(a.get("parent_category_id")),
        subcategory_id=_opt_int(a.get("subcategory_id")),
        bottom_pct=float(a.get("bottom_pct") if a.get("bottom_pct") is not None else 0.10),
        limit=int(a.get("limit") or 30),
    ),
    "get_sales_by_category_for_brand": lambda ps, pe, bid, a: tool_get_sales_by_category_for_brand(
        ps, pe, brand_id=int(_opt_int(a.get("brand_id")) or bid)
    ),
    "get_segment_breakdown_for_category": lambda ps, pe, bid, a: tool_get_segment_breakdown_for_category(
        ps,
        pe,
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        parent_category_id=_req_int(a.get("parent_category_id"), None, "parent_category_id"),
        subcategory_id=_opt_int(a.get("subcategory_id")),
    ),
    "get_purchasing_channel_mix": lambda ps, pe, bid, a: tool_get_purchasing_channel_mix(
        ps,
        pe,
        segment_id=_opt_int(a.get("segment_id")),
        parent_category_id=_opt_int(a.get("parent_category_id")),
    ),
    "get_segment_promo_responsiveness": lambda ps, pe, bid, a: tool_get_segment_promo_responsiveness(
        ps,
        pe,
        parent_category_id=_opt_int(a.get("parent_category_id")),
        subcategory_id=_opt_int(a.get("subcategory_id")),
        promo_type=(str(a.get("promo_type") or "").strip() or None),
    ),
    "get_product_segment_breakdown": lambda ps, pe, bid, a: tool_get_product_segment_breakdown(
        ps,
        pe,
        product_id=_req_int(a.get("product_id"), None, "product_id"),
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        category_filter=_opt_int(a.get("category_filter")),
    ),
    "get_brand_vs_market_subcategory_sales": lambda ps, pe, bid, a: tool_get_brand_vs_market_subcategory_sales(
        ps,
        pe,
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        parent_category_id=_opt_int(a.get("parent_category_id")),
        subcategory_id=_opt_int(a.get("subcategory_id")),
    ),
    "get_promo_roi_by_type_for_brand": lambda ps, pe, bid, a: tool_get_promo_roi_by_type_for_brand(
        ps,
        pe,
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        parent_category_id=_opt_int(a.get("parent_category_id")),
    ),
    "search_products_by_query": lambda ps, pe, bid, a: tool_search_products_by_query(
        ps,
        pe,
        query=str(a.get("query") or ""),
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        limit=int(a.get("limit") or 12),
    ),
    "list_competitors_in_category": lambda ps, pe, bid, a: tool_list_competitors_in_category(
        ps,
        pe,
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        parent_category_id=_opt_int(a.get("parent_category_id")),
        subcategory_id=_opt_int(a.get("subcategory_id")),
    ),
    "get_segment_marketing_summary": lambda ps, pe, bid, a: tool_get_segment_marketing_summary(
        ps,
        pe,
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        segment_id=_req_int(a.get("segment_id"), None, "segment_id"),
        parent_category_id=_opt_int(a.get("parent_category_id")),
        subcategory_id=_opt_int(a.get("subcategory_id")),
    ),
    "get_category_needstate_landscape": lambda ps, pe, bid, a: tool_get_category_needstate_landscape(
        ps,
        pe,
        parent_category_id=_req_int(a.get("parent_category_id"), None, "parent_category_id"),
    ),
    "get_needstate_dimensions_for_segment": lambda ps, pe, bid, a: tool_get_needstate_dimensions_for_segment(
        ps,
        pe,
        parent_category_id=_req_int(a.get("parent_category_id"), None, "parent_category_id"),
        segment_id=_req_int(a.get("segment_id"), None, "segment_id"),
    ),
    "get_media_touchpoints": lambda ps, pe, bid, a: tool_get_media_touchpoints(
        segment_id=_req_int(a.get("segment_id"), None, "segment_id"),
        parent_category_id=_opt_int(a.get("parent_category_id")),
    ),
    "get_purchasing_journey": lambda ps, pe, bid, a: tool_get_purchasing_journey(
        ps,
        pe,
        segment_id=_opt_int(a.get("segment_id")),
        parent_category_id=_opt_int(a.get("parent_category_id")),
    ),
}


def _opt_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _req_int(v: Any, fallback: int | None, name: str) -> int:
    x = _opt_int(v)
    if x is not None:
        return x
    if fallback is not None:
        return int(fallback)
    raise ValueError(f"Missing required integer: {name}")


def run_expert_tool(
    name: str,
    args: dict[str, Any] | None,
    *,
    period_start: str,
    period_end: str,
    default_brand_id: int,
) -> dict[str, Any]:
    """Execute a tool by name; returns a JSON-serializable dict (may include 'error' key)."""
    fn = _TOOL_IMPL.get(name)
    if not fn:
        return {"error": f"Unknown tool: {name}"}
    a = dict(args or {})
    try:
        return fn(period_start, period_end, default_brand_id, a)
    except ValueError as e:
        return {"error": str(e)}
    except Exception as e:  # pragma: no cover
        return {"error": f"Tool execution failed: {e!s}"}


def build_expert_gemini_tool() -> types.Tool:
    """Function declarations for Gemini (google-genai SDK)."""
    decls = [
        types.FunctionDeclaration(
            name="list_categories",
            description="Return all parent category ids (1–10) and subcategory ids (>=100) with English names. Use to map user language to ids.",
            parameters={"type": "object", "properties": {}},
        ),
        types.FunctionDeclaration(
            name="list_segments",
            description="Return the 6 HCG customer segments with descriptions and drivers.",
            parameters={"type": "object", "properties": {}},
        ),
        types.FunctionDeclaration(
            name="get_top_products",
            description="Top selling SKUs (gross PLN) for optional brand and category filters.",
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer", "description": "Brand id; defaults to logged-in user's brand."},
                    "parent_category_id": {"type": "integer", "description": "Parent category 1–10."},
                    "subcategory_id": {"type": "integer", "description": "Subcategory id >=100 (e.g. 501 Refrigerators)."},
                    "limit": {"type": "integer", "description": "Max rows (default 15, max 50)."},
                },
            },
        ),
        types.FunctionDeclaration(
            name="get_underperforming_products",
            description=(
                "Returns the bottom fraction of the brand's products by sales revenue (gross PLN) in the period, "
                "with percentile rank. Use for portfolio cleanup, delist candidates, underperformer audits, "
                "or when the user asks for weakest / lowest-selling SKUs. Not the same as get_top_products."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer", "description": "Defaults to logged-in user's brand."},
                    "parent_category_id": {"type": "integer", "description": "Optional parent category 1–10."},
                    "subcategory_id": {"type": "integer", "description": "Optional subcategory id >=100."},
                    "bottom_pct": {
                        "type": "number",
                        "description": "Fraction of lowest-selling products to return by percentile (default 0.10 = bottom 10%).",
                    },
                    "limit": {"type": "integer", "description": "Max rows after filter (default 30, max 80)."},
                },
            },
        ),
        types.FunctionDeclaration(
            name="get_sales_by_category_for_brand",
            description="Brand gross PLN vs total market per parent category where the brand has sales.",
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer", "description": "Defaults to logged-in user's brand."},
                },
            },
        ),
        types.FunctionDeclaration(
            name="get_segment_breakdown_for_category",
            description="How HCG segments contribute to the brand's sales in a parent category (optional subcategory).",
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer"},
                    "parent_category_id": {"type": "integer", "description": "1–10"},
                    "subcategory_id": {"type": "integer", "description": "Optional >=100"},
                },
                "required": ["parent_category_id"],
            },
        ),
        types.FunctionDeclaration(
            name="get_purchasing_channel_mix",
            description="Orders and revenue mix by channel (web/app/store). Optional filter by segment_id and/or parent_category_id.",
            parameters={
                "type": "object",
                "properties": {
                    "segment_id": {"type": "integer"},
                    "parent_category_id": {"type": "integer", "description": "Macro category 1–10"},
                },
            },
        ),
        types.FunctionDeclaration(
            name="get_segment_promo_responsiveness",
            description="Top segments by share of category sales on promotion. Needs parent_category_id or subcategory_id.",
            parameters={
                "type": "object",
                "properties": {
                    "parent_category_id": {"type": "integer"},
                    "subcategory_id": {"type": "integer"},
                    "promo_type": {
                        "type": "string",
                        "description": "Optional promo_type e.g. percentage_discount, flash_sale, app_only",
                    },
                },
            },
        ),
        types.FunctionDeclaration(
            name="get_product_segment_breakdown",
            description="Segment-level sales for a single product_id (SKU removal / dependency analysis).",
            parameters={
                "type": "object",
                "properties": {
                    "product_id": {"type": "integer"},
                    "brand_id": {"type": "integer", "description": "Defaults to logged-in user's brand."},
                    "category_filter": {
                        "type": "integer",
                        "description": "Optional: parent 1–10 or subcategory >=100 to filter lines",
                    },
                },
                "required": ["product_id"],
            },
        ),
        types.FunctionDeclaration(
            name="get_brand_vs_market_subcategory_sales",
            description="Subcategory-level brand gross vs market (media) gross. Filter by parent and/or subcategory.",
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer"},
                    "parent_category_id": {"type": "integer"},
                    "subcategory_id": {"type": "integer"},
                },
            },
        ),
        types.FunctionDeclaration(
            name="get_promo_roi_by_type_for_brand",
            description="Promo ROI benchmarks by promo type for the brand (optional parent category).",
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer"},
                    "parent_category_id": {"type": "integer"},
                },
            },
        ),
        types.FunctionDeclaration(
            name="search_products_by_query",
            description=(
                "Resolve a user's natural-language product description to real SKUs in the catalog "
                "(e.g. 'Samsung foldable', 'Galaxy premium'). Returns ranked matches with relevance scores — "
                "use these to ask the user for confirmation before delisting analysis; do not demand a numeric ID first."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "brand_id": {"type": "integer"},
                    "limit": {"type": "integer"},
                },
                "required": ["query"],
            },
        ),
        types.FunctionDeclaration(
            name="list_competitors_in_category",
            description="Brands competing in the same macro or subcategory as the user's brand.",
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer", "description": "Defaults to logged-in user's brand."},
                    "parent_category_id": {"type": "integer"},
                    "subcategory_id": {"type": "integer"},
                },
            },
        ),
        types.FunctionDeclaration(
            name="get_segment_marketing_summary",
            description="For one HCG segment: pain_points, needstates tags, top SKUs/categories for the user's brand. Use after picking target segments.",
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer"},
                    "segment_id": {"type": "integer", "description": "1–6"},
                    "parent_category_id": {"type": "integer", "description": "Macro 1–10"},
                    "subcategory_id": {"type": "integer", "description": "Optional >=100"},
                },
                "required": ["segment_id"],
            },
        ),
        types.FunctionDeclaration(
            name="get_category_needstate_landscape",
            description="Market-wide segment shares in a parent category plus dominant needstate label per segment. Use to choose who to win.",
            parameters={
                "type": "object",
                "properties": {"parent_category_id": {"type": "integer", "description": "1–10"}},
                "required": ["parent_category_id"],
            },
        ),
        types.FunctionDeclaration(
            name="get_needstate_dimensions_for_segment",
            description="Seven needstate dimensions with scores for one segment in a category (messaging and assortment angles).",
            parameters={
                "type": "object",
                "properties": {
                    "parent_category_id": {"type": "integer"},
                    "segment_id": {"type": "integer"},
                },
                "required": ["parent_category_id", "segment_id"],
            },
        ),
        types.FunctionDeclaration(
            name="get_media_touchpoints",
            description="Static media / touchpoint mix for how the segment discovers and evaluates (use with purchasing journey for full picture).",
            parameters={
                "type": "object",
                "properties": {
                    "segment_id": {"type": "integer"},
                    "parent_category_id": {"type": "integer"},
                },
                "required": ["segment_id"],
            },
        ),
        types.FunctionDeclaration(
            name="get_purchasing_journey",
            description="Purchase channel mix, peak events, online source mix, pre-purchase search themes. Optional segment_id and parent_category_id.",
            parameters={
                "type": "object",
                "properties": {
                    "segment_id": {"type": "integer"},
                    "parent_category_id": {"type": "integer"},
                },
            },
        ),
    ]
    return types.Tool(function_declarations=decls)


def tool_result_for_gemini(payload: dict[str, Any]) -> dict[str, Any]:
    """Wrap tool output for FunctionResponse.response (keep small)."""
    try:
        s = json.dumps(payload, ensure_ascii=False)
    except TypeError:
        s = json.dumps(_json_safe(payload), ensure_ascii=False)
    if len(s) > 120_000:
        return {"truncated": True, "preview": s[:80_000] + "…"}
    return payload
