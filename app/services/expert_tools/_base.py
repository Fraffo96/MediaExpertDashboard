"""Shared constants, segment definitions and utility helpers for expert_tools."""

from __future__ import annotations

import json
import re
from typing import Any

from app.constants import ADMIN_CATEGORIES, ADMIN_SUBCATEGORIES

_QUERY_STOPWORDS = frozenset(
    {
        "my", "the", "a", "an", "for", "and", "or", "to", "of", "in", "on",
        "at", "sku", "skus", "our", "your", "me", "we", "i", "it", "is",
        "be", "as", "by", "from", "with", "this", "that", "product", "item",
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
    "get_products_by_price": "Looking up your products by list price...",
    "get_products_in_price_range": "Filtering your catalog by price bracket...",
    "get_sales_trend_by_month": "Building your monthly sales trend...",
    "get_new_product_launches": "Checking your recent product launches...",
    "get_customer_stats": "Pulling your customer profile metrics...",
    "get_sales_by_gender": "Analyzing sales by customer gender...",
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


def tool_result_for_gemini(payload: dict[str, Any]) -> dict[str, Any]:
    """Wrap tool output for FunctionResponse.response (keep small)."""
    try:
        s = json.dumps(payload, ensure_ascii=False)
    except TypeError:
        s = json.dumps(_json_safe(payload), ensure_ascii=False)
    if len(s) > 120_000:
        return {"truncated": True, "preview": s[:80_000] + "…"}
    return payload
