"""Product-related tools: catalog, top/underperforming, search, price, new launches, competitors."""

from __future__ import annotations

from typing import Any

from app.constants import ADMIN_CATEGORIES, ADMIN_SUBCATEGORIES
from app.db.queries.basic import products as basic_products
from app.db.queries import brand_comparison as bc

from ._base import (
    STATIC_SEGMENTS,
    _bq_substring_tokens,
    _json_safe,
    _opt_int,
    _score_name_against_tokens,
    _tokenize_product_query,
    _truncate_rows,
)


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


def _format_underperformer_row(r: dict[str, Any]) -> None:
    raw = float(r.get("pct_rank") or 0)
    r["pct_rank"] = round(raw, 4)
    r["pct_rank_pct"] = f"{raw * 100:.1f}%"
    r["gross_pln"] = round(float(r.get("gross_pln") or 0), 2)
    u = r.get("units")
    if u is not None:
        r["units"] = round(float(u), 2)


def tool_get_underperforming_products(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
    bottom_pct: float = 0.10,
    limit: int = 80,
    top_n: int | None = None,
) -> dict[str, Any]:
    """Bottom fraction or worst-N brand SKUs by gross_pln (PERCENT_RANK ascending) for portfolio / delist analysis."""
    cat_filter: int | None = None
    if subcategory_id and int(subcategory_id) >= 100:
        cat_filter = int(subcategory_id)
    elif parent_category_id and 1 <= int(parent_category_id) <= 10:
        cat_filter = int(parent_category_id)
    bp = float(bottom_pct) if bottom_pct is not None else 0.10
    lim = max(1, min(int(limit or 80), 80))
    tn = _opt_int(top_n)
    rows = basic_products.query_underperforming_products(
        ps,
        pe,
        brand_id=int(brand_id),
        parent_category_id=cat_filter,
        bottom_pct=bp,
        limit=lim,
        top_n=tn,
    )
    cap = min(tn, 80) if tn and tn > 0 else lim
    for r in rows or []:
        _format_underperformer_row(r)
    out_rows = _truncate_rows(rows or [], cap)
    payload: dict[str, Any] = {
        "underperformers": out_rows,
        "returned_count": len(out_rows),
        "scope_note": (
            "pct_rank is PERCENT_RANK over gross_pln ascending within the brand (and optional category filter); "
            "0 = lowest sales among products with sales in the period. "
            "Use pct_rank_pct for user-facing percentile labels."
        ),
    }
    if tn and tn > 0:
        payload["mode"] = "top_n"
        payload["top_n"] = min(tn, 80)
    else:
        payload["mode"] = "bottom_pct"
        payload["bottom_pct"] = bp
    return payload


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


def tool_get_products_by_price(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
    order: str = "most_expensive",
    limit: int = 15,
) -> dict[str, Any]:
    """Products ranked by list price (price_pln). order='most_expensive' gives highest first, 'cheapest' gives lowest."""
    bq_order = "asc" if (order or "").lower() == "cheapest" else "desc"
    lim = max(1, min(int(limit or 15), 50))
    rows = basic_products.query_products_by_price(
        ps, pe,
        brand_id=int(brand_id),
        parent_category_id=int(parent_category_id) if parent_category_id else None,
        subcategory_id=int(subcategory_id) if subcategory_id else None,
        order=bq_order,
        limit=lim,
    )
    return {"products_by_price": _truncate_rows(rows or [], lim), "order": order}


def tool_get_products_in_price_range(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    price_min: float | None = None,
    price_max: float | None = None,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Products in a PLN price bracket with sales in the period. Both bounds are optional."""
    lim = max(1, min(int(limit or 20), 80))
    rows = basic_products.query_products_in_price_range(
        ps, pe,
        brand_id=int(brand_id),
        price_min=float(price_min) if price_min is not None else None,
        price_max=float(price_max) if price_max is not None else None,
        parent_category_id=int(parent_category_id) if parent_category_id else None,
        subcategory_id=int(subcategory_id) if subcategory_id else None,
        limit=lim,
    )
    return {
        "products_in_range": _truncate_rows(rows or [], lim),
        "price_min_pln": price_min,
        "price_max_pln": price_max,
        "returned_count": len(rows or []),
    }


def tool_get_new_product_launches(
    ps: str,
    pe: str,
    *,
    brand_id: int,
    min_launch_year: int,
    parent_category_id: int | None = None,
    subcategory_id: int | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Products launched in min_launch_year or later, with their sales performance in the period."""
    lim = max(1, min(int(limit or 20), 50))
    rows = basic_products.query_new_launches(
        ps, pe,
        brand_id=int(brand_id),
        min_launch_year=int(min_launch_year),
        parent_category_id=int(parent_category_id) if parent_category_id else None,
        subcategory_id=int(subcategory_id) if subcategory_id else None,
        limit=lim,
    )
    rows = list(rows) if rows else []
    return {
        "new_launches": _truncate_rows(rows, lim),
        "count": len(rows),
        "min_launch_year": int(min_launch_year),
    }
