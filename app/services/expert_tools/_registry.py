"""Tool registry: _TOOL_IMPL dispatch map, run_expert_tool and build_expert_gemini_tool (Gemini declarations)."""

from __future__ import annotations

from typing import Any

from google.genai import types

from ._base import _opt_int, _req_int
from ._tools_product import (
    tool_list_categories,
    tool_list_segments,
    tool_get_top_products,
    tool_get_underperforming_products,
    tool_search_products_by_query,
    tool_list_competitors_in_category,
    tool_get_products_by_price,
    tool_get_products_in_price_range,
    tool_get_new_product_launches,
)
from ._tools_market import (
    tool_get_sales_by_category_for_brand,
    tool_get_brand_vs_market_subcategory_sales,
    tool_get_sales_trend_by_month,
    tool_get_sales_by_gender,
)
from ._tools_segment import (
    tool_get_segment_breakdown_for_category,
    tool_get_segment_marketing_summary,
    tool_get_category_needstate_landscape,
    tool_get_needstate_dimensions_for_segment,
)
from ._tools_customer import (
    tool_get_purchasing_channel_mix,
    tool_get_media_touchpoints,
    tool_get_purchasing_journey,
    tool_get_customer_stats,
)
from ._tools_promo import (
    tool_get_segment_promo_responsiveness,
    tool_get_product_segment_breakdown,
    tool_get_promo_roi_by_type_for_brand,
)


# ---------------------------------------------------------------------------
# Dispatch map
# ---------------------------------------------------------------------------

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
        limit=int(a.get("limit") or 80),
        top_n=_opt_int(a.get("top_n")),
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
    "get_products_by_price": lambda ps, pe, bid, a: tool_get_products_by_price(
        ps, pe,
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        parent_category_id=_opt_int(a.get("parent_category_id")),
        subcategory_id=_opt_int(a.get("subcategory_id")),
        order=str(a.get("order") or "most_expensive"),
        limit=int(a.get("limit") or 15),
    ),
    "get_products_in_price_range": lambda ps, pe, bid, a: tool_get_products_in_price_range(
        ps, pe,
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        price_min=float(a["price_min"]) if a.get("price_min") is not None else None,
        price_max=float(a["price_max"]) if a.get("price_max") is not None else None,
        parent_category_id=_opt_int(a.get("parent_category_id")),
        subcategory_id=_opt_int(a.get("subcategory_id")),
        limit=int(a.get("limit") or 20),
    ),
    "get_sales_trend_by_month": lambda ps, pe, bid, a: tool_get_sales_trend_by_month(
        ps, pe,
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        parent_category_id=_opt_int(a.get("parent_category_id")),
        subcategory_id=_opt_int(a.get("subcategory_id")),
    ),
    "get_new_product_launches": lambda ps, pe, bid, a: tool_get_new_product_launches(
        ps, pe,
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        min_launch_year=int(a.get("min_launch_year") or 2024),
        parent_category_id=_opt_int(a.get("parent_category_id")),
        subcategory_id=_opt_int(a.get("subcategory_id")),
        limit=int(a.get("limit") or 20),
    ),
    "get_customer_stats": lambda ps, pe, bid, a: tool_get_customer_stats(
        ps, pe,
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        parent_category_id=_opt_int(a.get("parent_category_id")),
    ),
    "get_sales_by_gender": lambda ps, pe, bid, a: tool_get_sales_by_gender(
        ps, pe,
        brand_id=int(_opt_int(a.get("brand_id")) or bid),
        parent_category_id=_opt_int(a.get("parent_category_id")),
        subcategory_id=_opt_int(a.get("subcategory_id")),
    ),
}


# ---------------------------------------------------------------------------
# Public executor
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Gemini function declarations
# ---------------------------------------------------------------------------

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
                "Returns the brand's lowest-selling products by gross PLN in the period, with pct_rank and pct_rank_pct. "
                "Either set bottom_pct (e.g. 0.10) OR top_n (e.g. 5 for 'worst 5 products'); if top_n is set, bottom_pct is ignored. "
                "Includes returned_count and mode. Use limit 80 for full-portfolio percentile audits. Not get_top_products."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer", "description": "Defaults to logged-in user's brand."},
                    "parent_category_id": {"type": "integer", "description": "Optional parent category 1–10."},
                    "subcategory_id": {"type": "integer", "description": "Optional subcategory id >=100."},
                    "bottom_pct": {
                        "type": "number",
                        "description": "Fraction of lowest-selling products by percentile (default 0.10). Ignored if top_n is set.",
                    },
                    "top_n": {
                        "type": "integer",
                        "description": (
                            "If set, return exactly the N worst-selling SKUs (max 80). "
                            "Use when the user asks for 'worst 5 products' instead of a percentage."
                        ),
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Cap on rows after percentile filter (default 80, max 80). Not used as the primary control when top_n is set.",
                    },
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
        types.FunctionDeclaration(
            name="get_products_by_price",
            description=(
                "Return the brand's products ranked by list price (price_pln from the catalog). "
                "Use for questions like 'highest-priced', 'most expensive', 'cheapest', 'lowest-priced', "
                "'premium products', 'entry-level products'. "
                "order='most_expensive' (default) returns highest price first; 'cheapest' returns lowest first. "
                "Also returns premium_flag and launch_year."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer", "description": "Defaults to logged-in user's brand."},
                    "parent_category_id": {"type": "integer", "description": "Optional parent category 1–10."},
                    "subcategory_id": {"type": "integer", "description": "Optional subcategory id >=100."},
                    "order": {
                        "type": "string",
                        "description": "'most_expensive' (default) or 'cheapest'.",
                    },
                    "limit": {"type": "integer", "description": "Max rows (default 15, max 50)."},
                },
            },
        ),
        types.FunctionDeclaration(
            name="get_products_in_price_range",
            description=(
                "Return brand products whose catalog list price falls between price_min and price_max (PLN). "
                "Use for 'what do we sell between X and Y PLN', 'mid-range offering', 'products under N PLN', "
                "'products over N PLN'. Both bounds are optional (omit one for open-ended ranges)."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer"},
                    "price_min": {"type": "number", "description": "Lower bound in PLN (inclusive). Omit for no lower bound."},
                    "price_max": {"type": "number", "description": "Upper bound in PLN (inclusive). Omit for no upper bound."},
                    "parent_category_id": {"type": "integer", "description": "Optional parent category 1–10."},
                    "subcategory_id": {"type": "integer", "description": "Optional subcategory id >=100."},
                    "limit": {"type": "integer", "description": "Max rows (default 20)."},
                },
            },
        ),
        types.FunctionDeclaration(
            name="get_sales_trend_by_month",
            description=(
                "Monthly breakdown of gross PLN and units for the brand's sales. "
                "Use for: 'how are my sales trending?', 'month over month', 'best/worst month', "
                "'Q1 vs Q2', 'are TVs going up or down?', 'monthly revenue breakdown'. "
                "Returns each month's revenue, units, plus best and worst month highlighted. "
                "Scope to a category by providing parent_category_id or subcategory_id."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer"},
                    "parent_category_id": {"type": "integer", "description": "Optional parent category 1–10."},
                    "subcategory_id": {"type": "integer", "description": "Optional subcategory id >=100."},
                },
            },
        ),
        types.FunctionDeclaration(
            name="get_new_product_launches",
            description=(
                "Products launched in min_launch_year or later, with their sales performance. "
                "Use for: 'which products did we launch this year/in 2025?', 'how are our new launches doing?', "
                "'newest products', 'recent launches performance'. "
                "Set min_launch_year to the relevant year (e.g. 2025 for 'this year', 2024 for 'last 2 years')."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer"},
                    "min_launch_year": {"type": "integer", "description": "Minimum launch year (e.g. 2025)."},
                    "parent_category_id": {"type": "integer", "description": "Optional parent category 1–10."},
                    "subcategory_id": {"type": "integer", "description": "Optional subcategory id >=100."},
                    "limit": {"type": "integer", "description": "Max rows (default 20)."},
                },
                "required": ["min_launch_year"],
            },
        ),
        types.FunctionDeclaration(
            name="get_customer_stats",
            description=(
                "Aggregate buyer metrics for the brand: unique customers, average order value, "
                "orders per customer, loyalty card %, omnichannel %, app user %, and channel breakdown. "
                "Use for: 'how many customers?', 'average order value', 'loyalty breakdown', "
                "'omnichannel buyers', 'app usage', 'customer count by channel'. "
                "Optionally scoped to a parent category."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer"},
                    "parent_category_id": {"type": "integer", "description": "Optional: scope to a parent category 1–10."},
                },
            },
        ),
        types.FunctionDeclaration(
            name="get_sales_by_gender",
            description=(
                "Sales breakdown by customer gender (M/F): gross PLN, units, and percentage of total. "
                "Use for: 'gender split', 'do men or women buy more?', 'which gender drives revenue?', "
                "'are women buying our products?', 'male vs female sales'. "
                "Optionally scoped to a category or subcategory."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "brand_id": {"type": "integer"},
                    "parent_category_id": {"type": "integer", "description": "Optional parent category 1–10."},
                    "subcategory_id": {"type": "integer", "description": "Optional subcategory id >=100."},
                },
            },
        ),
    ]
    return types.Tool(function_declarations=decls)
