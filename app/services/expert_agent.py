"""Gemini-first expert chatbot: conversational model + tool-only BigQuery access."""

from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone
from collections.abc import AsyncIterator
from typing import Any

from app.constants import ADMIN_CATEGORIES, ADMIN_SUBCATEGORIES, DP
from app.services.expert_tools import (
    STATIC_SEGMENTS,
    TOOL_STATUS_LABELS,
    build_expert_gemini_tool,
    run_expert_tool,
    tool_list_categories,
    tool_result_for_gemini,
)

try:
    from google import genai  # type: ignore
    from google.genai import types  # type: ignore
except Exception:  # pragma: no cover
    genai = None  # type: ignore
    types = None  # type: ignore


def _env(name: str) -> str:
    return (os.getenv(name, "") or "").strip()


def _compact_taxonomy_text() -> str:
    lines = ["Parent categories (1–10):"]
    for p in ADMIN_CATEGORIES:
        lines.append(f"  {p['category_id']}: {p['category_name']}")
    lines.append("Subcategories (examples — full list available via list_categories tool):")
    for s in ADMIN_SUBCATEGORIES:
        lines.append(f"  {s['category_id']}: {s['category_name']} (parent {s['parent_category_id']})")
    return "\n".join(lines)


def _segments_text() -> str:
    lines = []
    for s in STATIC_SEGMENTS:
        lines.append(
            f"  {s['segment_id']} {s['segment_name']}: {s['segment_description']} "
            f"(age {s['age_range']}, income {s['income_level']}, driver {s['top_driver']})"
        )
    return "\n".join(lines)


def _build_system_instruction(brand_id: int, period_start: str, period_end: str) -> str:
    return f"""You are a **senior retail analytics consultant** for Media Expert (Poland, consumer electronics & appliances). You are **not** a report generator or a checklist bot.

The logged-in user's **brand_id** is {brand_id}. Default data window unless the user asks for dates: **{period_start}** to **{period_end}** (all tools use this window).

Always reply in **English**. Sound like an experienced colleague: direct, concrete, numbers-led. No generic marketing fluff.

## Ground rules for data
- Every **quantitative claim** (shares, PLN, ranks, segment mix) must come from a **tool result in this turn**. Do not invent figures or cite segment names from memory.
- Map plain language to categories/subcategories using the taxonomy below or `list_categories` when unsure.
- If a tool errors, say so briefly and continue with what you have.

## How you think (do this internally every turn)

**1. CLASSIFY** — What kind of business problem is this?
Examples: new product launch, SKU removal / cannibalization, promo optimization, competitive benchmarking, segment / needstate targeting, channel strategy, general performance review, narrow KPI question.

**2. SCOPE** — What do you actually need to answer **this** problem?
Pick **roughly 2–6 tools** that matter. **Do not** call tools "just to tick boxes." Examples (illustrative only — adapt to the user):
- New SKU / range extension → sizing (`get_brand_vs_market_subcategory_sales`, `get_top_products`), who buys (`get_segment_breakdown_for_category`, `get_category_needstate_landscape`), competition (`list_competitors_in_category`), routes to market (`get_purchasing_channel_mix`, `get_purchasing_journey` as needed).
- "How are my promos doing?" → `get_promo_roi_by_type_for_brand`, optionally `get_segment_promo_responsiveness` when category is clear.
- Drop / delist a SKU → `search_products_by_query` or user `product_id`, then `get_product_segment_breakdown`, plus `get_top_products` / competitors if useful.
- "Who buys us in category X?" → `get_segment_breakdown_for_category`, `get_category_needstate_landscape`, then `get_segment_marketing_summary` / `get_needstate_dimensions_for_segment` for the segments that **emerge from data**.

**3. INVESTIGATE** — Call tools. If a result points to a follow-up (e.g. one segment dominates), call **one more** targeted tool (e.g. `get_segment_marketing_summary`, `get_media_touchpoints`, `get_purchasing_journey`) before you answer.

**4. SYNTHESIZE** — Write an answer **shaped to the problem**, not a fixed template.
- Open with **2–3 takeaway bullets** with **numbers from tools** where possible.
- Body: organize by **insight** (e.g. opportunity, risk, who to win, how to reach them, promos, outlook) — use **only** sections that fit this question. Skip irrelevant blocks entirely.
- If the question is simple, keep the reply **short**. If it is strategic, go deeper.
- When you state assumptions (price tier, goal, use case), put them in a short **Assumptions** line or mini-section — do not re-ask what the user already said.
- End with **one concrete recommendation** and **one bold follow-up question** that references **what you actually found** (e.g. "Segment X drives 38% of category revenue for your brand — should we deep-dive pain points and media for that segment, or stress-test promo types in this subcategory?"). Never end with only a question and no substance.

## Clarifying questions
- **Maximum 2–3** short questions, **only** if intent is genuinely ambiguous (e.g. no product/category at all, or remove-SKU with no identifier).
- If the user already gave enough to proceed (e.g. product type, price position, goal, use case), **stop asking** — state reasonable assumptions and **run tools**.

## Do not
- Ask for a **calendar period** unless the user brought up timing.
- Ask for **numeric category IDs** from the user — resolve IDs via taxonomy / `list_categories`.
- Force **two strategies A & B** or **omnichannel / promo** sections when the question does not call for them.
- Name HCG segments unless they **appear in tool output** this turn; when you name them, tie to **evidence** (share %, rank, PLN).

## Tool reference (use as needed)
Sales & market: `get_sales_by_category_for_brand`, `get_brand_vs_market_subcategory_sales`, `get_top_products`, `search_products_by_query`, `list_competitors_in_category`.
Segments & marketing: `get_segment_breakdown_for_category`, `get_category_needstate_landscape`, `get_segment_marketing_summary`, `get_needstate_dimensions_for_segment`, `list_segments`.
Behaviour & channels: `get_purchasing_channel_mix`, `get_purchasing_journey`, `get_media_touchpoints`.
Promos: `get_promo_roi_by_type_for_brand`, `get_segment_promo_responsiveness`.
SKU removal: `get_product_segment_breakdown`.
Helpers: `list_categories`, `list_segments`.

---

### Appendix: taxonomy (ids are authoritative)
{_compact_taxonomy_text()}

### Appendix: HCG segments (definitions only — cite segments in prose only when tools return them)
{_segments_text()}
"""


def _client_messages_to_contents(messages: list[dict]) -> list[Any]:
    """Map stored chat history to Gemini Content objects (user/model text only)."""
    if types is None:
        return []
    out: list[Any] = []
    for m in messages:
        role = m.get("role")
        text = (m.get("text") or "").strip()
        if not text or role in ("system", None):
            continue
        if role == "user":
            out.append(types.Content(role="user", parts=[types.Part(text=text)]))
        elif role == "assistant":
            out.append(types.Content(role="model", parts=[types.Part(text=text)]))
    return out


class GeminiQuota:
    """Rough free-tier guard: sliding-window RPM and daily call cap (per process)."""

    def __init__(self, rpm: int | None = None, rpd: int | None = None) -> None:
        self.rpm = int(rpm if rpm is not None else _env("GEMINI_FREE_RPM") or 10)
        self.rpd = int(rpd if rpd is not None else _env("GEMINI_FREE_RPD") or 250)
        self._lock = asyncio.Lock()
        self._minute_calls: list[float] = []
        self._day_key: str = ""
        self._day_count: int = 0

    async def acquire_slot(self) -> bool:
        """Wait until an API call is allowed under RPM/RPD. Returns False if daily cap exceeded."""
        while True:
            async with self._lock:
                today = datetime.now(timezone.utc).date().isoformat()
                if self._day_key != today:
                    self._day_key = today
                    self._day_count = 0
                if self._day_count >= self.rpd:
                    return False
                now = time.monotonic()
                self._minute_calls = [t for t in self._minute_calls if now - t < 60.0]
                if len(self._minute_calls) < self.rpm:
                    self._minute_calls.append(now)
                    self._day_count += 1
                    return True
                wait = max(0.05, 60.0 - (now - self._minute_calls[0]) + 0.05)
            await asyncio.sleep(min(wait, 10.0))


_GLOBAL_QUOTA = GeminiQuota()


class ExpertAgent:
    """Gemini orchestrates the chat; Python executes declared tools only."""

    def __init__(self) -> None:
        self.api_key = _env("GEMINI_API_KEY")
        self.model = _env("GEMINI_MODEL") or "gemini-2.0-flash"
        self._client = None
        self._client_error: str | None = None
        if self.api_key and genai is not None:
            try:
                self._client = genai.Client(api_key=self.api_key)
            except Exception as e:  # pragma: no cover
                self._client = None
                self._client_error = str(e)

    async def handle_stream(self, *, messages: list[dict], user: Any) -> AsyncIterator[dict[str, Any]]:
        """Yield ``{"type":"status","text":...}`` per tool, then ``{"type":"answer","text":...}``, or ``error``."""
        msgs = [m for m in (messages or []) if isinstance(m, dict)]
        brand_id = getattr(user, "brand_id", None)
        if not brand_id:
            yield {"type": "error", "text": "I need a brand context to run data checks. Please make sure your account is linked to a brand."}
            return

        ps, pe = DP[0], DP[1]
        contents = _client_messages_to_contents(msgs)
        if not contents:
            yield {"type": "error", "text": "Please send a message."}
            return

        if types is None:
            yield {
                "type": "error",
                "text": (
                    "The Gemini Python SDK could not be loaded on this server. "
                    "Ensure `google-genai` is installed (see app/requirements.txt) and redeploy. "
                    f"Your brand_id is {brand_id}."
                ),
            }
            return
        if not self.api_key:
            yield {"type": "answer", "text": self._offline_answer(int(brand_id), msgs[-1].get("text") or "")}
            return
        if not self._client:
            err = (self._client_error or "unknown error").strip()
            yield {
                "type": "error",
                "text": (
                    "GEMINI_API_KEY is set but the Gemini client failed to initialize. "
                    f"Detail: {err}. Check the key and server logs. "
                    f"Your brand_id is {brand_id}."
                ),
            }
            return

        sys_text = _build_system_instruction(int(brand_id), ps, pe)
        tool = build_expert_gemini_tool()
        config = types.GenerateContentConfig(
            system_instruction=sys_text,
            tools=[tool],
            tool_config=types.ToolConfig(
                function_calling_config=types.FunctionCallingConfig(
                    mode=types.FunctionCallingConfigMode.AUTO,
                ),
            ),
            temperature=0.4,
            max_output_tokens=12288,
        )

        work = list(contents)
        max_iters = 12

        yield {
            "type": "status",
            "text": "Preparing analysis from your dashboard data…",
        }
        for _iter in range(max_iters):
            yield {
                "type": "status",
                "text": "Consulting the AI with your data (this may take a few seconds)…",
            }
            ok = await _GLOBAL_QUOTA.acquire_slot()
            if not ok:
                yield {
                    "type": "error",
                    "text": (
                        "I've reached today's Gemini API budget limit for this server. "
                        "Try again tomorrow, or ask an admin to raise GEMINI_FREE_RPD. "
                        "Meanwhile you can still use the dashboards for full analytics."
                    ),
                }
                return

            def _call() -> Any:
                return self._client.models.generate_content(
                    model=self.model,
                    contents=work,
                    config=config,
                )

            try:
                resp = await asyncio.to_thread(_call)
            except Exception as e:  # pragma: no cover
                yield {"type": "error", "text": f"The AI service returned an error ({e!s}). Please retry in a few seconds."}
                return

            if not resp.candidates:
                yield {"type": "error", "text": "I could not generate a reply (blocked or empty). Try rephrasing your question."}
                return

            cand = resp.candidates[0]
            content = cand.content
            if not content or not content.parts:
                yield {"type": "error", "text": "I could not generate a reply. Try again with a bit more detail."}
                return

            parts = content.parts
            fcalls = [p for p in parts if getattr(p, "function_call", None)]
            if not fcalls:
                text = "".join((p.text or "") for p in parts if p.text).strip()
                yield {"type": "answer", "text": text or "I don't have an answer right now. Please try rephrasing."}
                return

            work.append(content)
            fr_parts: list[Any] = []
            for p in fcalls:
                fc = p.function_call
                name = (fc.name or "").strip()
                yield {
                    "type": "status",
                    "text": TOOL_STATUS_LABELS.get(name, "Running data analysis..."),
                    "tool": name,
                }
                args = dict(fc.args or {})
                payload = run_expert_tool(
                    name,
                    args,
                    period_start=ps,
                    period_end=pe,
                    default_brand_id=int(brand_id),
                )
                fr_parts.append(
                    types.Part(
                        function_response=types.FunctionResponse(
                            id=getattr(fc, "id", None),
                            name=name,
                            response=tool_result_for_gemini(payload),
                        )
                    )
                )
            work.append(types.Content(role="user", parts=fr_parts))

        yield {
            "type": "error",
            "text": (
                "The analysis needed too many data steps in one turn. "
                "Please narrow the question (one category or one SKU) and try again."
            ),
        }

    async def handle(self, *, messages: list[dict], user: Any) -> str:
        out: str | None = None
        async for ev in self.handle_stream(messages=messages, user=user):
            if ev.get("type") == "error":
                return str(ev.get("text") or "Something went wrong.")
            if ev.get("type") == "answer":
                out = str(ev.get("text") or "")
        return out or "I don't have an answer right now. Please try rephrasing."

    def _offline_answer(self, brand_id: int, last_user_text: str) -> str:
        """Deterministic mini-answer when Gemini is not configured."""
        t = (last_user_text or "").lower()
        tc = tool_list_categories()
        n_sub = len(tc.get("subcategories") or [])
        bits = [
            "GEMINI_API_KEY is missing for this server (not a cache issue: the key must be in the process environment).",
            "Local: put GEMINI_API_KEY in the repo root `.env` and restart uvicorn.",
            "Cloud Run: set the variable on service `dashboard` (region europe-west1), e.g. run `scripts/push-gemini-env-to-cloud-run.ps1` after `gcloud auth login`.",
            f"When enabled, the catalog has {len(tc.get('parent_categories') or [])} parent categories and {n_sub} subcategories.",
        ]
        if any(w in t for w in ("remove", "delete", "drop", "sku")):
            bits.append(
                "For SKU removal impact, once the API is enabled I will call get_product_segment_breakdown with a product_id."
            )
        if any(w in t for w in ("add", "new", "launch", "introduce")):
            bits.append(
                "For new SKU scenarios, I will benchmark with get_top_products and get_brand_vs_market_subcategory_sales."
            )
        bits.append(f"Your brand_id is {brand_id}.")
        return " ".join(bits)
