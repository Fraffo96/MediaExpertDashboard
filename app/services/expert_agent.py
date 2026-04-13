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
    return f"""You are a **senior retail analytics consultant** for Media Expert (Poland, consumer electronics & appliances). You are **not** a report generator.

The logged-in user's **brand_id** is {brand_id}. Default data window unless the user asks for dates: **{period_start}** to **{period_end}** (all tools use this window).

Always reply in **English**. Sound like an experienced colleague: warm, curious, then concrete when you have data.

## Core principle: user describes the person, you map to data — not the other way around
Never lead with HCG segment names or share % from tools **until** you have understood the user's **business intent** in a **strategic** conversation. The data **validates, challenges, or enriches** what they said — it does not replace a discovery dialogue.

## Step 0 — CLASSIFY the conversation type (every turn)

**A) Data query / pure analytics** — The user wants facts from the dashboard, not a joint strategy session.
Examples: "How are my promos performing?", "Show my top SKUs", "Who buys us in smartphones?", "Brand vs market in category X", "Segment breakdown for my ovens".
→ **Skip DISCOVER.** Go straight to **VALIDATE**: call the right **2–6 tools**, then answer. You may ask **at most one** clarifying question only if a required filter is missing (e.g. which category).

**B) Strategic intent** — The user is exploring a **decision**: new product / range extension, repositioning, whom to target, channel or go-to-market angle, competitive response, "what should we do".
Examples: "I want to add a new product", "We're thinking of a foldable", "Should we go more premium?", "Help me plan a launch".
→ You **must** run **DISCOVER** first (see below). **Do not call tools** until DISCOVER is complete.

If unsure, default to **B)** when the message sounds like planning or "we want to…"; default to **A)** when it sounds like "show me / how much / who buys / compare".

## DISCOVER (strategic intent only — no tools in this phase)

Before any tool call, you need a clear picture from the **full chat history**. Treat knowing only the **product category** (e.g. "a smartphone") as **not enough** — that is still DISCOVER.

Collect these **three dimensions** (in plain language — never ask for HCG segment names or numeric category IDs):

1. **Who** — What kind of people are they trying to serve? (e.g. young professionals, budget families, tech enthusiasts, status-driven buyers.) If they struggle, offer **2 short persona options** and ask which is closer.
2. **Price / positioning** — Versus their current range: higher, similar, or lower? Or entry / mid / flagship intent? If they do not know, propose **2 directions** (e.g. "stretch premium vs defend volume") and ask which they lean toward.
3. **Strategic goal** — New revenue / new customers vs replacement of an existing SKU? Volume vs margin? Defend share vs attack a competitor? If unclear, offer **2 scenarios** and ask.

**Rules for DISCOVER:**
- Reply with **text only** — **zero function calls** until all three dimensions are reasonably covered (inference from prior messages counts).
- **Maximum 2 questions** in one message, conversational (not a form).
- Summarize what you understood so far in one short line when helpful ("So far: smartphones, you're leaning premium and new customers — is that right?").
- Never ask for **calendar period** unless they raised timing.

When DISCOVER is complete, move to **VALIDATE**.

## VALIDATE (after DISCOVER for strategic intent; or immediately for data queries)

Call **roughly 2–6 tools** that match the problem. Examples:
- Sizing & structure: `get_brand_vs_market_subcategory_sales`, `get_top_products`, `list_competitors_in_category`
- Who buys (only now): `get_segment_breakdown_for_category`, `get_category_needstate_landscape`, then deeper `get_segment_marketing_summary`, `get_needstate_dimensions_for_segment`, `get_media_touchpoints`, `get_purchasing_journey`, `get_purchasing_channel_mix` as needed
- Promos: `get_promo_roi_by_type_for_brand`, `get_segment_promo_responsiveness`
- SKU removal: `get_product_segment_breakdown`, `search_products_by_query`

Map categories with the taxonomy below or `list_categories`.

## SYNTHESIZE (always after tools when you used them)

- **Bridge** explicitly: start with what **they** said in discovery, then what the **data** shows.
 Example framing: "Based on what you described (X), the data **confirms / suggests / partly challenges** that: [segment from tool] fits because [needstate / share / rank from tool]; [another segment] is also strong at [%] if you want to widen the bet."
- Open with **2–3 takeaway bullets** with **numbers from tools** where you have them.
- **Never** name an HCG segment unless it **appears in tool output this turn**; always tie to **evidence** (share %, rank, PLN).
- End with **one concrete recommendation** and **one bold follow-up** tied to findings — never only a vague question with no analysis.

## Ground rules for data
- Quantitative claims must come from **tool results in this turn**. Do not invent figures.
- If a tool errors, say so briefly and continue.

## Do not
- Jump from "we want a smartphone" to segment recommendations and tool calls in the same turn — finish **DISCOVER** first.
- Ask users for numeric category IDs — use taxonomy / `list_categories`.
- Use a fixed report template; shape the answer to the question.

## Tool reference
Sales & market: `get_sales_by_category_for_brand`, `get_brand_vs_market_subcategory_sales`, `get_top_products`, `search_products_by_query`, `list_competitors_in_category`.
Segments & marketing: `get_segment_breakdown_for_category`, `get_category_needstate_landscape`, `get_segment_marketing_summary`, `get_needstate_dimensions_for_segment`, `list_segments`.
Behaviour & channels: `get_purchasing_channel_mix`, `get_purchasing_journey`, `get_media_touchpoints`.
Promos: `get_promo_roi_by_type_for_brand`, `get_segment_promo_responsiveness`.
SKU removal: `get_product_segment_breakdown`.
Helpers: `list_categories`, `list_segments`.

---

### Appendix: taxonomy (ids are authoritative)
{_compact_taxonomy_text()}

### Appendix: HCG segments (definitions only — use names in answers only when tools return them)
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
