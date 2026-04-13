"""Gemini-first expert chatbot: conversational model + tool-only BigQuery access."""

from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Any

from app.constants import ADMIN_CATEGORIES, ADMIN_SUBCATEGORIES, DP
from app.services.expert_tools import (
    STATIC_SEGMENTS,
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
    return f"""You are the Media Expert retail analytics assistant (Poland, consumer electronics & appliances).
The logged-in user's brand_id is {brand_id}. Default analysis window unless the user explicitly asks otherwise: {period_start} to {period_end} (use tools — they already use this window).

Always reply in English. Be concise, expert, and actionable. You are a chatbot: infer category/subcategory from natural language (e.g. fridge → Large Appliances / Refrigerators) using the taxonomy below or the list_categories tool — never ask the user for raw numeric category ids unless they volunteered them.

Behaviour:
- Recommend channels (web / app / store) using get_purchasing_channel_mix and data — do not expect the user to know channel strategy.
- Prioritise segments, needstates-style drivers, and promo types using tool outputs.
- For forecasts, give plausible ranges and state assumptions; never invent precise metrics not supported by tool data.
- If a tool returns an error, explain briefly and try another approach or ask one focused clarifying question.
- Do not ask about time period unless the user mentioned dates or a specific range.

Taxonomy (ids are authoritative):
{_compact_taxonomy_text()}

HCG segments:
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
        if self.api_key and genai is not None:
            try:
                self._client = genai.Client(api_key=self.api_key)
            except Exception:
                self._client = None

    async def handle(self, *, messages: list[dict], user: Any) -> str:
        msgs = [m for m in (messages or []) if isinstance(m, dict)]
        brand_id = getattr(user, "brand_id", None)
        if not brand_id:
            return "I need a brand context to run data checks. Please make sure your account is linked to a brand."

        ps, pe = DP[0], DP[1]
        contents = _client_messages_to_contents(msgs)
        if not contents:
            return "Please send a message."

        if types is None:
            return (
                "The Gemini Python SDK could not be loaded on this server. "
                "Ensure `google-genai` is installed (see app/requirements.txt) and redeploy. "
                f"Your brand_id is {brand_id}."
            )
        if not self.api_key or not self._client:
            return self._offline_answer(int(brand_id), msgs[-1].get("text") or "")

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
            max_output_tokens=8192,
        )

        work = list(contents)
        max_iters = 8

        for _ in range(max_iters):
            ok = await _GLOBAL_QUOTA.acquire_slot()
            if not ok:
                return (
                    "I've reached today's Gemini API budget limit for this server. "
                    "Try again tomorrow, or ask an admin to raise GEMINI_FREE_RPD. "
                    "Meanwhile you can still use the dashboards for full analytics."
                )

            def _call() -> Any:
                return self._client.models.generate_content(
                    model=self.model,
                    contents=work,
                    config=config,
                )

            try:
                resp = await asyncio.to_thread(_call)
            except Exception as e:  # pragma: no cover
                return f"The AI service returned an error ({e!s}). Please retry in a few seconds."

            if not resp.candidates:
                return "I could not generate a reply (blocked or empty). Try rephrasing your question."

            cand = resp.candidates[0]
            content = cand.content
            if not content or not content.parts:
                return "I could not generate a reply. Try again with a bit more detail."

            parts = content.parts
            fcalls = [p for p in parts if getattr(p, "function_call", None)]
            if not fcalls:
                text = "".join((p.text or "") for p in parts if p.text).strip()
                return text or "I don't have an answer right now. Please try rephrasing."

            work.append(content)
            fr_parts: list[Any] = []
            for p in fcalls:
                fc = p.function_call
                name = (fc.name or "").strip()
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

        return (
            "The analysis needed too many data steps in one turn. "
            "Please narrow the question (one category or one SKU) and try again."
        )

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
