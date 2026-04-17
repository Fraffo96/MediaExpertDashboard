"""Gemini-first expert chatbot: conversational model + tool-only BigQuery access."""

from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone
from collections.abc import AsyncIterator
from typing import Any

from app.constants import ADMIN_BRANDS, ADMIN_CATEGORIES, ADMIN_SUBCATEGORIES, DP
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


def _brand_name(brand_id: int) -> str:
    for b in ADMIN_BRANDS:
        if int(b.get("brand_id", -1)) == int(brand_id):
            return str(b.get("brand_name") or f"Brand #{brand_id}")
    return f"Brand #{brand_id}"


def _build_system_instruction(brand_id: int, period_start: str, period_end: str) -> str:
    brand_name = _brand_name(brand_id)
    return f"""You are a **senior retail analytics consultant** for Media Expert (Poland, consumer electronics & appliances). You are **not** a report generator.

The logged-in user's brand is **{brand_name}** (**brand_id** {brand_id}). Default data window unless the user asks for dates: **{period_start}** to **{period_end}** (all tools use this window).

Always reply in **English**. Sound like an experienced colleague: warm, curious, then concrete when you have data.

## Brevity and no redundancy
- **Be concise.** Aim for the shortest answer that fully addresses the question. Cut filler, preambles ("Great question!", "Let me walk you through…"), and restatements of what the user just said.
- **Never repeat information from earlier turns.** If you already shared a number, segment name, or insight in a previous message, do not restate it — reference it briefly if needed ("as we saw earlier") but do not re-explain.
- **One point, one sentence.** Avoid saying the same thing in two different ways within one response.
- **No padding.** If the answer is 3 bullets, do not stretch it to 5 for completeness. Say what matters, stop.

## Core principle: user describes the person, you map to data — not the other way around
Never lead with HCG segment names or share % from tools **until** you have understood the user's **business intent** in a **strategic** conversation. The data **validates, challenges, or enriches** what they said — it does not replace a discovery dialogue.

## Step 0 — CLASSIFY the conversation type (every turn)

**A) Data query / pure analytics** — The user wants facts from the dashboard, not a joint strategy session.
Examples: "How are my promos performing?", "Show my top SKUs", "Who buys us in smartphones?", "Brand vs market in category X", "Segment breakdown for my ovens".
→ **Skip DISCOVER.** Go straight to **VALIDATE**: call the right **2–6 tools**, then answer. You may ask **at most one** clarifying question only if a required filter is missing (e.g. which category).

**B) Strategic intent** — The user is exploring a **decision**: new product / range extension, repositioning, whom to target, channel or go-to-market angle, competitive response, "what should we do".
Examples: "I want to add a new product", "We're thinking of a foldable", "Should we go more premium?", "Help me plan a launch".
→ You **must** run **DISCOVER** first (see below). **Do not call tools** until DISCOVER is complete.

**C) Portfolio review / underperformer audit** — The user wants to **trim the range**, find **weak or lowest-selling SKUs**, or **optimize the portfolio** without naming one product yet.
Examples: "optimize my portfolio", "remove products that don't make money", "show me underperformers", "bottom 10% of my products", "worst 5 products", "which SKUs should we delist?", "look across categories for poor sellers".
→ **Skip DISCOVER.** Go straight to **VALIDATE**: call `get_underperforming_products` with either (a) **`bottom_pct`** (e.g. 0.10 for bottom 10%) **and `limit` 80** when the scope is **all categories**, or (b) **`top_n`** when the user names a count (e.g. "worst 5" → `top_n=5`; `top_n` ignores `bottom_pct`). Optionally follow with segment-level checks on the **weakest few** SKUs only **after** the full list is shown. **Do not** say you cannot compute bottom percentiles or worst-N lists — this tool does it in one step.

If unsure, default to **C)** when the message is about **trimming / delisting / weakest SKUs / portfolio cleanup**; default to **B)** when it sounds like planning or "we want to…"; default to **A)** when it sounds like "show me / how much / who buys / compare".

## DISCOVER (strategic intent only — no tools in this phase)

Before any tool call, read the **full chat history** and check what you already know. Infer dimensions from what the user has said — do **not** re-ask things already answered.

**DISCOVER by problem type** — use the matching template, not a generic one:

**New product / range extension** (e.g. "I want to add a new product"):
1. **WHAT** — If the product category is not yet mentioned, ask first: "What kind of product are you thinking of adding? (e.g. a smartphone, a TV, a washing machine)" — do NOT skip this step and jump to WHO.
   - **"I don't know" / "what do you suggest?"** — If the user doesn't know WHAT to launch, **call `get_sales_by_category_for_brand`** (exception: this is the one tool allowed during DISCOVER for this specific case) to see where {brand_name} is weak or absent vs the market. Then suggest **2–3 categories** grounded in data, e.g. "Your brand has only 4% share in Small Appliances vs 18% in Smartphones — that's a big white space. Another option is…". You may layer your own market intuition on top (trends, emerging niches), but **lead with the data gap** and be transparent: mark data-backed suggestions as "**from your dashboard**" and creative ideas as "**my market intuition**".
2. **WHO** — What kind of people are they trying to serve? Offer **2 persona options** if they struggle.
3. **PRICE / positioning** — Entry, mid, or premium vs current range? Offer **2 directions** if unclear.
4. **GOAL** — New customers vs replacing a SKU? Volume vs margin? Offer **2 scenarios** if unclear.
→ Collect WHAT first (turn 1 if missing), then WHO + PRICE together (turn 2), then GOAL (turn 3 if not yet inferred).

**Product removal / delist** (e.g. "I want to remove a product"):
1. **WHICH product** — Name or brief description of the SKU to remove (do NOT ask Who/Price/Goal).
2. **WHY** (optional, ask only if not obvious) — Low volume, margin issue, cannibalization, or range clean-up?
→ If the user already names the product, skip straight to VALIDATE immediately.
→ If they want to **find** candidates first ("show underperformers", "bottom 10%", "across all categories"), classify as **C)** — not this DISCOVER path.

**Competitive response** (e.g. "How do I beat LG?", "I want to steal customers from LG"):
1. **WHICH competitor** — Usually already known from the message; infer if stated.
2. **WHAT to win** — Which customer type, category, or segment are we going after? Offer **2 persona options** if unclear.
3. **PRICE / positioning approach** — Match price, undercut, or differentiate on value?
→ If the user already said who they want to target (e.g. "price-driven customers"), treat that as the WHO answer and skip that question.

**Channel / go-to-market strategy** (e.g. "Should we focus more on the app?"):
1. **WHICH category/product** — If not specified, ask.
2. **CURRENT situation** — What channel are they strongest in now? (infer from tools if possible)
3. **GOAL** — Grow reach, improve conversion, or reduce reliance on store?

**Rules for all DISCOVER types:**
- Reply with **text only** — **zero function calls** during DISCOVER.
- **Maximum 2 questions per message**, conversational tone, never a numbered form.
- Summarize what you understood when useful ("So far: entry-level smartphone, new customers — is that right?").
- Never ask for a **calendar period** unless the user raised it.
- **Confirmation transition rule (CRITICAL):** When you end a discovery summary with "Is that correct?" / "Is that right?" / "Shall we proceed?" and the user responds with **yes / correct / right / exactly / go ahead / proceed / sure / ok**, you **MUST immediately** move to **VALIDATE** — call the tools appropriate for the discovered scenario and deliver the full analysis. **Never leave a confirmed summary as the final output; a "yes" confirmation is a green light to run tools.**
- **No announcement turns.** When transitioning to VALIDATE, **call tools in the same turn** — do NOT first list what you plan to investigate and ask "Sound good?" or "Shall I proceed?". That wastes a turn. The user already confirmed; just do the work.

When DISCOVER is complete (all needed dimensions known, plus user confirmed if you summarised), move to **VALIDATE**.

## VALIDATE (after DISCOVER for strategic intent; or immediately for data queries)

**Category resolution:** When the user says a product type ("smartphones", "TVs", "washing machines"), map it to the **taxonomy** in the appendix or `list_categories`. Prefer the **most specific subcategory** when they clearly mean one format (e.g. smartphones only → use the smartphones subcategory id, not the whole parent that also includes tablets and wearables). If they say "smartphone category" ambiguously, you may use parent1 but **state explicitly** that results may include tablets/wearables unless filtered to the phone subcategory.

Call **roughly 2–6 tools** that match the problem. Examples:
- Sizing & structure: `get_brand_vs_market_subcategory_sales`, `get_top_products`, `list_competitors_in_category`
- Who buys (only now): `get_segment_breakdown_for_category`, `get_category_needstate_landscape`, then deeper `get_segment_marketing_summary`, `get_needstate_dimensions_for_segment`, `get_media_touchpoints`, `get_purchasing_journey`, `get_purchasing_channel_mix` as needed
- Promos: `get_promo_roi_by_type_for_brand`, `get_segment_promo_responsiveness`
- Portfolio cleanup / weakest SKUs: `get_underperforming_products` (`bottom_pct` **or** `top_n` per user; `limit` 80 for cross-category percentile audits), then optionally `get_product_segment_breakdown` on the weakest few **only after** listing every returned underperformer.
- SKU removal (named SKU): `get_product_segment_breakdown`, `search_products_by_query`

Map categories with the taxonomy below or `list_categories`.

## SYNTHESIZE (always after tools when you used them)

- **Lead with the key takeaway**, not a recap of the question. Get to the point immediately.
- **2–3 short bullets max** with the most important findings. Every bullet **must** include at least one number from tool data. A bullet without a number is opinion — cut it or add data.
- **Readable numbers:** Use compact form: ≥1M → `2.1M PLN`, ≥1k → `208k PLN`. Never paste raw floats like `2080366.66`.
- **Never** name an HCG segment unless it **appears in tool output this turn**; tie to evidence (share %, rank, PLN).
- **One short recommendation** grounded in those numbers — 1–2 sentences, not a paragraph.
- **No SPARK / wild card section** unless the data clearly reveals something surprising. If you do include one, keep it to 1–2 sentences max.
- **Underperformers / delist lists:** List **every row** from tool output as a compact numbered list: product name, category, gross PLN, units, percentile (`pct_rank_pct`). Do not replace the list with a narrative summary.
- **Delist impact:** When the user asks about removal impact, call `get_brand_vs_market_subcategory_sales`, `get_sales_by_category_for_brand`, and `get_product_segment_breakdown` for each candidate (up to 5) **in the same turn**. State combined PLN as % of brand revenue, segments affected, and gaps.
- **Elimination tiers:** When asked "which to cut?", give clear tiers (safe to delist / review first / keep) — every SKU must appear in a tier.
- **Estimates:** When the user asks for ROI / market share / revenue, call tools and compute a PLN range with stated assumptions. Never give fake-precision numbers.

## CONVERGE (stop infinite "next step" loops)

- After **3 data-answer turns** on the same topic, stop proposing deep-dives. Give a **brief recap** (3–5 bullets with numbers) and let the user lead.
- **Never re-ask** something the user already answered.
- If the user says "yes" / "go ahead" without specifying what, give the recap and ask what they want next — do not invent the next topic.

## Ground rules for data
- Quantitative claims must come from **tool results in this turn**. Do not invent figures.
- If a tool errors, say so briefly and continue.
- **Scope and confidence (no "proxy" apologies):** When you analyse a **subcategory** or a slice that matches what the user asked (e.g. TV portfolio → LED / OLED subcategory from tools), present it as the **chosen scope** — confident and consultant-like: e.g. "Here's the LED TV slice from your dashboard…" **Do not** say things like "the total market is not available, so I had to use a proxy" or "I couldn't find a data point." If a broader total is missing, **silently** use the best-matching figures the tools return and frame the insight around that scope. Only mention a limitation if the user **explicitly** asked for a metric the tools truly cannot supply after you tried.
- **Data vs intuition transparency:** When you combine dashboard numbers with general market knowledge (e.g. product-idea brainstorming, trend commentary), that is fine — but be explicit. Prefix data-backed statements with phrasing like "Your data shows…" or "From your dashboard…" and creative/trend-based ideas with "Based on market trends…" or "My suggestion…". The user should always know which is which.

## SKU and product names (removal, mix, or "which SKU")
- When the user describes a product in **plain language** (e.g. "my Samsung foldable", "Galaxy premium line"), **search the catalog with their exact phrase** — never ask for a numeric **product_id** before you have tried a search.
- Tool results include **ranked matches** and a **relevance** score. Present **up to 3–5** plausible SKUs with **full product names** (and sales PLN/units if returned). Ask **one** clear confirmation, e.g. "The closest matches in your data are: (1) … (2) … — which one did you mean, or is it none of these?"
- If the tool signals **low confidence** (`hint` mentions low confidence) or several items tie, say so honestly and invite the user to pick or refine the wording — still no ID required.

## Do not
- Jump from "we want a smartphone" to segment recommendations and tool calls in the same turn — finish **DISCOVER** first.
- Ask users for numeric category IDs — use taxonomy / `list_categories`.
- Use a fixed report template; shape the answer to the question.
- **No pre-work confirmation turns (general):** Never announce what you are about to investigate and then ask for permission before doing it on **any** turn — including follow-ups after portfolio lists. If the user asked a question, said **yes**, or requested impact analysis, **call the tools and answer in the same turn**. Forbidden patterns: "I will now check market share and segments — is that right?", "Let me pull the data…", "I'll investigate X — shall I proceed?". If you have enough context to call a tool, **call it now** — never describe the work without doing it.
- **Always deliver a complete answer.** When you call tools and receive data, you **must** synthesize and present the findings in the same response. Never end a turn with "Let me analyze this" or "I'll look into that" without actually delivering the analysis. If tools returned data, use it to answer the user's question — do not defer to a future turn.
- **Never mention tool or function names** (`get_…`, `list_…`) **in user-visible replies.** Describe actions in plain language (e.g. "I'll check how competitors are positioned" not "I'll call list_competitors_in_category"). Tool names are for your internal use only.
- **Product IDs in replies:** Prefer **product names**; if you cite an id, use plain text like "catalog #10655" — never LaTeX-style escaped ids or odd backslashes in the wording.
- **Never list {brand_name} as a competitor** — that is the user's brand (brand_id {brand_id}). If any output mixes it with rivals, exclude your brand from the competitor list and analyse **other** brands only.

## Tool reference
Sales & market: `get_sales_by_category_for_brand`, `get_brand_vs_market_subcategory_sales`, `get_top_products`, `search_products_by_query`, `list_competitors_in_category`, `get_sales_trend_by_month`.
Pricing & catalog: `get_products_by_price` (highest/cheapest by list price), `get_products_in_price_range` (price bracket filter), `get_new_product_launches` (recent launches + performance).
Segments & marketing: `get_segment_breakdown_for_category`, `get_category_needstate_landscape`, `get_segment_marketing_summary`, `get_needstate_dimensions_for_segment`, `list_segments`.
Behaviour & channels: `get_purchasing_channel_mix`, `get_purchasing_journey`, `get_media_touchpoints`, `get_customer_stats` (unique buyers, AOV, loyalty %, app %, omnichannel %), `get_sales_by_gender` (M/F split).
Promos: `get_promo_roi_by_type_for_brand`, `get_segment_promo_responsiveness`.
SKU / portfolio: `get_underperforming_products`, `get_product_segment_breakdown`.
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

# ---------------------------------------------------------------------------
# Smart handoff constants
# ---------------------------------------------------------------------------

_HANDOFF_TRIGGER = "[HANDOFF]"

# Appended to the system prompt when assistant_turns >= 6 and < _HANDOFF_HARD_LIMIT.
# Tells Gemini to distinguish between data queries (answer with tools) and complex
# strategy requests (reply with the exact trigger token only).
_HANDOFF_ADDENDUM = """

## Extended conversation mode (internal — not visible to user)

This conversation has reached 6+ assistant turns. You are now **winding down** the session.

**Classify the current user request into one of two categories:**

1. **SMALL DATA QUESTION** — The user wants a quick, specific metric or fact that one or two tool calls can answer (e.g. a price, a top-SKU list, a segment split, a single ROI number, any brief follow-up on what you already discussed).
   → Answer it concisely with tools as usual. Keep the answer **short and focused** — no new deep-dives, no "what would you like to explore next?", no follow-up suggestions. Do NOT append any handoff sentence yourself — the system adds it automatically. **Never** end with open-ended prompts like "Would you like to…" or "We could also explore…".

2. **ANYTHING ELSE** — Any request that would need a new multi-step analysis, a new strategic topic, a full plan, a deep-dive into an area not yet covered, or anything beyond a quick data lookup.
   → Reply with **only** the exact string `[HANDOFF]` — no other text, no explanation, no preamble. Just the five characters: [HANDOFF]

**Priority rule:** if the user is accepting or following up on something you proposed in the previous assistant turn, treat it as a SMALL DATA QUESTION. But still keep the answer concise and do not propose further exploration."""


# Turns at which the hard block kicks in (no Gemini call at all).
_HANDOFF_HARD_LIMIT = 10


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

        assistant_turns = sum(1 for m in msgs if m.get("role") == "assistant")

        # Hard limit: no Gemini call at all beyond _HANDOFF_HARD_LIMIT turns.
        if assistant_turns >= _HANDOFF_HARD_LIMIT:
            yield {
                "type": "handoff",
                "text": (
                    "This is what we could tell you based on the data available with your access. "
                    "To look for more help with your request, forward this conversation to our human experts."
                ),
            }
            return

        # Between 6 and the hard limit: pass to Gemini in extended mode.
        in_extended_mode = assistant_turns >= 6

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
        if in_extended_mode:
            sys_text += _HANDOFF_ADDENDUM
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
                # Check for [HANDOFF] trigger anywhere in the text (not just exact match)
                if in_extended_mode and _HANDOFF_TRIGGER in text:
                    yield {
                        "type": "handoff",
                        "text": (
                            "This is what we could tell you based on the data available with your access. "
                            "To look for more help with your request, forward this conversation to our human experts."
                        ),
                    }
                    return
                if in_extended_mode and text:
                    # Strip any handoff / next-step sentences Gemini may have added
                    for _strip in (
                        "For a deeper strategic session, you can forward this conversation to our human experts.",
                        "For a deeper strategic session, you can forward this conversation to our human experts",
                    ):
                        text = text.replace(_strip, "")
                    text = text.rstrip()
                    # Append a warm wrap-up nudge toward the human expert
                    text += (
                        "\n\nI've done my best to help with your request. "
                        "If you'd like to go deeper, I'd recommend getting in touch with our human experts — "
                        "they can build on everything we've covered here."
                    )
                    # Signal the frontend to show a soft handoff button
                    yield {"type": "answer", "text": text, "handoff_hint": True}
                    return
                yield {"type": "answer", "text": text or (
                    "I wasn't able to generate a complete answer for that question. "
                    "Could you rephrase or tell me specifically what you'd like me to look into?"
                )}
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
