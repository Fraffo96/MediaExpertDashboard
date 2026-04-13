"""Expert agent: clarifies intent, runs tool-only queries, answers in English.

Design goals:
- Tool-only data access (no free-form SQL generation).
- Lightweight: deterministic routing + LLM for natural language.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any

from app.constants import DP
from app.db.queries.market_intelligence import segment_sku as mi_segment_sku
from app.db.queries.basic import products as basic_products
from app.db.queries import promo_creator as promo_q
from app.services import marketing as marketing_svc

try:
    from google import genai  # type: ignore
except Exception:  # pragma: no cover
    genai = None  # type: ignore


def _env(name: str) -> str:
    return (os.getenv(name, "") or "").strip()

def _mentions_period(text: str) -> bool:
    t = (text or "").lower()
    if re.search(r"\b(20\d{2})\b", t):
        return True
    if re.search(r"\b\d{4}-\d{2}-\d{2}\b", t):
        return True
    return any(w in t for w in ("period", "date range", "from ", "to ", "last month", "last week", "ytd", "q1", "q2", "q3", "q4"))


def _extract_product_hint(text: str) -> str | None:
    t = (text or "").lower()
    # very light heuristic for category hints in the user's sentence
    for kw in ("fridge", "refrigerator", "freezer", "washing machine", "dishwasher", "tv", "laptop", "smartphone"):
        if kw in t:
            return kw
    return None


def _parse_product_id(text: str) -> int | None:
    m = re.search(r"\bproduct[_\s-]*id\s*[:=]?\s*(\d{1,12})\b", text, flags=re.I)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    m2 = re.search(r"\b(\d{4,12})\b", text)
    if m2:
        try:
            return int(m2.group(1))
        except ValueError:
            return None
    return None


def _parse_category_or_subcategory_id(text: str) -> tuple[int | None, int | None]:
    """
    Returns (category_id, subcategory_id).
    - category_id: 1..10
    - subcategory_id: >=100
    """
    t = text or ""
    # Explicit labels first
    msub = re.search(r"\bsubcat(?:egory)?\s*[:=]?\s*(\d{3,6})\b", t, flags=re.I)
    if msub:
        try:
            sid = int(msub.group(1))
            if sid >= 100:
                return (sid // 100 if sid // 100 in range(1, 11) else None, sid)
        except ValueError:
            pass
    mcat = re.search(r"\b(?:parent\s*)?cat(?:egory)?\s*[:=]?\s*(\d{1,2})\b", t, flags=re.I)
    if mcat:
        try:
            cid = int(mcat.group(1))
            if 1 <= cid <= 10:
                return (cid, None)
        except ValueError:
            pass
    # Fallback: stand-alone 3xx/4xx patterns are often subcategory ids
    m3 = re.search(r"\b([1-9]\d{2,4})\b", t)
    if m3:
        try:
            sid = int(m3.group(1))
            if sid >= 100:
                return (sid // 100 if 1 <= sid // 100 <= 10 else None, sid)
        except ValueError:
            pass
    return (None, None)


def _intent_heuristic(text: str) -> str:
    t = (text or "").lower()
    if any(w in t for w in ("new", "introduce", "launch", "add")) and any(w in t for w in ("fridge", "refrigerator", "freezer")):
        return "add_sku"
    if any(w in t for w in ("remove", "delete", "eliminate", "drop")) and ("sku" in t or "product" in t or "item" in t):
        return "remove_sku"
    if any(w in t for w in ("add", "introduce", "launch", "include")) and ("sku" in t or "product" in t or "item" in t):
        return "add_sku"
    return "general"


def _default_period(state: dict | None) -> tuple[str, str]:
    slots = (state or {}).get("slots") if isinstance(state, dict) else None
    if isinstance(slots, dict):
        ps = (slots.get("period_start") or "").strip()
        pe = (slots.get("period_end") or "").strip()
        if ps and pe:
            return ps, pe
    return DP[0], DP[1]


def _find_product_by_name(ps: str, pe: str, brand_id: int, name: str) -> dict | None:
    """Best-effort: search in top products for the brand in the period and match by substring."""
    if not name or not name.strip():
        return None
    rows = basic_products.query_top_products(ps, pe, limit=200, brand=str(brand_id))
    q = name.strip().lower()
    best = None
    for r in rows or []:
        pn = str(r.get("product_name") or "").strip().lower()
        if not pn:
            continue
        if q == pn:
            return r
        if q in pn:
            if best is None or len(pn) < len(str(best.get("product_name") or "")):
                best = r
    return best


@dataclass
class ToolResult:
    label: str
    payload: Any


class ExpertAgent:
    """
    Minimal agent:
    - Uses Gemini (AI Studio) for wording + clarifying questions.
    - Uses deterministic tool-only routing for data access.
    """

    def __init__(self) -> None:
        self.api_key = _env("GEMINI_API_KEY")
        self.model = _env("GEMINI_MODEL") or "gemini-2.0-flash"
        self._client = None
        if self.api_key and genai is not None:
            try:
                self._client = genai.Client(api_key=self.api_key)
            except Exception:
                self._client = None

    async def handle(self, *, message: str, user: Any, state: dict | None) -> tuple[str, dict]:
        state = state if isinstance(state, dict) else {}
        slots = state.get("slots") if isinstance(state.get("slots"), dict) else {}

        intent = slots.get("intent") or _intent_heuristic(message)
        slots["intent"] = intent

        ps, pe = _default_period(state)
        slots.setdefault("period_start", ps)
        slots.setdefault("period_end", pe)
        slots.setdefault("product_hint", _extract_product_hint(message) or slots.get("product_hint"))

        brand_id = getattr(user, "brand_id", None)
        if not brand_id:
            return ("I need a brand context to run data checks. Please make sure your account is linked to a brand.", {"slots": slots})

        # Slot: product_id (for remove_sku) or product_name.
        pid = slots.get("product_id")
        if pid is None:
            pid = _parse_product_id(message)
            if pid is not None:
                slots["product_id"] = pid
        if not slots.get("product_name"):
            # Best-effort: if user writes a name in quotes, capture it as a hint.
            mq = re.search(r"“([^”]{3,120})”|\"([^\"]{3,120})\"", message)
            if mq:
                slots["product_name"] = (mq.group(1) or mq.group(2) or "").strip()

        # Slot: category/subcategory ids (useful for add_sku).
        if not slots.get("category_id") and not slots.get("subcategory_id"):
            cid, sid = _parse_category_or_subcategory_id(message)
            if cid:
                slots["category_id"] = int(cid)
            if sid:
                slots["subcategory_id"] = int(sid)

        if intent == "remove_sku":
            return await self._handle_remove_sku(message=message, brand_id=int(brand_id), slots=slots)
        if intent == "add_sku":
            return await self._handle_add_sku(message=message, brand_id=int(brand_id), slots=slots)
        return await self._handle_general(message=message, brand_id=int(brand_id), slots=slots)

    def _llm(self, prompt: str) -> str | None:
        if not self._client:
            return None
        try:
            r = self._client.models.generate_content(
                model=self.model,
                contents=prompt,
            )
            t = getattr(r, "text", None)
            return (t or "").strip() or None
        except Exception:
            return None

    async def _handle_remove_sku(self, *, message: str, brand_id: int, slots: dict) -> tuple[str, dict]:
        ps = slots.get("period_start") or DP[0]
        pe = slots.get("period_end") or DP[1]

        pid = slots.get("product_id")
        product_name = (slots.get("product_name") or "").strip()
        if pid is None and product_name:
            hit = _find_product_by_name(ps, pe, brand_id, product_name)
            if hit and hit.get("product_id"):
                pid = int(hit["product_id"])
                slots["product_id"] = pid
                slots["product_name"] = hit.get("product_name") or product_name

        if pid is None:
            # Ask for clarification in English.
            base = "Which SKU do you mean? Please share the numeric `product_id` (best), or paste the exact product name."
            llm = self._llm(
                "You are an analytics assistant for Media Expert. Reply in English. "
                "Ask a short clarifying question (1–2 sentences), no bullets, no fluff.\n\n"
                f"User message: {message}\n\n"
                f"Base clarification: {base}"
            )
            return (llm or base, {"slots": slots})

        # Data: segment breakdown (all sales) -> sum totals.
        seg_rows = mi_segment_sku.query_segment_breakdown_for_product_all_sales(
            int(pid), int(brand_id), str(ps), str(pe), category_id=None, channel=None
        )
        total_gross = sum(float(r.get("gross_pln") or 0) for r in (seg_rows or []))
        total_units = sum(float(r.get("units") or 0) for r in (seg_rows or []))
        top_segments = (seg_rows or [])[:3]

        # Optional: suggest a few top brand products as substitutes (simple heuristic).
        top_brand = basic_products.query_top_products(str(ps), str(pe), limit=8, brand=str(brand_id))
        subs = []
        for r in (top_brand or []):
            try:
                if int(r.get("product_id") or 0) == int(pid):
                    continue
            except Exception:
                pass
            if r.get("product_name"):
                subs.append(str(r.get("product_name")))
            if len(subs) >= 3:
                break

        # Marketing enrichment: for the top segment, add pain points / needstates tags (static/HCG-driven).
        top_seg_enrichment = None
        try:
            if top_segments and top_segments[0].get("segment_id"):
                sid = int(top_segments[0]["segment_id"])
                ss = marketing_svc.get_segment_summary(ps, pe, segment_id=sid, brand_id=int(brand_id))
                if ss and ss.get("segments"):
                    top_seg_enrichment = ss["segments"][0]
        except Exception:
            top_seg_enrichment = None

        answer_draft = self._render_remove_sku_answer(
            product_id=int(pid),
            product_name=(slots.get("product_name") or product_name or ""),
            period_start=str(ps),
            period_end=str(pe),
            total_gross=total_gross,
            total_units=total_units,
            top_segments=top_segments,
            substitute_names=subs,
        )
        # If user did not ask for a period, keep the wording period-free in the final response.
        if not _mentions_period(message):
            answer_draft = answer_draft.replace(f"Period: {ps} → {pe}\n", "")
            answer_draft = answer_draft.replace(" (all channels)", "")
        llm = self._llm(
            "You are 'Ask to the expert' inside Media Expert dashboard. Always answer in English.\n"
            "- Use the provided numbers as-is. Do not invent extra metrics.\n"
            "- Be concise but actionable.\n"
            "- End with 1–2 follow-up questions needed for a scenario estimate.\n\n"
            "DATA (JSON):\n"
            + json.dumps(
                {
                    "product_id": int(pid),
                    "product_name": (slots.get("product_name") or product_name or ""),
                    "period_start": str(ps),
                    "period_end": str(pe),
                    "total_gross_pln": round(float(total_gross), 2),
                    "total_units": round(float(total_units), 2),
                    "top_segments": top_segments,
                    "substitutes": subs,
                    "top_segment_marketing": top_seg_enrichment,
                },
                ensure_ascii=False,
            )
            + "\n\nDRAFT:\n"
            + answer_draft
        )
        return (llm or answer_draft, {"slots": slots})

    async def _handle_add_sku(self, *, message: str, brand_id: int, slots: dict) -> tuple[str, dict]:
        ps = slots.get("period_start") or DP[0]
        pe = slots.get("period_end") or DP[1]

        # For "add sku" we need context to pick benchmarks.
        # We keep it simple and ask for: category/subcategory and expected price positioning.
        if not (slots.get("subcategory_id") or slots.get("category_id")):
            hint = (slots.get("product_hint") or "").strip()
            base = (
                "Got it — you want to introduce a new SKU."
                + (f" (It sounds like a {hint}.)" if hint else "")
                + "\n\nTo make this actionable, I need:\n"
                "- Which subcategory will it belong to? (subcategory id like 301)\n"
                "- Price positioning vs your current range (entry / mid / premium)\n"
                "- Primary channel to win (web / app / store)"
            )
            llm = self._llm(
                "You are an analytics assistant for Media Expert. Reply in English.\n"
                "Ask only the minimum clarifying questions needed to run a forecast (max 3 bullets).\n"
                "Do NOT ask about time period unless the user explicitly mentions it.\n\n"
                f"User message: {message}\n\n"
                f"Base: {base}"
            )
            return (llm or base, {"slots": slots})

        # MVP: use current top products as a benchmark and provide qualitative scenario.
        cat = slots.get("subcategory_id") or slots.get("category_id")
        top_brand = basic_products.query_top_products(str(ps), str(pe), limit=12, brand=str(brand_id), cat=str(cat) if cat else None)
        bench = [{"product_name": r.get("product_name"), "gross_pln": r.get("gross_pln"), "units": r.get("units")} for r in (top_brand or [])[:5]]

        # Segment/promo focus: find most promo-responsive segments for this (sub)category
        focus_segments = []
        try:
            if cat:
                cid = int(cat)
                parent_cat = cid if 1 <= cid <= 10 else (cid // 100)
                resp = promo_q.query_segment_promo_responsiveness(str(ps), str(pe), cat=parent_cat, subcat=(cid if cid >= 100 else None), promo_type=None) or []
                focus_segments = resp
        except Exception:
            focus_segments = []

        draft = self._render_add_sku_answer(period_start=str(ps), period_end=str(pe), benchmark=bench)
        if not _mentions_period(message):
            draft = draft.replace(f"Period used for benchmark: {ps} → {pe}\n\n", "")
        llm = self._llm(
            "You are 'Ask to the expert' inside Media Expert dashboard. Always answer in English.\n"
            "- Use the benchmark list. Do not fabricate numbers.\n"
            "- Recommend what to prioritize (segments, needstates, promos) based on the data.\n"
            "- Provide a simple forecast as a range, clearly stating assumptions.\n"
            "- Do NOT mention time period unless the user asked for it.\n\n"
            "DATA (JSON):\n"
            + json.dumps({"benchmark": bench, "promo_responsive_segments": focus_segments}, ensure_ascii=False)
            + "\n\nDRAFT:\n"
            + draft
        )
        return (llm or draft, {"slots": slots})

    async def _handle_general(self, *, message: str, brand_id: int, slots: dict) -> tuple[str, dict]:
        ps = slots.get("period_start") or DP[0]
        pe = slots.get("period_end") or DP[1]
        base = (
            "I can help with questions like:\n"
            "- “What happens if I remove this SKU?”\n"
            "- “What happens if I add this SKU?”\n\n"
            "If you share a `product_id` (numeric) and the time range, I can run a quick data check. "
            f"Right now I’ll default to {ps} to {pe}."
        )
        llm = self._llm(
            "You are an analytics assistant for Media Expert. Reply in English.\n"
            "Give a short helpful message (max 6 lines) and propose 2 example questions.\n\n"
            f"User message: {message}\n\n"
            f"Base: {base}"
        )
        return (llm or base, {"slots": slots})

    def _render_remove_sku_answer(
        self,
        *,
        product_id: int,
        product_name: str,
        period_start: str,
        period_end: str,
        total_gross: float,
        total_units: float,
        top_segments: list[dict],
        substitute_names: list[str],
    ) -> str:
        pn = product_name.strip()
        header = f"SKU removal impact (baseline) for product_id {product_id}{(' — ' + pn) if pn else ''}"
        lines = [
            header,
            "",
            f"Period: {period_start} → {period_end}",
            f"Baseline sales (all channels): {total_gross:,.0f} PLN gross revenue, {total_units:,.0f} units.",
        ]
        if top_segments:
            lines.append("")
            lines.append("Most impacted customer segments (by revenue):")
            for r in top_segments:
                sid = r.get("segment_id")
                sn = r.get("segment_name") or f"Segment {sid}"
                g = float(r.get("gross_pln") or 0)
                u = float(r.get("units") or 0)
                lines.append(f"- {sn}: {g:,.0f} PLN, {u:,.0f} units")
        if substitute_names:
            lines.append("")
            lines.append("Potential substitutes to evaluate (top SKUs for your brand in the same period):")
            for s in substitute_names[:3]:
                lines.append(f"- {s}")
        lines.append("")
        lines.append("Next step: tell me whether you expect customers to switch to another SKU (and to which one). "
                     "With that assumption, I can estimate net impact and segment-level shifts.")
        return "\n".join(lines)

    def _render_add_sku_answer(self, *, period_start: str, period_end: str, benchmark: list[dict]) -> str:
        lines = [
            "New SKU impact (first-pass estimate)",
            "",
            f"Period used for benchmark: {period_start} → {period_end}",
            "",
            "To estimate an uplift, I benchmark against your current top SKUs in the relevant (sub)category. Reference points:",
        ]
        for r in benchmark or []:
            pn = (r.get("product_name") or "").strip() or "—"
            g = float(r.get("gross_pln") or 0)
            u = float(r.get("units") or 0)
            lines.append(f"- {pn}: {g:,.0f} PLN, {u:,.0f} units")
        lines.append("")
        lines.append(
            "To make this concrete, I need:\n"
            "- Target subcategory (id)\n"
            "- Expected price positioning (lower/similar/higher)\n"
            "- Primary channel (web/app/store/all)\n"
            "- Whether this SKU replaces an existing one (cannibalization) or is incremental\n"
        )
        return "\n".join(lines)

