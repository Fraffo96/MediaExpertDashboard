"""API: Ask to the expert (LLM + tool-only BigQuery)."""

from __future__ import annotations

import json
import time
from typing import Optional

from fastapi import APIRouter, Body, Cookie, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.services.expert_agent import ExpertAgent
from app.web.context import get_user

router = APIRouter()

# Very small in-memory throttle (per-process).
_RL_WINDOW_SEC = 60.0
_RL_MAX_REQ = 12
_rl: dict[str, list[float]] = {}


def _normalize_chat_messages(body: dict) -> list[dict] | None:
    """Build message list for Gemini-first agent: prefer `messages`, fallback to legacy `message`."""
    raw = body.get("messages")
    if isinstance(raw, list) and raw:
        out: list[dict] = []
        for m in raw:
            if not isinstance(m, dict):
                continue
            role = m.get("role")
            text = (m.get("text") or "").strip()
            if not text or role not in ("user", "assistant", "system"):
                continue
            out.append({"role": str(role), "text": text})
        return out or None
    msg = (body.get("message") or "").strip()
    if msg:
        return [{"role": "user", "text": msg}]
    return None


def _rate_limit_key(request: Request, username: str | None) -> str:
    ip = (getattr(request.client, "host", None) or "").strip() or "unknown"
    u = (username or "").strip() or "anonymous"
    return f"{u}@{ip}"


def _allow_request(key: str) -> bool:
    now = time.monotonic()
    arr = _rl.get(key) or []
    arr = [t for t in arr if (now - t) <= _RL_WINDOW_SEC]
    if len(arr) >= _RL_MAX_REQ:
        _rl[key] = arr
        return False
    arr.append(now)
    _rl[key] = arr
    return True


@router.post("/api/expert-chat", tags=["Expert"])
async def api_expert_chat(
    request: Request,
    access_token: Optional[str] = Cookie(None),
    body: dict = Body(default=None),
):
    """Chat endpoint used by landing page widget. Requires auth cookie."""
    user = get_user(access_token)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not _allow_request(_rate_limit_key(request, getattr(user, "username", None))):
        return JSONResponse({"error": "Rate limit exceeded. Please wait a moment and retry."}, status_code=429)

    body = body or {}
    messages = _normalize_chat_messages(body)
    if not messages:
        return JSONResponse({"error": "Message or non-empty messages[] is required."}, status_code=400)

    agent = ExpertAgent()

    async def event_generator():
        try:
            async for ev in agent.handle_stream(messages=messages, user=user):
                line = json.dumps(ev, ensure_ascii=False)
                yield f"data: {line}\n\n"
        except Exception as e:  # pragma: no cover
            err = json.dumps({"type": "error", "text": f"Stream error: {e!s}"}, ensure_ascii=False)
            yield f"data: {err}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

