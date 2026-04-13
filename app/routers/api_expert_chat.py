"""API: Ask to the expert (LLM + tool-only BigQuery)."""

from __future__ import annotations

import time
from typing import Optional

from fastapi import APIRouter, Body, Cookie, HTTPException, Request
from fastapi.responses import JSONResponse

from app.services.expert_agent import ExpertAgent
from app.web.context import get_user

router = APIRouter()

# Very small in-memory throttle (per-process).
_RL_WINDOW_SEC = 60.0
_RL_MAX_REQ = 12
_rl: dict[str, list[float]] = {}


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
    msg = (body.get("message") or "").strip()
    if not msg:
        return JSONResponse({"error": "Message is required."}, status_code=400)

    state = body.get("state")
    if state is not None and not isinstance(state, dict):
        state = None

    agent = ExpertAgent()
    answer, next_state = await agent.handle(message=msg, user=user, state=state)
    return {"answer": answer, "state": next_state}

