# backend/services/command_queue.py
# ================================================================
# Redis-backed command queue — web process(es) -> worker process.
#
# The worker process is the ONLY process that holds live SymbolEngine
# instances (running in BotThread). Web processes (which may be
# multiple, behind a load balancer) cannot call engine methods
# directly — instead they push a command onto a Redis list, and the
# worker's BRPOP loop picks it up.
#
# For commands that need a result (modify SL, squareoff, start, stop),
# the web process polls a short-lived result key for up to
# RESULT_WAIT_SEC. If the worker hasn't responded in time, the web
# process returns a "queued" response rather than blocking forever —
# the action will still complete, just asynchronously.
#
# Queue key:   bot:commands              (Redis LIST, worker does BRPOP)
# Result key:  bot:cmd_result:{cmd_id}   (Redis STRING, short TTL)
# ================================================================

import json
import uuid
import time
import asyncio
from typing import Optional

from backend.services.redis_client import get_redis, get_redis_sync

QUEUE_KEY        = "bot:commands"
RESULT_TTL_SEC   = 30
RESULT_WAIT_SEC  = 5.0     # how long the web process waits for an ack
POLL_INTERVAL    = 0.15
MAX_QUEUE_LENGTH = 1000    # reject new commands when queue exceeds this
QUEUE_WARN_THRESHOLD = 500 # log warning when queue exceeds this


# ================================================================
# WEB SIDE (async) — push command, optionally wait for result
# ================================================================

async def push_command(action: str, user_id: int, payload: Optional[dict] = None) -> str:
    """
    Push a command onto the queue. Returns the command_id immediately
    (does NOT wait). Use wait_for_result() separately if you need the
    outcome.
    """
    cmd_id = uuid.uuid4().hex
    cmd = {
        "id":      cmd_id,
        "action":  action,
        "user_id": user_id,
        "payload": payload or {},
    }
    r = get_redis()
    queue_len = await r.llen(QUEUE_KEY)
    if queue_len >= MAX_QUEUE_LENGTH:
        return None  # caller should treat None as "rejected — queue full"
    await r.lpush(QUEUE_KEY, json.dumps(cmd, default=str))
    return cmd_id


async def wait_for_result(cmd_id: str, timeout: float = RESULT_WAIT_SEC) -> Optional[dict]:
    """
    Poll Redis for the result of a command. Returns the result dict
    when the worker posts it, or None on timeout.
    """
    key = f"bot:cmd_result:{cmd_id}"
    r = get_redis()
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        raw = await r.get(key)
        if raw:
            await r.delete(key)
            return json.loads(raw)
        await asyncio.sleep(POLL_INTERVAL)
    return None


async def send_command(action: str, user_id: int, payload: Optional[dict] = None,
                        timeout: float = RESULT_WAIT_SEC) -> dict:
    """
    Convenience: push + wait. If the worker doesn't respond in time,
    returns {"ok": True, "queued": True}.
    Returns {"ok": False, "error": "Command queue is full"} if rejected.
    """
    cmd_id = await push_command(action, user_id, payload)
    if cmd_id is None:
        return {"ok": False, "error": "Command queue is full — please retry shortly"}
    result = await wait_for_result(cmd_id, timeout)
    if result is None:
        return {"ok": True, "queued": True}
    return result


# ================================================================
# WORKER SIDE (sync) — pop commands, post results
# ================================================================

def send_command_sync(action: str, user_id: int, payload: Optional[dict] = None,
                       timeout: float = RESULT_WAIT_SEC) -> dict:
    """
    Synchronous version of send_command — pushes a command to the queue
    and polls for the result. Used by background threads (Telegram bot
    pollers) that cannot use asyncio.

    Returns {"ok": True, "queued": True} if the worker doesn't respond
    within `timeout` seconds.
    """
    import uuid as _uuid
    cmd_id = _uuid.uuid4().hex
    cmd = {
        "id":      cmd_id,
        "action":  action,
        "user_id": user_id,
        "payload": payload or {},
    }
    r = get_redis_sync()
    queue_len = r.llen(QUEUE_KEY)
    if queue_len >= MAX_QUEUE_LENGTH:
        return {"ok": False, "error": "Command queue is full — please retry shortly"}
    r.lpush(QUEUE_KEY, json.dumps(cmd, default=str))

    key      = f"bot:cmd_result:{cmd_id}"
    deadline = time.time() + timeout
    while time.time() < deadline:
        raw = r.get(key)
        if raw:
            r.delete(key)
            return json.loads(raw)
        time.sleep(0.15)
    return {"ok": True, "queued": True}


def pop_command_sync(timeout: int = 5) -> Optional[dict]:
    """
    Blocking pop with timeout (BRPOP). Returns None on timeout —
    caller should loop and check the stop event.
    """
    r    = get_redis_sync()
    item = r.brpop(QUEUE_KEY, timeout=timeout)
    if item is None:
        return None
    _, raw = item
    try:
        return json.loads(raw)
    except Exception:
        return None


def post_result_sync(cmd_id: str, result: dict):
    r = get_redis_sync()
    r.set(f"bot:cmd_result:{cmd_id}", json.dumps(result, default=str), ex=RESULT_TTL_SEC)


def get_queue_depth() -> int:
    """Return current command queue depth."""
    try:
        return get_redis_sync().llen(QUEUE_KEY)
    except Exception:
        return -1


def check_and_warn_queue():
    """Check queue depth and log warning if above threshold."""
    try:
        depth = get_queue_depth()
        if depth >= MAX_QUEUE_LENGTH:
            print(f"[cmd-queue] CRITICAL: Queue depth {depth} >= {MAX_QUEUE_LENGTH} — commands being rejected")
        elif depth >= QUEUE_WARN_THRESHOLD:
            print(f"[cmd-queue] WARNING: Queue depth {depth} >= {QUEUE_WARN_THRESHOLD}")
        return depth
    except Exception:
        return -1


def trim_queue():
    """Trim queue to MAX_QUEUE_LENGTH (called periodically by worker)."""
    try:
        get_redis_sync().ltrim(QUEUE_KEY, -MAX_QUEUE_LENGTH, -1)
    except Exception:
        pass
