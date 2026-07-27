# backend/services/state_store.py
# ================================================================
# Redis-backed state store.
#
# The worker process (worker.py) owns the actual SymbolEngine
# instances and writes live snapshots here. Web process(es) read
# these snapshots to answer /api/bot/status, /api/position, and
# /api/oc/analysis WITHOUT needing direct access to engine objects
# (which only exist in the worker's memory).
#
# Key layout:
#   bot:status:{user_id}     -> JSON {running, status, error, updated_at}
#                                TTL refreshed every HEARTBEAT_SEC by the
#                                worker while the bot is running. If the
#                                key expires, the web process treats the
#                                bot as "stopped" (worker crashed/killed).
#
#   bot:positions:{user_id}  -> JSON {positions:[...], count, updated_at}
#                                TTL ~ 2x snapshot interval.
#
#   bot:oc:{user_id}         -> JSON {analysis:{...}, chain_df:[...],
#                                      updated_at}
#                                TTL ~ 2x OC refresh interval (30s).
#
# Sync functions (set_*) are called from worker-side threads
# (SymbolEngine / OptionChainAnalyzer / BotThread) via the sync
# Redis client. Async functions (get_*) are called from FastAPI
# routes via the async Redis client.
# ================================================================

import json
from datetime import datetime
from typing import Optional

from backend.services.redis_client import get_redis, get_redis_sync

HEARTBEAT_SEC      = 8     # worker refreshes bot:status TTL this often
POSITIONS_TTL_SEC  = 15
OC_TTL_SEC         = 90    # OC refreshes every 30s; allow 3 misses


def _now() -> str:
    return datetime.utcnow().isoformat()


# ================================================================
# BOT STATUS  (worker writes, web reads)
# ================================================================

def set_bot_status_sync(user_id: int, status: str, error: Optional[str] = None,
                         ttl: int = HEARTBEAT_SEC * 3):
    """Called by worker (BotThread / heartbeat loop)."""
    r = get_redis_sync()
    payload = {
        "running":    status == "running",
        "status":     status,
        "error":      error,
        "updated_at": _now(),
    }
    r.set(f"bot:status:{user_id}", json.dumps(payload, default=str), ex=ttl)


def clear_bot_status_sync(user_id: int):
    get_redis_sync().delete(f"bot:status:{user_id}")


async def get_bot_status(user_id: int) -> dict:
    """
    Called by web process (/api/bot/status, /api/bot/debug).
    If the key has expired (worker crashed without cleanup, or never
    started), returns running=False with no error.
    """
    raw = await get_redis().get(f"bot:status:{user_id}")
    if not raw:
        return {"running": False, "status": "stopped", "error": None, "updated_at": None}
    return json.loads(raw)


async def get_all_bot_statuses() -> dict[int, dict]:
    """Used by admin dashboard — scans all bot:status:* keys."""
    r = get_redis()
    out = {}
    async for key in r.scan_iter(match="bot:status:*"):
        try:
            user_id = int(key.split(":")[-1])
            raw     = await r.get(key)
            if raw:
                out[user_id] = json.loads(raw)
        except Exception:
            continue
    return out


async def count_running_bots() -> int:
    statuses = await get_all_bot_statuses()
    return sum(1 for s in statuses.values() if s.get("running"))


# ================================================================
# POSITIONS SNAPSHOT  (worker writes, web reads)
# ================================================================

def set_positions_sync(user_id: int, positions: list):
    r = get_redis_sync()
    payload = {
        "positions":  positions,
        "count":      len(positions),
        "updated_at": _now(),
    }
    r.set(f"bot:positions:{user_id}", json.dumps(payload, default=str), ex=POSITIONS_TTL_SEC)


async def get_positions(user_id: int) -> dict:
    raw = await get_redis().get(f"bot:positions:{user_id}")
    if not raw:
        return {"positions": [], "count": 0, "updated_at": None}
    return json.loads(raw)


# ================================================================
# OPTION CHAIN ANALYSIS SNAPSHOT  (worker writes, web reads)
# ================================================================

def set_oc_snapshot_sync(user_id: int, analysis: Optional[dict], chain_df: Optional[list]):
    r = get_redis_sync()
    payload = {
        "analysis":   analysis,
        "chain_df":   chain_df,
        "updated_at": _now(),
    }
    r.set(f"bot:oc:{user_id}", json.dumps(payload, default=str), ex=OC_TTL_SEC)


async def get_oc_snapshot(user_id: int) -> dict:
    raw = await get_redis().get(f"bot:oc:{user_id}")
    if not raw:
        return {"analysis": None, "chain_df": None, "updated_at": None}
    return json.loads(raw)
