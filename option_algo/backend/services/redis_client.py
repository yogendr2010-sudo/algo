# backend/services/redis_client.py
# ================================================================
# Redis connection singletons.
#
# Two clients are exposed:
#   get_redis()       -> redis.asyncio.Redis   (FastAPI / web process)
#   get_redis_sync()  -> redis.Redis           (worker process /
#                                                SymbolEngine threads)
#
# Both connect to the same REDIS_URL and share the same key-space —
# this is the backbone that lets the stateless web process(es) and
# the single worker process communicate:
#
#   command_queue  — web -> worker  (start/stop/modify commands)
#   state_store    — worker -> web  (bot status, positions, OC snapshots)
#   event_bus      — worker -> web  (live trade/SL/status events -> WS)
#   rate_limit     — web <-> web    (shared counters across instances)
# ================================================================

from functools import lru_cache
import redis as redis_sync
import redis.asyncio as redis_async

from backend.config import get_settings

settings = get_settings()


@lru_cache()
def get_redis_sync() -> "redis_sync.Redis":
    """
    Synchronous Redis client — safe to use from worker threads
    (SymbolEngine, BotThread, OptionChainAnalyzer). decode_responses=True
    so callers get str, not bytes.
    """
    return redis_sync.Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        socket_keepalive=True,
        health_check_interval=30,
    )


@lru_cache()
def get_redis() -> "redis_async.Redis":
    """
    Async Redis client — used by FastAPI routes/dependencies.
    """
    return redis_async.Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        socket_keepalive=True,
        health_check_interval=30,
    )


async def ping() -> bool:
    """Health-check helper — used by /health endpoint."""
    try:
        return bool(await get_redis().ping())
    except Exception:
        return False
