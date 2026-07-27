# backend/services/rate_limit.py
# ================================================================
# Redis-backed per-user rate limiter.
#
# Why Redis instead of in-memory: with multiple web process
# instances (horizontal scaling) sitting behind a load balancer, an
# in-memory dict per process can't enforce a shared limit — a user
# hitting different instances would get a fresh quota on each.
# Redis SET ... NX EX gives an atomic, shared "one call per
# min_interval_sec" check across all instances.
# ================================================================

from fastapi import HTTPException, Depends

from backend.db.models import User
from backend.services.auth_service import get_current_user
from backend.services.redis_client import get_redis


def rate_limit(key: str, min_interval_sec: float):
    """
    FastAPI dependency factory. Limits a user to one call to the
    given `key` (e.g. "bot_start") per `min_interval_sec` seconds,
    enforced atomically via Redis.

    Usage:
        @router.post("/start")
        async def start_bot(user: User = Depends(get_current_user),
                             _rl: None = Depends(rate_limit("bot_start", 5))):
            ...
    """
    async def _checker(user: User = Depends(get_current_user)):
        r  = get_redis()
        rk = f"ratelimit:{user.id}:{key}"
        # SET NX EX — succeeds only if the key doesn't already exist.
        ok = await r.set(rk, "1", ex=max(int(min_interval_sec), 1), nx=True)
        if not ok:
            ttl  = await r.ttl(rk)
            wait = max(ttl, 1)
            raise HTTPException(
                status_code=429,
                detail=f"Too many requests — please wait {wait}s before retrying.")
        return None
    return _checker
