# backend/shared/dist_locks.py
# ================================================================
# Distributed Locks — Redis-backed mutual exclusion.
#
# Used to ensure only ONE worker performs a given task across all
# processes/threads. Critical for:
#   - Historical data downloads (one per symbol)
#   - Candle creation (one candle builder per symbol)
#   - Daily initialization (one per symbol per day)
#   - Option Chain refresh (one per symbol/expiry)
#   - Scheduler tasks (one scheduler instance)
#   - Indicator calculation (one per symbol/timeframe)
# ================================================================

import time
import threading
import contextlib

from backend.shared.redis_infra import lock_key
from backend.services.redis_client import get_redis_sync

DEFAULT_LOCK_TTL_SEC = 30   # Auto-release after 30s if holder crashes
RETRY_INTERVAL_SEC   = 0.05
MAX_RETRIES          = 20   # 20 × 0.05s = 1s total wait

# Lua script: only delete the lock if the value matches (ownership check)
_RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

_thread_local = threading.local()


def _make_value() -> str:
    return f"{threading.get_ident()}:{time.time()}"


def _store_value(key: str, value: str):
    if not hasattr(_thread_local, "lock_values"):
        _thread_local.lock_values = {}
    _thread_local.lock_values[key] = value


def _pop_value(key: str) -> str:
    if hasattr(_thread_local, "lock_values"):
        return _thread_local.lock_values.pop(key, "")
    return ""


def acquire_lock(task: str, identifier: str = "",
                 ttl: int = DEFAULT_LOCK_TTL_SEC) -> bool:
    """
    Try to acquire a distributed lock. Returns True if acquired.
    Non-blocking — use acquire_lock_wait() for blocking.
    """
    key = lock_key(task, identifier)
    r = get_redis_sync()
    value = _make_value()
    acquired = bool(r.set(key, value, nx=True, ex=ttl))
    if acquired:
        _store_value(key, value)
    return acquired


def acquire_lock_wait(task: str, identifier: str = "",
                       ttl: int = DEFAULT_LOCK_TTL_SEC,
                       timeout: float = 5.0) -> bool:
    """
    Block until lock is acquired or timeout expires.
    Returns True if acquired, False if timed out.
    """
    key = lock_key(task, identifier)
    r = get_redis_sync()
    value = _make_value()
    deadline = time.time() + timeout
    while time.time() < deadline:
        if r.set(key, value, nx=True, ex=ttl):
            _store_value(key, value)
            return True
        time.sleep(RETRY_INTERVAL_SEC)
    return False


def release_lock(task: str, identifier: str = ""):
    """Release a distributed lock (only if held by this thread)."""
    key = lock_key(task, identifier)
    value = _pop_value(key)
    if not value:
        return
    r = get_redis_sync()
    r.eval(_RELEASE_SCRIPT, 1, key, value)


def is_locked(task: str, identifier: str = "") -> bool:
    """Check if a lock is currently held."""
    return bool(get_redis_sync().exists(lock_key(task, identifier)))


@contextlib.contextmanager
def locked(task: str, identifier: str = "", ttl: int = DEFAULT_LOCK_TTL_SEC):
    """
    Context manager for distributed locking.
    Usage:
        with locked("historical_download", "NIFTY"):
            # only one worker runs this at a time
            download_data()
    """
    acquired = acquire_lock_wait(task, identifier, ttl)
    if not acquired:
        raise RuntimeError(f"Could not acquire lock: {task}:{identifier}")
    try:
        yield
    finally:
        release_lock(task, identifier)
