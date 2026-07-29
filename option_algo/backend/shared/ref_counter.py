# backend/shared/ref_counter.py
# ================================================================
# Reference Counter — manages lifecycle of shared resources.
#
# Every shared resource (WebSocket, candle builder, indicator engine,
# market structure engine, option chain, strategy engine) gets a
# Redis-backed reference counter keyed by resource_type + identifier
# (usually symbol).
#
# Lifecycle:
#   increment(resource, identifier) → counter += 1
#     if counter was 0 → signal "create resource" → returns True
#
#   decrement(resource, identifier) → counter -= 1
#     if counter hits 0 → set a configurable timeout
#     after timeout → signal "destroy resource" → returns True
#
# The caller is responsible for creating/destroying the actual
# resource — this module only tracks the counts and signals.
# ================================================================

import time
import threading
from typing import Optional

from backend.shared.redis_infra import ref_counter_key, ref_counter_timeout_key
from backend.services.redis_client import get_redis_sync

DESTROY_TIMEOUT_SEC = 60  # Wait 60s after last ref before destroying


def _r():
    return get_redis_sync()


def increment(resource_type: str, identifier: str) -> int:
    """
    Increment reference count. Returns the new count.
    If count goes from 0→1, the resource should be created.
    """
    key = ref_counter_key(resource_type, identifier)
    timeout_key = ref_counter_timeout_key(resource_type, identifier)
    r = _r()

    # Clear any pending destroy timeout — the resource is in use again
    r.delete(timeout_key)

    new_count = r.incr(key)
    return new_count


def decrement(resource_type: str, identifier: str) -> int:
    """
    Decrement reference count. Returns the new count.
    If count hits 0, set a destroy timeout.
    After the timeout, the resource should be destroyed.
    """
    key = ref_counter_key(resource_type, identifier)
    timeout_key = ref_counter_timeout_key(resource_type, identifier)
    r = _r()

    new_count = max(0, r.decr(key))
    if new_count <= 0:
        # Set timeout — if key expires before next increment, destroy
        r.set(timeout_key, str(time.time()), ex=DESTROY_TIMEOUT_SEC)
    return new_count


def get_count(resource_type: str, identifier: str) -> int:
    """Get current reference count."""
    raw = _r().get(ref_counter_key(resource_type, identifier))
    return int(raw) if raw else 0


def is_destroy_pending(resource_type: str, identifier: str) -> bool:
    """Check if a destroy timeout was set and has expired."""
    timeout_key = ref_counter_timeout_key(resource_type, identifier)
    return not _r().exists(timeout_key)


def force_destroy(resource_type: str, identifier: str):
    """Force remove all tracking keys for a resource."""
    r = _r()
    r.delete(ref_counter_key(resource_type, identifier))
    r.delete(ref_counter_timeout_key(resource_type, identifier))


def get_active_resources(resource_type: str) -> list[str]:
    """List all identifiers with ref count > 0 for a resource type."""
    pattern = f"shared:ref:{resource_type}:*"
    result = []
    prefix = f"shared:ref:{resource_type}:"
    r = _r()
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=100)
        for key in keys:
            val = r.get(key)
            if val and int(val) > 0:
                identifier = key[len(prefix):]
                result.append(identifier)
        if cursor == 0:
            break
    return result
