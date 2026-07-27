# backend/services/event_bus.py
# ================================================================
# Redis Pub/Sub event bus — worker process -> web process(es).
#
# The worker process runs the actual SymbolEngine instances, which
# emit ENTRY/EXIT/SL_TRAIL/BOT_STATUS events. Browser WebSocket
# connections live in web process(es) — possibly a DIFFERENT process
# (or, with horizontal scaling, a different machine) than the worker.
#
# Flow:
#   worker:  publish_event(user_id, payload)        -> PUBLISH events:{user_id}
#   web:     async for msg in subscribe(user_id):   -> SUBSCRIBE events:{user_id}
#            forward msg to all WS connections for that user
#
# Each WebSocket connection subscribes independently — Redis Pub/Sub
# fans out to all subscribers, so multiple browser tabs for the same
# user (each with its own WS) all receive the same events.
# ================================================================

import json
import asyncio
from typing import AsyncIterator

from backend.services.redis_client import get_redis, get_redis_sync


def _channel(user_id: int) -> str:
    return f"events:{user_id}"


# ================================================================
# WORKER SIDE (sync) — publish
# ================================================================

def publish_event_sync(user_id: int, payload: dict):
    """Called by SymbolEngine / BotThread callbacks (worker process)."""
    try:
        get_redis_sync().publish(_channel(user_id), json.dumps(payload, default=str))
    except Exception as e:
        print(f"[event_bus] publish failed for user {user_id}: {e}")


# ================================================================
# WEB SIDE (async) — subscribe
# ================================================================

async def subscribe(user_id: int) -> AsyncIterator[dict]:
    """
    Async generator — yields decoded JSON messages published to this
    user's channel. Used by the WebSocket endpoint:

        async for msg in subscribe(user.id):
            await websocket.send_json(msg)

    The generator runs until the WebSocket disconnects (caller breaks
    out of the loop / cancels the task) — pubsub.unsubscribe/close is
    handled in a try/finally.
    """
    r      = get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe(_channel(user_id))
    try:
        while True:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg is None:
                # No message — yield control so caller can check for
                # WebSocket disconnect / send periodic pings.
                await asyncio.sleep(0.05)
                continue
            try:
                yield json.loads(msg["data"])
            except Exception:
                continue
    finally:
        await pubsub.unsubscribe(_channel(user_id))
        await pubsub.close()
