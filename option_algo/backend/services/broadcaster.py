# backend/services/broadcaster.py
# ================================================================
# WebSocket connection manager — bridges local browser WebSocket
# connections to the Redis event bus (backend/services/event_bus.py).
#
# Architecture:
#   - The worker process (running SymbolEngine/BotThread) has NO
#     direct reference to any WebSocket — it only PUBLISHes events to
#     Redis via event_bus.publish_event_sync().
#   - Each web process instance keeps its own local WebSocket
#     connections. For every connected user, it runs a background
#     task that SUBSCRIBEs to that user's Redis channel and forwards
#     messages to all of that user's local sockets (e.g. multiple
#     browser tabs hitting the same web instance).
#
# This means WS delivery works correctly regardless of which process
# (web or worker) — or which web instance, if horizontally scaled —
# triggered the event.
# ================================================================

import asyncio
from collections import defaultdict
from fastapi import WebSocket

from backend.services.event_bus import subscribe


class ConnectionManager:
    def __init__(self):
        # user_id → list of active local WebSocket connections
        self._connections: dict[int, list[WebSocket]] = defaultdict(list)
        # user_id → background Redis-subscriber task (one per user per
        # process, shared across that user's connections on this instance)
        self._sub_tasks: dict[int, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    async def connect(self, user_id: int, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self._connections[user_id].append(ws)
            if user_id not in self._sub_tasks or self._sub_tasks[user_id].done():
                self._sub_tasks[user_id] = asyncio.create_task(
                    self._relay_loop(user_id))

    async def disconnect(self, user_id: int, ws: WebSocket):
        async with self._lock:
            conns = self._connections.get(user_id, [])
            if ws in conns:
                conns.remove(ws)
            if not conns:
                self._connections.pop(user_id, None)
                task = self._sub_tasks.pop(user_id, None)
                if task:
                    task.cancel()

    async def _relay_loop(self, user_id: int):
        """
        Background task: subscribes to events:{user_id} on Redis and
        forwards every message to all local WS connections for this
        user. Exits when cancelled (last connection disconnects).
        """
        try:
            async for payload in subscribe(user_id):
                async with self._lock:
                    conns = list(self._connections.get(user_id, []))
                if not conns:
                    break
                dead = []
                for ws in conns:
                    try:
                        await ws.send_json(payload)
                    except Exception:
                        dead.append(ws)
                if dead:
                    async with self._lock:
                        for ws in dead:
                            if ws in self._connections.get(user_id, []):
                                self._connections[user_id].remove(ws)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[broadcaster] relay loop error for user {user_id}: {e}")

    def connected_users(self) -> list[int]:
        return [uid for uid, conns in self._connections.items() if conns]


# Singleton — imported by routers
manager = ConnectionManager()
