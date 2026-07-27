# backend/services/bot_manager.py
# Manages one trading engine thread per user.
#
# Used exclusively by the WORKER process (worker.py) — the web
# process (backend.main) never imports this module directly.

import threading
import asyncio
import traceback
from datetime import datetime
from typing import Optional

from backend.db.models import BotStatus

# ── Worker process event loop — captured at startup ──────────────
# Bot threads need this to post async callbacks (_on_status_change,
# _on_trade) back onto worker.py's asyncio loop for DB writes /
# Redis publishes. Set once in worker.py's main() via set_main_loop(loop).
_main_loop: Optional[asyncio.AbstractEventLoop] = None


def set_main_loop(loop: asyncio.AbstractEventLoop):
    global _main_loop
    _main_loop = loop


def _post_to_main(coro):
    """Safely schedule a coroutine on the main FastAPI event loop."""
    if _main_loop and _main_loop.is_running():
        asyncio.run_coroutine_threadsafe(coro, _main_loop)


class BotThread(threading.Thread):
    def __init__(self, user_id: int, config: dict, access_token: str,
                 on_status_change, on_trade):
        super().__init__(daemon=True, name=f"bot-user-{user_id}")
        self.user_id          = user_id
        self.config           = config
        self.access_token     = access_token
        self.on_status_change = on_status_change
        self.on_trade         = on_trade
        self._stop_event      = threading.Event()
        self.error_msg: Optional[str] = None

    def request_stop(self):
        self._stop_event.set()

    def run(self):
        try:
            from backend.engine.engine_v6 import TradingEngine
            engine = TradingEngine(
                user_id      = self.user_id,
                config       = self.config,
                access_token = self.access_token,
                stop_event   = self._stop_event,
                on_trade     = self._sync_on_trade,
            )
            engine.run_sync()
        except Exception as e:
            self.error_msg = traceback.format_exc()
            print(f"[Bot user {self.user_id}] CRASH:\n{self.error_msg}")
            # Notify dashboard about the error
            _post_to_main(
                self.on_status_change(self.user_id, BotStatus.error, self.error_msg)
            )

    def _sync_on_trade(self, trade_data: dict):
        """Called synchronously by engine → post to FastAPI main loop."""
        _post_to_main(self.on_trade(self.user_id, trade_data))


class BotManager:
    def __init__(self):
        self._bots: dict[int, BotThread] = {}
        self._lock = threading.Lock()

    def is_running(self, user_id: int) -> bool:
        with self._lock:
            t = self._bots.get(user_id)
            return t is not None and t.is_alive()

    def get_error(self, user_id: int) -> Optional[str]:
        with self._lock:
            t = self._bots.get(user_id)
            return t.error_msg if t else None

    def start(self, user_id: int, config: dict, access_token: str,
              on_status_change, on_trade) -> bool:
        with self._lock:
            existing = self._bots.get(user_id)
            if existing and existing.is_alive():
                return False   # already running
            # Remove dead/finished thread if any — clears stale error_msg too
            if existing and not existing.is_alive():
                self._bots.pop(user_id, None)

            t = BotThread(
                user_id          = user_id,
                config           = config,
                access_token     = access_token,
                on_status_change = on_status_change,
                on_trade         = on_trade,
            )
            self._bots[user_id] = t
            t.start()   # non-blocking — returns immediately
            return True

    def stop(self, user_id: int):
        with self._lock:
            t = self._bots.get(user_id)
            if t:
                t.request_stop()

    def stop_all(self, join_timeout: float = 10.0):
        """
        Signal all running bot threads to stop and wait (briefly) for them
        to exit cleanly — called from the FastAPI lifespan shutdown handler
        so positions/orders/Telegram pollers are torn down before the
        process exits.
        """
        with self._lock:
            threads = list(self._bots.values())
        for t in threads:
            t.request_stop()
        for t in threads:
            if t.is_alive():
                t.join(timeout=join_timeout)
                if t.is_alive():
                    print(f"[BotManager] WARNING: bot-user-{t.user_id} "
                          f"did not stop within {join_timeout}s")

    def status_all(self) -> dict[int, str]:
        with self._lock:
            return {
                uid: ("running" if t.is_alive() else "stopped")
                for uid, t in self._bots.items()
            }

    def running_count(self) -> int:
        with self._lock:
            return sum(1 for t in self._bots.values() if t.is_alive())


# Singleton
bot_manager = BotManager()
