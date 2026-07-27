# backend/services/admin_config_cache.py
# ================================================================
# Read cache for admin-managed Exchange Holidays and Streamer Symbol
# Tokens (backend.db.models.ExchangeHoliday / StreamerSymbolToken).
#
# The web process (admin API) and worker process (engine threads)
# are separate processes sharing only the DB, so an in-process cache
# in one won't see writes from the other. Reads use the same sync
# engine precedent as push_notifications.send_push_sync() (see
# backend.db.database._sync_engine_for_push) rather than routing
# through async sessions from worker threads. A TTL keeps the two
# processes eventually consistent; refresh(force=True) is called
# right after an admin write to update the writer's own process
# immediately.
# ================================================================

import threading
import time
from datetime import date
from typing import Optional

from backend.db.database import _sync_engine_for_push

_TTL_SECONDS = 300

_lock = threading.Lock()
_last_loaded_at = 0.0
_holidays: set = set()
_tokens: dict = {}


def _load_holidays_from_db() -> set:
    from sqlalchemy import text
    with _sync_engine_for_push().connect() as conn:
        rows = conn.execute(text("SELECT holiday_date FROM exchange_holidays")).fetchall()
    return {r[0] for r in rows}


def _load_tokens_from_db() -> dict:
    from sqlalchemy import text
    with _sync_engine_for_push().connect() as conn:
        rows = conn.execute(text(
            "SELECT symbol, streamer_token, history_key, strike_step "
            "FROM streamer_symbol_tokens WHERE is_active = :active"
        ), {"active": True}).fetchall()
    return {
        r[0]: {"streamer_token": r[1], "history_key": r[2], "strike_step": r[3]}
        for r in rows
    }


def refresh(force: bool = False) -> None:
    """Reloads holidays + streamer tokens from the DB if the TTL expired (or forced)."""
    global _last_loaded_at, _holidays, _tokens
    with _lock:
        if not force and (time.time() - _last_loaded_at) < _TTL_SECONDS:
            return
        try:
            _holidays = _load_holidays_from_db()
            _tokens = _load_tokens_from_db()
        except Exception as e:
            # DB unreachable — keep the last-known-good cache; callers
            # (is_holiday) also fall back to the hardcoded holiday set.
            print(f"⚠️  admin_config_cache refresh failed, using stale cache: {e}")
        _last_loaded_at = time.time()


def is_holiday(d: date) -> bool:
    """
    True if `d` is an admin-configured holiday OR one of the hardcoded
    fallback holidays in backend.engine.history_loader — the hardcoded
    set is a safety net so a DB hiccup / empty table never turns a
    known holiday into a trading day.
    """
    refresh()
    iso = d.strftime("%Y-%m-%d")
    if iso in _holidays:
        return True
    from backend.engine.history_loader import NSE_HOLIDAYS
    return iso in NSE_HOLIDAYS


def get_all_holidays() -> set:
    refresh()
    return set(_holidays)


def get_streamer_token(symbol: str) -> Optional[dict]:
    """Returns {"streamer_token", "history_key", "strike_step"} for an active DB row, or None."""
    refresh()
    return _tokens.get((symbol or "").upper())


def get_all_streamer_tokens() -> dict:
    refresh()
    return dict(_tokens)
