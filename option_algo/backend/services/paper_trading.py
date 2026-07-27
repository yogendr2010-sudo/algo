# backend/services/paper_trading.py
# ================================================================
# Paper Trading Engine
# Simulates order fills, SL hits, and target exits without
# placing any real orders on the broker.
# All paper trades are logged to the database exactly like real trades.
# ================================================================

import time
import threading
from datetime import datetime
from typing import Optional
import random

_paper_lock  = threading.Lock()
_paper_id_counter = 1000


def _new_order_id() -> str:
    global _paper_id_counter
    _paper_id_counter += 1
    return f"PAPER-{_paper_id_counter}"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class PaperOrderBook:
    """
    Tracks all simulated open orders for one user.
    Simulates realistic fill prices with a small slippage model.
    """

    SLIPPAGE_PCT = 0.001   # 0.1% slippage on market orders

    def __init__(self, user_id: int):
        self.user_id = user_id
        self.orders  = {}   # order_id → order dict
        self._lock   = threading.Lock()

    def place_market_order(self, side: str, qty: int,
                           ltp: float, instrument_key: str,
                           tag: str = "") -> dict:
        """Simulates MARKET order fill at LTP ± slippage."""
        slip   = ltp * self.SLIPPAGE_PCT
        # Buy fills slightly higher, sell slightly lower (realistic)
        fill   = round(ltp + slip if side == "BUY" else ltp - slip, 2)
        oid    = _new_order_id()
        order  = {
            "order_id":       oid,
            "side":           side,
            "qty":            qty,
            "order_type":     "MARKET",
            "status":         "complete",
            "fill_price":     fill,
            "trigger_price":  0,
            "instrument_key": instrument_key,
            "tag":            tag,
            "ts":             _now(),
        }
        with self._lock:
            self.orders[oid] = order
        print(_now(), f"[PAPER user {self.user_id}] MARKET {side} {qty} @ {fill} "
              f"({instrument_key[:20]}...)")
        return order

    def place_sl_order(self, side: str, qty: int,
                       trigger_price: float, instrument_key: str,
                       tag: str = "") -> dict:
        """Places a simulated SL-M order (pending until triggered)."""
        oid   = _new_order_id()
        order = {
            "order_id":       oid,
            "side":           side,
            "qty":            qty,
            "order_type":     "SL-M",
            "status":         "open",        # pending trigger
            "fill_price":     None,
            "trigger_price":  trigger_price,
            "instrument_key": instrument_key,
            "tag":            tag,
            "ts":             _now(),
        }
        with self._lock:
            self.orders[oid] = order
        print(_now(), f"[PAPER user {self.user_id}] SL-M {side} {qty} "
              f"trigger={trigger_price}")
        return order

    def modify_sl_order(self, order_id: str, new_trigger: float) -> bool:
        with self._lock:
            if order_id not in self.orders:
                return False
            if self.orders[order_id]["status"] != "open":
                return False
            self.orders[order_id]["trigger_price"] = new_trigger
        print(_now(), f"[PAPER user {self.user_id}] SL modified → {new_trigger}")
        return True

    def cancel_order(self, order_id: str) -> bool:
        with self._lock:
            if order_id in self.orders:
                self.orders[order_id]["status"] = "cancelled"
                return True
        return False

    def check_sl_filled(self, order_id: str, current_ltp: float) -> Optional[float]:
        """
        Checks if SL order should be triggered at current LTP.
        Returns fill price if triggered, None otherwise.
        """
        with self._lock:
            order = self.orders.get(order_id)
            if not order or order["status"] != "open":
                return None
            if order["order_type"] != "SL-M":
                return None
            trigger = order["trigger_price"]
            # SELL SL triggers when LTP falls below trigger
            if order["side"] == "SELL" and current_ltp <= trigger:
                # Fill at trigger with slippage (worse for the trader)
                fill = round(trigger * (1 - self.SLIPPAGE_PCT), 2)
                order["status"]     = "complete"
                order["fill_price"] = fill
                return fill
            # BUY SL (for short positions) triggers when LTP rises above trigger
            if order["side"] == "BUY" and current_ltp >= trigger:
                fill = round(trigger * (1 + self.SLIPPAGE_PCT), 2)
                order["status"]     = "complete"
                order["fill_price"] = fill
                return fill
        return None

    def get_order(self, order_id: str) -> Optional[dict]:
        with self._lock:
            return self.orders.get(order_id)


# ── Global registry: user_id → PaperOrderBook ──────────────────
_paper_books: dict[int, PaperOrderBook] = {}
_books_lock  = threading.Lock()


def get_paper_book(user_id: int) -> PaperOrderBook:
    with _books_lock:
        if user_id not in _paper_books:
            _paper_books[user_id] = PaperOrderBook(user_id)
        return _paper_books[user_id]


def clear_paper_book(user_id: int):
    with _books_lock:
        _paper_books.pop(user_id, None)
