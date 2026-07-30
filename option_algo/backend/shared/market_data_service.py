# backend/shared/market_data_service.py
# ================================================================
# Shared Market Data Service — ONE broker WebSocket per symbol.
#
# Responsibilities:
#   - Maintain ONE Upstox WebSocket connection per active symbol
#   - Receive live ticks from Upstox feed
#   - Validate and normalize market data
#   - Publish ticks to Redis Stream (tick buffer)
#   - Publish ticks to Redis Pub/Sub (real-time)
#   - Maintain latest tick snapshot in Redis Hash
#   - Auto-reconnect on disconnect
#   - Lifecycle managed via symbol_manager reference counts
#
# This replaces the per-user _start_streamer() in SymbolEngine.
# ================================================================

import json
import threading
import time
from datetime import datetime
from typing import Optional

import upstox_client

from backend.shared.redis_infra import (
    shared_tick_stream,
    shared_tick_buffer,
    shared_tick_channel,
    TICK_STREAM_MAXLEN,
)
from backend.shared.symbol_manager import get_subscriber_count, is_symbol_active
from backend.shared.shared_cache import get_streamer_token
from backend.services.redis_client import get_redis_sync


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class SharedMarketDataService:
    """
    Per-symbol shared market data feed. Only ONE instance exists per
    active symbol, regardless of how many users trade that symbol.

    Usage:
        svc = SharedMarketDataService("NIFTY", access_token)
        svc.start()
        ...
        svc.stop()
    """

    def __init__(self, symbol: str, access_token: str):
        self.symbol = symbol.upper()
        self.access_token = access_token
        print(f"{_now()} [shared_md:{self.symbol}] Token received: {access_token[:20]}...{access_token[-10:] if len(access_token) > 30 else '***'}")
        self._stop_event = threading.Event()
        self._streamer: Optional[object] = None
        self._streamer_lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._connected = threading.Event()

        # Get the streamer token from cache/admin config
        self._token = get_streamer_token(self.symbol)
        print(f"{_now()} [shared_md:{self.symbol}] Instrument token: {self._token}")

        # Track additional token subscriptions (option instruments)
        self._additional_tokens: set[str] = set()
        self._tokens_lock = threading.Lock()

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    def start(self):
        """Start the WebSocket connection in a background daemon thread."""
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._run, daemon=True,
            name=f"shared-md-{self.symbol}",
        )
        self._thread.start()
        print(f"{_now()} [shared_md:{self.symbol}] Started")

    def stop(self):
        """Stop the WebSocket connection."""
        self._stop_event.set()
        with self._streamer_lock:
            streamer = self._streamer
            self._streamer = None
        if streamer:
            try:
                streamer.disconnect()
            except Exception:
                pass
        print(f"{_now()} [shared_md:{self.symbol}] Stopped")

    def subscribe_option(self, instrument_key: str):
        """Add an option instrument key to the stream subscription."""
        with self._tokens_lock:
            if instrument_key not in self._additional_tokens:
                self._additional_tokens.add(instrument_key)
                with self._streamer_lock:
                    streamer = self._streamer
                if streamer:
                    try:
                        streamer.subscribe([instrument_key], "full")
                    except Exception as e:
                        print(f"{_now()} [shared_md:{self.symbol}] sub opt err: {e}")

    def unsubscribe_option(self, instrument_key: str):
        """Remove an option instrument key from the stream subscription."""
        with self._tokens_lock:
            if instrument_key in self._additional_tokens:
                self._additional_tokens.discard(instrument_key)
                with self._streamer_lock:
                    streamer = self._streamer
                if streamer:
                    try:
                        streamer.unsubscribe([instrument_key])
                    except Exception as e:
                        print(f"{_now()} [shared_md:{self.symbol}] unsub opt err: {e}")

    def get_subscribed_tokens(self) -> list[str]:
        """Get all currently subscribed tokens."""
        tokens = [self._token]
        with self._tokens_lock:
            tokens.extend(sorted(self._additional_tokens))
        return tokens

    def _run(self):
        """Main WebSocket loop with auto-reconnect."""
        while not self._stop_event.is_set():
            try:
                self._connect_and_stream()
            except Exception as e:
                print(f"{_now()} [shared_md:{self.symbol}] Stream error: {e}")
            finally:
                with self._streamer_lock:
                    self._streamer = None

            if not self._stop_event.is_set():
                print(f"{_now()} [shared_md:{self.symbol}] Reconnecting in 3s...")
                time.sleep(3)

    def _connect_and_stream(self):
        """Connect to Upstox WebSocket and process messages."""
        cfg = upstox_client.Configuration()
        cfg.access_token = self.access_token
        print(f"{_now()} [shared_md:{self.symbol}] Connecting with token: {self.access_token[:20]}...{self.access_token[-10:] if len(self.access_token) > 30 else '***'}")
        streamer = upstox_client.MarketDataStreamerV3(upstox_client.ApiClient(cfg))
        with self._streamer_lock:
            self._streamer = streamer

        def on_open():
            self._connected.set()
            print(f"{_now()} [shared_md:{self.symbol}] Stream open")
            tokens = self.get_subscribed_tokens()
            try:
                streamer.subscribe(tokens, "full")
                print(f"{_now()} [shared_md:{self.symbol}] Subscribed: {tokens}")
            except Exception as e:
                print(f"{_now()} [shared_md:{self.symbol}] Subscribe err: {e}")

        def on_message(msg):
            try:
                self._process_message(msg)
            except Exception as e:
                print(f"{_now()} [shared_md:{self.symbol}] msg err: {e}")

        def on_error(e):
            print(f"{_now()} [shared_md:{self.symbol}] Stream err: {e}")

        def on_close(code, reason):
            self._connected.clear()
            print(f"{_now()} [shared_md:{self.symbol}] Stream closed: {code}")

        streamer.on("open", on_open)
        streamer.on("message", on_message)
        streamer.on("error", on_error)
        streamer.on("close", on_close)

        streamer.connect()

    def _process_message(self, msg: dict):
        """
        Process an Upstox WebSocket message.
        Extract LTP/volume for all subscribed tokens and publish to Redis.
        """
        r = get_redis_sync()
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:23]
        timestamp = now.timestamp()

        if not isinstance(msg, dict):
            return

        feeds = msg.get("feeds", {})
        if not feeds:
            return

        has_data = False

        for token, feed in feeds.items():
            full = feed.get("fullFeed", {})
            if not isinstance(full, dict):
                continue

            ltp = None
            ltq = 0.0

            if "marketFF" in full and full["marketFF"].get("ltpc"):
                ltpc = full["marketFF"]["ltpc"]
                ltp = float(ltpc.get("ltp", 0))
                ltq_val = ltpc.get("ltq")
                if ltq_val is not None:
                    ltq = float(ltq_val)
            elif "indexFF" in full and full["indexFF"].get("ltpc"):
                ltp = float(full["indexFF"]["ltpc"]["ltp"])
            elif full.get("ltp"):
                ltp = float(full["ltp"])

            if ltp is None or ltp <= 0:
                continue

            has_data = True

            # Build tick data
            tick = {
                "symbol": self.symbol,
                "token": token,
                "ltp": ltp,
                "ltq": ltq,
                "ts": now_str,
                "timestamp": timestamp,
            }

            # 1. Store latest tick snapshot in Redis Hash
            r.hset(shared_tick_buffer(self.symbol), key=token, value=json.dumps(tick, default=str))

            # 2. Publish to Redis Stream (tick buffer for replay)
            try:
                stream_key = shared_tick_stream(self.symbol)
                r.xadd(stream_key, tick, maxlen=TICK_STREAM_MAXLEN)
            except Exception:
                pass  # Redis Streams not supported (Redis < 5.0)

            # 3. Publish to Redis Pub/Sub (real-time)
            r.publish(shared_tick_channel(self.symbol), json.dumps(tick, default=str))

        if not has_data:
            return

    # ================================================================
    # Static factory — manages per-symbol service instances
    # ================================================================

    _instances: dict[str, "SharedMarketDataService"] = {}
    _instances_lock = threading.Lock()

    @classmethod
    def get_or_create(cls, symbol: str, access_token: str) -> "SharedMarketDataService":
        """
        Get or create a SharedMarketDataService for a symbol.
        Multiple callers for the same symbol get the SAME instance.
        """
        sym = symbol.upper()
        with cls._instances_lock:
            if sym not in cls._instances:
                svc = cls(sym, access_token)
                cls._instances[sym] = svc
                svc.start()
            return cls._instances[sym]

    @classmethod
    def stop_symbol(cls, symbol: str):
        """Stop and remove the service for a symbol."""
        sym = symbol.upper()
        with cls._instances_lock:
            svc = cls._instances.pop(sym, None)
            if svc:
                svc.stop()

    @classmethod
    def stop_all(cls):
        """Stop all running services."""
        with cls._instances_lock:
            for svc in list(cls._instances.values()):
                svc.stop()
            cls._instances.clear()

    @classmethod
    def active_symbols(cls) -> list[str]:
        """Get list of symbols with active services."""
        with cls._instances_lock:
            return list(cls._instances.keys())


# ================================================================
# TICK READER — for downstream consumers
# ================================================================

def read_latest_tick(symbol: str, token: str) -> Optional[dict]:
    """Read the latest tick snapshot for a token from Redis Hash."""
    raw = get_redis_sync().hget(shared_tick_buffer(symbol), token)
    if raw:
        return json.loads(raw)
    return None


def read_all_latest_ticks(symbol: str) -> dict[str, dict]:
    """Read all latest tick snapshots for a symbol."""
    raw = get_redis_sync().hgetall(shared_tick_buffer(symbol))
    return {k: json.loads(v) for k, v in raw.items()}
