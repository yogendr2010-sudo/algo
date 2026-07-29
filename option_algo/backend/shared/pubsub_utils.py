# backend/shared/pubsub_utils.py
# ================================================================
# Resilient Redis Pub/Sub consumer with auto-reconnect.
#
# Wraps the common subscribe → get_message loop pattern used by all
# shared engines. Automatically detects Redis disconnection and
# reconnects, restoring all subscriptions transparently.
# ================================================================

import time
import threading
from datetime import datetime
from typing import Callable, Optional

from backend.services.redis_client import get_redis_sync

RECONNECT_DELAY_INITIAL = 0.5
RECONNECT_DELAY_MAX = 10.0


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def resilient_pubsub_consumer(
    tag: str,
    channels: list[str],
    handler: Callable[[dict], None],
    stop_event: threading.Event,
    poll_timeout: float = 1.0,
    idle_sleep: float = 0.05,
):
    """
    Subscribe to one or more Redis Pub/Sub channels and call `handler(data_dict)`
    for every message received. Automatically reconnects on Redis disconnection.

    Parameters:
        tag:         Log prefix (e.g. "candle:NIFTY").
        channels:    List of channel names to subscribe to.
        handler:     Callback receiving the parsed message dict.
        stop_event:  Set to signal graceful shutdown.
        poll_timeout: Seconds to block waiting for a message.
        idle_sleep:  Seconds to sleep between empty poll iterations.
    """
    reconnect_delay = RECONNECT_DELAY_INITIAL

    while not stop_event.is_set():
        pubsub = None
        try:
            r = get_redis_sync()
            pubsub = r.pubsub()
            pubsub.subscribe(*channels)

            if reconnect_delay > RECONNECT_DELAY_INITIAL:
                print(f"{_now()} [{tag}] PubSub reconnected after {reconnect_delay:.1f}s delay")
            reconnect_delay = RECONNECT_DELAY_INITIAL

            while not stop_event.is_set():
                try:
                    msg = pubsub.get_message(timeout=poll_timeout)
                except Exception:
                    print(f"{_now()} [{tag}] PubSub connection lost — reconnecting...")
                    break

                if msg and msg.get("type") == "message":
                    try:
                        data = msg.get("data")
                        if isinstance(data, (bytes, str)):
                            import json
                            data = json.loads(data)
                        handler(data)
                    except Exception as e:
                        print(f"{_now()} [{tag}] handler err: {e}")
                else:
                    stop_event.wait(idle_sleep)

        except Exception as e:
            print(f"{_now()} [{tag}] PubSub error: {e}")

        finally:
            if pubsub:
                try:
                    pubsub.close()
                except Exception:
                    pass

        if not stop_event.is_set():
            print(f"{_now()} [{tag}] PubSub reconnecting in {reconnect_delay:.1f}s...")
            stop_event.wait(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_DELAY_MAX)
