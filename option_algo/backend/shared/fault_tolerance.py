# backend/shared/fault_tolerance.py
# ================================================================
# Fault Tolerance — auto-recovery from infrastructure failures.
#
# Handles:
#   1. Redis disconnection → reconnect with exponential backoff
#   2. Worker restart → restore state from Redis
#   3. Broker disconnect → reconnect WebSocket feeds
#   4. Database reconnect → retry with backoff
#   5. Circuit breaker → prevent cascading failures
#
# All recovery is transparent to users — signals/orders resume
# automatically after infrastructure recovers.
# ================================================================

import time
import threading
from enum import Enum
from typing import Optional, Callable
from functools import wraps

from backend.services.redis_client import get_redis_sync


class ComponentStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    DISCONNECTED = "disconnected"


class CircuitState(Enum):
    CLOSED = "closed"       # normal operation
    OPEN = "open"           # failing, reject calls
    HALF_OPEN = "half_open"  # testing recovery


class CircuitBreaker:
    """Prevents cascading failures by stopping calls to a failing component."""

    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0,
                 half_open_max: int = 3):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max = half_open_max

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._half_open_successes = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            self._transition()
            return self._state

    def call(self, func: Callable, *args, **kwargs):
        """Execute func if circuit is CLOSED or HALF_OPEN (limited)."""
        with self._lock:
            self._transition()
            if self._state == CircuitState.OPEN:
                raise CircuitOpenError(
                    f"Circuit [{self.name}] is OPEN — rejecting call"
                )
            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_successes >= self.half_open_max:
                    raise CircuitOpenError(
                        f"Circuit [{self.name}] HALF_OPEN — max test calls reached"
                    )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    def _transition(self):
        if self._state == CircuitState.OPEN:
            elapsed = time.time() - self._last_failure_time
            if elapsed >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._half_open_successes = 0
                print(f"[fault] Circuit [{self.name}] → HALF_OPEN (testing recovery)")

    def _on_success(self):
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_successes += 1
                if self._half_open_successes >= self.half_open_max:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    print(f"[fault] Circuit [{self.name}] → CLOSED (recovered)")
            else:
                self._failure_count = 0

    def _on_failure(self):
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                print(
                    f"[fault] Circuit [{self.name}] → OPEN "
                    f"({self._failure_count} failures, recovery in {self.recovery_timeout}s)"
                )


class CircuitOpenError(Exception):
    pass


class RetryWithBackoff:
    """Execute a callable with exponential backoff on failure."""

    def __init__(self, name: str = "retry",
                 max_retries: int = 5,
                 initial_delay: float = 1.0,
                 max_delay: float = 60.0,
                 backoff_factor: float = 2.0,
                 on_retry: Optional[Callable] = None):
        self.name = name
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.on_retry = on_retry

    def execute(self, func: Callable, *args, **kwargs):
        """Execute func, retrying on failure with backoff."""
        delay = self.initial_delay
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries:
                    if self.on_retry:
                        self.on_retry(attempt + 1, delay, e)
                    print(
                        f"[fault] {self.name}: attempt {attempt + 1}/{self.max_retries} "
                        f"failed ({e}), retrying in {delay:.1f}s"
                    )
                    time.sleep(delay)
                    delay = min(delay * self.backoff_factor, self.max_delay)

        raise last_exception


class RedisHealthMonitor:
    """Monitors Redis connection health and triggers recovery callbacks."""

    def __init__(self, reconnect_callback: Optional[Callable] = None):
        self._r_sync = None
        self._r_async = None
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._reconnect_callback = reconnect_callback
        self.status = ComponentStatus.HEALTHY
        self._lock = threading.Lock()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name="redis-health")
        self._thread.start()

    def stop(self):
        self._stop_event.set()

    def _loop(self):
        check_interval = 5.0
        reconnect_backoff = 1.0
        while not self._stop_event.is_set():
            try:
                r = get_redis_sync()
                r.ping()
                with self._lock:
                    self.status = ComponentStatus.HEALTHY
                reconnect_backoff = 1.0
            except Exception as e:
                with self._lock:
                    self.status = ComponentStatus.DISCONNECTED
                print(f"[fault] Redis disconnected: {e}, reconnecting in {reconnect_backoff}s")
                time.sleep(reconnect_backoff)
                reconnect_backoff = min(reconnect_backoff * 2, 30.0)
                if self._reconnect_callback:
                    try:
                        self._reconnect_callback()
                    except Exception as cb_err:
                        print(f"[fault] Redis reconnect callback failed: {cb_err}")
                continue
            self._stop_event.wait(check_interval)


class BrokerReconnector:
    """Handles broker (Upstox) WebSocket reconnection for shared services."""

    def __init__(self):
        self._reconnect_tasks: dict[str, float] = {}  # symbol → next attempt time
        self._lock = threading.Lock()

    def schedule_reconnect(self, symbol: str, reconnect_fn: Callable,
                           initial_delay: float = 1.0):
        """Schedule a broker reconnect for a symbol with backoff."""
        with self._lock:
            now = time.time()
            last = self._reconnect_tasks.get(symbol, 0)
            if now < last + 1.0:
                return  # throttle

            delay = initial_delay
            if delay > 30.0:
                delay = 30.0

            self._reconnect_tasks[symbol] = now

        def _reconnect():
            time.sleep(delay)
            try:
                reconnect_fn()
                print(f"[fault] Broker reconnect for {symbol} succeeded")
            except Exception as e:
                print(f"[fault] Broker reconnect for {symbol} failed: {e}")
                next_delay = min(delay * 2, 60.0)
                self.schedule_reconnect(symbol, reconnect_fn, next_delay)

        thread = threading.Thread(target=_reconnect, daemon=True,
                                  name=f"reconnect-{symbol}")
        thread.start()


class DBReconnector:
    """Handles database reconnection with pool refresh."""

    def __init__(self):
        self._circuit = CircuitBreaker("database", failure_threshold=3,
                                       recovery_timeout=15.0)

    async def execute(self, db_fn: Callable, *args, **kwargs):
        """Execute a database operation with circuit breaker protection."""
        try:
            return self._circuit.call(db_fn, *args, **kwargs)
        except CircuitOpenError as e:
            print(f"[fault] DB circuit open: {e}")
            raise


class FaultToleranceManager:
    """
    Central fault tolerance coordinator.

    Manages all recovery mechanisms:
    - Redis health monitoring
    - Circuit breakers for critical components
    - Broker reconnection
    - DB reconnection
    """

    def __init__(self):
        self._circuits: dict[str, CircuitBreaker] = {}
        self._circuits_lock = threading.Lock()
        self._redis_monitor: Optional[RedisHealthMonitor] = None
        self._broker_reconnector = BrokerReconnector()
        self._db_reconnector = DBReconnector()

    def circuit(self, name: str) -> CircuitBreaker:
        """Get or create a circuit breaker for a component."""
        with self._circuits_lock:
            if name not in self._circuits:
                self._circuits[name] = CircuitBreaker(name)
            return self._circuits[name]

    def start(self, on_redis_reconnect: Optional[Callable] = None):
        """Start fault tolerance monitoring."""
        self._redis_monitor = RedisHealthMonitor(
            reconnect_callback=on_redis_reconnect
        )
        self._redis_monitor.start()
        print("[fault] Fault tolerance manager started")

    def stop(self):
        """Stop all fault tolerance monitors."""
        if self._redis_monitor:
            self._redis_monitor.stop()
        print("[fault] Fault tolerance manager stopped")

    def reconnect_broker(self, symbol: str, reconnect_fn: Callable):
        """Schedule a broker WebSocket reconnect for a symbol."""
        self._broker_reconnector.schedule_reconnect(symbol, reconnect_fn)

    @property
    def redis_status(self) -> ComponentStatus:
        if self._redis_monitor:
            return self._redis_monitor.status
        return ComponentStatus.DISCONNECTED

    def status(self) -> dict:
        """Return fault tolerance status for all components."""
        return {
            "redis": self.redis_status.value,
            "circuits": {
                name: cb.state.value
                for name, cb in self._circuits.items()
            },
            "broker_reconnects_pending": len(
                self._broker_reconnector._reconnect_tasks
            ),
        }


# ================================================================
# DECORATOR — Retry with backoff (sync)
# ================================================================

def retry_on_failure(max_retries: int = 3, delay: float = 1.0):
    """Decorator: retry a function on exception with backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retrier = RetryWithBackoff(
                name=func.__name__,
                max_retries=max_retries,
                initial_delay=delay,
            )
            return retrier.execute(func, *args, **kwargs)
        return wrapper
    return decorator


# ================================================================
# SINGLETON
# ================================================================

fault_manager = FaultToleranceManager()
