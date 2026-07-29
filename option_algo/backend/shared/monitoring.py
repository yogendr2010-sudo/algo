# backend/shared/monitoring.py
# ================================================================
# Monitoring & Observability for the shared architecture.
#
# Tracks:
#   - System metrics (CPU, RAM, Redis memory, latency)
#   - Application metrics (active users, symbols, WebSockets)
#   - Pipeline metrics (signal latency, queue sizes)
#   - Health checks (Redis, broker, workers)
#   - Alert conditions (threshold breaches)
#
# All metrics are stored in Redis with TTLs and can be queried
# by the admin dashboard or external monitoring systems.
# ================================================================

import os
import time
import json
import threading
from datetime import datetime
from collections import deque
from typing import Optional

from backend.services.redis_client import get_redis_sync
from backend.shared.redis_infra import (
    metrics_key, worker_health_key, worker_heartbeat,
    active_symbols_key, HEARTBEAT_TTL_SEC,
)


def _now() -> str:
    return datetime.utcnow().isoformat()


def _now_ts() -> float:
    return time.time()


class MetricsCollector:
    """Collects and publishes system + application metrics."""

    COLLECT_INTERVAL = 5.0  # seconds between metric collection
    METRIC_TTL = 30          # TTL for metric keys in Redis
    HISTORY_SIZE = 60        # rolling window size for time-series metrics

    def __init__(self):
        self._r = get_redis_sync()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._history: dict[str, deque] = {
            "signal_latency_ms": deque(maxlen=self.HISTORY_SIZE),
            "cpu_percent": deque(maxlen=self.HISTORY_SIZE),
            "ram_percent": deque(maxlen=self.HISTORY_SIZE),
            "redis_latency_us": deque(maxlen=self.HISTORY_SIZE),
            "redis_memory_mb": deque(maxlen=self.HISTORY_SIZE),
            "messages_per_sec": deque(maxlen=self.HISTORY_SIZE),
        }

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name="metrics-collector")
        self._thread.start()

    def stop(self):
        self._stop_event.set()

    def record_signal_latency(self, symbol: str, latency_ms: float):
        """Record end-to-end signal latency (tick → signal published)."""
        with self._lock:
            self._history["signal_latency_ms"].append(latency_ms)

    def _loop(self):
        while not self._stop_event.is_set():
            try:
                snapshot = self._collect()
                self._publish(snapshot)
                self._check_alerts(snapshot)
            except Exception as e:
                print(f"[metrics] collection error: {e}")
            self._stop_event.wait(self.COLLECT_INTERVAL)

    def _collect(self) -> dict:
        """Gather all current metrics."""
        return {
            "timestamp": _now(),
            "system": self._system_metrics(),
            "application": self._application_metrics(),
            "redis": self._redis_metrics(),
            "pipeline": self._pipeline_metrics(),
        }

    def _system_metrics(self) -> dict:
        """CPU, RAM, disk usage."""
        cpu = 0.0
        ram = 0.0
        try:
            import psutil
            cpu = psutil.cpu_percent(interval=1)
            ram = psutil.virtual_memory().percent
        except ImportError:
            cpu = os.getloadavg()[0] if hasattr(os, 'getloadavg') else 0.0
            try:
                with open('/proc/meminfo') as f:
                    lines = f.readlines()
                    total = int(lines[0].split()[1])
                    avail = int(lines[2].split()[1])
                    ram = ((total - avail) / total) * 100
            except Exception:
                pass

        self._history["cpu_percent"].append(cpu)
        self._history["ram_percent"].append(ram)
        return {
            "cpu_percent": cpu,
            "ram_percent": ram,
        }

    def _application_metrics(self) -> dict:
        """Active users, symbols, WebSocket connections, shared services."""
        r = self._r
        active_users = 0
        active_symbols = 0
        registered_workers = 0
        healthy_workers = 0

        try:
            active_users = self._sync_count_running()
        except Exception:
            pass

        try:
            members = r.smembers(active_symbols_key())
            active_symbols = len(members) if members else 0
        except Exception:
            pass

        try:
            workers = r.smembers(worker_health_key())
            registered_workers = len(workers) if workers else 0
            for w in workers:
                wid = w.decode() if isinstance(w, bytes) else w
                if r.exists(worker_heartbeat("orchestrator", wid)):
                    healthy_workers += 1
        except Exception:
            pass

        shared_services = self._shared_service_counts()

        return {
            "active_users": active_users,
            "active_symbols": active_symbols,
            "registered_workers": registered_workers,
            "healthy_workers": healthy_workers,
            "shared_services": shared_services,
        }

    def _shared_service_counts(self) -> dict:
        """Count active shared service instances."""
        counts = {}
        try:
            from backend.shared.market_data_service import SharedMarketDataService
            from backend.shared.candle_builder import SharedCandleBuilder
            from backend.shared.indicator_engine import SharedIndicatorEngine
            from backend.shared.market_structure_engine import (
                SharedMarketStructureEngine, SharedUnderlyingMarketStructureEngine,
            )
            from backend.shared.strategy_engine import SharedStrategyEngine
            from backend.shared.option_chain_service import SharedOptionChainService

            counts["market_data_ws"] = len(SharedMarketDataService._instances)
            counts["candle_builders"] = len(SharedCandleBuilder._instances)
            counts["indicator_engines"] = len(SharedIndicatorEngine._instances)
            counts["structure_engines_1m"] = len(SharedMarketStructureEngine._instances)
            counts["structure_engines_5m"] = len(SharedUnderlyingMarketStructureEngine._instances)
            counts["strategy_engines"] = len(SharedStrategyEngine._instances)
            counts["option_chain_services"] = len(SharedOptionChainService._instances)
        except Exception:
            pass

        try:
            from backend.shared.shared_cache import computation_cache
            cache_stats = dict(computation_cache.stats)
            counts["computation_cache"] = cache_stats
        except Exception:
            pass

        return counts

    def _sync_count_running(self) -> int:
        """Synchronous count of running bots from Redis."""
        r = self._r
        count = 0
        try:
            for key in r.scan_iter(match="bot:status:*"):
                raw = r.get(key)
                if raw:
                    try:
                        data = json.loads(raw)
                        if data.get("running"):
                            count += 1
                    except Exception:
                        pass
        except Exception:
            pass
        return count

    def _redis_metrics(self) -> dict:
        """Redis INFO — memory, connected clients, ops/sec."""
        try:
            info = self._r.info()
            mem = info.get("used_memory_human", "0")
            mem_bytes = info.get("used_memory", 0)
            mem_mb = round(mem_bytes / (1024 * 1024), 2) if mem_bytes else 0
            clients = info.get("connected_clients", 0)
            ops = info.get("instantaneous_ops_per_sec", 0)

            self._history["redis_memory_mb"].append(mem_mb)
            return {
                "memory_mb": mem_mb,
                "memory_human": mem,
                "connected_clients": clients,
                "ops_per_sec": ops,
            }
        except Exception:
            return {
                "memory_mb": 0,
                "memory_human": "unknown",
                "connected_clients": 0,
                "ops_per_sec": 0,
            }

    def _pipeline_metrics(self) -> dict:
        """Signal latency, queue sizes, cache hit ratio."""
        r = self._r
        signal_latency = 0.0
        with self._lock:
            latencies = list(self._history["signal_latency_ms"])
        if latencies:
            signal_latency = sum(latencies) / len(latencies)

        cmd_queue_len = 0
        try:
            cmd_queue_len = r.llen("bot:commands")
        except Exception:
            pass

        active_syms = 0
        try:
            active_syms = r.scard(active_symbols_key())
        except Exception:
            pass

        return {
            "avg_signal_latency_ms": round(signal_latency, 2),
            "command_queue_length": cmd_queue_len,
            "active_symbols": active_syms,
        }

    def _publish(self, snapshot: dict):
        """Write metrics to Redis for dashboard / external polling."""
        r = self._r
        try:
            r.setex(
                metrics_key("overview"),
                self.METRIC_TTL,
                json.dumps(snapshot, default=str),
            )
            r.setex(
                metrics_key("application"),
                self.METRIC_TTL,
                json.dumps(snapshot.get("application", {}), default=str),
            )
            r.setex(
                metrics_key("system"),
                self.METRIC_TTL,
                json.dumps(snapshot.get("system", {}), default=str),
            )
            r.setex(
                metrics_key("redis"),
                self.METRIC_TTL,
                json.dumps(snapshot.get("redis", {}), default=str),
            )
            r.setex(
                metrics_key("pipeline"),
                self.METRIC_TTL,
                json.dumps(snapshot.get("pipeline", {}), default=str),
            )
        except Exception as e:
            print(f"[metrics] publish error: {e}")

    def _check_alerts(self, snapshot: dict):
        """Check thresholds and log warnings."""
        app = snapshot.get("application", {})
        sys_m = snapshot.get("system", {})
        redis_m = snapshot.get("redis", {})
        pipe = snapshot.get("pipeline", {})

        warnings = []

        if app.get("healthy_workers", 1) == 0:
            warnings.append("NO_HEALTHY_WORKERS")

        if sys_m.get("cpu_percent", 0) > 90:
            warnings.append(f"CPU_HIGH: {sys_m['cpu_percent']:.0f}%")

        if sys_m.get("ram_percent", 0) > 90:
            warnings.append(f"RAM_HIGH: {sys_m['ram_percent']:.0f}%")

        if redis_m.get("memory_mb", 0) > 2048:
            warnings.append(f"REDIS_MEM_HIGH: {redis_m['memory_mb']}MB")

        if pipe.get("avg_signal_latency_ms", 0) > 5000:
            warnings.append(f"SIGNAL_LATENCY_HIGH: {pipe['avg_signal_latency_ms']}ms")

        if pipe.get("command_queue_length", 0) > 100:
            warnings.append(f"CMD_QUEUE_BACKLOG: {pipe['command_queue_length']}")

        if warnings:
            print(f"[metrics:alert] {_now()} | {' | '.join(warnings)}")


class HealthChecker:
    """Standalone health check — verifies critical systems are operational."""

    def __init__(self):
        self._r = get_redis_sync()

    def check_all(self) -> dict:
        """Run all health checks, return {component: ok/error/detail}."""
        return {
            "redis": self._check_redis(),
            "workers": self._check_workers(),
            "market_data": self._check_market_data(),
            "symbols": self._check_active_symbols(),
        }

    def _check_redis(self) -> dict:
        try:
            self._r.ping()
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "detail": str(e)}

    def _check_workers(self) -> dict:
        try:
            workers = self._r.smembers(worker_health_key())
            healthy = 0
            dead = []
            for w in workers:
                wid = w.decode() if isinstance(w, bytes) else w
                if self._r.exists(worker_heartbeat("orchestrator", wid)):
                    healthy += 1
                else:
                    dead.append(wid)
            return {
                "status": "ok" if healthy > 0 else "error",
                "total": len(workers),
                "healthy": healthy,
                "dead": dead,
            }
        except Exception as e:
            return {"status": "error", "detail": str(e)}

    def _check_market_data(self) -> dict:
        """Check if market data is flowing for active symbols."""
        try:
            symbols = self._r.smembers(active_symbols_key())
            active = []
            dead = []
            for sym in symbols:
                sym_str = sym.decode() if isinstance(sym, bytes) else sym
                tick_key = f"shared:tick_buffer:{sym_str}"
                if self._r.exists(tick_key):
                    active.append(sym_str)
                else:
                    dead.append(sym_str)
            return {
                "status": "ok" if active else "degraded",
                "active_tick_feeds": len(active),
                "missing_feeds": dead,
            }
        except Exception as e:
            return {"status": "error", "detail": str(e)}

    def _check_active_symbols(self) -> dict:
        try:
            symbols = self._r.smembers(active_symbols_key())
            sym_list = [
                s.decode() if isinstance(s, bytes) else s
                for s in (symbols or [])
            ]
            return {
                "status": "ok",
                "count": len(sym_list),
                "symbols": sym_list,
            }
        except Exception as e:
            return {"status": "error", "detail": str(e)}


# ================================================================
# SINGLETONS
# ================================================================

metrics_collector = MetricsCollector()
health_checker = HealthChecker()
