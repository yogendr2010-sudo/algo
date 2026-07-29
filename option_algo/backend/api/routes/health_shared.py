# backend/api/routes/health_shared.py
# ================================================================
# Shared-architecture health endpoint.
#
#   GET /health/worker  — worker status, service instances, cache stats
#
# All imports use try/except to avoid breaking if shared modules
# are not installed (e.g. pandas, or a minimal web-only environment).
# When imports fail the endpoint returns {"shared_mode": false}.
# ================================================================

import time
import json
from typing import Optional

from fastapi import APIRouter
from sqlalchemy import text

from backend.db.database import AsyncSessionLocal
from backend.services.redis_client import ping as redis_ping

router = APIRouter(tags=["health"])

_START_TIME = time.time()


# ────────────────────────────────────────────────────────────────
# Graceful import helpers
# ────────────────────────────────────────────────────────────────

_shared_available: bool = False
_mod_user_registry = None
_mod_symbol_manager = None
_mod_ref_counter = None

try:
    from backend.shared.user_execution_manager import user_registry as _mod_user_registry  # noqa: F811
    _shared_available = True or True  # keep going
except Exception:
    pass

try:
    from backend.shared import symbol_manager as _mod_symbol_manager
except Exception:
    pass

try:
    from backend.shared import ref_counter as _mod_ref_counter
except Exception:
    pass


def _safe_service_instance_map() -> dict[str, int]:
    """Try to read per-class _instances dict lengths safely."""
    svc_map: dict[str, int] = {}
    _class_queries = [
        ("market_data",        "backend.shared.market_data_service",        "SharedMarketDataService"),
        ("candle_builder",      "backend.shared.candle_builder",              "SharedCandleBuilder"),
        ("indicator_engine",    "backend.shared.indicator_engine",            "SharedIndicatorEngine"),
        ("market_structure_1m", "backend.shared.market_structure_engine",     "SharedMarketStructureEngine"),
        ("market_structure_5m", "backend.shared.market_structure_engine",     "SharedUnderlyingMarketStructureEngine"),
        ("strategy_engine",     "backend.shared.strategy_engine",             "SharedStrategyEngine"),
        ("option_chain",        "backend.shared.option_chain_service",        "SharedOptionChainService"),
    ]

    for key, mod_name, cls_name in _class_queries:
        try:
            mod = __import__(mod_name, fromlist=[cls_name])
            cls = getattr(mod, cls_name, None)
            if cls is not None:
                instances = getattr(cls, "_instances", {})
                svc_map[key] = len(instances)
        except Exception:
            svc_map[key] = -1  # unavailable

    # user_execution count — try registry
    try:
        if _mod_user_registry is not None:
            svc_map["user_execution"] = len(_mod_user_registry.running_users())
        else:
            svc_map["user_execution"] = -1
    except Exception:
        svc_map["user_execution"] = -1

    return svc_map


def _safe_active_symbols() -> list[str]:
    if _mod_symbol_manager is not None:
        try:
            raw = _mod_symbol_manager.get_active_symbols()
            if isinstance(raw, set):
                return sorted([s.decode() if isinstance(s, bytes) else str(s) for s in raw])
        except Exception:
            pass
    # fallback via Redis directly
    try:
        from backend.services.redis_client import get_redis_sync
        from backend.shared.redis_infra import active_symbols_key
        r = get_redis_sync()
        members = r.smembers(active_symbols_key())
        return sorted([m.decode() if isinstance(m, bytes) else str(m) for m in (members or [])])
    except Exception:
        return []


def _safe_active_users() -> int:
    try:
        from backend.services import state_store
        count = state_store.count_running_bots_sync() if hasattr(state_store, "count_running_bots_sync") else None  # noqa
        if count is not None:
            return count
    except Exception:
        pass

    # fallback: scan Redis directly
    try:
        from backend.services.redis_client import get_redis_sync
        r = get_redis_sync()
        c = 0
        for key in r.scan_iter(match="bot:status:*"):
            raw = r.get(key)
            if raw:
                try:
                    if json.loads(raw).get("running"):
                        c += 1
                except Exception:
                    pass
        return c
    except Exception:
        return 0


def _safe_worker_meta() -> tuple[Optional[str], int]:
    """Return (worker_id, uptime_seconds)."""
    worker_id: Optional[str] = None
    uptime = int(time.time() - _START_TIME)

    try:
        from backend.services.redis_client import get_redis_sync
        r = get_redis_sync()
        # Try to find a healthy orchestrator worker
        try:
            from backend.shared.redis_infra import worker_health_key, worker_heartbeat
            workers = r.smembers(worker_health_key())
            for w in (workers or []):
                wid = w.decode() if isinstance(w, bytes) else w
                if r.exists(worker_heartbeat("orchestrator", wid)):
                    worker_id = wid
                    break
        except Exception:
            pass
    except Exception:
        pass

    return worker_id, uptime


def _safe_computation_cache_stats() -> Optional[dict]:
    try:
        from backend.shared.shared_cache import computation_cache
        st = dict(computation_cache.stats)
        total = sum(st.values())
        return {
            "entry_counts": st,
            "total_entries": total,
        }
    except Exception:
        return None


# ────────────────────────────────────────────────────────────────
# Broker status — check if tick data is flowing into Redis
# ────────────────────────────────────────────────────────────────

async def _broker_status(active_symbols: list[str]) -> dict:
    """Check whether broker feeds are delivering ticks."""
    if not active_symbols:
        return {"status": "unknown", "detail": "no active symbols"}

    try:
        from backend.services.redis_client import get_redis
        r = get_redis()
        active_feeds = 0
        dead_feeds: list[str] = []
        for sym in active_symbols:
            tick_key = f"shared:tick_buffer:{sym.upper()}"
            if await r.exists(tick_key):
                active_feeds += 1
            else:
                dead_feeds.append(sym)
        status = "ok" if active_feeds > 0 else "degraded"
        return {
            "status": status,
            "active_feeds": active_feeds,
            "missing_feeds": dead_feeds,
        }
    except Exception:
        return {"status": "error", "detail": "Redis unavailable"}


# ────────────────────────────────────────────────────────────────
# Route
# ────────────────────────────────────────────────────────────────

@router.get("/health/worker")
async def health_worker():
    """
    Shared-architecture worker health endpoint.

    When shared modules are available returns comprehensive
    worker + service health.  Otherwise gracefully degrades to
    ``{"shared_mode": false}``.
    """
    if not _shared_available:
        return {"shared_mode": False}

    worker_id, uptime = _safe_worker_meta()

    active_users = _safe_active_users()
    active_symbols = _safe_active_symbols()
    broker = await _broker_status(active_symbols)

    # DB
    db_status = "ok"
    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text("SELECT 1"))
    except Exception:
        db_status = "error"

    # Redis
    try:
        r_ok = await redis_ping()
        redis_status = "ok" if r_ok else "error"
    except Exception:
        redis_status = "error"

    # Service instance counts
    service_instances = _safe_service_instance_map()

    # Computation cache stats
    cache_stats = _safe_computation_cache_stats()

    return {
        "shared_mode": True,
        "worker_id": worker_id,
        "uptime_seconds": uptime,
        "active_users": active_users,
        "active_symbols": active_symbols,
        "redis_status": redis_status,
        "db_status": db_status,
        "broker_status": broker,
        "service_instances": service_instances,
        "computation_cache": cache_stats,
    }
