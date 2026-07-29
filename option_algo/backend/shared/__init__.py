# backend/shared/__init__.py
# ================================================================
# Shared Infrastructure Package
# ================================================================
# This package contains all the shared (non-user-specific) components
# that replace the per-user duplication in the old architecture.
#
# Components:
#   redis_infra              — Redis keys, channels, streams, helper functions
#   ref_counter              — Reference counting for shared resource lifecycle
#   dist_locks               — Redis distributed locks for safe concurrent access
#   shared_cache             — Shared in-memory cache for instrument master, metadata
#   symbol_manager           — Dynamic symbol subscription manager
#   market_data_service      — Shared Upstox WebSocket feeds (one per symbol)
#   candle_builder           — Shared candle builder (1m + 5m aggregation)
#   indicator_engine         — Shared indicator calculations (EMA, RSI, VWAP, etc.)
#   market_structure_engine  — Shared market structure analysis (1m + 5m)
#   option_chain_service     — Shared option chain analysis (per symbol/expiry)
#   strategy_engine          — Shared strategy signal generation
#   user_execution_manager   — Per-user isolated execution context
#   websocket_gateway        — Enhanced WebSocket gateway with delta-only updates
#   shared_worker            — SharedWorkerOrchestrator (replaces per-user BotThread)
#   monitoring               — Metrics collection, health checks, alerting
#   fault_tolerance          — Auto-recovery from Redis/broker/DB failures
#   stress_test              — Validation utilities for shared architecture
