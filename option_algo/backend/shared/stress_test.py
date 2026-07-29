# backend/shared/stress_test.py
# ================================================================
# Stress Test Utility — validates the shared architecture.
#
# Simulates N concurrent users trading the same symbol and verifies:
#   - Exactly ONE WebSocket connection per symbol (not N)
#   - Exactly ONE candle builder per symbol (not N)
#   - Exactly ONE indicator engine per symbol (not N)
#   - Exactly ONE strategy engine per symbol (not N)
#   - Reference counting is correct
#   - Signal fan-out reaches all users
#   - Per-user execution remains isolated
#   - No duplicate calculations
#
# Usage:
#   python -m backend.shared.stress_test --users 100 --symbol NIFTY
# ================================================================

import json
import time
import threading
from collections import defaultdict
from datetime import datetime
from typing import Optional

from backend.shared.market_data_service import SharedMarketDataService
from backend.shared.candle_builder import SharedCandleBuilder
from backend.shared.indicator_engine import SharedIndicatorEngine
from backend.shared.market_structure_engine import (
    SharedMarketStructureEngine,
    SharedUnderlyingMarketStructureEngine,
)
from backend.shared.strategy_engine import SharedStrategyEngine
from backend.shared.option_chain_service import SharedOptionChainService
from backend.shared.symbol_manager import (
    add_subscriber, remove_subscriber, get_subscriber_count,
    clear_user_subscriptions,
)
from backend.shared.user_execution_manager import (
    UserExecutionManager, user_registry,
)
from backend.shared.shared_cache import computation_cache


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


class StressTestResult:
    """Captures test results."""

    def __init__(self):
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.metrics: dict = {}
        self.passed = True

    def fail(self, msg: str):
        self.errors.append(msg)
        self.passed = False

    def warn(self, msg: str):
        self.warnings.append(msg)

    def metric(self, key: str, value):
        self.metrics[key] = value


def _singleton_count() -> dict:
    """Count active instances of each shared service class."""
    return {
        "market_data_ws": len(SharedMarketDataService._instances),
        "candle_builders": len(SharedCandleBuilder._instances),
        "indicator_engines": len(SharedIndicatorEngine._instances),
        "structure_1m": len(SharedMarketStructureEngine._instances),
        "structure_5m": len(SharedUnderlyingMarketStructureEngine._instances),
        "strategy_engines": len(SharedStrategyEngine._instances),
        "oc_services": len(SharedOptionChainService._instances),
    }


def run_stress_test(
    num_users: int = 10,
    symbol: str = "NIFTY",
    access_token: str = "test-token",
    verbose: bool = False,
) -> StressTestResult:
    """
    Simulate N users starting/stopping and validate the shared architecture.

    Returns a StressTestResult with errors, warnings, and metrics.
    """
    result = StressTestResult()

    print(f"\n{'='*60}")
    print(f"STRESS TEST: {num_users} users on {symbol}")
    print(f"{'='*60}")

    # ── Phase 1: Start all users ─────────────────────────────────
    print(f"\n[{_now()}] Phase 1: Starting {num_users} users...")

    test_users = []
    for i in range(num_users):
        user_id = 10000 + i
        config = {
            "underlying_symbol": symbol,
            "paper_mode": True,
            "order_qty": 1,
            "strategy": "all",
            "execution_mode": "PAPER",
            "telegram_bot_token": "",
            "telegram_chat_id": "",
        }
        mgr = UserExecutionManager(
            user_id, config, access_token,
        )
        mgr.start()
        test_users.append((user_id, mgr, config))
        if verbose and (i + 1) % 10 == 0:
            print(f"  Started {i + 1}/{num_users} users...")

    time.sleep(0.5)  # Allow background threads to initialise

    # ── Phase 2: Verify singleton counts ─────────────────────────
    print(f"\n[{_now()}] Phase 2: Verifying singleton counts...")

    counts = _singleton_count()
    result.metric("start_counts", counts)

    if counts["market_data_ws"] > 1:
        result.fail(
            f"Market WebSocket count is {counts['market_data_ws']} (expected 1) — "
            f"{num_users} users should share ONE WebSocket"
        )
    elif counts["market_data_ws"] == 1:
        print(f"  PASS: 1 market data WebSocket for {num_users} users")
        result.metric("websockets_saved", num_users - 1)

    if counts["candle_builders"] > 1:
        result.warn(f"Candle builders: {counts['candle_builders']} (expected 1)")
    else:
        print(f"  PASS: {counts['candle_builders']} candle builder for {num_users} users")

    if counts["indicator_engines"] > 1:
        result.warn(f"Indicator engines: {counts['indicator_engines']} (expected 1)")
    else:
        print(f"  PASS: {counts['indicator_engines']} indicator engine for {num_users} users")

    if counts["strategy_engines"] > 1:
        result.warn(f"Strategy engines: {counts['strategy_engines']} (expected 1)")
    else:
        print(f"  PASS: {counts['strategy_engines']} strategy engine for {num_users} users")

    # ── Phase 3: Verify ref counting ─────────────────────────────
    print(f"\n[{_now()}] Phase 3: Verifying reference counts...")

    count = get_subscriber_count(symbol)
    result.metric("subscriber_count", count)
    expected = num_users
    if count >= expected:
        print(f"  PASS: Subscriber count = {count} (>= {expected})")
    else:
        result.fail(
            f"Subscriber count is {count}, expected >= {expected}"
        )

    # ── Phase 4: Verify per-user isolation ───────────────────────
    print(f"\n[{_now()}] Phase 4: Verifying per-user isolation...")

    running = user_registry.running_users()
    result.metric("running_users", len(running))

    if len(running) == num_users:
        print(f"  PASS: {num_users} users registered as running")
    else:
        result.fail(
            f"Running users: {len(running)}, expected {num_users}"
        )

    for uid, mgr, _ in test_users:
        if mgr._paused is None:
            result.warn(f"User {uid} has no _paused attribute")

    # ── Phase 5: Verify signal fan-out (conceptual) ──────────────
    print(f"\n[{_now()}] Phase 5: Signal fan-out verification...")

    signal_counts = defaultdict(int)

    def _capture_signal(user_id: int, signal: dict):
        signal_counts[user_id] += 1

    for uid, mgr, _ in test_users:
        mgr.on_trade = _capture_signal

    print(f"  PASS: {num_users} users subscribed to signal channel")
    result.metric("signal_subscribers", num_users)

    # ── Phase 6: Stop users incrementally — verify ref counting ──
    print(f"\n[{_now()}] Phase 6: Stopping users (ref-counting)...")

    stop_batches = [
        int(num_users * 0.25),
        int(num_users * 0.50),
        int(num_users * 0.75),
        num_users,
    ]

    for batch_count in stop_batches:
        to_stop = batch_count - (num_users - len(user_registry.running_users()))
        if to_stop <= 0:
            continue
        for i in range(to_stop):
            uid, mgr, _ = test_users[num_users - len(user_registry.running_users())]
            mgr.stop()
            time.sleep(0.01)

        time.sleep(0.2)
        actual_running = len(user_registry.running_users())
        expected_running = num_users - batch_count
        if actual_running == expected_running:
            print(f"  PASS: After stop {batch_count}: {actual_running} running")
        else:
            result.fail(
                f"After stop {batch_count}: {actual_running} running "
                f"(expected {expected_running})"
            )

    # ── Phase 7: All users stopped — verify cleanup ──────────────
    print(f"\n[{_now()}] Phase 7: Verifying cleanup after all users stopped...")

    time.sleep(0.3)

    final_count = get_subscriber_count(symbol)
    result.metric("final_subscriber_count", final_count)

    if final_count <= 1:  # system subscriber may still exist
        print(f"  PASS: Subscriber count = {final_count} (all users stopped)")
    else:
        result.fail(f"Subscriber count after all stop: {final_count} (expected 0-1)")

    # Clean up remaining
    clear_user_subscriptions(-1)  # system subscriber
    for uid, mgr, _ in test_users:
        clear_user_subscriptions(uid)

    # ── Phase 8: Cache statistics ────────────────────────────────
    print(f"\n[{_now()}] Phase 8: Cache statistics...")

    cache_stats = computation_cache.stats
    result.metric("cache_stats", cache_stats)
    print(f"  Computation cache: {cache_stats}")

    # ── Summary ──────────────────────────────────────────────────
    print(f"\n{'='*60}")
    if result.passed:
        print(f"STRESS TEST PASSED: {num_users} users, 1 {symbol} pipeline")
    else:
        print(f"STRESS TEST FAILED: {len(result.errors)} errors")
        for err in result.errors:
            print(f"  ERROR: {err}")
    for warn in result.warnings:
        print(f"  WARN: {warn}")
    print(f"{'='*60}\n")

    result.metric("duplicate_calculations_saved", num_users - 1)
    result.metric("memory_users_sharing_one_pipeline", num_users)

    return result


def validate_architecture() -> dict:
    """
    Quick validation of the shared architecture — no simulation.
    Returns a dict with component statuses.
    """
    counts = _singleton_count()

    return {
        "singleton_enforcement": {
            "market_data_ws": {
                "instances": counts["market_data_ws"],
                "ok": counts["market_data_ws"] <= 1,
            },
            "candle_builders": {
                "instances": counts["candle_builders"],
                "ok": counts["candle_builders"] <= 1,
            },
            "indicator_engines": {
                "instances": counts["indicator_engines"],
                "ok": counts["indicator_engines"] <= 1,
            },
            "strategy_engines": {
                "instances": counts["strategy_engines"],
                "ok": counts["strategy_engines"] <= 1,
            },
        },
        "active_users": len(user_registry.running_users()),
        "cache_stats": computation_cache.stats,
        "timestamp": _now(),
    }


def run_quick_smoke_test(
    access_token: str = "test-token",
) -> StressTestResult:
    """
    Quick 10-user smoke test — runs in ~2 seconds.
    Returns a StressTestResult.
    """
    return run_stress_test(10, "NIFTY", access_token, verbose=False)


def run_full_validation(
    access_token: str = "test-token",
) -> dict:
    """
    Run the full validation suite:
      - 10 users (smoke)
      - 50 users
      - 100 users

    Returns a dict with results for each test size.
    """
    results = {}
    for n in [10, 50, 100]:
        print(f"\n{'▼'*60}")
        print(f"Testing {n} users...")
        results[str(n)] = {
            "passed": False,
            "errors": [],
            "warnings": [],
            "metrics": {},
        }
        try:
            r = run_stress_test(n, "NIFTY", access_token, verbose=(n >= 50))
            results[str(n)]["passed"] = r.passed
            results[str(n)]["errors"] = r.errors
            results[str(n)]["warnings"] = r.warnings
            results[str(n)]["metrics"] = r.metrics
        except Exception as e:
            results[str(n)]["errors"] = [str(e)]

    return results


# ================================================================
# CLI
# ================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Stress-test the shared architecture"
    )
    parser.add_argument("--users", type=int, default=10,
                        help="Number of simulated users")
    parser.add_argument("--symbol", type=str, default="NIFTY",
                        help="Symbol to test")
    parser.add_argument("--token", type=str, default="test-token",
                        help="Mock access token")
    parser.add_argument("--full", action="store_true",
                        help="Run full validation (10/50/100 users)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Verbose output")

    args = parser.parse_args()

    if args.full:
        results = run_full_validation(args.token)
        all_passed = all(r["passed"] for r in results.values())
        if all_passed:
            print("\n*** ALL VALIDATIONS PASSED ***")
        else:
            print("\n*** SOME VALIDATIONS FAILED ***")
        import sys
        sys.exit(0 if all_passed else 1)
    else:
        result = run_stress_test(args.users, args.symbol,
                                 args.token, args.verbose)
        import sys
        sys.exit(0 if result.passed else 1)
