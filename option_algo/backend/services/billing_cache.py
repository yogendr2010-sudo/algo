# backend/services/billing_cache.py
# ================================================================
# Read cache for admin-managed Subscription Plans (+ their allowed
# symbols/lot-limits) and BillingSettings — same cross-process TTL
# pattern as backend.services.admin_config_cache (built for Exchange
# Holidays / Streamer Symbol Tokens): the web process (admin API) and
# worker process (resolve_start_inputs' trading-permission check) are
# separate processes sharing only the DB, so reads go through the
# sync engine (backend.db.database._sync_engine_for_push) with a TTL,
# and refresh(force=True) is called right after every admin write.
# ================================================================

import threading
import time
from typing import Optional

from backend.db.database import _sync_engine_for_push

_TTL_SECONDS = 300

_lock = threading.Lock()
_last_loaded_at = 0.0
_plans: dict = {}          # plan_code -> plan dict (with "symbols": {SYM: lot_limit})
_settings: dict = {"gst_enabled": True, "upgrade_mode": "immediate", "invoice_prefix": "INV",
                   "payment_mode": "RAZORPAY", "manual_upi_id": None,
                   "manual_qr_code_path": None, "manual_instructions": None}


def _load_plans_from_db() -> dict:
    from sqlalchemy import text
    with _sync_engine_for_push().connect() as conn:
        plan_rows = conn.execute(text(
            "SELECT id, plan_code, name, monthly_price, gst_percentage, "
            "duration_days, is_contact_sales, is_active FROM subscription_plans"
        )).fetchall()
        symbol_rows = conn.execute(text(
            "SELECT plan_id, symbol, lot_limit FROM subscription_plan_symbols"
        )).fetchall()

    symbols_by_plan: dict = {}
    for plan_id, symbol, lot_limit in symbol_rows:
        symbols_by_plan.setdefault(plan_id, {})[symbol.upper()] = lot_limit

    plans = {}
    for pid, code, name, price, gst, duration, contact_sales, is_active in plan_rows:
        plans[code] = {
            "id": pid, "plan_code": code, "name": name,
            "monthly_price": float(price), "gst_percentage": float(gst),
            "duration_days": duration, "is_contact_sales": bool(contact_sales),
            "is_active": bool(is_active), "symbols": symbols_by_plan.get(pid, {}),
        }
    return plans


def _load_settings_from_db() -> dict:
    from sqlalchemy import text
    with _sync_engine_for_push().connect() as conn:
        row = conn.execute(text(
            "SELECT gst_enabled, upgrade_mode, invoice_prefix, "
            "payment_mode, manual_upi_id, manual_qr_code_path, manual_instructions "
            "FROM billing_settings WHERE id = 1"
        )).fetchone()
    if not row:
        return {"gst_enabled": True, "upgrade_mode": "immediate", "invoice_prefix": "INV",
                "payment_mode": "RAZORPAY", "manual_upi_id": None,
                "manual_qr_code_path": None, "manual_instructions": None}
    return {
        "gst_enabled": bool(row[0]),
        "upgrade_mode": row[1],
        "invoice_prefix": row[2],
        "payment_mode": row[3] or "RAZORPAY",
        "manual_upi_id": row[4],
        "manual_qr_code_path": row[5],
        "manual_instructions": row[6],
    }


def refresh(force: bool = False) -> None:
    global _last_loaded_at, _plans, _settings
    with _lock:
        if not force and (time.time() - _last_loaded_at) < _TTL_SECONDS:
            return
        try:
            _plans = _load_plans_from_db()
            _settings = _load_settings_from_db()
        except Exception as e:
            print(f"⚠️  billing_cache refresh failed, using stale cache: {e}")
        _last_loaded_at = time.time()


def get_plan(plan_code: str) -> Optional[dict]:
    refresh()
    return _plans.get((plan_code or "").lower())


def get_plan_by_id(plan_id: int) -> Optional[dict]:
    refresh()
    for p in _plans.values():
        if p["id"] == plan_id:
            return p
    return None


def get_all_plans() -> dict:
    refresh()
    return dict(_plans)


def get_billing_settings() -> dict:
    refresh()
    return dict(_settings)
