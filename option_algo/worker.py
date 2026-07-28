# worker.py
# ================================================================
# AlgoBot WORKER PROCESS
#
# Runs SEPARATELY from the web (uvicorn/FastAPI) process. This is
# the ONLY process that holds live SymbolEngine / BotThread
# instances. It communicates with the web process(es) exclusively
# via Redis:
#
#   bot:commands          <- web pushes start/stop/modify commands
#   bot:cmd_result:{id}   -> worker posts command results
#   bot:status:{user}     -> worker heartbeats running bot status
#   bot:positions:{user}  -> SymbolEngine pushes live position snapshots
#   bot:oc:{user}         -> OptionChainAnalyzer pushes OC snapshots
#   events:{user}         -> worker publishes ENTRY/EXIT/SL_TRAIL/BOT_STATUS
#
# Run with:
#   python worker.py
#
# Exactly ONE instance of this process should run at a time — it
# owns in-process global state (bot_manager._bots, the Telegram
# engine registry). See SETUP_GUIDE.txt "PRODUCTION / MULTI-USER
# NOTES" for the full architecture explanation.
# ================================================================

import asyncio
import signal
import sys
import threading
import traceback
from datetime import datetime

from sqlalchemy import select

from backend.db.database import init_db, AsyncSessionLocal, close_db
from backend.db.models import BotConfig, BotStatus, Trade, TradeStatus
from backend.services.bot_manager import bot_manager, set_main_loop
from backend.services.bot_config_builder import resolve_start_inputs
from backend.services.command_queue import pop_command_sync, post_result_sync
from backend.services.event_bus import publish_event_sync
from backend.services.state_store import set_bot_status_sync, clear_bot_status_sync

_now = lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")

HEARTBEAT_SEC = 8


# ================================================================
# ENGINE CALLBACKS  (called synchronously from BotThread)
# ================================================================

from backend.services.push_notifications import send_push_sync


def _push(user_id: int, title: str, body: str, data: dict = None, tag: str = None):
    """Best-effort push — never let a notification failure break trading."""
    try:
        send_push_sync(user_id, title, body, data, tag)
    except Exception as e:
        print(f"{_now()} [push] error for user {user_id}: {e}")


def _send_pending_trade_telegram_alert(user_id: int, trade_data: dict):
    """
    Send a Telegram notification for a PENDING_TRADE event.
    Fetches the user's bot token and chat ID from BotConfig.
    Runs synchronously — safe to call from any thread.
    """
    try:
        import asyncio
        from backend.db.database import AsyncSessionLocal
        from backend.db.models import BotConfig
        from sqlalchemy import select

        # Fetch user's Telegram config
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        async def _fetch():
            async with AsyncSessionLocal() as db:
                res = await db.execute(
                    select(BotConfig).where(BotConfig.user_id == user_id)
                )
                cfg = res.scalar_one_or_none()
                if not cfg:
                    return None
                return {
                    "bot_token": cfg.telegram_bot_token or "",
                    "chat_id": cfg.telegram_chat_id or "",
                }
        result = loop.run_until_complete(_fetch())
        loop.close()

        if not result or not result["bot_token"] or not result["chat_id"]:
            print(f"{_now()} [worker] No Telegram config for user {user_id} - cannot send pending trade alert")
            return

        bot_token = result["bot_token"]
        chat_id = result["chat_id"]

        from backend.services import telegram_alerts as tg
        trade_id = trade_data.get("pending_trade_id")
        symbol = trade_data.get("trading_symbol") or trade_data.get("symbol", "")
        opt_type = trade_data.get("opt_type", "")
        entry_price = trade_data.get("entry_price", 0)
        stop_loss = trade_data.get("stop_loss", 0)
        quantity = trade_data.get("quantity", 0)
        strategy = trade_data.get("strategy", "")
        confidence = trade_data.get("confidence")
        expires_at = trade_data.get("expires_at")
        trading_symbol = trade_data.get("trading_symbol")

        if trade_id:
            tg.alert_pending_trade(
                bot_token, chat_id,
                trade_id=trade_id,
                symbol=symbol,
                opt_type=opt_type,
                entry_price=entry_price,
                sl=stop_loss,
                quantity=quantity,
                strategy=strategy,
                confidence=confidence,
                expires_at=expires_at,
                trading_symbol=trading_symbol or symbol,
            )
            print(f"{_now()} [worker] Telegram pending trade alert sent for trade #{trade_id}")
    except Exception as e:
        print(f"{_now()} [worker] Failed to send Telegram pending trade alert: {e}")


async def _on_status_change(user_id: int, status: BotStatus, error_msg: str = None):
    """
    Called by BotThread on crash (status=error). Persists to DB,
    updates the Redis status snapshot, publishes a BOT_STATUS event
    for any connected browser WebSockets, and pushes a notification
    on errors (most actionable — the bot has stopped unexpectedly).
    """
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(BotConfig).where(BotConfig.user_id == user_id))
        cfg = res.scalar_one_or_none()
        if cfg:
            cfg.status    = status
            cfg.error_msg = error_msg
            if status == BotStatus.running:
                cfg.last_started = datetime.utcnow()
            else:
                cfg.last_stopped = datetime.utcnow()
            db.add(cfg)
            await db.commit()

    set_bot_status_sync(user_id, status.value, error_msg)
    publish_event_sync(user_id, {
        "event": "BOT_STATUS", "status": status.value, "error": error_msg,
    })

    if status == BotStatus.error and error_msg:
        _push(user_id, "🚨 Bot Stopped — Error",
              error_msg[:180], data={"event": "BOT_STATUS"}, tag="bot-status")


async def _on_trade(user_id: int, trade_data: dict):
    """
    Called by SymbolEngine on ENTRY / EXIT / SL_TRAIL / ORDER_ALERT.
    - For EXIT: saves to DB first, THEN publishes to Redis event bus so
      the dashboard's loadTodayTrades() always finds the trade already
      committed when it queries immediately after receiving the WS event.
    - For all other events: publishes immediately (no DB write needed).
    - Sends a Web Push notification for ENTRY/EXIT/ORDER_ALERT.
    """
    event = trade_data.get("event")
    mode  = trade_data.get("mode", "live")

    # Publish non-EXIT events immediately — no DB write, no delay needed
    if event != "EXIT":
        publish_event_sync(user_id, trade_data)
    sym   = trade_data.get("trading_symbol") or trade_data.get("symbol", "")
    mode_emoji = "📄" if mode == "paper" else "💰"

    if event == "ENTRY":
        _push(user_id, f"{mode_emoji} Entry: {sym}",
              f"{trade_data.get('opt_type','')} {trade_data.get('strike','')} "
              f"@ ₹{trade_data.get('entry_price')} | SL ₹{trade_data.get('sl_trigger')} "
              f"| Target ₹{trade_data.get('target')}",
              data={"event": event}, tag=f"trade-{sym}")

    elif event == "EXIT":
        pnl    = trade_data.get("pnl", 0)
        status = trade_data.get("status", "")
        pnl_word = f"Profit ₹{pnl}" if pnl >= 0 else f"Loss ₹{abs(pnl)}"
        _push(user_id, f"{mode_emoji} Exit ({status}): {sym}",
              f"@ ₹{trade_data.get('exit_price')} — {pnl_word}",
              data={"event": event}, tag=f"trade-{sym}")

    elif event == "ORDER_ALERT":
        _push(user_id, f"🚨 Order Alert: {sym}",
              trade_data.get("reason", "")[:180],
              data={"event": event}, tag=f"order-{sym}")

    elif event == "RISK_LIMIT_HIT":
        _push(user_id, f"🚫 Risk Limit Hit: {sym}",
              f"{trade_data.get('reason', '')} — no new entries today",
              data={"event": event}, tag="risk-limit")

    elif event == "PENDING_TRADE":
        # Send Telegram notification for semi-auto trade approval
        _push(user_id, f"⏳ Pending Trade: {sym}",
              f"{trade_data.get('opt_type','')} {trade_data.get('strike','')} "
              f"@ ₹{trade_data.get('entry_price')} — Action required",
              data={"event": event}, tag=f"pending-{trade_data.get('pending_trade_id')}")
        # Send Telegram alert with approve/reject buttons
        _send_pending_trade_telegram_alert(user_id, trade_data)

    elif event == "SL_TRAIL":
        _push(user_id, f"📍 SL Trailed: {sym}",
              f"New SL ₹{trade_data.get('new_sl')} | LTP ₹{trade_data.get('ltp')}",
              data={"event": event}, tag=f"trail-{sym}")

    elif event == "SL_CANCEL":
        reason = trade_data.get("reason", "")
        _push(user_id, f"❎ SL Cancelled: {sym}",
              f"SL ₹{trade_data.get('sl_trigger')} cancelled"
              + (f" — {reason}" if reason else ""),
              data={"event": event}, tag=f"slcancel-{sym}")

    # NOTE: ORDER_UPDATE events come from the Upstox webhook
    # (backend/routers/webhook.py) and are published directly onto
    # the Redis event bus there — they do NOT flow through _on_trade.
    # Push notifications for ORDER_UPDATE are handled by
    # _on_order_update below.

    if event != "EXIT":
        return

    raw_status = trade_data.get("status", "SL")
    status_map = {
        "NEAR_TARGET":          "TARGET",
        "TARGET":               "TARGET",
        "SL":                   "SL",
        "MANUAL_SQUAREOFF":     "SL",
        "DIRECTION_FLIP_EXIT":  "SL",
    }
    db_status_str = status_map.get(raw_status, "SL")
    try:
        db_status = TradeStatus(db_status_str)
    except ValueError:
        db_status = TradeStatus("SL")

    async with AsyncSessionLocal() as db:
        # entry_ts comes from the engine as a datetime object (fixed),
        # but handle string form for backward compat with any in-flight
        # events that might still carry the old string format.
        raw_entry_ts = trade_data.get("entry_ts")
        if isinstance(raw_entry_ts, str):
            try:
                from datetime import datetime as _dt
                raw_entry_ts = _dt.strptime(raw_entry_ts, "%Y-%m-%d %H:%M:%S")
            except Exception:
                raw_entry_ts = datetime.utcnow()
        elif raw_entry_ts is None:
            raw_entry_ts = datetime.utcnow()

        t = Trade(
            user_id        = user_id,
            instrument_key = trade_data.get("instrument_key", ""),
            trading_symbol = trade_data.get("trading_symbol", ""),
            opt_type       = trade_data.get("opt_type", ""),
            strike         = float(trade_data.get("strike") or 0),
            expiry         = str(trade_data.get("expiry", "")),
            side           = "BUY",
            qty            = int(trade_data.get("qty", 0)),
            entry_price    = float(trade_data.get("entry_price", 0)),
            exit_price     = float(trade_data.get("exit_price", 0)),
            sl_trigger     = float(trade_data.get("sl_trigger", 0)),
            target         = float(trade_data.get("target", 0)),
            pnl            = float(trade_data.get("pnl", 0)),
            status         = db_status,
            strategy       = trade_data.get("strategy", ""),
            mode           = trade_data.get("mode", "live"),
            entry_ts       = raw_entry_ts,
            exit_ts        = datetime.utcnow(),
        )
        db.add(t)
        await db.commit()
        print(f"{_now()} [DB] Trade saved: {trade_data.get('trading_symbol')} "
              f"{raw_status}→{db_status_str} P&L ₹{trade_data.get('pnl')}")

    # Publish EXIT event AFTER DB commit — dashboard loadTodayTrades()
    # fires immediately on receiving this; the trade must already be
    # committed or the query will return 0 results.
    if event == "EXIT":
        publish_event_sync(user_id, trade_data)


# ================================================================
# COMMAND HANDLERS
# ================================================================

def _get_engines(user_id: int) -> list:
    try:
        from backend.services.telegram_bot import get_engines
        return get_engines(user_id)
    except Exception:
        return []


def _handle_start(main_loop: asyncio.AbstractEventLoop, user_id: int) -> dict:
    fut = asyncio.run_coroutine_threadsafe(resolve_start_inputs(user_id), main_loop)
    try:
        config, access_token, error = fut.result(timeout=15)
    except Exception as e:
        return {"ok": False, "error": f"start failed: {e}"}

    if error:
        return {"ok": False, "error": error}

    if bot_manager.is_running(user_id):
        return {"ok": False, "error": "Bot already running"}

    started = bot_manager.start(
        user_id          = user_id,
        config           = config,
        access_token     = access_token,
        on_status_change = _on_status_change,
        on_trade         = _on_trade,
    )
    if not started:
        return {"ok": False, "error": "Bot already running"}

    set_bot_status_sync(user_id, "running")
    return {"ok": True, "status": "running"}


def _handle_stop(user_id: int) -> dict:
    bot_manager.stop(user_id)
    set_bot_status_sync(user_id, "stopped")
    return {"ok": True, "status": "stopping"}


def _handle_modify_sl(user_id: int, payload: dict) -> dict:
    new_sl = payload.get("new_sl")
    symbol = payload.get("symbol")
    engines = _get_engines(user_id)
    if not engines:
        return {"ok": False, "error": "Bot not running"}
    changed, errors = [], []
    for eng in engines:
        if symbol and eng.symbol != symbol:
            continue
        pos = getattr(eng, "position", None)
        if not pos:
            continue
        entry = pos.get("entry_price", 0)
        if new_sl >= entry:
            errors.append(f"{eng.symbol}: SL must be below entry ₹{entry}")
            continue
        try:
            eng._modify_sl_from_telegram(new_sl)
            changed.append(eng.symbol)
        except Exception as e:
            errors.append(f"{eng.symbol}: {e}")
    if not changed and not errors:
        return {"ok": False, "error": "No open positions found"}
    return {"ok": True, "changed": changed, "errors": errors}


def _handle_modify_target(user_id: int, payload: dict) -> dict:
    new_target = payload.get("new_target")
    symbol     = payload.get("symbol")
    engines = _get_engines(user_id)
    if not engines:
        return {"ok": False, "error": "Bot not running"}
    changed, errors = [], []
    for eng in engines:
        if symbol and eng.symbol != symbol:
            continue
        pos = getattr(eng, "position", None)
        if not pos:
            continue
        try:
            eng._modify_target_from_telegram(new_target)
            changed.append({
                "symbol":      eng.symbol,
                "new_target":  new_target,
                "near_target": eng.position["near_target"],
            })
        except Exception as e:
            errors.append(f"{eng.symbol}: {e}")
    if not changed and not errors:
        return {"ok": False, "error": "No open positions found"}
    return {"ok": True, "changed": changed, "errors": errors}


def _handle_squareoff(user_id: int, payload: dict) -> dict:
    symbol  = payload.get("symbol")
    engines = _get_engines(user_id)
    if not engines:
        return {"ok": False, "error": "Bot not running"}
    closed, errors = [], []
    for eng in engines:
        if symbol and eng.symbol != symbol:
            continue
        pos = getattr(eng, "position", None)
        if not pos:
            continue
        try:
            eng._squareoff_from_telegram()
            closed.append(eng.symbol)
        except Exception as e:
            errors.append(f"{eng.symbol}: {e}")
    if not closed and not errors:
        return {"ok": True, "message": "No open positions to close"}
    return {"ok": True, "closed": closed, "errors": errors}


def _handle_approve_pending_trade(user_id: int, payload: dict, main_loop: asyncio.AbstractEventLoop = None) -> dict:
    """
    Approve a pending trade and execute it.
    Called from the dashboard or Telegram approve button.
    """
    trade_id = payload.get("trade_id")
    if not trade_id:
        return {"ok": False, "error": "trade_id required"}

    engines = _get_engines(user_id)
    if not engines:
        # Still log the approval even if engine not running — trade is recorded
        pass

    # Use the execution layer's PendingTradeManager
    from backend.services.execution_layer import pending_trade_manager as ptm

    def _place_order_from_signal(signal):
        """Callback: place a real order using the signal data."""
        # Find the right engine for this symbol
        for eng in engines:
            if eng.symbol == signal.symbol:
                # Build params for _place_order using per-symbol lots
                num_lots = getattr(eng, 'symbol_lots', max(1, int(eng.cfg.get("order_qty", 1))))
                custom_ls = eng.cfg.get("custom_lot_sizes") or {}
                lot_size = get_lot_size_from_engine(eng.symbol, custom_ls)
                qty = num_lots * lot_size
                if getattr(eng, '_regime', None) and eng._regime.regime == "VOLATILE":
                    qty = max(lot_size, (num_lots // 2) * lot_size)

                eid = eng._place_order("BUY", qty)
                if not eid:
                    return None
                fill = eng._get_fill_price(eid)
                if not fill:
                    return None

                sl_id = eng._place_order("SELL", qty, order_type="SL-M", trigger=signal.stop_loss)
                if not sl_id:
                    eng._place_order("SELL", qty)
                    return None

                # Set up position state for the engine
                rr = eng.cfg.get("target_rr", 1.3)
                risk = abs(fill - signal.stop_loss)
                target = round(fill + risk * rr, 2)
                near_pct = eng.cfg.get("target_near_pct", 0.003)
                eng.position = {
                    "entry_price": fill, "qty": qty,
                    "entry_order_id": eid, "sl_order_id": sl_id,
                    "sl_trigger": signal.stop_loss, "target": target,
                    "near_target": round(target * (1 - near_pct), 2),
                    "strategy": signal.strategy_name or signal.strategy or "",
                    "entry_ts": datetime.utcnow(),
                    "instrument_key": signal.instrument_key or eng.instrument_key,
                    "trading_symbol": signal.trading_symbol or eng.trading_symbol,
                    "opt_type": signal.opt_type or eng.opt_type,
                    "strike": signal.strike or eng.strike,
                    "paper_mode": eng.paper_mode, "symbol": signal.symbol,
                    "regime": getattr(eng, '_regime', None).regime if getattr(eng, '_regime', None) else "",
                }
                eng.sl_order_id = sl_id
                eng.trailing_sl = signal.stop_loss
                eng._sl_mod_ts = time.time()
                eng._risk.record_entry()
                eng._last_entry_price = fill

                # Notify
                mode = "paper" if eng.paper_mode else "live"
                if eng._tg_token and eng._tg_chat and eng.cfg.get("telegram_on_entry", True):
                    from backend.services import telegram_alerts as tg
                    tg.alert_entry(eng._tg_token, eng._tg_chat,
                                   signal.trading_symbol or signal.symbol,
                                   signal.opt_type or "", fill, signal.stop_loss,
                                   target, qty, signal.strategy_name or "", mode)
                eng.on_trade({
                    "event": "ENTRY", "user_id": user_id, "mode": mode,
                    **eng.position,
                })

                # Notify approval event
                eng.on_trade({
                    "event": "SIGNAL_APPROVED", "user_id": user_id,
                    "mode": mode, "symbol": signal.symbol,
                    "trading_symbol": signal.trading_symbol,
                    "opt_type": signal.opt_type,
                    "strike": signal.strike,
                    "entry_price": fill,
                    "pending_trade_id": trade_id,
                })

                return eid

        # No engine running — use generic order placement as fallback
        print(f"{_now()} [worker] ⚠️ No engine available for approved trade #{trade_id} "
              f"on {signal.symbol}")
        return None

    def get_lot_size_from_engine(sym: str, custom_ls: dict) -> int:
        from backend.engine.engine_v6 import get_lot_size
        return get_lot_size(sym, custom_ls)

    # Run async approve() on the main event loop via run_coroutine_threadsafe
    # to avoid creating/destroying new event loops (which corrupts ProactorEventLoop on Windows).
    if main_loop is not None:
        fut = asyncio.run_coroutine_threadsafe(
            ptm.approve(trade_id, user_id, _place_order_from_signal),
            main_loop,
        )
        try:
            result = fut.result(timeout=30)
            if result.status.value in ("EXECUTED",):
                return {
                    "ok": True,
                    "status": "approved",
                    "message": result.message,
                    "trade_id": result.trade_id,
                }
            elif result.status.value == "EXPIRED":
                return {"ok": False, "error": result.message}
            else:
                return {"ok": False, "error": result.message}
        except Exception as e:
            return {"ok": False, "error": f"Approval failed: {e}"}
    else:
        # Fallback: run in a temporary loop (should not happen in normal operation)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                ptm.approve(trade_id, user_id, _place_order_from_signal)
            )
            loop.close()
            if result.status.value in ("EXECUTED",):
                return {
                    "ok": True,
                    "status": "approved",
                    "message": result.message,
                    "trade_id": result.trade_id,
                }
            elif result.status.value == "EXPIRED":
                return {"ok": False, "error": result.message}
            else:
                return {"ok": False, "error": result.message}
        except Exception as e:
            return {"ok": False, "error": f"Approval failed: {e}"}


def _handle_reject_pending_trade(user_id: int, payload: dict, main_loop: asyncio.AbstractEventLoop = None) -> dict:
    """Reject a pending trade."""
    trade_id = payload.get("trade_id")
    if not trade_id:
        return {"ok": False, "error": "trade_id required"}

    from backend.services.execution_layer import pending_trade_manager as ptm

    if main_loop is not None:
        fut = asyncio.run_coroutine_threadsafe(
            ptm.reject(trade_id, user_id), main_loop
        )
        try:
            result = fut.result(timeout=15)
            if result.status.value in ("REJECTED",):
                return {"ok": True, "status": "rejected", "message": result.message}
            return {"ok": False, "error": result.message}
        except Exception as e:
            return {"ok": False, "error": f"Rejection failed: {e}"}
    else:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(ptm.reject(trade_id, user_id))
            loop.close()
            if result.status.value in ("REJECTED",):
                return {"ok": True, "status": "rejected", "message": result.message}
            return {"ok": False, "error": result.message}
        except Exception as e:
            return {"ok": False, "error": f"Rejection failed: {e}"}


def _handle_pause_resume(user_id: int, paused: bool) -> dict:
    engines = _get_engines(user_id)
    if not engines:
        return {"ok": False, "error": "Bot not running"}
    for eng in engines:
        eng._paused = paused
    key = "paused" if paused else "resumed"
    return {"ok": True, key: [e.symbol for e in engines]}


_command_loop_main_loop = None  # set by main() to allow command handlers to dispatch async work


def _expire_stale_pending_trades():
    """
    Background task runner: marks WAITING pending trades past their
    expiry as EXPIRED. Runs periodically from the command loop thread.
    """
    try:
        from backend.services.execution_layer import pending_trade_manager as ptm
        if _command_loop_main_loop is not None:
            fut = asyncio.run_coroutine_threadsafe(
                ptm.expire_stale_trades(), _command_loop_main_loop
            )
            fut.result(timeout=15)
        else:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(ptm.expire_stale_trades())
            loop.close()
    except Exception as e:
        print(f"{_now()} [worker] expire_stale: {e}")


# ================================================================
# COMMAND CONSUMER LOOP  (runs in a background thread)
# ================================================================

def _command_loop(main_loop: asyncio.AbstractEventLoop, stop_event: threading.Event):
    print(f"{_now()} [worker] Command consumer started")
    global _command_loop_main_loop
    _command_loop_main_loop = main_loop
    while not stop_event.is_set():
        try:
            cmd = pop_command_sync(timeout=5)
        except Exception as e:
            print(f"{_now()} [worker] command pop error: {e}")
            continue
        if cmd is None:
            continue

        cmd_id  = cmd.get("id")
        action  = cmd.get("action")
        user_id = cmd.get("user_id")
        payload = cmd.get("payload") or {}

        try:
            if action == "start":
                result = _handle_start(main_loop, user_id)
            elif action == "stop":
                result = _handle_stop(user_id)
            elif action == "modify_sl":
                result = _handle_modify_sl(user_id, payload)
            elif action == "modify_target":
                result = _handle_modify_target(user_id, payload)
            elif action == "squareoff":
                result = _handle_squareoff(user_id, payload)
            elif action == "pause":
                result = _handle_pause_resume(user_id, True)
            elif action == "resume":
                result = _handle_pause_resume(user_id, False)
            elif action == "approve_pending_trade":
                result = _handle_approve_pending_trade(user_id, payload, main_loop)
            elif action == "reject_pending_trade":
                result = _handle_reject_pending_trade(user_id, payload, main_loop)
            else:
                result = {"ok": False, "error": f"Unknown action: {action}"}
        except Exception:
            tb = traceback.format_exc()
            print(f"{_now()} [worker] command '{action}' for user {user_id} crashed:\n{tb}")
            result = {"ok": False, "error": "Internal worker error"}

        if cmd_id:
            try:
                post_result_sync(cmd_id, result)
            except Exception as e:
                print(f"{_now()} [worker] post_result error: {e}")

    print(f"{_now()} [worker] Command consumer stopped")


# ================================================================
# HEARTBEAT LOOP  (asyncio task — refreshes bot:status TTL)
# ================================================================

async def _heartbeat_loop(stop_event: threading.Event):
    while not stop_event.is_set():
        try:
            statuses = bot_manager.status_all()
            for user_id, status in statuses.items():
                if status == "running":
                    set_bot_status_sync(user_id, "running")
        except Exception as e:
            print(f"{_now()} [worker] heartbeat error: {e}")
        await asyncio.sleep(HEARTBEAT_SEC)


# ================================================================
# MAIN
# ================================================================

async def _order_update_push_loop(stop_event: threading.Event):
    """
    Subscribes to order_update_push Redis channel (published by the
    webhook endpoint for all users' ORDER_UPDATE events that need a
    push notification). Fires in the worker because push_notifications
    does sync DB queries — simpler than doing it from the async web process.
    """
    from backend.services.redis_client import get_redis
    r      = get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe("order_update_push")
    try:
        while not stop_event.is_set():
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if not msg:
                await asyncio.sleep(0.05)
                continue
            try:
                import json as _json
                data    = _json.loads(msg["data"])
                user_id = data.get("user_id")
                if not user_id:
                    continue
                sym     = data.get("trading_symbol") or data.get("symbol", "")
                st      = (data.get("status", "")).lower()
                icon    = "✅" if st == "complete" else ("❌" if "reject" in st or "cancel" in st else "📋")
                if st == "complete":
                    body = (f"{data.get('side','')} {data.get('qty_filled','')} "
                            f"@ ₹{data.get('average_price')} | {sym}")
                else:
                    body = f"{st}: {sym}" + (f" — {data.get('message','')}" if data.get("message") else "")
                _push(user_id, f"{icon} Order {st.title()}: {sym}", body[:180],
                      data={"event": "ORDER_UPDATE"}, tag=f"order-update-{sym}")
            except Exception as e:
                print(f"{_now()} [worker] order_update_push: {e}")
    finally:
        await pubsub.unsubscribe("order_update_push")
        await pubsub.close()


async def main():
    await init_db()

    # Preload the instrument master (~96k rows) ONCE into memory before
    # any bot threads start. Without this, the first SymbolEngine to
    # need it triggers the disk load — and since multiple symbols can
    # start near-simultaneously, this avoids a thundering-herd of
    # redundant disk reads/parses at startup, and ensures every later
    # load_instruments() call (direction changes, ITM lookups, strike-
    # step detection) reuses the same in-memory DataFrame instead of
    # re-reading the file from disk every time. Runs in a thread since
    # it does blocking file/network I/O.
    print(f"{_now()} [worker] Preloading instrument master...")
    from backend.engine.instruments import preload_instruments
    await asyncio.to_thread(preload_instruments)
    print(f"{_now()} [worker] Instrument master ready")

    loop = asyncio.get_event_loop()
    set_main_loop(loop)   # bot_manager._post_to_main targets THIS loop

    stop_event = threading.Event()

    cmd_thread = threading.Thread(
        target=_command_loop, args=(loop, stop_event),
        daemon=True, name="command-consumer")
    cmd_thread.start()

    heartbeat_task  = asyncio.create_task(_heartbeat_loop(stop_event))
    order_push_task = asyncio.create_task(_order_update_push_loop(stop_event))

    # ── Graceful shutdown ────────────────────────────────────────
    shutdown_event = asyncio.Event()

    def _signal_handler():
        print(f"\n{_now()} [worker] Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler for SIGTERM
            pass

    print(f"{_now()} [worker] AlgoBot worker started — waiting for commands")
    await shutdown_event.wait()

    print(f"{_now()} [worker] Stopping all bot threads...")
    stop_event.set()
    bot_manager.stop_all(join_timeout=10.0)
    heartbeat_task.cancel()
    order_push_task.cancel()

    # Clear status keys for anything we were tracking
    for user_id in bot_manager.status_all().keys():
        clear_bot_status_sync(user_id)

    # Gracefully close the async DB engine pool to prevent Windows
    # ProactorEventLoop "NoneType has no attribute 'send'" errors.
    await close_db()

    print(f"{_now()} [worker] Shutdown complete")


if __name__ == "__main__":
    # Windows uses ProactorEventLoop by default for subprocess support,
    # but ProactorEventLoop has a known bug where _proactor becomes None
    # during shutdown while asyncpg connections still try to use it,
    # causing: AttributeError: 'NoneType' object has no attribute 'send'.
    # SelectorEventLoop does not have this issue.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
