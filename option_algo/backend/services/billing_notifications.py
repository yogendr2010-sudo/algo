# backend/services/billing_notifications.py
# ================================================================
# Subscription/billing notifications across the channels this
# codebase already supports (email, Telegram, Web Push) — no new
# channel infra, just new message templates layered on top of the
# existing senders:
#   - backend.services.email_service.send_html_email
#   - backend.services.telegram_alerts.alert_generic
#   - backend.services.push_notifications.send_push_sync
#
# SMS is explicitly out of scope (spec marks it optional; no
# provider/library exists in this codebase) — add it later as one
# more branch in _dispatch() without touching any caller.
# ================================================================

import asyncio
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.models import User, BotConfig
from backend.services.email_service import send_html_email
from backend.services.telegram_alerts import alert_generic
from backend.services.push_notifications import send_push_sync

TEMPLATES = {
    "trial_started": lambda ctx: (
        "Your 7-day free trial has started",
        f"Paper trading is now enabled for {ctx.get('days', 7)} days. "
        "Live orders are disabled during the trial."),
    "trial_expiring": lambda ctx: (
        "Your free trial is expiring soon",
        f"Your free trial ends in {ctx.get('days_left', 2)} day(s). "
        "Subscribe to a plan to keep trading after it ends."),
    "trial_expired": lambda ctx: (
        "Your free trial has ended",
        "Paper and live trading are now disabled. Subscribe to a plan to continue."),
    "payment_success": lambda ctx: (
        "Payment successful",
        f"Your payment of Rs. {ctx.get('amount', 0):.2f} for the {ctx.get('plan_name', '')} "
        f"plan was successful. Invoice: {ctx.get('invoice_number', '')}"),
    "payment_failed": lambda ctx: (
        "Payment failed",
        f"Your payment for the {ctx.get('plan_name', '')} plan could not be completed. "
        "Please retry from the Billing page."),
    "subscription_activated": lambda ctx: (
        "Subscription activated",
        f"Your {ctx.get('plan_name', '')} plan is now active until "
        f"{ctx.get('end_date', '')}. Live trading is enabled."),
    "subscription_expiring": lambda ctx: (
        "Your subscription is expiring soon",
        f"Your {ctx.get('plan_name', '')} plan expires in {ctx.get('days_left', 0)} day(s) "
        f"on {ctx.get('end_date', '')}. Renew to avoid interruption."),
    "subscription_expired": lambda ctx: (
        "Your subscription has expired",
        "Paper and live trading are now disabled. Renew your plan to continue."),
    "reward_request_submitted": lambda ctx: (
        "Upstox Partner Reward Submitted",
        f"Your request for 7 Days of FREE Paper Trading using Upstox Client ID {ctx.get('client_id', '')} "
        f"has been submitted successfully and is currently under review."
    ),
    "reward_verification_approved": lambda ctx: (
        "Upstox Reward Request Approved",
        "Your Upstox reward request has been approved! 7 Days of FREE Paper Trading has been activated on your account."
    ),
    "reward_verification_rejected": lambda ctx: (
        "Upstox Reward Request Rejected",
        f"Your Upstox reward request was rejected. Reason: {ctx.get('reason', '')}"
    ),
    "reward_paper_trading_activated": lambda ctx: (
        "Paper Trading Activated",
        "7 Days of FREE Paper Trading has been activated on your account. Enjoy trading!"
    ),
    "reward_paper_trading_expired": lambda ctx: (
        "Paper Trading Reward Expired",
        "Your 7 Days of FREE Paper Trading reward has expired. Subscribe to a plan to continue."
    ),
    # ── Manual Payment notifications ──────────────────────────────
    "payment_submitted": lambda ctx: (
        "Payment Submitted for Verification",
        f"Your payment of Rs. {ctx.get('amount', 0):.2f} for the {ctx.get('plan_name', '')} plan "
        f"has been submitted (UTR: {ctx.get('utr', '')}). It is now pending admin verification. "
        "You will be notified once it is approved or rejected."
    ),
    "payment_approved": lambda ctx: (
        "Payment Approved — Subscription Activated",
        f"Your payment of Rs. {ctx.get('amount', 0):.2f} for the {ctx.get('plan_name', '')} plan "
        "has been approved. Your subscription is now active and trading is enabled."
    ),
    "payment_rejected": lambda ctx: (
        "Payment Rejected",
        f"Your payment of Rs. {ctx.get('amount', 0):.2f} for the {ctx.get('plan_name', '')} plan "
        f"was rejected. Reason: {ctx.get('reason', '')}. "
        "Please submit a new payment from the Billing page."
    ),
}


async def notify(db: AsyncSession, user: User, event_type: str, **context) -> None:
    """Fires the given billing/subscription event across email + Telegram + push."""
    builder = TEMPLATES.get(event_type)
    if not builder:
        return
    subject, body = builder(context)

    # Email
    html = f"<p>Hi {user.full_name or 'there'},</p><p>{body}</p>"
    asyncio.create_task(send_html_email(user.email, subject, html, text=body))

    # Telegram — per-user creds live on BotConfig
    res = await db.execute(select(BotConfig).where(BotConfig.user_id == user.id))
    cfg = res.scalar_one_or_none()
    if cfg and cfg.telegram_bot_token and cfg.telegram_chat_id:
        asyncio.create_task(asyncio.to_thread(
            alert_generic, cfg.telegram_bot_token, cfg.telegram_chat_id, subject, body))

    # Web Push
    asyncio.create_task(asyncio.to_thread(send_push_sync, user.id, subject, body))
