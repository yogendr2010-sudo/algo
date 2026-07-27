#!/usr/bin/env python
# scripts/subscription_daily_job.py
# ================================================================
# Daily subscription lifecycle job — run once a day via an external
# scheduler (cron on the Linux VPS, Task Scheduler on Windows dev).
# This codebase already relies on external process orchestration for
# worker.py (systemd), so a new in-process scheduler dependency
# (APScheduler etc.) isn't introduced for this — same philosophy.
#
# Responsibilities:
#   1. Expire trials/subscriptions whose end_date has passed —
#      applying any queued pending_plan_id instead of expiring, when
#      a downgrade/queued-upgrade was scheduled.
#   2. Send expiry-warning notifications (trial: 2 days before;
#      paid subscription: 7/3/1 days before).
#   3. Log every transition to AuditLog.
#
# Usage (cron, once daily e.g. 00:15 IST):
#   15 0 * * * cd /path/to/option_algo && venv/bin/python scripts/subscription_daily_job.py
# ================================================================

import asyncio
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.db.database import AsyncSessionLocal
from backend.db.models import User, SubscriptionStatus
from backend.repositories.subscription_repository import SubscriptionRepository
from backend.services import subscription_service, audit_log
from backend.services.billing_notifications import notify

TRIAL_WARNING_DAYS = {2}
SUBSCRIPTION_WARNING_DAYS = {7, 3, 1}


async def _get_user(db, user_id):
    from sqlalchemy import select
    res = await db.execute(select(User).where(User.id == user_id))
    return res.scalar_one_or_none()


async def expire_past_due():
    async with AsyncSessionLocal() as db:
        repo = SubscriptionRepository(db)
        now = datetime.utcnow()
        past_due = await repo.list_past_end_date(now)
        for sub in past_due:
            user = await _get_user(db, sub.user_id)
            was_trial = sub.status == SubscriptionStatus.trial
            if sub.pending_plan_id:
                from backend.repositories.plan_repository import PlanRepository
                plan = await PlanRepository(db).get_by_id(sub.pending_plan_id)
                duration = plan.duration_days if plan else 30
                new_sub = await subscription_service.activate_subscription(
                    db, sub.user_id, plan_id=sub.pending_plan_id, duration_days=duration)
                await audit_log.log_event(
                    db, sub.user_id, "subscription_plan_changed",
                    f"Queued plan change to plan_id={sub.pending_plan_id} applied at period end")
                if user:
                    await notify(db, user, "subscription_activated",
                                 plan_name=plan.name if plan else "", end_date=new_sub.end_date.strftime("%d %b %Y"))
            else:
                sub.status = SubscriptionStatus.expired
                db.add(sub)
                event = "trial_expired" if was_trial else "subscription_expired"
                if was_trial:
                    from sqlalchemy import select
                    from backend.db.models import PartnerRewardRequest, PartnerRewardStatus
                    reward_res = await db.execute(select(PartnerRewardRequest).where(
                        PartnerRewardRequest.user_id == sub.user_id,
                        PartnerRewardRequest.status == PartnerRewardStatus.Verified
                    ))
                    reward_req = reward_res.scalar_one_or_none()
                    if reward_req:
                        event = "reward_paper_trading_expired"
                await audit_log.log_event(db, sub.user_id, event,
                                           "Trial period ended" if was_trial else "Subscription period ended")
                if user:
                    await notify(db, user, event)
            await db.commit()
            print(f"  expired sub #{sub.id} (user {sub.user_id}), was_trial={was_trial}, "
                  f"queued_plan={sub.pending_plan_id}")


async def send_expiry_warnings():
    async with AsyncSessionLocal() as db:
        repo = SubscriptionRepository(db)
        now = datetime.utcnow()
        # Look 8 days ahead to cover both trial (2d) and subscription (7/3/1d) windows
        candidates = await repo.list_expiring_between(now, now + timedelta(days=8))
        for sub in candidates:
            remaining = (sub.end_date - now).days
            user = await _get_user(db, sub.user_id)
            if not user:
                continue
            if sub.status == SubscriptionStatus.trial and remaining in TRIAL_WARNING_DAYS:
                await notify(db, user, "trial_expiring", days_left=remaining)
                await audit_log.log_event(db, sub.user_id, "trial_expiring_notice",
                                           f"{remaining} day(s) left in trial")
                await db.commit()
                print(f"  trial expiring notice sent: user {sub.user_id}, {remaining}d left")
            elif sub.status == SubscriptionStatus.active and remaining in SUBSCRIPTION_WARNING_DAYS:
                await notify(db, user, "subscription_expiring", days_left=remaining,
                             end_date=sub.end_date.strftime("%d %b %Y"))
                await audit_log.log_event(db, sub.user_id, "subscription_expiring_notice",
                                           f"{remaining} day(s) left in subscription")
                await db.commit()
                print(f"  subscription expiring notice sent: user {sub.user_id}, {remaining}d left")


async def main():
    print("=" * 50)
    print("Subscription Daily Job")
    print("=" * 50)
    print("\n[1/2] Expiring past-due trials/subscriptions...")
    await expire_past_due()
    print("\n[2/2] Sending expiry-warning notifications...")
    await send_expiry_warnings()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
