# backend/services/push_notifications.py
# ================================================================
# Web Push notifications (VAPID) — sent from the WORKER process
# when a live trade event occurs (ENTRY, EXIT, ORDER_ALERT, and bot
# errors). Complements Telegram alerts and in-browser voice alerts:
# push notifications work even when the dashboard tab is closed or
# the phone is locked (subject to OS/browser support).
#
# Setup:
#   1. python scripts/generate_vapid_keys.py  -> set VAPID_* in .env
#   2. User clicks "Enable Push Notifications" in Settings — browser
#      creates a PushSubscription, POSTed to /api/push/subscribe
#   3. Worker calls send_push_sync(user_id, title, body, ...) on
#      ENTRY/EXIT/ORDER_ALERT/BOT_STATUS events
#
# If VAPID keys aren't configured (settings.push_enabled is False),
# all functions become no-ops — the rest of the app works normally
# without push support.
# ================================================================

import json
from typing import Optional

from backend.config import get_settings

settings = get_settings()

try:
    from pywebpush import webpush, WebPushException
    _PYWEBPUSH_AVAILABLE = True
except ImportError:
    _PYWEBPUSH_AVAILABLE = False


def _vapid_claims() -> dict:
    return {"sub": f"mailto:{settings.VAPID_CLAIM_EMAIL}"}


def send_to_subscription_sync(subscription: dict, title: str, body: str,
                               data: Optional[dict] = None, tag: Optional[str] = None) -> bool:
    """
    Sends one push message to one subscription.
    subscription = {"endpoint": ..., "keys": {"p256dh": ..., "auth": ...}}
    Returns True on success, False on failure (including expired
    subscriptions — caller should remove those, see send_push_sync).
    Raises WebPushException with response.status_code == 410/404 for
    expired/invalid subscriptions, which the caller can catch to
    clean up the DB row.
    """
    if not _PYWEBPUSH_AVAILABLE or not settings.push_enabled:
        return False
    payload = json.dumps({"title": title, "body": body, "data": data or {}, "tag": tag}, default=str)
    webpush(
        subscription_info=subscription,
        data=payload,
        vapid_private_key=settings.VAPID_PRIVATE_KEY,
        vapid_claims=_vapid_claims(),
    )
    return True


def send_push_sync(user_id: int, title: str, body: str,
                    data: Optional[dict] = None, tag: Optional[str] = None):
    """
    Sends a push notification to ALL of this user's registered
    devices/browsers. Called from the worker process (sync DB +
    pywebpush). Silently does nothing if push isn't configured or
    the user has no subscriptions. Expired subscriptions (410/404)
    are removed from the DB automatically.
    """
    if not _PYWEBPUSH_AVAILABLE or not settings.push_enabled:
        return

    import sqlalchemy as sa
    from backend.db.database import _sync_engine_for_push

    engine = _sync_engine_for_push()
    with engine.connect() as conn:
        rows = conn.execute(sa.text(
            "SELECT id, endpoint, p256dh, auth FROM push_subscriptions WHERE user_id = :uid"
        ), {"uid": user_id}).fetchall()

        for row in rows:
            sub = {
                "endpoint": row.endpoint,
                "keys": {"p256dh": row.p256dh, "auth": row.auth},
            }
            try:
                send_to_subscription_sync(sub, title, body, data, tag)
            except WebPushException as e:
                status = getattr(e.response, "status_code", None)
                if status in (404, 410):
                    # Subscription expired / unsubscribed — clean up
                    conn.execute(sa.text(
                        "DELETE FROM push_subscriptions WHERE id = :id"), {"id": row.id})
                    conn.commit()
                else:
                    print(f"[push] send failed for user {user_id}: {e}")
            except Exception as e:
                print(f"[push] send error for user {user_id}: {e}")
