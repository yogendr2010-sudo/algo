# backend/services/razorpay_service.py
# ================================================================
# Thin wrapper around the Razorpay SDK/API contract.
#
# Signature verification (verify_payment_signature / webhook) is
# pure HMAC-SHA256 — it can be exercised and trusted without a live
# Razorpay account, using Razorpay's documented test vectors. Order
# creation requires real RAZORPAY_KEY_ID/SECRET (settings.razorpay_enabled).
#
# Security: the frontend result is NEVER trusted for activation —
# backend.services.subscription_service.verify_and_activate() only
# proceeds after verify_payment_signature() returns True here.
# ================================================================

import hashlib
import hmac

from backend.config import get_settings

settings = get_settings()


def _client():
    import razorpay
    if not settings.razorpay_enabled:
        raise RuntimeError(
            "Razorpay is not configured — set RAZORPAY_KEY_ID and "
            "RAZORPAY_KEY_SECRET in .env")
    return razorpay.Client(auth=(settings.RAZORPAY_KEY_ID, settings.RAZORPAY_KEY_SECRET))


def create_order(amount_rupees: float, receipt: str, notes: dict | None = None) -> dict:
    """
    Creates a Razorpay order for `amount_rupees` (converted to paise,
    Razorpay's smallest-unit convention). Returns the raw Razorpay
    order dict (contains "id", "amount", "currency", "status", ...).
    """
    amount_paise = int(round(amount_rupees * 100))
    client = _client()
    return client.order.create({
        "amount": amount_paise,
        "currency": "INR",
        "receipt": receipt,
        "notes": notes or {},
        "payment_capture": 1,
    })


def verify_payment_signature(order_id: str, payment_id: str, signature: str) -> bool:
    """
    Razorpay's documented checkout signature algorithm:
        expected = HMAC_SHA256(key=RAZORPAY_KEY_SECRET,
                                msg=f"{order_id}|{payment_id}")
    Constant-time compare via hmac.compare_digest to avoid timing attacks.
    """
    if not settings.RAZORPAY_KEY_SECRET:
        return False
    payload = f"{order_id}|{payment_id}".encode()
    expected = hmac.new(settings.RAZORPAY_KEY_SECRET.encode(), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature or "")


def verify_webhook_signature(raw_body: bytes, signature: str) -> bool:
    """
    Razorpay webhook signature: HMAC-SHA256 of the raw request body,
    keyed with the separate RAZORPAY_WEBHOOK_SECRET (configured under
    Razorpay Dashboard -> Settings -> Webhooks, distinct from the API secret).
    """
    if not settings.RAZORPAY_WEBHOOK_SECRET:
        return False
    expected = hmac.new(settings.RAZORPAY_WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature or "")


# ================================================================
# RazorpayProvider — PaymentManager-compatible wrapper class.
#
# Keeps the existing module-level functions intact for backward
# compatibility (subscription_router imports them directly).
# ================================================================

from backend.db.models import PaymentProvider as PaymentProviderEnum

class RazorpayProvider:
    """Wraps the existing razorpay_service functions for PaymentManager."""

    @property
    def provider_name(self) -> str:
        return PaymentProviderEnum.RAZORPAY.value

    def create_order(self, amount_rupees: float, receipt: str,
                     notes: dict | None = None) -> dict:
        return create_order(amount_rupees, receipt, notes)

    def verify_payment(self, payment_data: dict) -> bool:
        """
        payment_data must contain: razorpay_order_id, razorpay_payment_id,
        razorpay_signature.
        """
        return verify_payment_signature(
            payment_data.get("razorpay_order_id", ""),
            payment_data.get("razorpay_payment_id", ""),
            payment_data.get("razorpay_signature", ""),
        )

    def process_webhook(self, raw_body: bytes, headers: dict) -> dict:
        from fastapi import HTTPException
        signature = headers.get("x-razorpay-signature", "")
        if not verify_webhook_signature(raw_body, signature):
            raise HTTPException(400, "Invalid webhook signature")
        import json
        body = json.loads(raw_body)
        return {
            "event_type": body.get("event", ""),
            "event_id": headers.get("x-razorpay-event-id") or body.get("id") or "",
            "payload": body,
            "raw_body": raw_body,
        }
