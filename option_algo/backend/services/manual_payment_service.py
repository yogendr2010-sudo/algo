# ================================================================
# Manual Payment Service — PhonePe QR / UPI payment flow.
#
# Handles:
#   - Submitting manual payment with UTR
#   - Uploading payment screenshot
#   - Admin approval → subscription activation
#   - Admin rejection with remarks
#   - Duplicate UTR validation
# ================================================================

from __future__ import annotations
import os
import uuid
from datetime import datetime
from typing import Optional

from fastapi import UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.models import (
    Payment, PaymentStatus, PaymentProvider, User, SubscriptionPlan,
)
from backend.repositories.payment_repository import PaymentRepository
from backend.repositories.plan_repository import PlanRepository
from backend.services import subscription_service, invoice_service, audit_log
from backend.services.billing_notifications import notify
from backend.services.billing_cache import get_billing_settings
from backend.db.database import _sync_engine_for_push

# Allowed screenshot file extensions
ALLOWED_SCREENSHOT_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp"}
MAX_SCREENSHOT_SIZE_MB = 5
MAX_SCREENSHOT_SIZE_BYTES = MAX_SCREENSHOT_SIZE_MB * 1024 * 1024

# Upload directory (relative to project root)
UPLOAD_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "uploads", "payment_screenshots"
)


class ManualPaymentProvider:
    """Manual payment provider for PaymentManager."""

    @property
    def provider_name(self) -> str:
        return PaymentProvider.MANUAL.value

    def create_order(self, amount_rupees: float, receipt: str,
                     notes: dict | None = None) -> dict:
        """
        For manual payments, "order creation" returns the payment
        instructions (UPI ID, QR code path) that the frontend needs.
        """
        settings = get_billing_settings()
        return {
            "provider": self.provider_name,
            "amount": amount_rupees,
            "currency": "INR",
            "upi_id": settings.get("manual_upi_id", ""),
            "qr_code_path": settings.get("manual_qr_code_path", ""),
            "instructions": settings.get("manual_instructions", ""),
            "receipt": receipt,
        }

    def verify_payment(self, payment_data: dict) -> bool:
        """
        Manual payments don't have automated signature verification.
        Verification happens via admin approval.
        Payment_data should contain payment_id for lookup.
        """
        return True

    def process_webhook(self, raw_body: bytes, headers: dict) -> dict:
        """Manual payments have no webhook integration."""
        return {"event_type": "manual", "event_id": "", "payload": {}}


async def submit_manual_payment(
    db: AsyncSession,
    user_id: int,
    plan_id: int,
    utr_number: str,
    screenshot_file: Optional[UploadFile] = None,
) -> tuple[Payment, Optional[str]]:
    """
    Submits a manual payment for admin verification.
    Returns (payment, error_message).
    """
    # 1. Validate plan
    plan = await PlanRepository(db).get_by_id(plan_id)
    if not plan or not plan.is_active:
        return None, "Plan not found or inactive"
    if plan.is_contact_sales:
        return None, "This plan requires contacting sales"

    # 2. Validate UTR format (alphanumeric, reasonable length)
    utr_number = utr_number.strip()
    if not utr_number or len(utr_number) < 4 or len(utr_number) > 64:
        return None, "Invalid UTR number — must be between 4 and 64 characters"

    # 3. Check for duplicate UTR
    existing = await PaymentRepository(db).get_by_utr(utr_number)
    if existing:
        return None, "This UTR number has already been submitted"

    # 4. Handle screenshot upload
    screenshot_path = None
    if screenshot_file and screenshot_file.filename:
        # Validate file type
        ext = os.path.splitext(screenshot_file.filename)[1].lower()
        if ext not in ALLOWED_SCREENSHOT_EXTENSIONS:
            return None, f"Invalid file type '{ext}'. Allowed: {', '.join(ALLOWED_SCREENSHOT_EXTENSIONS)}"

        # Validate size
        contents = await screenshot_file.read()
        if len(contents) > MAX_SCREENSHOT_SIZE_BYTES:
            return None, f"Screenshot too large — maximum {MAX_SCREENSHOT_SIZE_MB} MB"

        # Save file
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        unique_name = f"{uuid.uuid4().hex}{ext}"
        file_path = os.path.join(UPLOAD_DIR, unique_name)
        with open(file_path, "wb") as f:
            f.write(contents)
        screenshot_path = f"uploads/payment_screenshots/{unique_name}"
        # Reset file cursor for potential re-read
        await screenshot_file.seek(0)

    # 5. Compute amounts
    from backend.routers.subscription_router import _compute_amounts
    base, gst, total = _compute_amounts(
        float(plan.monthly_price), float(plan.gst_percentage)
    )

    # 6. Create payment record
    payment = Payment(
        user_id=user_id,
        plan_id=plan_id,
        payment_provider=PaymentProvider.MANUAL.value,
        utr_number=utr_number,
        screenshot_path=screenshot_path,
        base_amount=base,
        gst_amount=gst,
        total_amount=total,
        currency="INR",
        status=PaymentStatus.pending_verification,
    )
    payment = await PaymentRepository(db).create(payment)
    await audit_log.log_event(
        db, user_id, "manual_payment_submitted",
        f"Manual payment submitted for plan {plan.plan_code} — UTR: {utr_number}",
    )
    await db.commit()

    # 7. Send notification
    user_res = await db.execute(select(User).where(User.id == user_id))
    user = user_res.scalar_one_or_none()
    if user:
        await notify(db, user, "payment_submitted",
                     amount=float(total), plan_name=plan.name,
                     utr=utr_number)

    return payment, None


async def approve_manual_payment(
    db: AsyncSession,
    payment_id: int,
    admin_user_id: int,
    remarks: Optional[str] = None,
) -> tuple[Optional[Payment], Optional[str]]:
    """
    Admin approves a manual payment → activates subscription.
    Returns (payment, error_message).
    """
    payment = await PaymentRepository(db).get_by_id(payment_id)
    if not payment:
        return None, "Payment not found"
    if payment.status != PaymentStatus.pending_verification:
        return None, f"Payment is not pending verification — current status: {payment.status.value}"
    if payment.payment_provider != PaymentProvider.MANUAL.value:
        return None, "Payment is not a manual payment"

    # Update payment status
    previous_status = payment.status.value
    payment.status = PaymentStatus.approved
    payment.verified_by = admin_user_id
    payment.verified_at = datetime.utcnow()
    payment.paid_at = datetime.utcnow()
    if remarks:
        payment.remarks = remarks
    db.add(payment)
    await db.flush()

    # Activate subscription using the existing logic
    from backend.routers.subscription_router import _finalize_successful_payment
    try:
        sub = await _finalize_successful_payment(db, payment)
    except Exception as e:
        await db.rollback()
        return None, f"Failed to activate subscription: {e}"

    # Audit log
    await audit_log.log_event(
        db, payment.user_id, "manual_payment_approved",
        f"Manual payment #{payment_id} approved by admin #{admin_user_id} — subscription activated",
        metadata={
            "previous_status": previous_status,
            "new_status": PaymentStatus.approved.value,
            "admin_id": admin_user_id,
        },
    )
    await db.commit()

    # Send notification
    user_res = await db.execute(select(User).where(User.id == payment.user_id))
    user = user_res.scalar_one_or_none()
    if user:
        plan_res = await db.execute(
            select(SubscriptionPlan).where(SubscriptionPlan.id == payment.plan_id)
        )
        plan = plan_res.scalar_one_or_none()
        await notify(db, user, "payment_approved",
                     amount=float(payment.total_amount),
                     plan_name=plan.name if plan else "")

    return payment, None


async def reject_manual_payment(
    db: AsyncSession,
    payment_id: int,
    admin_user_id: int,
    remarks: str,
) -> tuple[Optional[Payment], Optional[str]]:
    """
    Admin rejects a manual payment.
    Returns (payment, error_message).
    """
    if not remarks or not remarks.strip():
        return None, "Rejection reason (remarks) is required"

    payment = await PaymentRepository(db).get_by_id(payment_id)
    if not payment:
        return None, "Payment not found"
    if payment.status != PaymentStatus.pending_verification:
        return None, f"Payment is not pending verification — current status: {payment.status.value}"
    if payment.payment_provider != PaymentProvider.MANUAL.value:
        return None, "Payment is not a manual payment"

    previous_status = payment.status.value
    payment.status = PaymentStatus.rejected
    payment.verified_by = admin_user_id
    payment.verified_at = datetime.utcnow()
    payment.remarks = remarks.strip()
    payment.failure_reason = remarks.strip()
    db.add(payment)
    await db.commit()

    # Audit log
    await audit_log.log_event(
        db, payment.user_id, "manual_payment_rejected",
        f"Manual payment #{payment_id} rejected by admin #{admin_user_id} — reason: {remarks}",
        metadata={
            "previous_status": previous_status,
            "new_status": PaymentStatus.rejected.value,
            "admin_id": admin_user_id,
            "remarks": remarks,
        },
    )

    # Send notification
    user_res = await db.execute(select(User).where(User.id == payment.user_id))
    user = user_res.scalar_one_or_none()
    if user:
        plan_res = await db.execute(
            select(SubscriptionPlan).where(SubscriptionPlan.id == payment.plan_id)
        )
        plan = plan_res.scalar_one_or_none()
        await notify(db, user, "payment_rejected",
                     amount=float(payment.total_amount),
                     plan_name=plan.name if plan else "",
                     reason=remarks)

    return payment, None

