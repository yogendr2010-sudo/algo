# ================================================================
# Manual Payment Router — User-facing submission + Admin verification
#
# User endpoints:
#   POST /api/payment/manual/submit      — Submit manual payment with UTR
#   GET  /api/payment/manual/my-payments  — User's manual payment history
#
# Admin endpoints:
#   GET  /api/admin/manual-payments           — List all manual payments
#   POST /api/admin/manual-payments/{id}/approve  — Approve payment
#   POST /api/admin/manual-payments/{id}/reject   — Reject payment
#
# Payment settings (Admin):
#   GET  /api/admin/payment-settings   — Get payment mode settings
#   PUT  /api/admin/payment-settings   — Update payment mode settings
# ================================================================

from __future__ import annotations
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, File, Form
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.database import get_db
from backend.db.models import (
    User, Payment, PaymentStatus, PaymentProvider, SubscriptionPlan,
    BillingSettings,
)
from backend.services.auth_service import get_current_user, get_admin_user
from backend.services.rate_limit import rate_limit
from backend.repositories.payment_repository import PaymentRepository
from backend.repositories.plan_repository import PlanRepository
from backend.services import manual_payment_service, audit_log
from backend.services.billing_cache import get_billing_settings, refresh

router = APIRouter(tags=["manual-payment"])


def _client_ip(request: Request) -> Optional[str]:
    return request.client.host if request and request.client else None


def _payment_out(p: Payment) -> dict:
    """Normalizes a payment record for API response."""
    return {
        "id": p.id,
        "user_id": p.user_id,
        "plan_id": p.plan_id,
        "payment_provider": p.payment_provider,
        "utr_number": p.utr_number,
        "screenshot_path": p.screenshot_path,
        "base_amount": float(p.base_amount),
        "gst_amount": float(p.gst_amount),
        "total_amount": float(p.total_amount),
        "currency": p.currency,
        "status": p.status.value,
        "remarks": p.remarks,
        "verified_by": p.verified_by,
        "verified_at": p.verified_at.isoformat() if p.verified_at else None,
        "paid_at": p.paid_at.isoformat() if p.paid_at else None,
        "created_at": p.created_at.isoformat(),
    }


# ================================================================
# USER ENDPOINTS
# ================================================================

@router.post("/api/payment/manual/submit")
async def submit_manual_payment(
    request: Request,
    plan_id: int = Form(...),
    utr_number: str = Form(...),
    screenshot: Optional[UploadFile] = File(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    _rl: None = Depends(rate_limit("manual_payment_submit", 3)),
):
    """
    User submits a manual payment with UTR number and optional screenshot.
    Payment is recorded as pending_verification until admin approves.
    """
    payment, error = await manual_payment_service.submit_manual_payment(
        db=db,
        user_id=user.id,
        plan_id=plan_id,
        utr_number=utr_number,
        screenshot_file=screenshot,
    )
    if error:
        raise HTTPException(400, error)

    await audit_log.log_event(
        db, user.id, "manual_payment_submitted",
        f"Manual payment #{payment.id} submitted for plan #{plan_id}",
        ip_address=_client_ip(request),
    )

    return {
        "ok": True,
        "payment": _payment_out(payment),
        "message": "Payment submitted for verification. Admin will review and activate your subscription.",
    }


@router.get("/api/payment/manual/my-payments")
async def my_manual_payments(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Returns the current user's manual payment history."""
    payments = await PaymentRepository(db).list_for_user(user.id)
    # Filter only manual payments
    manual = [p for p in payments if p.payment_provider == PaymentProvider.MANUAL.value]
    return [_payment_out(p) for p in manual]


@router.get("/api/payment/settings")
async def get_payment_settings_public(
    db: AsyncSession = Depends(get_db),
):
    """
    Public endpoint returning current payment mode and manual payment
    details (UPI ID, QR code path, instructions). Used by checkout page
    to determine whether to show Razorpay or manual payment UI.
    """
    settings = get_billing_settings()
    return {
        "payment_mode": settings.get("payment_mode", "RAZORPAY"),
        "manual_upi_id": settings.get("manual_upi_id", ""),
        "manual_qr_code_path": settings.get("manual_qr_code_path", ""),
        "manual_instructions": settings.get("manual_instructions", ""),
    }


# ================================================================
# ADMIN ENDPOINTS
# ================================================================

@router.get("/api/admin/manual-payments")
async def list_manual_payments(
    admin=Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Admin lists all manual payment submissions."""
    payments = await PaymentRepository(db).list_manual_pending()
    result = []
    for p in payments:
        ures = await db.execute(select(User).where(User.id == p.user_id))
        u = ures.scalar_one_or_none()
        plan_name = None
        if p.plan_id:
            pres = await db.execute(
                select(SubscriptionPlan).where(SubscriptionPlan.id == p.plan_id)
            )
            plan = pres.scalar_one_or_none()
            plan_name = plan.name if plan else None
        entry = _payment_out(p)
        entry["user_email"] = u.email if u else None
        entry["user_name"] = u.full_name if u else None
        entry["plan_name"] = plan_name
        result.append(entry)
    return result


class ApproveRejectIn(BaseModel):
    remarks: Optional[str] = Field(default=None, max_length=500)


@router.post("/api/admin/manual-payments/{payment_id}/approve")
async def approve_manual_payment(
    payment_id: int,
    body: ApproveRejectIn = None,
    admin=Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin approves a manual payment → activates subscription automatically.
    Generates invoice, sends notifications, enables trading.
    """
    remarks = body.remarks if body and body.remarks else None
    payment, error = await manual_payment_service.approve_manual_payment(
        db=db,
        payment_id=payment_id,
        admin_user_id=admin.id,
        remarks=remarks,
    )
    if error:
        raise HTTPException(400, error)

    await audit_log.log_event(
        db, payment.user_id, "manual_payment_approved",
        f"Manual payment #{payment_id} approved by admin #{admin.id}",
        ip_address=None,
    )

    return {
        "ok": True,
        "payment": _payment_out(payment),
        "message": "Payment approved and subscription activated.",
    }


@router.post("/api/admin/manual-payments/{payment_id}/reject")
async def reject_manual_payment(
    payment_id: int,
    body: ApproveRejectIn = None,
    admin=Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin rejects a manual payment. Remarks/reason is required.
    User can submit a new payment after rejection.
    """
    if not body or not body.remarks:
        raise HTTPException(400, "Rejection reason (remarks) is required")

    payment, error = await manual_payment_service.reject_manual_payment(
        db=db,
        payment_id=payment_id,
        admin_user_id=admin.id,
        remarks=body.remarks,
    )
    if error:
        raise HTTPException(400, error)

    await audit_log.log_event(
        db, payment.user_id, "manual_payment_rejected",
        f"Manual payment #{payment_id} rejected by admin #{admin.id}: {body.remarks}",
        ip_address=None,
    )

    return {
        "ok": True,
        "payment": _payment_out(payment),
        "message": "Payment rejected.",
    }


# ================================================================
# PAYMENT SETTINGS (Admin)
# ================================================================

class PaymentSettingsIn(BaseModel):
    payment_mode: str = Field(default="RAZORPAY", pattern=r"^(RAZORPAY|MANUAL)$")
    manual_upi_id: Optional[str] = Field(default=None, max_length=100)
    manual_qr_code_path: Optional[str] = Field(default=None, max_length=512)
    manual_instructions: Optional[str] = Field(default=None, max_length=2000)


@router.get("/api/admin/payment-settings")
async def get_payment_settings_admin(
    admin=Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Returns current payment mode and manual payment configuration."""
    settings = get_billing_settings()
    return {
        "payment_mode": settings.get("payment_mode", "RAZORPAY"),
        "manual_upi_id": settings.get("manual_upi_id", ""),
        "manual_qr_code_path": settings.get("manual_qr_code_path", ""),
        "manual_instructions": settings.get("manual_instructions", ""),
    }


@router.put("/api/admin/payment-settings")
async def update_payment_settings(
    body: PaymentSettingsIn,
    admin=Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Updates payment mode and manual payment configuration.
    Switching payment_mode immediately affects all new subscriptions.
    Validates that MANUAL mode has UPI ID configured.
    """
    if body.payment_mode == "MANUAL" and not body.manual_upi_id:
        raise HTTPException(400, "UPI ID is required when Manual Payment mode is selected")

    res = await db.execute(select(BillingSettings).where(BillingSettings.id == 1))
    row = res.scalar_one_or_none()
    if not row:
        row = BillingSettings(id=1)

    row.payment_mode = body.payment_mode
    row.manual_upi_id = body.manual_upi_id
    row.manual_qr_code_path = body.manual_qr_code_path
    row.manual_instructions = body.manual_instructions
    db.add(row)
    await db.commit()
    refresh(force=True)

    await audit_log.log_event(
        db, admin.id, "payment_settings_updated",
        f"Payment mode changed to {body.payment_mode} by admin #{admin.id}",
        metadata={
            "payment_mode": body.payment_mode,
            "manual_upi_id": body.manual_upi_id,
        },
    )

    return {"ok": True, "payment_mode": body.payment_mode}

