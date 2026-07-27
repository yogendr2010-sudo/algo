# backend/services/campaign_service.py
from datetime import datetime, timedelta
from typing import Optional, List
from fastapi import HTTPException
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.models import (
    User, CampaignSettings, PartnerRewardRequest, PartnerRewardStatus,
    Subscription, SubscriptionStatus
)
from backend.repositories.subscription_repository import SubscriptionRepository
from backend.services import audit_log
from backend.services.billing_notifications import notify


async def get_campaign_settings(db: AsyncSession) -> CampaignSettings:
    """Fetch singleton campaign settings or create a default row if it doesn't exist."""
    res = await db.execute(select(CampaignSettings).where(CampaignSettings.id == 1))
    row = res.scalar_one_or_none()
    if not row:
        row = CampaignSettings(
            id=1,
            enable_campaign=False,
            campaign_title="🎁 Get 7 Days FREE Paper Trading",
            campaign_description=(
                "Open your Upstox Demat account using our official partner referral link "
                "and receive 7 Days of FREE Paper Trading after successful verification."
            ),
            partner_referral_url="",
            button_text="Open Upstox Account",
            banner_image="",
            terms_conditions="",
            campaign_start_date=None,
            campaign_end_date=None
        )
        db.add(row)
        await db.commit()
        # Re-fetch in session
        res = await db.execute(select(CampaignSettings).where(CampaignSettings.id == 1))
        row = res.scalar_one()
    return row


async def update_campaign_settings(db: AsyncSession, data: dict) -> CampaignSettings:
    """Update campaign settings."""
    row = await get_campaign_settings(db)
    
    row.enable_campaign = data.get("enable_campaign", False)
    row.campaign_title = data.get("campaign_title", row.campaign_title)
    row.campaign_description = data.get("campaign_description", row.campaign_description)
    row.partner_referral_url = data.get("partner_referral_url", "")
    row.button_text = data.get("button_text", row.button_text)
    row.banner_image = data.get("banner_image", "")
    row.terms_conditions = data.get("terms_conditions", "")
    row.campaign_start_date = data.get("campaign_start_date")
    row.campaign_end_date = data.get("campaign_end_date")
    
    db.add(row)
    await db.commit()
    return row


async def is_campaign_active(db: AsyncSession) -> bool:
    """Checks if the referral campaign is active based on enable flag and start/end dates."""
    settings = await get_campaign_settings(db)
    if not settings.enable_campaign:
        return False
    
    now = datetime.utcnow()
    if settings.campaign_start_date and now < settings.campaign_start_date:
        return False
    if settings.campaign_end_date and now > settings.campaign_end_date:
        return False
        
    return True


async def submit_reward_request(
    db: AsyncSession, user_id: int, client_id: str, comments: Optional[str]
) -> PartnerRewardRequest:
    """Submits a new Upstox Client ID reward request."""
    # Check if campaign is active
    active = await is_campaign_active(db)
    if not active:
        raise HTTPException(status_code=400, detail="Referral campaign is not currently active.")
        
    # Trim and validate
    if not client_id:
        raise HTTPException(status_code=400, detail="Client ID cannot be empty.")
    client_id = client_id.strip().upper()
    if not client_id:
        raise HTTPException(status_code=400, detail="Client ID cannot be empty.")

    # Check for one pending request per user limit
    pending_res = await db.execute(
        select(PartnerRewardRequest).where(
            PartnerRewardRequest.user_id == user_id,
            PartnerRewardRequest.status == PartnerRewardStatus.Pending
        )
    )
    if pending_res.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="You already have a pending verification request.")

    # Check client ID uniqueness globally
    dup_res = await db.execute(
        select(PartnerRewardRequest).where(PartnerRewardRequest.client_id == client_id)
    )
    if dup_res.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="This Upstox Client ID has already been submitted.")

    # Save request
    req = PartnerRewardRequest(
        user_id=user_id,
        client_id=client_id,
        comments=comments,
        status=PartnerRewardStatus.Pending,
        submitted_at=datetime.utcnow()
    )
    db.add(req)
    await db.flush()

    # Log to AuditLog
    await audit_log.log_event(
        db, user_id, "reward_request_submitted",
        f"Submitted Upstox Client ID: {client_id}"
    )

    # Dispatch notification
    user_res = await db.execute(select(User).where(User.id == user_id))
    user = user_res.scalar_one()
    await notify(db, user, "reward_request_submitted", client_id=client_id)
    
    await db.commit()
    return req


async def get_user_request(db: AsyncSession, user_id: int) -> Optional[PartnerRewardRequest]:
    """Fetch the latest reward request for a given user."""
    res = await db.execute(
        select(PartnerRewardRequest)
        .where(PartnerRewardRequest.user_id == user_id)
        .order_by(PartnerRewardRequest.submitted_at.desc())
    )
    return res.scalars().first()


async def list_reward_requests(
    db: AsyncSession, status: Optional[PartnerRewardStatus] = None, q: Optional[str] = None
) -> List[dict]:
    """List all reward requests with status filter and search query."""
    stmt = select(PartnerRewardRequest)
    
    # Filter status
    if status:
        stmt = stmt.where(PartnerRewardRequest.status == status)
        
    # Join with User
    stmt = stmt.join(User, User.id == PartnerRewardRequest.user_id)
    
    # Search filter
    if q:
        like_pattern = f"%{q}%"
        stmt = stmt.where(
            or_(
                User.email.ilike(like_pattern),
                User.full_name.ilike(like_pattern),
                PartnerRewardRequest.client_id.ilike(like_pattern)
            )
        )
        
    stmt = stmt.order_by(PartnerRewardRequest.submitted_at.desc())
    res = await db.execute(stmt)
    requests = res.scalars().all()
    
    output = []
    sub_repo = SubscriptionRepository(db)
    for r in requests:
        current_sub = await sub_repo.get_current(r.user_id)
        current_sub_desc = "None"
        if current_sub:
            plan_name = "Trial"
            if current_sub.plan:
                plan_name = current_sub.plan.name
            current_sub_desc = f"{plan_name} ({current_sub.status.value}, expires {current_sub.end_date.strftime('%Y-%m-%d')})"
            
        output.append({
            "id": r.id,
            "user_id": r.user_id,
            "user_name": r.user.full_name,
            "user_email": r.user.email,
            "client_id": r.client_id,
            "comments": r.comments,
            "status": r.status.value,
            "submitted_at": r.submitted_at.isoformat(),
            "reward_start_date": r.reward_start_date.isoformat() if r.reward_start_date else None,
            "reward_end_date": r.reward_end_date.isoformat() if r.reward_end_date else None,
            "verified_at": r.verified_at.isoformat() if r.verified_at else None,
            "verified_by": r.verified_by,
            "admin_notes": r.admin_notes,
            "current_subscription": current_sub_desc
        })
    return output


async def verify_reward_request(
    db: AsyncSession, request_id: int, admin_id: int, admin_notes: Optional[str]
) -> PartnerRewardRequest:
    """Verify and approve a user's Upstox partner reward request, granting 15 days free paper trading."""
    res = await db.execute(
        select(PartnerRewardRequest).where(PartnerRewardRequest.id == request_id)
    )
    req = res.scalar_one_or_none()
    if not req:
        raise HTTPException(status_code=404, detail="Request not found.")
    if req.status != PartnerRewardStatus.Pending:
        raise HTTPException(status_code=400, detail="This request has already been processed.")

    now = datetime.utcnow()
    req.status = PartnerRewardStatus.Verified
    req.reward_start_date = now
    req.reward_end_date = now + timedelta(days=15)
    req.verified_at = now
    req.verified_by = admin_id
    req.admin_notes = admin_notes

    # Grant Paper Trading by creating a new 'trial' subscription
    sub_repo = SubscriptionRepository(db)
    await sub_repo.clear_current(req.user_id)
    
    sub = Subscription(
        user_id=req.user_id,
        plan_id=None,
        custom_subscription_id=None,
        status=SubscriptionStatus.trial,
        is_current=True,
        start_date=req.reward_start_date,
        end_date=req.reward_end_date
    )
    await sub_repo.create(sub)

    # Log to AuditLog
    await audit_log.log_event(
        db, req.user_id, "reward_request_verified",
        f"Verified Upstox Client ID: {req.client_id}. Free Paper Trading granted."
    )

    # Notify User
    user_res = await db.execute(select(User).where(User.id == req.user_id))
    user = user_res.scalar_one()
    await notify(db, user, "reward_verification_approved")
    await notify(db, user, "reward_paper_trading_activated")

    await db.commit()
    return req


async def reject_reward_request(
    db: AsyncSession, request_id: int, admin_id: int, admin_notes: str
) -> PartnerRewardRequest:
    """Reject a user's Upstox partner reward request."""
    if not admin_notes or not admin_notes.strip():
        raise HTTPException(status_code=400, detail="Rejection reason is required.")

    res = await db.execute(
        select(PartnerRewardRequest).where(PartnerRewardRequest.id == request_id)
    )
    req = res.scalar_one_or_none()
    if not req:
        raise HTTPException(status_code=404, detail="Request not found.")
    if req.status != PartnerRewardStatus.Pending:
        raise HTTPException(status_code=400, detail="This request has already been processed.")

    now = datetime.utcnow()
    req.status = PartnerRewardStatus.Rejected
    req.verified_at = now
    req.verified_by = admin_id
    req.admin_notes = admin_notes.strip()

    # Log to AuditLog
    await audit_log.log_event(
        db, req.user_id, "reward_request_rejected",
        f"Rejected Upstox Client ID: {req.client_id}. Reason: {req.admin_notes}"
    )

    # Notify User
    user_res = await db.execute(select(User).where(User.id == req.user_id))
    user = user_res.scalar_one()
    await notify(db, user, "reward_verification_rejected", reason=req.admin_notes)

    await db.commit()
    return req
