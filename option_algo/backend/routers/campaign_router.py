# backend/routers/campaign_router.py
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.database import get_db
from backend.db.models import User, PartnerRewardStatus
from backend.services.auth_service import get_current_user, get_admin_user
from backend.services import campaign_service

router = APIRouter(prefix="/api/campaign", tags=["campaign"])


# ── Pydantic Schemas ───────────────────────────────────────────────

class CampaignSettingsOut(BaseModel):
    enable_campaign: bool
    campaign_title: str
    campaign_description: str
    partner_referral_url: str
    button_text: str
    banner_image: str
    terms_conditions: str
    campaign_start_date: Optional[datetime]
    campaign_end_date: Optional[datetime]

    class Config:
        from_attributes = True


class CampaignSettingsUpdateIn(BaseModel):
    enable_campaign: bool = False
    campaign_title: str = Field(..., min_length=1, max_length=255)
    campaign_description: str = Field(...)
    partner_referral_url: str = Field(...)
    button_text: str = Field("Open Upstox Account", min_length=1, max_length=100)
    banner_image: Optional[str] = ""
    terms_conditions: Optional[str] = ""
    campaign_start_date: Optional[datetime] = None
    campaign_end_date: Optional[datetime] = None


class RewardRequestSubmitIn(BaseModel):
    client_id: str = Field(..., min_length=1, max_length=100)
    comments: Optional[str] = ""
    confirm_link: bool

    @field_validator("confirm_link")
    @classmethod
    def validate_confirm_link(cls, v: bool) -> bool:
        if not v:
            raise ValueError("You must confirm that you opened the account using the official partner link.")
        return v

    @field_validator("client_id", mode="before")
    @classmethod
    def clean_client_id(cls, v):
        if isinstance(v, str):
            cleaned = v.strip().upper()
            if not cleaned:
                raise ValueError("Client ID cannot be empty.")
            return cleaned
        return v


class VerifyRequestIn(BaseModel):
    admin_notes: Optional[str] = ""


class RejectRequestIn(BaseModel):
    admin_notes: str = Field(..., min_length=1, description="Rejection reason is required")


# ── Public Campaign Routes ──────────────────────────────────────────

@router.get("/settings", response_model=CampaignSettingsOut)
async def get_settings(db: AsyncSession = Depends(get_db)):
    """Retrieve public campaign settings."""
    row = await campaign_service.get_campaign_settings(db)
    return row


@router.get("/my-request")
async def get_my_request(
    user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)
):
    """Retrieve the current user's request details."""
    req = await campaign_service.get_user_request(db, user.id)
    if not req:
        return None
    return {
        "id": req.id,
        "client_id": req.client_id,
        "comments": req.comments,
        "status": req.status.value,
        "submitted_at": req.submitted_at.isoformat(),
        "reward_start_date": req.reward_start_date.isoformat() if req.reward_start_date else None,
        "reward_end_date": req.reward_end_date.isoformat() if req.reward_end_date else None,
        "admin_notes": req.admin_notes
    }


@router.post("/submit")
async def submit_request(
    body: RewardRequestSubmitIn,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Submit Upstox Client ID to claim paper trading reward."""
    req = await campaign_service.submit_reward_request(
        db, user.id, body.client_id, body.comments
    )
    return {
        "ok": True,
        "id": req.id,
        "status": req.status.value,
        "client_id": req.client_id
    }


# ── Admin Campaign Routes ───────────────────────────────────────────

@router.get("/admin/settings", response_model=CampaignSettingsOut)
async def admin_get_settings(
    admin: User = Depends(get_admin_user), db: AsyncSession = Depends(get_db)
):
    """Retrieve campaign settings for admin settings form."""
    row = await campaign_service.get_campaign_settings(db)
    return row


@router.put("/admin/settings", response_model=CampaignSettingsOut)
async def admin_update_settings(
    body: CampaignSettingsUpdateIn,
    admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db)
):
    """Update campaign settings (admin only)."""
    row = await campaign_service.update_campaign_settings(db, body.model_dump())
    return row


@router.get("/admin/requests")
async def admin_list_requests(
    status: Optional[PartnerRewardStatus] = None,
    q: Optional[str] = None,
    admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db)
):
    """List and search all referral reward requests (admin only)."""
    requests = await campaign_service.list_reward_requests(db, status, q)
    return requests


@router.post("/admin/requests/{request_id}/verify")
async def admin_verify_request(
    request_id: int,
    body: VerifyRequestIn,
    admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db)
):
    """Verify and approve a reward request, activating 15 days trial subscription (admin only)."""
    req = await campaign_service.verify_reward_request(
        db, request_id, admin.id, body.admin_notes
    )
    return {
        "ok": True,
        "id": req.id,
        "status": req.status.value,
        "reward_start_date": req.reward_start_date.isoformat() if req.reward_start_date else None,
        "reward_end_date": req.reward_end_date.isoformat() if req.reward_end_date else None
    }


@router.post("/admin/requests/{request_id}/reject")
async def admin_reject_request(
    request_id: int,
    body: RejectRequestIn,
    admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db)
):
    """Reject a reward request (admin only)."""
    req = await campaign_service.reject_reward_request(
        db, request_id, admin.id, body.admin_notes
    )
    return {
        "ok": True,
        "id": req.id,
        "status": req.status.value
    }
