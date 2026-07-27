# backend/repositories/billing_profile_repository.py
from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.models import BillingProfile


class BillingProfileRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_for_user(self, user_id: int) -> Optional[BillingProfile]:
        res = await self.db.execute(select(BillingProfile).where(BillingProfile.user_id == user_id))
        return res.scalar_one_or_none()

    async def upsert(self, user_id: int, fields: dict) -> BillingProfile:
        profile = await self.get_for_user(user_id)
        if profile is None:
            profile = BillingProfile(user_id=user_id, **fields)
            self.db.add(profile)
        else:
            for k, v in fields.items():
                setattr(profile, k, v)
            self.db.add(profile)
        await self.db.flush()
        return profile
