# backend/repositories/subscription_repository.py
from typing import Optional, List
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.models import Subscription, CustomSubscription, CustomSubscriptionSymbol


class SubscriptionRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_current(self, user_id: int) -> Optional[Subscription]:
        stmt = (select(Subscription)
                .where(Subscription.user_id == user_id, Subscription.is_current.is_(True))
                .order_by(Subscription.created_at.desc()))
        res = await self.db.execute(stmt)
        return res.scalars().first()

    async def list_for_user(self, user_id: int) -> List[Subscription]:
        stmt = (select(Subscription)
                .where(Subscription.user_id == user_id)
                .order_by(Subscription.created_at.desc()))
        res = await self.db.execute(stmt)
        return list(res.scalars().all())

    async def get_by_id(self, sub_id: int) -> Optional[Subscription]:
        res = await self.db.execute(select(Subscription).where(Subscription.id == sub_id))
        return res.scalar_one_or_none()

    async def clear_current(self, user_id: int) -> None:
        await self.db.execute(
            update(Subscription)
            .where(Subscription.user_id == user_id, Subscription.is_current.is_(True))
            .values(is_current=False))

    async def create(self, sub: Subscription) -> Subscription:
        self.db.add(sub)
        await self.db.flush()
        return sub

    async def list_expiring_between(self, start, end):
        stmt = (select(Subscription)
                .where(Subscription.is_current.is_(True),
                       Subscription.end_date >= start,
                       Subscription.end_date <= end))
        res = await self.db.execute(stmt)
        return list(res.scalars().all())

    async def list_past_end_date(self, now):
        stmt = (select(Subscription)
                .where(Subscription.is_current.is_(True),
                       Subscription.end_date < now,
                       Subscription.status.in_(["trial", "active", "pending_payment"])))
        res = await self.db.execute(stmt)
        return list(res.scalars().all())

    async def get_custom(self, custom_id: int) -> Optional[CustomSubscription]:
        res = await self.db.execute(
            select(CustomSubscription).where(CustomSubscription.id == custom_id))
        return res.scalar_one_or_none()

    async def list_custom_for_user(self, user_id: int) -> List[CustomSubscription]:
        res = await self.db.execute(
            select(CustomSubscription).where(CustomSubscription.user_id == user_id)
            .order_by(CustomSubscription.created_at.desc()))
        return list(res.scalars().all())

    async def create_custom(self, custom: CustomSubscription) -> CustomSubscription:
        self.db.add(custom)
        await self.db.flush()
        return custom

    async def add_custom_symbol(self, row: CustomSubscriptionSymbol) -> CustomSubscriptionSymbol:
        self.db.add(row)
        await self.db.flush()
        return row
