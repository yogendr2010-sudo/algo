# backend/repositories/plan_repository.py
from typing import Optional, List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from backend.db.models import SubscriptionPlan, SubscriptionPlanSymbol


class PlanRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def list(self, active_only: bool = False) -> List[SubscriptionPlan]:
        stmt = select(SubscriptionPlan).options(selectinload(SubscriptionPlan.symbols))
        if active_only:
            stmt = stmt.where(SubscriptionPlan.is_active.is_(True))
        stmt = stmt.order_by(SubscriptionPlan.monthly_price)
        res = await self.db.execute(stmt)
        return list(res.scalars().unique().all())

    async def get_by_id(self, plan_id: int) -> Optional[SubscriptionPlan]:
        stmt = (select(SubscriptionPlan)
                .options(selectinload(SubscriptionPlan.symbols))
                .where(SubscriptionPlan.id == plan_id))
        res = await self.db.execute(stmt)
        return res.scalar_one_or_none()

    async def get_by_code(self, plan_code: str) -> Optional[SubscriptionPlan]:
        stmt = (select(SubscriptionPlan)
                .options(selectinload(SubscriptionPlan.symbols))
                .where(SubscriptionPlan.plan_code == plan_code))
        res = await self.db.execute(stmt)
        return res.scalar_one_or_none()

    async def create(self, plan: SubscriptionPlan) -> SubscriptionPlan:
        self.db.add(plan)
        await self.db.flush()
        return plan

    async def add_symbol(self, symbol_row: SubscriptionPlanSymbol) -> SubscriptionPlanSymbol:
        self.db.add(symbol_row)
        await self.db.flush()
        return symbol_row

    async def delete_symbols(self, plan_id: int) -> None:
        res = await self.db.execute(
            select(SubscriptionPlanSymbol).where(SubscriptionPlanSymbol.plan_id == plan_id))
        for row in res.scalars().all():
            await self.db.delete(row)

    async def delete(self, plan: SubscriptionPlan) -> None:
        await self.db.delete(plan)
