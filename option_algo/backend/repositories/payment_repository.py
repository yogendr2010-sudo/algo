# backend/repositories/payment_repository.py
from typing import Optional, List
from datetime import datetime
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.models import Payment, PaymentLog, PaymentStatus


class PaymentRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(self, payment: Payment) -> Payment:
        self.db.add(payment)
        await self.db.flush()
        return payment

    async def get_by_id(self, payment_id: int) -> Optional[Payment]:
        res = await self.db.execute(select(Payment).where(Payment.id == payment_id))
        return res.scalar_one_or_none()

    async def get_by_order_id(self, order_id: str) -> Optional[Payment]:
        res = await self.db.execute(select(Payment).where(Payment.razorpay_order_id == order_id))
        return res.scalar_one_or_none()

    async def get_by_utr(self, utr_number: str) -> Optional[Payment]:
        """Looks up a payment by UTR number."""
        if not utr_number:
            return None
        res = await self.db.execute(
            select(Payment).where(Payment.utr_number == utr_number))
        return res.scalar_one_or_none()

    async def list_manual_pending(self) -> List[Payment]:
        """Lists all manual payments pending verification."""
        from backend.db.models import PaymentProvider
        res = await self.db.execute(
            select(Payment)
            .where(Payment.payment_provider == PaymentProvider.MANUAL.value)
            .order_by(Payment.created_at.desc()))
        return list(res.scalars().all())

    async def list_for_user(self, user_id: int) -> List[Payment]:
        res = await self.db.execute(
            select(Payment).where(Payment.user_id == user_id).order_by(Payment.created_at.desc()))
        return list(res.scalars().all())

    async def list_all(self, search: Optional[str] = None, status: Optional[str] = None) -> List[Payment]:
        stmt = select(Payment).order_by(Payment.created_at.desc())
        if status:
            stmt = stmt.where(Payment.status == status)
        res = await self.db.execute(stmt)
        return list(res.scalars().all())

    async def add_log(self, log: PaymentLog) -> PaymentLog:
        self.db.add(log)
        await self.db.flush()
        return log

    async def event_already_processed(self, razorpay_event_id: str) -> bool:
        if not razorpay_event_id:
            return False
        res = await self.db.execute(
            select(PaymentLog.id).where(PaymentLog.razorpay_event_id == razorpay_event_id))
        return res.scalar_one_or_none() is not None

    async def revenue_summary(self, start: Optional[datetime] = None, end: Optional[datetime] = None) -> dict:
        stmt = select(func.count(Payment.id), func.sum(Payment.total_amount)).where(
            Payment.status == PaymentStatus.success)
        if start:
            stmt = stmt.where(Payment.paid_at >= start)
        if end:
            stmt = stmt.where(Payment.paid_at <= end)
        row = (await self.db.execute(stmt)).one()
        return {"count": row[0] or 0, "total": float(row[1] or 0)}
