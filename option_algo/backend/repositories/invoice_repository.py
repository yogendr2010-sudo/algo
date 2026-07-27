# backend/repositories/invoice_repository.py
from typing import Optional, List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.models import Invoice, InvoiceSequence


class InvoiceRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(self, invoice: Invoice) -> Invoice:
        self.db.add(invoice)
        await self.db.flush()
        return invoice

    async def get_by_id(self, invoice_id: int) -> Optional[Invoice]:
        res = await self.db.execute(select(Invoice).where(Invoice.id == invoice_id))
        return res.scalar_one_or_none()

    async def get_by_payment_id(self, payment_id: int) -> Optional[Invoice]:
        res = await self.db.execute(select(Invoice).where(Invoice.payment_id == payment_id))
        return res.scalar_one_or_none()

    async def list_for_user(self, user_id: int) -> List[Invoice]:
        res = await self.db.execute(
            select(Invoice).where(Invoice.user_id == user_id).order_by(Invoice.issued_at.desc()))
        return list(res.scalars().all())

    async def list_all(self) -> List[Invoice]:
        res = await self.db.execute(select(Invoice).order_by(Invoice.issued_at.desc()))
        return list(res.scalars().all())

    async def next_sequence_number(self, year: int) -> int:
        """
        Atomically increments and returns the next invoice sequence
        number for `year`. Must be called inside the same transaction
        that creates the Invoice row to avoid a MAX()+1 race.
        """
        res = await self.db.execute(select(InvoiceSequence).where(InvoiceSequence.year == year))
        seq = res.scalar_one_or_none()
        if seq is None:
            seq = InvoiceSequence(year=year, last_number=0)
            self.db.add(seq)
            await self.db.flush()
        seq.last_number += 1
        await self.db.flush()
        return seq.last_number
