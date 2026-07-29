# backend/services/audit_log.py
# ================================================================
# Generic activity log for subscription/billing/security events.
# No such table existed anywhere in this codebase before this
# feature (see backend.db.models.AuditLog) — called from every
# subscription/payment mutation point (trial start, payment
# verified, subscription activated/renewed/upgraded/downgraded/
# cancelled, invoice generated, admin actions, etc).
# ================================================================

import json
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.models import AuditLog


async def log_event(db: AsyncSession, user_id: Optional[int], event_type: str,
                     description: str, ip_address: Optional[str] = None,
                     metadata: Optional[dict] = None) -> AuditLog:
    entry = AuditLog(
        user_id=user_id,
        event_type=event_type,
        description=description,
        ip_address=ip_address,
        log_metadata=json.dumps(metadata, default=str) if metadata else None,
    )
    db.add(entry)
    await db.flush()
    return entry
