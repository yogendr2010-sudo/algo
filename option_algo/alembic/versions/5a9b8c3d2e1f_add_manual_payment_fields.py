"""
Add Manual Payment fields to payments + payment settings to billing_settings

Revision ID: 5a9b8c3d2e1f
Revises: 46bcd527f8bf
Create Date: 2026-07-26 12:00:00.000000

Adds:
  - payments.payment_provider (MANUAL / RAZORPAY)
  - payments.utr_number (nullable, unique)
  - payments.screenshot_path (nullable)
  - payments.verified_by (FK → users.id, nullable)
  - payments.verified_at (nullable)
  - payments.remarks (nullable)
  - Make payments.razorpay_order_id nullable
  - Make payments.razorpay_payment_id nullable
  - Make payments.razorpay_signature nullable
  - billing_settings.payment_mode (MANUAL / RAZORPAY)
  - billing_settings.manual_upi_id (nullable)
  - billing_settings.manual_qr_code_path (nullable)
  - billing_settings.manual_instructions (nullable)

Backward-compatible: existing Razorpay rows get payment_provider='RAZORPAY',
new nullable fields are NULL, and the BillingSettings row gets defaults.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "5a9b8c3d2e1f"
down_revision: Union[str, None] = "46bcd527f8bf"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    is_postgres = bind.engine.name == "postgresql"
    
    # ── 1. Add new columns to payments table ─────────────────────
    
    # payment_provider: MANUAL or RAZORPAY (default RAZORPAY for existing rows)
    op.add_column(
        "payments",
        sa.Column("payment_provider", sa.String(length=20), nullable=False, server_default="RAZORPAY"),
    )
    
    # utr_number: unique transaction reference for manual payments
    op.add_column(
        "payments",
        sa.Column("utr_number", sa.String(length=100), nullable=True),
    )
    op.create_index(op.f("ix_payments_utr_number"), "payments", ["utr_number"], unique=True)
    
    # screenshot_path: uploaded payment screenshot
    op.add_column(
        "payments",
        sa.Column("screenshot_path", sa.String(length=512), nullable=True),
    )
    
    # verified_by: admin who approved/rejected
    op.add_column(
        "payments",
        sa.Column("verified_by", sa.Integer(), sa.ForeignKey("users.id"), nullable=True),
    )
    
    # verified_at: when the manual payment was verified
    op.add_column(
        "payments",
        sa.Column("verified_at", sa.DateTime(), nullable=True),
    )
    
    # remarks: admin remarks (rejection reason, etc.)
    op.add_column(
        "payments",
        sa.Column("remarks", sa.Text(), nullable=True),
    )
    
    # Make Razorpay-specific fields nullable (manual payments don't use them)
    if is_postgres:
        op.alter_column("payments", "razorpay_order_id", nullable=True)
        op.alter_column("payments", "razorpay_payment_id", nullable=True)
        op.alter_column("payments", "razorpay_signature", nullable=True)
    # SQLite needs full column recreation for nullable changes — skip for simplicity,
    # the columns are already nullable at the ORM level.

    # ── 2. Add payment settings to billing_settings ──────────────
    op.add_column(
        "billing_settings",
        sa.Column("payment_mode", sa.String(length=20), nullable=False, server_default="RAZORPAY"),
    )
    op.add_column(
        "billing_settings",
        sa.Column("manual_upi_id", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "billing_settings",
        sa.Column("manual_qr_code_path", sa.String(length=512), nullable=True),
    )
    op.add_column(
        "billing_settings",
        sa.Column("manual_instructions", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    bind = op.get_bind()
    is_postgres = bind.engine.name == "postgresql"

    # ── 1. Remove billing_settings columns ───────────────────────
    op.drop_column("billing_settings", "manual_instructions")
    op.drop_column("billing_settings", "manual_qr_code_path")
    op.drop_column("billing_settings", "manual_upi_id")
    op.drop_column("billing_settings", "payment_mode")

    # ── 2. Remove payments columns ───────────────────────────────
    if is_postgres:
        op.alter_column("payments", "razorpay_order_id", nullable=False)
        op.alter_column("payments", "razorpay_payment_id", nullable=True)
        op.alter_column("payments", "razorpay_signature", nullable=True)
    
    op.drop_index(op.f("ix_payments_utr_number"), table_name="payments")
    op.drop_column("payments", "remarks")
    op.drop_column("payments", "verified_at")
    op.drop_column("payments", "verified_by")
    op.drop_column("payments", "screenshot_path")
    op.drop_column("payments", "utr_number")
    op.drop_column("payments", "payment_provider")
