"""
Add execution_mode, pending_trades, daily_auto_consent, audit_log

Revision ID: 46bcd527f8bf
Revises:
Create Date: 2026-07-25 19:49:12.318330

Adds:
  - users.execution_mode (default SEMI_AUTO for new registrations)
  - bot_configs.execution_mode (default PAPER for backward compatibility)
  - pending_trades table (Semi Auto approval queue)
  - daily_auto_consents table (Fully Automatic daily disclosure)
  - audit_logs table (compliance audit trail)

Backward-compatible: existing rows get SEMI_AUTO / PAPER defaults.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "46bcd527f8bf"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# ── Enum type names ──────────────────────────────────────────────
# PostgreSQL: actual ENUM type. SQLite: VARCHAR check constraint.
EXECUTION_MODE_ENUM = "executionmode"
PENDING_TRADE_STATUS_ENUM = "pendingtradestatus"


def upgrade() -> None:
    bind = op.get_bind()
    is_postgres = bind.engine.name == "postgresql"

    # ── 1. Create ENUM types (PostgreSQL only) ───────────────────
    if is_postgres:
        sa.Enum("PAPER", "SEMI_AUTO", "AUTO",
                name=EXECUTION_MODE_ENUM).create(bind)
        sa.Enum("WAITING", "APPROVED", "REJECTED", "EXPIRED",
                name=PENDING_TRADE_STATUS_ENUM).create(bind)

    # ── 2. Add execution_mode to users (default SEMI_AUTO) ───────
    # Multi-step for PostgreSQL: add nullable, set default, make NOT NULL.
    # SQLite: add nullable, cannot alter NOT NULL after creation.
    if is_postgres:
        op.add_column(
            "users",
            sa.Column(
                "execution_mode",
                sa.Enum("PAPER", "SEMI_AUTO", "AUTO", name=EXECUTION_MODE_ENUM),
                nullable=True,
            ),
        )
        op.execute("UPDATE users SET execution_mode = 'SEMI_AUTO' WHERE execution_mode IS NULL")
        op.alter_column(
            "users",
            "execution_mode",
            existing_type=sa.Enum("PAPER", "SEMI_AUTO", "AUTO", name=EXECUTION_MODE_ENUM),
            nullable=False,
            existing_server_default=None,
        )
        op.alter_column(
            "users",
            "execution_mode",
            server_default=sa.text("'SEMI_AUTO'"),
        )
    else:
        # SQLite: just add with default
        op.add_column(
            "users",
            sa.Column(
                "execution_mode",
                sa.VARCHAR(length=20),
                nullable=False,
                server_default="SEMI_AUTO",
            ),
        )

    # ── 3. Add execution_mode to bot_configs (default PAPER) ─────
    if is_postgres:
        op.add_column(
            "bot_configs",
            sa.Column(
                "execution_mode",
                sa.Enum("PAPER", "SEMI_AUTO", "AUTO", name=EXECUTION_MODE_ENUM),
                nullable=True,
            ),
        )
        op.execute("UPDATE bot_configs SET execution_mode = 'PAPER' WHERE execution_mode IS NULL")
        op.alter_column(
            "bot_configs",
            "execution_mode",
            existing_type=sa.Enum("PAPER", "SEMI_AUTO", "AUTO", name=EXECUTION_MODE_ENUM),
            nullable=False,
            existing_server_default=None,
        )
        op.alter_column(
            "bot_configs",
            "execution_mode",
            server_default=sa.text("'PAPER'"),
        )
    else:
        op.add_column(
            "bot_configs",
            sa.Column(
                "execution_mode",
                sa.VARCHAR(length=20),
                nullable=False,
                server_default="PAPER",
            ),
        )

    # ── 4. Create pending_trades table ───────────────────────────
    op.create_table(
        "pending_trades",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False, index=True),
        sa.Column("signal_id", sa.String(length=64), nullable=False, unique=True, index=True),
        sa.Column("symbol", sa.String(length=50), nullable=False, server_default=""),
        sa.Column("opt_type", sa.String(length=10), nullable=False, server_default=""),
        sa.Column("strategy", sa.String(length=50), nullable=False, server_default=""),
        sa.Column("entry_price", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("stop_loss", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("quantity", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column(
            "status",
            sa.VARCHAR(length=20) if not is_postgres
            else sa.Enum("WAITING", "APPROVED", "REJECTED", "EXPIRED",
                         name=PENDING_TRADE_STATUS_ENUM),
            nullable=False,
            server_default="WAITING",
        ),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.Column("approved_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("signal_payload", sa.Text(), nullable=True),
    )

    # ── 5. Create daily_auto_consents table ─────────────────────
    op.create_table(
        "daily_auto_consents",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False, index=True),
        sa.Column("consent_date", sa.DateTime(), nullable=False,
                  server_default=sa.func.now(), index=True),
        sa.Column("accepted", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("accepted_at", sa.DateTime(), nullable=True),
        sa.Column("valid_until", sa.DateTime(), nullable=True),
        sa.Column("risk_version", sa.String(length=20), nullable=False, server_default="v1.0"),
        sa.Column("risk_text_snapshot", sa.Text(), nullable=False, server_default=""),
        sa.Column("ip_address", sa.String(length=64), nullable=True),
        sa.Column("device_information", sa.String(length=255), nullable=True),
        sa.Column("browser_information", sa.String(length=255), nullable=True),
        sa.Column("user_agent", sa.String(length=255), nullable=True),
        sa.Column("audit_hash", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )

    # ── 6. Create audit_logs table ──────────────────────────────
    op.create_table(
        "audit_logs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=True, index=True),
        sa.Column("event_type", sa.String(length=50), nullable=False, index=True),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("ip_address", sa.String(length=64), nullable=True),
        sa.Column("log_metadata", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False,
                  server_default=sa.func.now(), index=True),
    )

    # ── 7. Existing column tweaks from autogenerate ──────────────
    op.alter_column(
        "trades",
        "mode",
        existing_type=sa.VARCHAR(length=10),
        nullable=False,
        existing_server_default=sa.text("'live'::character varying") if is_postgres else sa.text("'live'"),
    )

    op.alter_column(
        "users",
        "trial_used",
        existing_type=sa.BOOLEAN(),
        nullable=False,
        existing_server_default=sa.text("false") if is_postgres else sa.text("'0'"),
    )

    op.create_index(op.f("ix_users_mobile_number"), "users", ["mobile_number"], unique=True)
    op.create_index(op.f("ix_users_password_reset_token"), "users", ["password_reset_token"], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    is_postgres = bind.engine.name == "postgresql"

    # Drop tables in reverse dependency order
    op.drop_table("audit_logs")
    op.drop_table("daily_auto_consents")
    op.drop_table("pending_trades")

    # Drop indexes on users
    op.drop_index(op.f("ix_users_password_reset_token"), table_name="users")
    op.drop_index(op.f("ix_users_mobile_number"), table_name="users")

    # Revert alter_column on users.trial_used
    op.alter_column(
        "users",
        "trial_used",
        existing_type=sa.BOOLEAN(),
        nullable=True,
        existing_server_default=sa.text("false") if is_postgres else sa.text("'0'"),
    )

    # Remove execution_mode from users
    op.drop_column("users", "execution_mode")

    # Revert alter_column on trades.mode
    op.alter_column(
        "trades",
        "mode",
        existing_type=sa.VARCHAR(length=10),
        nullable=True,
        existing_server_default=sa.text("'live'::character varying") if is_postgres else sa.text("'live'"),
    )

    # Remove execution_mode from bot_configs
    op.drop_column("bot_configs", "execution_mode")

    # Drop ENUM types (PostgreSQL only)
    if is_postgres:
        sa.Enum("PAPER", "SEMI_AUTO", "AUTO",
                name=EXECUTION_MODE_ENUM).drop(bind)
        sa.Enum("WAITING", "APPROVED", "REJECTED", "EXPIRED",
                name=PENDING_TRADE_STATUS_ENUM).drop(bind)


