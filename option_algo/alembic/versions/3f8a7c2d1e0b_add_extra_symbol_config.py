"""
Add extra_symbol_config column to bot_configs

Revision ID: 3f8a7c2d1e0b
Revises: 46bcd527f8bf
Create Date: 2026-08-01 12:00:00.000000

Adds:
  - bot_configs.extra_symbol_config (JSON Text) — stores per-symbol
    independent lot configuration for additional trading symbols.
    Each entry: {"symbol":"BANKNIFTY","enabled":true,"trade_mode":"SEMI_AUTO","lots":2}

Backward-compatible: existing rows get NULL, which the code treats
as "no additional symbol configs" (falls back to order_qty for main
symbol and defaults for extra symbols).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "3f8a7c2d1e0b"
down_revision: Union[str, None] = "46bcd527f8bf"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add extra_symbol_config column to bot_configs
    # JSON Text field — nullable so existing rows are unaffected
    op.add_column(
        "bot_configs",
        sa.Column(
            "extra_symbol_config",
            sa.Text(),
            nullable=True,
            comment="JSON array of per-symbol configs: symbol, enabled, trade_mode, lots",
        ),
    )


def downgrade() -> None:
    op.drop_column("bot_configs", "extra_symbol_config")

