"""merge_heads

Revision ID: de1d29df9408
Revises: 3f8a7c2d1e0b, 5a9b8c3d2e1f
Create Date: 2026-07-27 09:51:55.185105

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'de1d29df9408'
down_revision: Union[str, None] = ('3f8a7c2d1e0b', '5a9b8c3d2e1f')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

