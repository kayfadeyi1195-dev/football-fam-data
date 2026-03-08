"""add player confidence columns

Adds ``overall_confidence`` (Numeric 3,2) and ``confidence_detail``
(JSONB) to the ``players`` table for the nightly confidence-scoring
pipeline.

Revision ID: d8b4f1a36c05
Revises: c7a3e9f25b04
Create Date: 2026-03-08 16:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d8b4f1a36c05"
down_revision: Union[str, None] = "c7a3e9f25b04"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "players",
        sa.Column("overall_confidence", sa.Numeric(3, 2), nullable=True),
    )
    op.add_column(
        "players",
        sa.Column(
            "confidence_detail",
            sa.dialects.postgresql.JSONB(),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("players", "confidence_detail")
    op.drop_column("players", "overall_confidence")
