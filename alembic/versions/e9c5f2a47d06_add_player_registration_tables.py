"""add player registration tables

Adds ``email`` and ``phone`` columns to ``players``, and creates the
``pending_updates`` table for the self-registration moderation queue.

Revision ID: e9c5f2a47d06
Revises: d8b4f1a36c05
Create Date: 2026-03-08 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "e9c5f2a47d06"
down_revision: Union[str, None] = "d8b4f1a36c05"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("players", sa.Column("email", sa.String(254), nullable=True))
    op.add_column("players", sa.Column("phone", sa.String(30), nullable=True))

    op.create_table(
        "pending_updates",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("player_id", sa.Integer(), sa.ForeignKey("players.id"), nullable=False),
        sa.Column("submitted_data", postgresql.JSONB(), nullable=False),
        sa.Column("submitter_email", sa.String(254), nullable=True),
        sa.Column("submitter_phone", sa.String(30), nullable=True),
        sa.Column("status", sa.String(10), server_default="pending", nullable=False),
        sa.Column("reviewed_by", sa.String(100), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("review_notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_pending_updates_status", "pending_updates", ["status"])
    op.create_index("ix_pending_updates_player_id", "pending_updates", ["player_id"])


def downgrade() -> None:
    op.drop_index("ix_pending_updates_player_id", table_name="pending_updates")
    op.drop_index("ix_pending_updates_status", table_name="pending_updates")
    op.drop_table("pending_updates")
    op.drop_column("players", "phone")
    op.drop_column("players", "email")
