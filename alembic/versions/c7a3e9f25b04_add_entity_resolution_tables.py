"""add entity resolution tables

Adds ``merge_candidates`` table for tracking duplicate player pairs,
and a ``merged_into_id`` self-referencing FK on ``players`` for
soft-deleting merged duplicates.

Revision ID: c7a3e9f25b04
Revises: b5e2d8f14a03
Create Date: 2026-03-08 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c7a3e9f25b04"
down_revision: Union[str, None] = "b5e2d8f14a03"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # merged_into_id on players
    op.add_column(
        "players",
        sa.Column(
            "merged_into_id",
            sa.Integer(),
            sa.ForeignKey("players.id"),
            nullable=True,
        ),
    )

    # merge_candidates table
    op.create_table(
        "merge_candidates",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "player_a_id",
            sa.Integer(),
            sa.ForeignKey("players.id"),
            nullable=False,
        ),
        sa.Column(
            "player_b_id",
            sa.Integer(),
            sa.ForeignKey("players.id"),
            nullable=False,
        ),
        sa.Column("score", sa.Integer(), nullable=False),
        sa.Column("match_reasons", sa.dialects.postgresql.JSONB(), nullable=False),
        sa.Column(
            "status",
            sa.String(10),
            server_default="pending",
            nullable=False,
        ),
        sa.Column("reviewed_by", sa.String(100), nullable=True),
        sa.Column(
            "reviewed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "player_a_id", "player_b_id", name="uq_merge_candidate_pair",
        ),
    )

    op.create_index(
        "ix_merge_candidates_status",
        "merge_candidates",
        ["status"],
    )


def downgrade() -> None:
    op.drop_index("ix_merge_candidates_status", table_name="merge_candidates")
    op.drop_table("merge_candidates")
    op.drop_column("players", "merged_into_id")
