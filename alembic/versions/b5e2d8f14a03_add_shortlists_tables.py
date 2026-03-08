"""add shortlists tables

Creates the ``shortlists`` and ``shortlist_players`` tables used by
the player-shortlisting feature in the marketplace API.

Revision ID: b5e2d8f14a03
Revises: a3f1c7d92e01
Create Date: 2026-03-08 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b5e2d8f14a03"
down_revision: Union[str, None] = "a3f1c7d92e01"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "shortlists",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
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
    )

    op.create_table(
        "shortlist_players",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "shortlist_id",
            sa.Integer(),
            sa.ForeignKey("shortlists.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "player_id",
            sa.Integer(),
            sa.ForeignKey("players.id"),
            nullable=False,
        ),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=True),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
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
            "shortlist_id", "player_id", name="uq_shortlist_player"
        ),
    )

    op.create_index(
        "ix_shortlist_players_shortlist_id",
        "shortlist_players",
        ["shortlist_id"],
    )
    op.create_index(
        "ix_shortlist_players_player_id",
        "shortlist_players",
        ["player_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_shortlist_players_player_id", table_name="shortlist_players")
    op.drop_index("ix_shortlist_players_shortlist_id", table_name="shortlist_players")
    op.drop_table("shortlist_players")
    op.drop_table("shortlists")
