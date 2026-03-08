"""add player full-text search

Adds a ``search_vector`` tsvector column to the ``players`` table,
a GIN index for fast full-text queries, and a trigger that keeps
the vector in sync on every INSERT / UPDATE.

Also adds supplementary B-tree indexes for the most common filter
columns used alongside text search.

Revision ID: a3f1c7d92e01
Revises: d267a5efed06
Create Date: 2026-03-08 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a3f1c7d92e01"
down_revision: Union[str, None] = "d267a5efed06"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── 1. tsvector column ────────────────────────────────────────────
    op.execute("""
        ALTER TABLE players
        ADD COLUMN search_vector tsvector;
    """)

    # ── 2. Back-fill existing rows ────────────────────────────────────
    op.execute("""
        UPDATE players SET search_vector =
            setweight(to_tsvector('english', coalesce(full_name, '')), 'A') ||
            setweight(to_tsvector('english', coalesce(position_detail, '')), 'B') ||
            setweight(to_tsvector('english', coalesce(nationality, '')), 'B') ||
            setweight(to_tsvector('english', coalesce(bio, '')), 'C');
    """)

    # ── 3. GIN index ──────────────────────────────────────────────────
    op.execute("""
        CREATE INDEX ix_players_search_vector
        ON players USING GIN (search_vector);
    """)

    # ── 4. Trigger function ───────────────────────────────────────────
    op.execute("""
        CREATE OR REPLACE FUNCTION players_search_vector_update() RETURNS trigger AS $$
        BEGIN
            NEW.search_vector :=
                setweight(to_tsvector('english', coalesce(NEW.full_name, '')), 'A') ||
                setweight(to_tsvector('english', coalesce(NEW.position_detail, '')), 'B') ||
                setweight(to_tsvector('english', coalesce(NEW.nationality, '')), 'B') ||
                setweight(to_tsvector('english', coalesce(NEW.bio, '')), 'C');
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    # ── 5. Attach trigger ─────────────────────────────────────────────
    op.execute("""
        CREATE TRIGGER trg_players_search_vector
        BEFORE INSERT OR UPDATE OF full_name, position_detail, nationality, bio
        ON players
        FOR EACH ROW
        EXECUTE FUNCTION players_search_vector_update();
    """)

    # ── 6. Supplementary B-tree indexes for filter columns ────────────
    #    ix_players_position_primary and ix_players_current_club_id
    #    already exist from the initial migration.
    op.create_index(
        "ix_players_date_of_birth",
        "players",
        ["date_of_birth"],
    )
    op.create_index(
        "ix_players_availability",
        "players",
        ["availability"],
    )
    op.create_index(
        "ix_players_nationality",
        "players",
        ["nationality"],
    )


def downgrade() -> None:
    op.drop_index("ix_players_nationality", table_name="players")
    op.drop_index("ix_players_availability", table_name="players")
    op.drop_index("ix_players_date_of_birth", table_name="players")

    op.execute("DROP TRIGGER IF EXISTS trg_players_search_vector ON players;")
    op.execute("DROP FUNCTION IF EXISTS players_search_vector_update();")
    op.execute("DROP INDEX IF EXISTS ix_players_search_vector;")
    op.execute("ALTER TABLE players DROP COLUMN IF EXISTS search_vector;")
