"""SQLAlchemy ORM models for the Football Fam database.

Fourteen tables covering the English football pyramid (Steps 1-6):
leagues, clubs, players, player_seasons, player_career, matches,
match_appearances, player_media, staging_raw, data_source_runs,
shortlists, shortlist_players, merge_candidates.

Alembic uses these models to auto-generate migration scripts.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import enum

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class PositionPrimary(str, enum.Enum):
    GK = "GK"
    DEF = "DEF"
    MID = "MID"
    FWD = "FWD"


class PreferredFoot(str, enum.Enum):
    L = "L"
    R = "R"
    B = "B"


class ContractStatus(str, enum.Enum):
    CONTRACTED = "contracted"
    OUT_OF_CONTRACT = "out_of_contract"
    LOAN = "loan"
    TRIAL = "trial"
    RELEASED = "released"
    UNKNOWN = "unknown"


class Availability(str, enum.Enum):
    AVAILABLE = "available"
    NOT_AVAILABLE = "not_available"
    OPEN_TO_OFFERS = "open_to_offers"
    UNKNOWN = "unknown"


class CareerRole(str, enum.Enum):
    PLAYER = "player"
    LOAN = "loan"
    TRIAL = "trial"
    YOUTH = "youth"


class MediaType(str, enum.Enum):
    VIDEO = "video"
    IMAGE = "image"
    DOCUMENT = "document"


class RunStatus(str, enum.Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class MergeStatus(str, enum.Enum):
    PENDING = "pending"
    MERGED = "merged"
    REJECTED = "rejected"


class UpdateStatus(str, enum.Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    """Base class for all ORM models."""
    pass


class TimestampMixin:
    """Adds created_at / updated_at columns to any model that inherits it."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


# ---------------------------------------------------------------------------
# 1. leagues
# ---------------------------------------------------------------------------

class League(TimestampMixin, Base):
    """A league within the English football pyramid (e.g. National League South)."""

    __tablename__ = "leagues"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    short_name: Mapped[Optional[str]] = mapped_column(String(50))
    step: Mapped[int] = mapped_column(Integer, nullable=False)
    region: Mapped[Optional[str]] = mapped_column(String(100))
    division: Mapped[Optional[str]] = mapped_column(String(100))
    parent_league_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("leagues.id"),
    )
    season: Mapped[Optional[str]] = mapped_column(String(10))
    fa_competition_code: Mapped[Optional[str]] = mapped_column(String(50))

    parent_league: Mapped[Optional["League"]] = relationship(
        remote_side="League.id",
        back_populates="child_leagues",
    )
    child_leagues: Mapped[list["League"]] = relationship(
        back_populates="parent_league",
    )
    clubs: Mapped[list["Club"]] = relationship(back_populates="league")

    __table_args__ = (
        UniqueConstraint("name", "season", name="uq_league_name_season"),
    )

    def __repr__(self) -> str:
        return f"<League {self.name} (Step {self.step})>"


# ---------------------------------------------------------------------------
# 2. clubs
# ---------------------------------------------------------------------------

class Club(TimestampMixin, Base):
    """A football club competing in the lower leagues."""

    __tablename__ = "clubs"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    short_name: Mapped[Optional[str]] = mapped_column(String(50))
    league_id: Mapped[Optional[int]] = mapped_column(ForeignKey("leagues.id"))
    ground_name: Mapped[Optional[str]] = mapped_column(String(200))
    postcode: Mapped[Optional[str]] = mapped_column(String(10))
    latitude: Mapped[Optional[Decimal]] = mapped_column(Numeric(9, 6))
    longitude: Mapped[Optional[Decimal]] = mapped_column(Numeric(9, 6))
    website_url: Mapped[Optional[str]] = mapped_column(String(300))
    pitchero_url: Mapped[Optional[str]] = mapped_column(String(300))
    twitter_url: Mapped[Optional[str]] = mapped_column(String(300))
    facebook_url: Mapped[Optional[str]] = mapped_column(String(300))
    instagram_url: Mapped[Optional[str]] = mapped_column(String(300))
    contact_email: Mapped[Optional[str]] = mapped_column(String(254))
    logo_url: Mapped[Optional[str]] = mapped_column(String(500))
    api_football_id: Mapped[Optional[int]] = mapped_column(Integer)
    is_active: Mapped[bool] = mapped_column(Boolean, server_default="true")

    league: Mapped[Optional["League"]] = relationship(back_populates="clubs")
    players: Mapped[list["Player"]] = relationship(back_populates="current_club")

    __table_args__ = (
        UniqueConstraint("name", name="uq_club_name"),
        Index("ix_clubs_league_id", "league_id"),
    )

    def __repr__(self) -> str:
        return f"<Club {self.name}>"


# ---------------------------------------------------------------------------
# 3. players
# ---------------------------------------------------------------------------

class Player(TimestampMixin, Base):
    """An individual football player."""

    __tablename__ = "players"

    id: Mapped[int] = mapped_column(primary_key=True)
    full_name: Mapped[str] = mapped_column(String(200), nullable=False)
    first_name: Mapped[Optional[str]] = mapped_column(String(100))
    last_name: Mapped[Optional[str]] = mapped_column(String(100))
    date_of_birth: Mapped[Optional[date]] = mapped_column(Date)
    nationality: Mapped[Optional[str]] = mapped_column(String(100))
    position_primary: Mapped[Optional[PositionPrimary]] = mapped_column(
        String(3),
    )
    position_detail: Mapped[Optional[str]] = mapped_column(String(50))
    height_cm: Mapped[Optional[int]] = mapped_column(Integer)
    weight_kg: Mapped[Optional[int]] = mapped_column(Integer)
    preferred_foot: Mapped[Optional[PreferredFoot]] = mapped_column(
        String(1),
    )
    current_club_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("clubs.id"),
    )
    contract_status: Mapped[Optional[ContractStatus]] = mapped_column(
        String(20), server_default="unknown",
    )
    availability: Mapped[Optional[Availability]] = mapped_column(
        String(20), server_default="unknown",
    )
    email: Mapped[Optional[str]] = mapped_column(String(254))
    phone: Mapped[Optional[str]] = mapped_column(String(30))
    profile_photo_url: Mapped[Optional[str]] = mapped_column(String(500))
    bio: Mapped[Optional[str]] = mapped_column(Text)
    is_verified: Mapped[bool] = mapped_column(Boolean, server_default="false")
    overall_confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2))
    confidence_detail: Mapped[Optional[dict]] = mapped_column(JSONB)
    merged_into_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("players.id"),
    )
    search_vector = mapped_column(TSVECTOR)

    current_club: Mapped[Optional["Club"]] = relationship(back_populates="players")
    seasons: Mapped[list["PlayerSeason"]] = relationship(back_populates="player")
    career_entries: Mapped[list["PlayerCareer"]] = relationship(back_populates="player")
    appearances: Mapped[list["MatchAppearance"]] = relationship(back_populates="player")
    media: Mapped[list["PlayerMedia"]] = relationship(back_populates="player")

    __table_args__ = (
        Index("ix_players_full_name", "full_name"),
        Index("ix_players_position_primary", "position_primary"),
        Index("ix_players_current_club_id", "current_club_id"),
        Index("ix_players_date_of_birth", "date_of_birth"),
        Index("ix_players_availability", "availability"),
        Index("ix_players_nationality", "nationality"),
        Index("ix_players_search_vector", "search_vector", postgresql_using="gin"),
    )

    def __repr__(self) -> str:
        return f"<Player {self.full_name}>"


# ---------------------------------------------------------------------------
# 4. player_seasons
# ---------------------------------------------------------------------------

class PlayerSeason(TimestampMixin, Base):
    """Season-level aggregate statistics for a player at a specific club."""

    __tablename__ = "player_seasons"

    id: Mapped[int] = mapped_column(primary_key=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.id"), nullable=False)
    club_id: Mapped[int] = mapped_column(ForeignKey("clubs.id"), nullable=False)
    league_id: Mapped[Optional[int]] = mapped_column(ForeignKey("leagues.id"))
    season: Mapped[str] = mapped_column(String(10), nullable=False)
    appearances: Mapped[Optional[int]] = mapped_column(Integer)
    starts: Mapped[Optional[int]] = mapped_column(Integer)
    sub_appearances: Mapped[Optional[int]] = mapped_column(Integer)
    goals: Mapped[Optional[int]] = mapped_column(Integer)
    assists: Mapped[Optional[int]] = mapped_column(Integer)
    yellow_cards: Mapped[Optional[int]] = mapped_column(Integer)
    red_cards: Mapped[Optional[int]] = mapped_column(Integer)
    clean_sheets: Mapped[Optional[int]] = mapped_column(Integer)
    minutes_played: Mapped[Optional[int]] = mapped_column(Integer)
    data_source: Mapped[Optional[str]] = mapped_column(String(50))
    confidence_score: Mapped[Optional[int]] = mapped_column(Integer)

    player: Mapped["Player"] = relationship(back_populates="seasons")
    club: Mapped["Club"] = relationship()
    league: Mapped[Optional["League"]] = relationship()

    __table_args__ = (
        UniqueConstraint(
            "player_id", "club_id", "season", "data_source",
            name="uq_player_club_season_source",
        ),
        Index("ix_player_seasons_player_season", "player_id", "season"),
    )

    def __repr__(self) -> str:
        return f"<PlayerSeason player={self.player_id} season={self.season}>"


# ---------------------------------------------------------------------------
# 5. player_career
# ---------------------------------------------------------------------------

class PlayerCareer(TimestampMixin, Base):
    """A career stint linking a player to a club over a date range."""

    __tablename__ = "player_career"

    id: Mapped[int] = mapped_column(primary_key=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.id"), nullable=False)
    club_id: Mapped[int] = mapped_column(ForeignKey("clubs.id"), nullable=False)
    season_start: Mapped[str] = mapped_column(String(10), nullable=False)
    season_end: Mapped[Optional[str]] = mapped_column(String(10))
    role: Mapped[Optional[CareerRole]] = mapped_column(String(10))
    source: Mapped[Optional[str]] = mapped_column(String(50))

    player: Mapped["Player"] = relationship(back_populates="career_entries")
    club: Mapped["Club"] = relationship()

    def __repr__(self) -> str:
        return f"<PlayerCareer player={self.player_id} club={self.club_id} {self.season_start}>"


# ---------------------------------------------------------------------------
# 6. matches
# ---------------------------------------------------------------------------

class Match(TimestampMixin, Base):
    """A single football match."""

    __tablename__ = "matches"

    id: Mapped[int] = mapped_column(primary_key=True)
    league_id: Mapped[Optional[int]] = mapped_column(ForeignKey("leagues.id"))
    home_club_id: Mapped[int] = mapped_column(ForeignKey("clubs.id"), nullable=False)
    away_club_id: Mapped[int] = mapped_column(ForeignKey("clubs.id"), nullable=False)
    match_date: Mapped[Optional[date]] = mapped_column(Date)
    home_score: Mapped[Optional[int]] = mapped_column(Integer)
    away_score: Mapped[Optional[int]] = mapped_column(Integer)
    attendance: Mapped[Optional[int]] = mapped_column(Integer)
    venue: Mapped[Optional[str]] = mapped_column(String(200))
    external_id: Mapped[Optional[str]] = mapped_column(String(100))
    data_source: Mapped[Optional[str]] = mapped_column(String(50))

    league: Mapped[Optional["League"]] = relationship()
    home_club: Mapped["Club"] = relationship(foreign_keys=[home_club_id])
    away_club: Mapped["Club"] = relationship(foreign_keys=[away_club_id])
    appearances: Mapped[list["MatchAppearance"]] = relationship(back_populates="match")

    def __repr__(self) -> str:
        return f"<Match {self.home_club_id} vs {self.away_club_id} on {self.match_date}>"


# ---------------------------------------------------------------------------
# 7. match_appearances
# ---------------------------------------------------------------------------

class MatchAppearance(TimestampMixin, Base):
    """An individual player's appearance in a specific match."""

    __tablename__ = "match_appearances"

    id: Mapped[int] = mapped_column(primary_key=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.id"), nullable=False)
    match_id: Mapped[int] = mapped_column(ForeignKey("matches.id"), nullable=False)
    club_id: Mapped[int] = mapped_column(ForeignKey("clubs.id"), nullable=False)
    started: Mapped[Optional[bool]] = mapped_column(Boolean)
    minutes_played: Mapped[Optional[int]] = mapped_column(Integer)
    goals: Mapped[Optional[int]] = mapped_column(Integer)
    assists: Mapped[Optional[int]] = mapped_column(Integer)
    yellow_card: Mapped[Optional[bool]] = mapped_column(Boolean)
    red_card: Mapped[Optional[bool]] = mapped_column(Boolean)
    rating: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 2))
    data_source: Mapped[Optional[str]] = mapped_column(String(50))

    player: Mapped["Player"] = relationship(back_populates="appearances")
    match: Mapped["Match"] = relationship(back_populates="appearances")
    club: Mapped["Club"] = relationship()

    def __repr__(self) -> str:
        return f"<MatchAppearance player={self.player_id} match={self.match_id}>"


# ---------------------------------------------------------------------------
# 8. player_media
# ---------------------------------------------------------------------------

class PlayerMedia(TimestampMixin, Base):
    """Media assets (video highlights, images, documents) linked to a player."""

    __tablename__ = "player_media"

    id: Mapped[int] = mapped_column(primary_key=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.id"), nullable=False)
    media_type: Mapped[MediaType] = mapped_column(String(10), nullable=False)
    url: Mapped[str] = mapped_column(String(500), nullable=False)
    title: Mapped[Optional[str]] = mapped_column(String(200))
    description: Mapped[Optional[str]] = mapped_column(Text)
    uploaded_by: Mapped[Optional[str]] = mapped_column(String(100))
    is_primary: Mapped[bool] = mapped_column(Boolean, server_default="false")

    player: Mapped["Player"] = relationship(back_populates="media")

    def __repr__(self) -> str:
        return f"<PlayerMedia {self.media_type} player={self.player_id}>"


# ---------------------------------------------------------------------------
# 9. staging_raw
# ---------------------------------------------------------------------------

class StagingRaw(TimestampMixin, Base):
    """Raw records landed from any data source before transformation.

    Stores the original payload as JSONB so we can always re-process.
    """

    __tablename__ = "staging_raw"

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    source_entity_type: Mapped[str] = mapped_column(String(50), nullable=False)
    raw_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    external_id: Mapped[Optional[str]] = mapped_column(String(100))
    processed: Mapped[bool] = mapped_column(Boolean, server_default="false")
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = (
        UniqueConstraint(
            "source", "source_entity_type", "external_id",
            name="uq_staging_source_type_extid",
        ),
        Index("ix_staging_raw_source_processed", "source", "processed"),
    )

    def __repr__(self) -> str:
        return f"<StagingRaw {self.source}/{self.source_entity_type} ext={self.external_id}>"


# ---------------------------------------------------------------------------
# 10. data_source_runs
# ---------------------------------------------------------------------------

class DataSourceRun(TimestampMixin, Base):
    """Tracks each execution of a data-fetching pipeline."""

    __tablename__ = "data_source_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    run_type: Mapped[str] = mapped_column(String(50), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    records_fetched: Mapped[Optional[int]] = mapped_column(Integer)
    records_loaded: Mapped[Optional[int]] = mapped_column(Integer)
    records_errored: Mapped[Optional[int]] = mapped_column(Integer)
    status: Mapped[RunStatus] = mapped_column(String(10), nullable=False)
    error_log: Mapped[Optional[str]] = mapped_column(Text)

    def __repr__(self) -> str:
        return f"<DataSourceRun {self.source} {self.run_type} status={self.status}>"


# ---------------------------------------------------------------------------
# 11. shortlists
# ---------------------------------------------------------------------------

class Shortlist(TimestampMixin, Base):
    """A named collection of players curated by an agent/scout."""

    __tablename__ = "shortlists"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)

    entries: Mapped[list["ShortlistPlayer"]] = relationship(
        back_populates="shortlist",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Shortlist {self.name}>"


# ---------------------------------------------------------------------------
# 12. shortlist_players
# ---------------------------------------------------------------------------

class ShortlistPlayer(TimestampMixin, Base):
    """A player entry within a shortlist, with optional notes and priority."""

    __tablename__ = "shortlist_players"

    id: Mapped[int] = mapped_column(primary_key=True)
    shortlist_id: Mapped[int] = mapped_column(
        ForeignKey("shortlists.id", ondelete="CASCADE"), nullable=False,
    )
    player_id: Mapped[int] = mapped_column(
        ForeignKey("players.id"), nullable=False,
    )
    notes: Mapped[Optional[str]] = mapped_column(Text)
    priority: Mapped[Optional[int]] = mapped_column(Integer)
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False,
    )

    shortlist: Mapped["Shortlist"] = relationship(back_populates="entries")
    player: Mapped["Player"] = relationship()

    __table_args__ = (
        UniqueConstraint(
            "shortlist_id", "player_id",
            name="uq_shortlist_player",
        ),
        Index("ix_shortlist_players_shortlist_id", "shortlist_id"),
        Index("ix_shortlist_players_player_id", "player_id"),
    )

    def __repr__(self) -> str:
        return f"<ShortlistPlayer shortlist={self.shortlist_id} player={self.player_id}>"


# ---------------------------------------------------------------------------
# 13. merge_candidates
# ---------------------------------------------------------------------------

class MergeCandidate(TimestampMixin, Base):
    """A pair of players identified as potential duplicates.

    Status flows: pending → merged / rejected.
    """

    __tablename__ = "merge_candidates"

    id: Mapped[int] = mapped_column(primary_key=True)
    player_a_id: Mapped[int] = mapped_column(
        ForeignKey("players.id"), nullable=False,
    )
    player_b_id: Mapped[int] = mapped_column(
        ForeignKey("players.id"), nullable=False,
    )
    score: Mapped[int] = mapped_column(Integer, nullable=False)
    match_reasons: Mapped[dict] = mapped_column(JSONB, nullable=False)
    status: Mapped[MergeStatus] = mapped_column(
        String(10), server_default="pending", nullable=False,
    )
    reviewed_by: Mapped[Optional[str]] = mapped_column(String(100))
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    player_a: Mapped["Player"] = relationship(foreign_keys=[player_a_id])
    player_b: Mapped["Player"] = relationship(foreign_keys=[player_b_id])

    __table_args__ = (
        UniqueConstraint(
            "player_a_id", "player_b_id",
            name="uq_merge_candidate_pair",
        ),
        Index("ix_merge_candidates_status", "status"),
    )

    def __repr__(self) -> str:
        return (
            f"<MergeCandidate {self.player_a_id}↔{self.player_b_id} "
            f"score={self.score} status={self.status}>"
        )


# ---------------------------------------------------------------------------
# 14. pending_updates
# ---------------------------------------------------------------------------

class PendingUpdate(TimestampMixin, Base):
    """Self-reported data submitted via registration that needs human review.

    When a player self-registers and matches an existing record, the
    incoming data is stored here rather than overwriting verified data.
    An agent reviews and approves or rejects.
    """

    __tablename__ = "pending_updates"

    id: Mapped[int] = mapped_column(primary_key=True)
    player_id: Mapped[int] = mapped_column(
        ForeignKey("players.id"), nullable=False,
    )
    submitted_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    submitter_email: Mapped[Optional[str]] = mapped_column(String(254))
    submitter_phone: Mapped[Optional[str]] = mapped_column(String(30))
    status: Mapped[UpdateStatus] = mapped_column(
        String(10), server_default="pending", nullable=False,
    )
    reviewed_by: Mapped[Optional[str]] = mapped_column(String(100))
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    review_notes: Mapped[Optional[str]] = mapped_column(Text)

    player: Mapped["Player"] = relationship()

    __table_args__ = (
        Index("ix_pending_updates_status", "status"),
        Index("ix_pending_updates_player_id", "player_id"),
    )

    def __repr__(self) -> str:
        return f"<PendingUpdate player={self.player_id} status={self.status}>"
