"""v1 REST API routes for the Football Fam marketplace.

Endpoints
---------
GET  /api/v1/players/search        – full-text + filtered player search
GET  /api/v1/players/compare       – side-by-side comparison (up to 5)
GET  /api/v1/players/{id}          – full player profile
GET  /api/v1/players/{id}/stats    – all season stats
GET  /api/v1/players/{id}/career   – career timeline
GET  /api/v1/players/{id}/similar  – 10 most similar players
GET  /api/v1/clubs                 – paginated club list
GET  /api/v1/clubs/{id}            – club detail with squad
GET  /api/v1/leagues               – leagues grouped by step
GET  /api/v1/stats/overview        – dashboard totals
POST /api/v1/shortlists            – create a shortlist
GET  /api/v1/shortlists            – list all shortlists
GET  /api/v1/shortlists/{id}       – shortlist detail with players
POST /api/v1/shortlists/{id}/players   – add player to shortlist
DELETE /api/v1/shortlists/{id}/players/{pid} – remove player
"""

from __future__ import annotations

import math
from datetime import date
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import and_, case, func, select
from sqlalchemy.orm import joinedload

from src.db.models import (
    Club,
    League,
    Player,
    PlayerCareer,
    PlayerMedia,
    PlayerSeason,
    Shortlist,
    ShortlistPlayer,
)
from src.db.session import get_session
from src.search import CURRENT_SEASON, PlayerSearch

router = APIRouter(prefix="/api/v1", tags=["v1"])

_searcher = PlayerSearch()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Pydantic response schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class ClubBrief(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    logo_url: str | None = None


class LeagueBrief(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    step: int


class SeasonStats(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    season: str | None = None
    appearances: int | None = None
    starts: int | None = None
    goals: int | None = None
    assists: int | None = None
    yellow_cards: int | None = None
    red_cards: int | None = None
    clean_sheets: int | None = None
    minutes_played: int | None = None
    confidence_score: int | None = None


class PlayerSearchResult(BaseModel):
    """One player in a search-results list."""
    id: int
    full_name: str
    first_name: str | None = None
    last_name: str | None = None
    date_of_birth: str | None = None
    age: int | None = None
    nationality: str | None = None
    position_primary: str | None = None
    position_detail: str | None = None
    height_cm: int | None = None
    weight_kg: int | None = None
    preferred_foot: str | None = None
    contract_status: str | None = None
    availability: str | None = None
    profile_photo_url: str | None = None
    is_verified: bool = False
    club: ClubBrief | None = None
    league: LeagueBrief | None = None
    season_stats: SeasonStats | None = None
    relevance: float | None = None


class PaginatedSearch(BaseModel):
    results: list[PlayerSearchResult]
    total: int
    page: int
    pages: int


# ── player detail ─────────────────────────────────────────────────────────

class SeasonStatsFull(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    season: str
    club_id: int
    club_name: str | None = None
    league_id: int | None = None
    league_name: str | None = None
    appearances: int | None = None
    starts: int | None = None
    sub_appearances: int | None = None
    goals: int | None = None
    assists: int | None = None
    yellow_cards: int | None = None
    red_cards: int | None = None
    clean_sheets: int | None = None
    minutes_played: int | None = None
    data_source: str | None = None
    confidence_score: int | None = None


class CareerEntry(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    club_id: int
    club_name: str | None = None
    season_start: str
    season_end: str | None = None
    role: str | None = None
    source: str | None = None


class MediaItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    media_type: str
    url: str
    title: str | None = None
    description: str | None = None
    is_primary: bool = False


class PlayerDetail(BaseModel):
    id: int
    full_name: str
    first_name: str | None = None
    last_name: str | None = None
    date_of_birth: str | None = None
    age: int | None = None
    nationality: str | None = None
    position_primary: str | None = None
    position_detail: str | None = None
    height_cm: int | None = None
    weight_kg: int | None = None
    preferred_foot: str | None = None
    contract_status: str | None = None
    availability: str | None = None
    profile_photo_url: str | None = None
    bio: str | None = None
    is_verified: bool = False
    club: ClubBrief | None = None
    league: LeagueBrief | None = None
    seasons: list[SeasonStatsFull] = []
    career: list[CareerEntry] = []
    media: list[MediaItem] = []


# ── clubs ─────────────────────────────────────────────────────────────────

class ClubListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    short_name: str | None = None
    league_id: int | None = None
    league_name: str | None = None
    league_step: int | None = None
    ground_name: str | None = None
    logo_url: str | None = None
    is_active: bool = True
    player_count: int = 0


class PaginatedClubs(BaseModel):
    results: list[ClubListItem]
    total: int
    page: int
    pages: int


class SquadMember(BaseModel):
    id: int
    full_name: str
    position_primary: str | None = None
    position_detail: str | None = None
    nationality: str | None = None
    profile_photo_url: str | None = None
    contract_status: str | None = None
    availability: str | None = None


class ClubDetail(BaseModel):
    id: int
    name: str
    short_name: str | None = None
    league: LeagueBrief | None = None
    ground_name: str | None = None
    postcode: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    website_url: str | None = None
    pitchero_url: str | None = None
    twitter_url: str | None = None
    facebook_url: str | None = None
    instagram_url: str | None = None
    contact_email: str | None = None
    logo_url: str | None = None
    is_active: bool = True
    squad: list[SquadMember] = []


# ── leagues ───────────────────────────────────────────────────────────────

class LeagueItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    short_name: str | None = None
    step: int
    region: str | None = None
    division: str | None = None
    season: str | None = None
    club_count: int = 0


class LeaguesByStep(BaseModel):
    step: int
    leagues: list[LeagueItem]


# ── overview ──────────────────────────────────────────────────────────────

class StepCoverage(BaseModel):
    step: int
    leagues: int
    clubs: int
    players: int


class OverviewStats(BaseModel):
    total_players: int
    total_clubs: int
    total_leagues: int
    total_seasons_records: int
    coverage_by_step: list[StepCoverage]


# ── compare ───────────────────────────────────────────────────────────────

class ComparePlayer(BaseModel):
    """One column in the comparison table."""
    id: int
    full_name: str
    date_of_birth: str | None = None
    age: int | None = None
    nationality: str | None = None
    position_primary: str | None = None
    position_detail: str | None = None
    height_cm: int | None = None
    weight_kg: int | None = None
    preferred_foot: str | None = None
    contract_status: str | None = None
    availability: str | None = None
    profile_photo_url: str | None = None
    is_verified: bool = False
    club: ClubBrief | None = None
    league: LeagueBrief | None = None
    season_stats: SeasonStatsFull | None = None
    career: list[CareerEntry] = []
    confidence_score: int | None = None


class CompareResponse(BaseModel):
    players: list[ComparePlayer]
    fields: list[str]


# ── shortlists ────────────────────────────────────────────────────────────

class ShortlistCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: str | None = None


class ShortlistBrief(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    description: str | None = None
    player_count: int = 0
    created_at: str
    updated_at: str


class ShortlistPlayerAdd(BaseModel):
    player_id: int
    notes: str | None = None
    priority: int | None = Field(default=None, ge=1, le=5)


class ShortlistPlayerItem(BaseModel):
    id: int
    player_id: int
    full_name: str
    position_primary: str | None = None
    nationality: str | None = None
    profile_photo_url: str | None = None
    club: ClubBrief | None = None
    league: LeagueBrief | None = None
    notes: str | None = None
    priority: int | None = None
    added_at: str


class ShortlistDetail(BaseModel):
    id: int
    name: str
    description: str | None = None
    created_at: str
    updated_at: str
    players: list[ShortlistPlayerItem] = []


# ── similar players ───────────────────────────────────────────────────────

class SimilarPlayer(BaseModel):
    id: int
    full_name: str
    date_of_birth: str | None = None
    age: int | None = None
    nationality: str | None = None
    position_primary: str | None = None
    position_detail: str | None = None
    profile_photo_url: str | None = None
    club: ClubBrief | None = None
    league: LeagueBrief | None = None
    season_stats: SeasonStats | None = None
    similarity_score: float


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Endpoints
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


# ── 1. Player search ─────────────────────────────────────────────────────

@router.get("/players/search", response_model=PaginatedSearch)
def search_players(
    q: str | None = Query(default=None, description="Free-text search"),
    position: str | None = Query(default=None, description="GK / DEF / MID / FWD"),
    step: int | None = Query(default=None, ge=1, le=6, description="Pyramid step"),
    league_id: int | None = Query(default=None),
    club_id: int | None = Query(default=None),
    min_age: int | None = Query(default=None, ge=15, le=60),
    max_age: int | None = Query(default=None, ge=15, le=60),
    nationality: str | None = Query(default=None),
    availability: str | None = Query(default=None),
    min_confidence: float | None = Query(default=None, ge=1, le=5),
    min_appearances: int | None = Query(default=None, ge=0),
    min_goals: int | None = Query(default=None, ge=0),
    has_photo: bool | None = Query(default=None),
    sort_by: str = Query(default="relevance", pattern="^(relevance|name|age|confidence)$"),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=20, ge=1, le=100),
) -> PaginatedSearch:
    """Full-text search combined with structured filters.

    When *q* is provided the results are ranked by text relevance.
    All filters narrow the SQL query directly — nothing is post-filtered.
    """
    raw = _searcher.search_players(
        query=q,
        position=position,
        step=step,
        league_id=league_id,
        club_id=club_id,
        min_age=min_age,
        max_age=max_age,
        nationality=nationality,
        availability=availability,
        min_confidence=min_confidence,
        min_appearances=min_appearances,
        min_goals=min_goals,
        has_photo=has_photo,
        sort_by=sort_by,
        page=page,
        per_page=per_page,
    )
    return PaginatedSearch(
        results=[PlayerSearchResult(**r) for r in raw["results"]],
        total=raw["total"],
        page=raw["page"],
        pages=raw["pages"],
    )


# ── 2. Player compare ────────────────────────────────────────────────────

@router.get("/players/compare", response_model=CompareResponse)
def compare_players(
    ids: str = Query(..., description="Comma-separated player IDs (max 5)"),
) -> CompareResponse:
    """Side-by-side comparison of up to 5 players.

    The ``fields`` list tells the frontend which rows to render in a
    comparison table; the ``players`` list is the column data.
    """
    try:
        player_ids = [int(x.strip()) for x in ids.split(",") if x.strip()]
    except ValueError:
        raise HTTPException(status_code=400, detail="ids must be comma-separated integers")

    if not player_ids:
        raise HTTPException(status_code=400, detail="At least one player ID required")
    if len(player_ids) > 5:
        raise HTTPException(status_code=400, detail="Maximum 5 players for comparison")

    # Deduplicate while preserving order
    seen: set[int] = set()
    unique_ids: list[int] = []
    for pid in player_ids:
        if pid not in seen:
            seen.add(pid)
            unique_ids.append(pid)

    with get_session() as session:
        players_out: list[ComparePlayer] = []

        for pid in unique_ids:
            player = session.get(Player, pid)
            if not player:
                raise HTTPException(status_code=404, detail=f"Player {pid} not found")

            club_obj = session.get(Club, player.current_club_id) if player.current_club_id else None
            league_obj = session.get(League, club_obj.league_id) if club_obj and club_obj.league_id else None

            age = None
            if player.date_of_birth:
                today = date.today()
                age = today.year - player.date_of_birth.year - (
                    (today.month, today.day) < (player.date_of_birth.month, player.date_of_birth.day)
                )

            # Latest season stats
            latest_row = session.execute(
                select(
                    PlayerSeason,
                    Club.name.label("club_name"),
                    League.name.label("league_name"),
                )
                .outerjoin(Club, PlayerSeason.club_id == Club.id)
                .outerjoin(League, PlayerSeason.league_id == League.id)
                .where(PlayerSeason.player_id == pid)
                .order_by(PlayerSeason.season.desc())
                .limit(1)
            ).first()

            season_stats = None
            confidence = None
            if latest_row:
                ps = latest_row.PlayerSeason
                season_stats = SeasonStatsFull(
                    id=ps.id,
                    season=ps.season,
                    club_id=ps.club_id,
                    club_name=latest_row.club_name,
                    league_id=ps.league_id,
                    league_name=latest_row.league_name,
                    appearances=ps.appearances,
                    starts=ps.starts,
                    sub_appearances=ps.sub_appearances,
                    goals=ps.goals,
                    assists=ps.assists,
                    yellow_cards=ps.yellow_cards,
                    red_cards=ps.red_cards,
                    clean_sheets=ps.clean_sheets,
                    minutes_played=ps.minutes_played,
                    data_source=ps.data_source,
                    confidence_score=ps.confidence_score,
                )
                confidence = ps.confidence_score

            # Career
            career_rows = session.execute(
                select(PlayerCareer, Club.name.label("club_name"))
                .outerjoin(Club, PlayerCareer.club_id == Club.id)
                .where(PlayerCareer.player_id == pid)
                .order_by(PlayerCareer.season_start.desc())
            ).all()

            career = [
                CareerEntry(
                    id=row.PlayerCareer.id,
                    club_id=row.PlayerCareer.club_id,
                    club_name=row.club_name,
                    season_start=row.PlayerCareer.season_start,
                    season_end=row.PlayerCareer.season_end,
                    role=row.PlayerCareer.role,
                    source=row.PlayerCareer.source,
                )
                for row in career_rows
            ]

            players_out.append(ComparePlayer(
                id=player.id,
                full_name=player.full_name,
                date_of_birth=player.date_of_birth.isoformat() if player.date_of_birth else None,
                age=age,
                nationality=player.nationality,
                position_primary=player.position_primary,
                position_detail=player.position_detail,
                height_cm=player.height_cm,
                weight_kg=player.weight_kg,
                preferred_foot=player.preferred_foot,
                contract_status=player.contract_status,
                availability=player.availability,
                profile_photo_url=player.profile_photo_url,
                is_verified=player.is_verified,
                club=ClubBrief(id=club_obj.id, name=club_obj.name, logo_url=club_obj.logo_url) if club_obj else None,
                league=LeagueBrief(id=league_obj.id, name=league_obj.name, step=league_obj.step) if league_obj else None,
                season_stats=season_stats,
                career=career,
                confidence_score=confidence,
            ))

    fields = [
        "full_name", "age", "nationality", "position_primary",
        "position_detail", "height_cm", "weight_kg", "preferred_foot",
        "club", "league", "contract_status", "availability",
        "appearances", "goals", "assists", "yellow_cards", "red_cards",
        "clean_sheets", "minutes_played", "confidence_score", "career",
    ]

    return CompareResponse(players=players_out, fields=fields)


# ── 3. Player detail ─────────────────────────────────────────────────────

@router.get("/players/{player_id}", response_model=PlayerDetail)
def get_player(player_id: int) -> PlayerDetail:
    """Full player profile: bio, career history, all season stats, media."""
    with get_session() as session:
        player = session.get(Player, player_id)
        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        club_obj = session.get(Club, player.current_club_id) if player.current_club_id else None
        league_obj = session.get(League, club_obj.league_id) if club_obj and club_obj.league_id else None

        age = None
        if player.date_of_birth:
            today = date.today()
            age = today.year - player.date_of_birth.year - (
                (today.month, today.day) < (player.date_of_birth.month, player.date_of_birth.day)
            )

        # Season stats with club/league names
        season_rows = session.execute(
            select(
                PlayerSeason,
                Club.name.label("club_name"),
                League.name.label("league_name"),
            )
            .outerjoin(Club, PlayerSeason.club_id == Club.id)
            .outerjoin(League, PlayerSeason.league_id == League.id)
            .where(PlayerSeason.player_id == player_id)
            .order_by(PlayerSeason.season.desc())
        ).all()

        seasons = [
            SeasonStatsFull(
                id=row.PlayerSeason.id,
                season=row.PlayerSeason.season,
                club_id=row.PlayerSeason.club_id,
                club_name=row.club_name,
                league_id=row.PlayerSeason.league_id,
                league_name=row.league_name,
                appearances=row.PlayerSeason.appearances,
                starts=row.PlayerSeason.starts,
                sub_appearances=row.PlayerSeason.sub_appearances,
                goals=row.PlayerSeason.goals,
                assists=row.PlayerSeason.assists,
                yellow_cards=row.PlayerSeason.yellow_cards,
                red_cards=row.PlayerSeason.red_cards,
                clean_sheets=row.PlayerSeason.clean_sheets,
                minutes_played=row.PlayerSeason.minutes_played,
                data_source=row.PlayerSeason.data_source,
                confidence_score=row.PlayerSeason.confidence_score,
            )
            for row in season_rows
        ]

        # Career entries with club names
        career_rows = session.execute(
            select(PlayerCareer, Club.name.label("club_name"))
            .outerjoin(Club, PlayerCareer.club_id == Club.id)
            .where(PlayerCareer.player_id == player_id)
            .order_by(PlayerCareer.season_start.desc())
        ).all()

        career = [
            CareerEntry(
                id=row.PlayerCareer.id,
                club_id=row.PlayerCareer.club_id,
                club_name=row.club_name,
                season_start=row.PlayerCareer.season_start,
                season_end=row.PlayerCareer.season_end,
                role=row.PlayerCareer.role,
                source=row.PlayerCareer.source,
            )
            for row in career_rows
        ]

        # Media
        media_rows = session.execute(
            select(PlayerMedia).where(PlayerMedia.player_id == player_id)
        ).scalars().all()

        media = [
            MediaItem(
                id=m.id,
                media_type=m.media_type,
                url=m.url,
                title=m.title,
                description=m.description,
                is_primary=m.is_primary,
            )
            for m in media_rows
        ]

        return PlayerDetail(
            id=player.id,
            full_name=player.full_name,
            first_name=player.first_name,
            last_name=player.last_name,
            date_of_birth=player.date_of_birth.isoformat() if player.date_of_birth else None,
            age=age,
            nationality=player.nationality,
            position_primary=player.position_primary,
            position_detail=player.position_detail,
            height_cm=player.height_cm,
            weight_kg=player.weight_kg,
            preferred_foot=player.preferred_foot,
            contract_status=player.contract_status,
            availability=player.availability,
            profile_photo_url=player.profile_photo_url,
            bio=player.bio,
            is_verified=player.is_verified,
            club=ClubBrief(id=club_obj.id, name=club_obj.name, logo_url=club_obj.logo_url) if club_obj else None,
            league=LeagueBrief(id=league_obj.id, name=league_obj.name, step=league_obj.step) if league_obj else None,
            seasons=seasons,
            career=career,
            media=media,
        )


# ── 3. Player stats ──────────────────────────────────────────────────────

@router.get("/players/{player_id}/stats", response_model=list[SeasonStatsFull])
def get_player_stats(player_id: int) -> list[SeasonStatsFull]:
    """All player_seasons records for a player, newest first."""
    with get_session() as session:
        player = session.get(Player, player_id)
        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        rows = session.execute(
            select(
                PlayerSeason,
                Club.name.label("club_name"),
                League.name.label("league_name"),
            )
            .outerjoin(Club, PlayerSeason.club_id == Club.id)
            .outerjoin(League, PlayerSeason.league_id == League.id)
            .where(PlayerSeason.player_id == player_id)
            .order_by(PlayerSeason.season.desc())
        ).all()

        return [
            SeasonStatsFull(
                id=row.PlayerSeason.id,
                season=row.PlayerSeason.season,
                club_id=row.PlayerSeason.club_id,
                club_name=row.club_name,
                league_id=row.PlayerSeason.league_id,
                league_name=row.league_name,
                appearances=row.PlayerSeason.appearances,
                starts=row.PlayerSeason.starts,
                sub_appearances=row.PlayerSeason.sub_appearances,
                goals=row.PlayerSeason.goals,
                assists=row.PlayerSeason.assists,
                yellow_cards=row.PlayerSeason.yellow_cards,
                red_cards=row.PlayerSeason.red_cards,
                clean_sheets=row.PlayerSeason.clean_sheets,
                minutes_played=row.PlayerSeason.minutes_played,
                data_source=row.PlayerSeason.data_source,
                confidence_score=row.PlayerSeason.confidence_score,
            )
            for row in rows
        ]


# ── 4. Player career ─────────────────────────────────────────────────────

@router.get("/players/{player_id}/career", response_model=list[CareerEntry])
def get_player_career(player_id: int) -> list[CareerEntry]:
    """Career timeline with clubs and dates, newest first."""
    with get_session() as session:
        player = session.get(Player, player_id)
        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        rows = session.execute(
            select(PlayerCareer, Club.name.label("club_name"))
            .outerjoin(Club, PlayerCareer.club_id == Club.id)
            .where(PlayerCareer.player_id == player_id)
            .order_by(PlayerCareer.season_start.desc())
        ).all()

        return [
            CareerEntry(
                id=row.PlayerCareer.id,
                club_id=row.PlayerCareer.club_id,
                club_name=row.club_name,
                season_start=row.PlayerCareer.season_start,
                season_end=row.PlayerCareer.season_end,
                role=row.PlayerCareer.role,
                source=row.PlayerCareer.source,
            )
            for row in rows
        ]


# ── 5. Club list ─────────────────────────────────────────────────────────

@router.get("/clubs", response_model=PaginatedClubs)
def list_clubs(
    step: int | None = Query(default=None, ge=1, le=6, description="Filter by pyramid step"),
    league_id: int | None = Query(default=None, description="Filter by league ID"),
    search: str | None = Query(default=None, description="Search club names"),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=20, ge=1, le=100),
) -> PaginatedClubs:
    """Paginated club list with player count, league info, and optional filters."""
    with get_session() as session:
        player_count_sq = (
            select(
                Player.current_club_id,
                func.count(Player.id).label("player_count"),
            )
            .where(Player.merged_into_id.is_(None))
            .group_by(Player.current_club_id)
            .subquery("pc")
        )

        stmt = (
            select(
                Club.id,
                Club.name,
                Club.short_name,
                Club.league_id,
                League.name.label("league_name"),
                League.step.label("league_step"),
                Club.ground_name,
                Club.logo_url,
                Club.is_active,
                func.coalesce(player_count_sq.c.player_count, 0).label("player_count"),
            )
            .outerjoin(League, Club.league_id == League.id)
            .outerjoin(player_count_sq, Club.id == player_count_sq.c.current_club_id)
        )

        if step is not None:
            stmt = stmt.where(League.step == step)
        if league_id is not None:
            stmt = stmt.where(Club.league_id == league_id)
        if search:
            stmt = stmt.where(Club.name.ilike(f"%{search}%"))

        count_stmt = select(func.count()).select_from(stmt.subquery())
        total: int = session.execute(count_stmt).scalar_one()

        pages = max(1, math.ceil(total / per_page))
        actual_page = min(page, pages) if pages else 1
        offset = (actual_page - 1) * per_page

        stmt = stmt.order_by(Club.name).offset(offset).limit(per_page)
        rows = session.execute(stmt).all()

        results = [
            ClubListItem(
                id=r.id,
                name=r.name,
                short_name=r.short_name,
                league_id=r.league_id,
                league_name=r.league_name,
                league_step=r.league_step,
                ground_name=r.ground_name,
                logo_url=r.logo_url,
                is_active=r.is_active,
                player_count=r.player_count,
            )
            for r in rows
        ]

        return PaginatedClubs(
            results=results,
            total=total,
            page=actual_page,
            pages=pages,
        )


# ── 6. Club detail ───────────────────────────────────────────────────────

@router.get("/clubs/{club_id}", response_model=ClubDetail)
def get_club(club_id: int) -> ClubDetail:
    """Club detail including current squad."""
    with get_session() as session:
        club = session.get(Club, club_id)
        if not club:
            raise HTTPException(status_code=404, detail="Club not found")

        league_obj = session.get(League, club.league_id) if club.league_id else None

        squad_rows = session.execute(
            select(Player)
            .where(
                Player.current_club_id == club_id,
                Player.merged_into_id.is_(None),
            )
            .order_by(Player.position_primary, Player.full_name)
        ).scalars().all()

        squad = [
            SquadMember(
                id=p.id,
                full_name=p.full_name,
                position_primary=p.position_primary,
                position_detail=p.position_detail,
                nationality=p.nationality,
                profile_photo_url=p.profile_photo_url,
                contract_status=p.contract_status,
                availability=p.availability,
            )
            for p in squad_rows
        ]

        return ClubDetail(
            id=club.id,
            name=club.name,
            short_name=club.short_name,
            league=LeagueBrief(id=league_obj.id, name=league_obj.name, step=league_obj.step) if league_obj else None,
            ground_name=club.ground_name,
            postcode=club.postcode,
            latitude=float(club.latitude) if club.latitude else None,
            longitude=float(club.longitude) if club.longitude else None,
            website_url=club.website_url,
            pitchero_url=club.pitchero_url,
            twitter_url=club.twitter_url,
            facebook_url=club.facebook_url,
            instagram_url=club.instagram_url,
            contact_email=club.contact_email,
            logo_url=club.logo_url,
            is_active=club.is_active,
            squad=squad,
        )


# ── 7. Leagues ────────────────────────────────────────────────────────────

@router.get("/leagues", response_model=list[LeaguesByStep])
def list_leagues(
    step: int | None = Query(default=None, ge=1, le=6, description="Filter to a single step"),
) -> list[LeaguesByStep]:
    """All leagues grouped by pyramid step, with club counts."""
    with get_session() as session:
        club_count_sq = (
            select(
                Club.league_id,
                func.count(Club.id).label("club_count"),
            )
            .group_by(Club.league_id)
            .subquery("cc")
        )

        stmt = (
            select(
                League,
                func.coalesce(club_count_sq.c.club_count, 0).label("club_count"),
            )
            .outerjoin(club_count_sq, League.id == club_count_sq.c.league_id)
        )
        if step is not None:
            stmt = stmt.where(League.step == step)

        stmt = stmt.order_by(League.step, League.name)
        rows = session.execute(stmt).all()

        by_step: dict[int, list[LeagueItem]] = {}
        for row in rows:
            lg = row.League
            item = LeagueItem(
                id=lg.id,
                name=lg.name,
                short_name=lg.short_name,
                step=lg.step,
                region=lg.region,
                division=lg.division,
                season=lg.season,
                club_count=row.club_count,
            )
            by_step.setdefault(lg.step, []).append(item)

        return [
            LeaguesByStep(step=s, leagues=lgs)
            for s, lgs in sorted(by_step.items())
        ]


# ── 8. Stats overview ────────────────────────────────────────────────────

@router.get("/stats/overview", response_model=OverviewStats)
def stats_overview() -> OverviewStats:
    """Dashboard-level counts: players, clubs, leagues, and per-step coverage."""
    with get_session() as session:
        total_players: int = session.execute(
            select(func.count(Player.id)).where(Player.merged_into_id.is_(None))
        ).scalar_one()
        total_clubs: int = session.execute(select(func.count(Club.id))).scalar_one()
        total_leagues: int = session.execute(select(func.count(League.id))).scalar_one()
        total_seasons: int = session.execute(select(func.count(PlayerSeason.id))).scalar_one()

        coverage_rows = session.execute(
            select(
                League.step,
                func.count(League.id.distinct()).label("leagues"),
                func.count(Club.id.distinct()).label("clubs"),
                func.count(Player.id.distinct()).label("players"),
            )
            .outerjoin(Club, Club.league_id == League.id)
            .outerjoin(
                Player,
                and_(
                    Player.current_club_id == Club.id,
                    Player.merged_into_id.is_(None),
                ),
            )
            .group_by(League.step)
            .order_by(League.step)
        ).all()

        coverage = [
            StepCoverage(step=r.step, leagues=r.leagues, clubs=r.clubs, players=r.players)
            for r in coverage_rows
        ]

        return OverviewStats(
            total_players=total_players,
            total_clubs=total_clubs,
            total_leagues=total_leagues,
            total_seasons_records=total_seasons,
            coverage_by_step=coverage,
        )


# ── 10. Similar players ──────────────────────────────────────────────────

@router.get("/players/{player_id}/similar", response_model=list[SimilarPlayer])
def get_similar_players(player_id: int) -> list[SimilarPlayer]:
    """Find the 10 most similar players based on position, age, step, and stats."""
    with get_session() as session:
        player = session.get(Player, player_id)
        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        club_obj = session.get(Club, player.current_club_id) if player.current_club_id else None
        league_obj = session.get(League, club_obj.league_id) if club_obj and club_obj.league_id else None
        player_step = league_obj.step if league_obj else None

        player_age: int | None = None
        if player.date_of_birth:
            today = date.today()
            player_age = today.year - player.date_of_birth.year - (
                (today.month, today.day) < (player.date_of_birth.month, player.date_of_birth.day)
            )

        # Get reference player's latest season stats
        ref_stats = session.execute(
            select(PlayerSeason)
            .where(
                PlayerSeason.player_id == player_id,
                PlayerSeason.season == CURRENT_SEASON,
            )
            .order_by(PlayerSeason.confidence_score.desc().nulls_last())
            .limit(1)
        ).scalar_one_or_none()

        ref_apps = ref_stats.appearances if ref_stats and ref_stats.appearances else 0
        ref_goals = ref_stats.goals if ref_stats and ref_stats.goals else 0

        # --- Build candidate query ---
        age_expr = func.extract(
            "year",
            func.age(func.current_date(), Player.date_of_birth),
        )

        # Latest season subquery (same pattern as PlayerSearch)
        latest_sq = (
            select(
                PlayerSeason.player_id,
                func.max(PlayerSeason.id).label("max_ps_id"),
            )
            .where(PlayerSeason.season == CURRENT_SEASON)
            .group_by(PlayerSeason.player_id)
            .subquery("ls")
        )

        stmt = (
            select(
                Player.id,
                Player.full_name,
                Player.date_of_birth,
                age_expr.label("age"),
                Player.nationality,
                Player.position_primary,
                Player.position_detail,
                Player.profile_photo_url,
                Club.id.label("club_id"),
                Club.name.label("club_name"),
                Club.logo_url.label("club_logo_url"),
                League.id.label("league_id"),
                League.name.label("league_name"),
                League.step.label("league_step"),
                PlayerSeason.season.label("ps_season"),
                PlayerSeason.appearances.label("ps_appearances"),
                PlayerSeason.starts.label("ps_starts"),
                PlayerSeason.goals.label("ps_goals"),
                PlayerSeason.assists.label("ps_assists"),
                PlayerSeason.yellow_cards.label("ps_yellow_cards"),
                PlayerSeason.red_cards.label("ps_red_cards"),
                PlayerSeason.clean_sheets.label("ps_clean_sheets"),
                PlayerSeason.minutes_played.label("ps_minutes_played"),
                PlayerSeason.confidence_score.label("ps_confidence"),
            )
            .outerjoin(Club, Player.current_club_id == Club.id)
            .outerjoin(League, Club.league_id == League.id)
            .outerjoin(latest_sq, Player.id == latest_sq.c.player_id)
            .outerjoin(PlayerSeason, PlayerSeason.id == latest_sq.c.max_ps_id)
            .where(Player.id != player_id)
            .where(Player.merged_into_id.is_(None))
        )

        # Hard filter: same position (most important constraint)
        if player.position_primary:
            stmt = stmt.where(Player.position_primary == player.position_primary)

        # Soft scoring via CASE expressions (higher = more similar)
        score_parts = []

        # Position match: 30 points (already filtered, so always awarded)
        # But give bonus for matching position_detail
        if player.position_detail:
            score_parts.append(
                case(
                    (Player.position_detail == player.position_detail, 10),
                    else_=0,
                )
            )

        # Age similarity: up to 25 points (3-year window)
        if player_age is not None:
            age_diff = func.abs(age_expr - player_age)
            score_parts.append(
                case(
                    (Player.date_of_birth.is_(None), 0),
                    (age_diff <= 1, 25),
                    (age_diff <= 2, 18),
                    (age_diff <= 3, 10),
                    (age_diff <= 5, 5),
                    else_=0,
                )
            )

        # Step similarity: up to 25 points (+-1 step window)
        if player_step is not None:
            step_diff = func.abs(League.step - player_step)
            score_parts.append(
                case(
                    (League.step.is_(None), 0),
                    (step_diff == 0, 25),
                    (step_diff == 1, 15),
                    (step_diff == 2, 5),
                    else_=0,
                )
            )

        # Stats similarity: up to 20 points
        if ref_apps > 0:
            apps_diff = func.abs(func.coalesce(PlayerSeason.appearances, 0) - ref_apps)
            score_parts.append(
                case(
                    (PlayerSeason.appearances.is_(None), 0),
                    (apps_diff <= 3, 10),
                    (apps_diff <= 8, 6),
                    (apps_diff <= 15, 3),
                    else_=0,
                )
            )

        if ref_goals > 0:
            goals_diff = func.abs(func.coalesce(PlayerSeason.goals, 0) - ref_goals)
            score_parts.append(
                case(
                    (PlayerSeason.goals.is_(None), 0),
                    (goals_diff <= 2, 10),
                    (goals_diff <= 5, 6),
                    (goals_diff <= 10, 3),
                    else_=0,
                )
            )

        total_score = sum(score_parts) if score_parts else func.literal(0)
        stmt = stmt.add_columns(total_score.label("sim_score"))
        stmt = stmt.order_by(total_score.desc(), Player.full_name)
        stmt = stmt.limit(10)

        rows = session.execute(stmt).all()

        results: list[SimilarPlayer] = []
        for r in rows:
            dob = r.date_of_birth
            club = ClubBrief(id=r.club_id, name=r.club_name, logo_url=r.club_logo_url) if r.club_id else None
            league = LeagueBrief(id=r.league_id, name=r.league_name, step=r.league_step) if r.league_id else None
            stats = None
            if r.ps_season:
                stats = SeasonStats(
                    season=r.ps_season,
                    appearances=r.ps_appearances,
                    starts=r.ps_starts,
                    goals=r.ps_goals,
                    assists=r.ps_assists,
                    yellow_cards=r.ps_yellow_cards,
                    red_cards=r.ps_red_cards,
                    clean_sheets=r.ps_clean_sheets,
                    minutes_played=r.ps_minutes_played,
                    confidence_score=r.ps_confidence,
                )

            # Normalise score to 0-1 range (max possible ~100)
            max_possible = 100.0
            norm_score = round(min(r.sim_score / max_possible, 1.0), 3) if r.sim_score else 0.0

            results.append(SimilarPlayer(
                id=r.id,
                full_name=r.full_name,
                date_of_birth=dob.isoformat() if dob else None,
                age=int(r.age) if r.age is not None else None,
                nationality=r.nationality,
                position_primary=r.position_primary,
                position_detail=r.position_detail,
                profile_photo_url=r.profile_photo_url,
                club=club,
                league=league,
                season_stats=stats,
                similarity_score=norm_score,
            ))

        return results


# ── 11. Create shortlist ─────────────────────────────────────────────────

@router.post("/shortlists", response_model=ShortlistBrief, status_code=201)
def create_shortlist(body: ShortlistCreate) -> ShortlistBrief:
    """Create a new player shortlist."""
    with get_session() as session:
        sl = Shortlist(name=body.name, description=body.description)
        session.add(sl)
        session.flush()

        return ShortlistBrief(
            id=sl.id,
            name=sl.name,
            description=sl.description,
            player_count=0,
            created_at=sl.created_at.isoformat(),
            updated_at=sl.updated_at.isoformat(),
        )


# ── 12. List shortlists ──────────────────────────────────────────────────

@router.get("/shortlists", response_model=list[ShortlistBrief])
def list_shortlists() -> list[ShortlistBrief]:
    """Return all shortlists with player counts."""
    with get_session() as session:
        count_sq = (
            select(
                ShortlistPlayer.shortlist_id,
                func.count(ShortlistPlayer.id).label("cnt"),
            )
            .group_by(ShortlistPlayer.shortlist_id)
            .subquery("pc")
        )

        rows = session.execute(
            select(
                Shortlist,
                func.coalesce(count_sq.c.cnt, 0).label("player_count"),
            )
            .outerjoin(count_sq, Shortlist.id == count_sq.c.shortlist_id)
            .order_by(Shortlist.updated_at.desc())
        ).all()

        return [
            ShortlistBrief(
                id=row.Shortlist.id,
                name=row.Shortlist.name,
                description=row.Shortlist.description,
                player_count=row.player_count,
                created_at=row.Shortlist.created_at.isoformat(),
                updated_at=row.Shortlist.updated_at.isoformat(),
            )
            for row in rows
        ]


# ── 13. Shortlist detail ─────────────────────────────────────────────────

@router.get("/shortlists/{shortlist_id}", response_model=ShortlistDetail)
def get_shortlist(shortlist_id: int) -> ShortlistDetail:
    """Return a shortlist with all its players and their data."""
    with get_session() as session:
        sl = session.get(Shortlist, shortlist_id)
        if not sl:
            raise HTTPException(status_code=404, detail="Shortlist not found")

        rows = session.execute(
            select(
                ShortlistPlayer,
                Player.full_name,
                Player.position_primary,
                Player.nationality,
                Player.profile_photo_url,
                Player.current_club_id,
                Club.id.label("club_id"),
                Club.name.label("club_name"),
                Club.logo_url.label("club_logo_url"),
                League.id.label("league_id"),
                League.name.label("league_name"),
                League.step.label("league_step"),
            )
            .join(Player, ShortlistPlayer.player_id == Player.id)
            .outerjoin(Club, Player.current_club_id == Club.id)
            .outerjoin(League, Club.league_id == League.id)
            .where(ShortlistPlayer.shortlist_id == shortlist_id)
            .order_by(
                ShortlistPlayer.priority.asc().nulls_last(),
                ShortlistPlayer.added_at.desc(),
            )
        ).all()

        players = [
            ShortlistPlayerItem(
                id=r.ShortlistPlayer.id,
                player_id=r.ShortlistPlayer.player_id,
                full_name=r.full_name,
                position_primary=r.position_primary,
                nationality=r.nationality,
                profile_photo_url=r.profile_photo_url,
                club=ClubBrief(id=r.club_id, name=r.club_name, logo_url=r.club_logo_url) if r.club_id else None,
                league=LeagueBrief(id=r.league_id, name=r.league_name, step=r.league_step) if r.league_id else None,
                notes=r.ShortlistPlayer.notes,
                priority=r.ShortlistPlayer.priority,
                added_at=r.ShortlistPlayer.added_at.isoformat(),
            )
            for r in rows
        ]

        return ShortlistDetail(
            id=sl.id,
            name=sl.name,
            description=sl.description,
            created_at=sl.created_at.isoformat(),
            updated_at=sl.updated_at.isoformat(),
            players=players,
        )


# ── 14. Add player to shortlist ──────────────────────────────────────────

@router.post(
    "/shortlists/{shortlist_id}/players",
    response_model=ShortlistPlayerItem,
    status_code=201,
)
def add_player_to_shortlist(
    shortlist_id: int,
    body: ShortlistPlayerAdd,
) -> ShortlistPlayerItem:
    """Add a player to a shortlist. Returns 409 if already present."""
    with get_session() as session:
        sl = session.get(Shortlist, shortlist_id)
        if not sl:
            raise HTTPException(status_code=404, detail="Shortlist not found")

        player = session.get(Player, body.player_id)
        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        existing = session.execute(
            select(ShortlistPlayer).where(
                ShortlistPlayer.shortlist_id == shortlist_id,
                ShortlistPlayer.player_id == body.player_id,
            )
        ).scalar_one_or_none()
        if existing:
            raise HTTPException(
                status_code=409,
                detail="Player already on this shortlist",
            )

        sp = ShortlistPlayer(
            shortlist_id=shortlist_id,
            player_id=body.player_id,
            notes=body.notes,
            priority=body.priority,
        )
        session.add(sp)
        session.flush()

        club_obj = session.get(Club, player.current_club_id) if player.current_club_id else None
        league_obj = session.get(League, club_obj.league_id) if club_obj and club_obj.league_id else None

        return ShortlistPlayerItem(
            id=sp.id,
            player_id=sp.player_id,
            full_name=player.full_name,
            position_primary=player.position_primary,
            nationality=player.nationality,
            profile_photo_url=player.profile_photo_url,
            club=ClubBrief(id=club_obj.id, name=club_obj.name, logo_url=club_obj.logo_url) if club_obj else None,
            league=LeagueBrief(id=league_obj.id, name=league_obj.name, step=league_obj.step) if league_obj else None,
            notes=sp.notes,
            priority=sp.priority,
            added_at=sp.added_at.isoformat(),
        )


# ── 15. Remove player from shortlist ─────────────────────────────────────

@router.delete(
    "/shortlists/{shortlist_id}/players/{player_id}",
    status_code=204,
)
def remove_player_from_shortlist(shortlist_id: int, player_id: int) -> None:
    """Remove a player from a shortlist."""
    with get_session() as session:
        sp = session.execute(
            select(ShortlistPlayer).where(
                ShortlistPlayer.shortlist_id == shortlist_id,
                ShortlistPlayer.player_id == player_id,
            )
        ).scalar_one_or_none()
        if not sp:
            raise HTTPException(
                status_code=404,
                detail="Player not on this shortlist",
            )
        session.delete(sp)
