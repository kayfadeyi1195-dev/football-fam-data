"""Admin / moderation endpoints for Football Fam.

Provides a simple moderation queue for reviewing:

1. **Unverified players** — newly self-registered, visible but flagged.
2. **Pending updates** — self-reported data that matched an existing
   player and needs human review before applying.

GET    /api/v1/admin/unverified              – list unverified players
POST   /api/v1/admin/players/{id}/verify     – mark a player as verified
POST   /api/v1/admin/players/{id}/reject     – soft-delete a fake registration
GET    /api/v1/admin/pending-updates         – list pending update queue
GET    /api/v1/admin/pending-updates/{id}    – view a specific pending update
POST   /api/v1/admin/pending-updates/{id}/approve – apply the update
POST   /api/v1/admin/pending-updates/{id}/reject  – discard the update
"""

from __future__ import annotations

import logging
import math
from datetime import date, datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select

from src.db.models import (
    Club,
    League,
    PendingUpdate,
    Player,
    PlayerCareer,
    PlayerSeason,
)
from src.db.session import get_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])

DATA_SOURCE = "self_registration"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Response schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class ClubBrief(BaseModel):
    id: int
    name: str


class UnverifiedPlayer(BaseModel):
    id: int
    full_name: str
    email: str | None = None
    date_of_birth: str | None = None
    nationality: str | None = None
    position_primary: str | None = None
    club: ClubBrief | None = None
    registered_at: str
    overall_confidence: float | None = None


class PaginatedUnverified(BaseModel):
    results: list[UnverifiedPlayer]
    total: int
    page: int
    pages: int


class PendingUpdateBrief(BaseModel):
    id: int
    player_id: int
    player_name: str
    submitter_email: str | None = None
    status: str
    created_at: str
    field_count: int = 0


class PendingUpdateDetail(BaseModel):
    id: int
    player_id: int
    player_name: str
    submitter_email: str | None = None
    submitter_phone: str | None = None
    status: str
    submitted_data: dict
    current_data: dict
    created_at: str
    reviewed_by: str | None = None
    reviewed_at: str | None = None
    review_notes: str | None = None


class ReviewAction(BaseModel):
    reviewed_by: str = Field(default="admin", max_length=100)
    notes: str | None = Field(default=None, max_length=1000)


class ActionResponse(BaseModel):
    success: bool
    message: str


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. Unverified players
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@router.get("/unverified", response_model=PaginatedUnverified)
def list_unverified(
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=20, ge=1, le=100),
) -> PaginatedUnverified:
    """List all unverified players, newest first."""
    with get_session() as session:
        base = (
            select(Player, Club.id.label("club_id"), Club.name.label("club_name"))
            .outerjoin(Club, Player.current_club_id == Club.id)
            .where(
                Player.is_verified.is_(False),
                Player.merged_into_id.is_(None),
            )
        )

        total: int = session.execute(
            select(func.count()).select_from(base.subquery())
        ).scalar_one()

        pages = max(1, math.ceil(total / per_page))
        offset = (min(page, pages) - 1) * per_page

        rows = session.execute(
            base.order_by(Player.created_at.desc())
            .offset(offset).limit(per_page)
        ).all()

        results = [
            UnverifiedPlayer(
                id=r.Player.id,
                full_name=r.Player.full_name,
                email=r.Player.email,
                date_of_birth=r.Player.date_of_birth.isoformat() if r.Player.date_of_birth else None,
                nationality=r.Player.nationality,
                position_primary=r.Player.position_primary,
                club=ClubBrief(id=r.club_id, name=r.club_name) if r.club_id else None,
                registered_at=r.Player.created_at.isoformat(),
                overall_confidence=float(r.Player.overall_confidence) if r.Player.overall_confidence else None,
            )
            for r in rows
        ]

        return PaginatedUnverified(
            results=results, total=total,
            page=min(page, pages), pages=pages,
        )


@router.post("/players/{player_id}/verify", response_model=ActionResponse)
def verify_player(player_id: int, body: ReviewAction | None = None) -> ActionResponse:
    """Mark a player as verified (approved by agent)."""
    with get_session() as session:
        player = session.get(Player, player_id)
        if not player:
            raise HTTPException(status_code=404, detail="Player not found")
        if player.is_verified:
            return ActionResponse(success=True, message="Player already verified")

        player.is_verified = True
        session.flush()

        reviewer = body.reviewed_by if body else "admin"
        logger.info("Player %d verified by %s", player_id, reviewer)

        return ActionResponse(success=True, message=f"Player {player.full_name} verified")


@router.post("/players/{player_id}/reject", response_model=ActionResponse)
def reject_player(player_id: int, body: ReviewAction | None = None) -> ActionResponse:
    """Soft-delete a fake or spam registration."""
    with get_session() as session:
        player = session.get(Player, player_id)
        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        player.merged_into_id = player.id
        session.flush()

        reviewer = body.reviewed_by if body else "admin"
        logger.info("Player %d rejected (self-ref soft-delete) by %s", player_id, reviewer)

        return ActionResponse(
            success=True,
            message=f"Player {player.full_name} rejected and soft-deleted",
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. Pending updates
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@router.get("/pending-updates", response_model=list[PendingUpdateBrief])
def list_pending_updates(
    status: str = Query(default="pending", pattern="^(pending|approved|rejected|all)$"),
) -> list[PendingUpdateBrief]:
    """List pending update requests, filterable by status."""
    with get_session() as session:
        stmt = (
            select(PendingUpdate, Player.full_name.label("player_name"))
            .join(Player, PendingUpdate.player_id == Player.id)
        )
        if status != "all":
            stmt = stmt.where(PendingUpdate.status == status)

        stmt = stmt.order_by(PendingUpdate.created_at.desc())
        rows = session.execute(stmt).all()

        return [
            PendingUpdateBrief(
                id=r.PendingUpdate.id,
                player_id=r.PendingUpdate.player_id,
                player_name=r.player_name,
                submitter_email=r.PendingUpdate.submitter_email,
                status=r.PendingUpdate.status,
                created_at=r.PendingUpdate.created_at.isoformat(),
                field_count=len(r.PendingUpdate.submitted_data or {}),
            )
            for r in rows
        ]


@router.get("/pending-updates/{update_id}", response_model=PendingUpdateDetail)
def get_pending_update(update_id: int) -> PendingUpdateDetail:
    """View a pending update alongside the current player data for comparison."""
    with get_session() as session:
        pu = session.get(PendingUpdate, update_id)
        if not pu:
            raise HTTPException(status_code=404, detail="Pending update not found")

        player = session.get(Player, pu.player_id)
        club_obj = session.get(Club, player.current_club_id) if player and player.current_club_id else None

        current = {
            "full_name": player.full_name if player else None,
            "email": player.email if player else None,
            "date_of_birth": player.date_of_birth.isoformat() if player and player.date_of_birth else None,
            "nationality": player.nationality if player else None,
            "position_primary": player.position_primary if player else None,
            "position_detail": player.position_detail if player else None,
            "height_cm": player.height_cm if player else None,
            "preferred_foot": player.preferred_foot if player else None,
            "current_club": club_obj.name if club_obj else None,
            "contract_status": player.contract_status if player else None,
            "availability": player.availability if player else None,
            "bio": player.bio if player else None,
        }

        return PendingUpdateDetail(
            id=pu.id,
            player_id=pu.player_id,
            player_name=player.full_name if player else "Unknown",
            submitter_email=pu.submitter_email,
            submitter_phone=pu.submitter_phone,
            status=pu.status,
            submitted_data=pu.submitted_data,
            current_data=current,
            created_at=pu.created_at.isoformat(),
            reviewed_by=pu.reviewed_by,
            reviewed_at=pu.reviewed_at.isoformat() if pu.reviewed_at else None,
            review_notes=pu.review_notes,
        )


@router.post("/pending-updates/{update_id}/approve", response_model=ActionResponse)
def approve_pending_update(update_id: int, body: ReviewAction | None = None) -> ActionResponse:
    """Approve a pending update: apply the submitted data to the player record.

    Career history and season stats from the submission are also
    created if not already present.
    """
    with get_session() as session:
        pu = session.get(PendingUpdate, update_id)
        if not pu:
            raise HTTPException(status_code=404, detail="Pending update not found")
        if pu.status != "pending":
            raise HTTPException(status_code=409, detail=f"Update already {pu.status}")

        player = session.get(Player, pu.player_id)
        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        data = pu.submitted_data

        # Apply scalar fields (only update if the submitted value is non-null)
        _apply_field(player, "full_name", data)
        _apply_field(player, "email", data)
        _apply_field(player, "phone", data)
        _apply_field(player, "nationality", data)
        _apply_field(player, "position_primary", data)
        _apply_field(player, "position_detail", data)
        _apply_field(player, "height_cm", data)
        _apply_field(player, "preferred_foot", data)
        _apply_field(player, "contract_status", data)
        _apply_field(player, "availability", data)
        _apply_field(player, "bio", data)

        if data.get("current_club_id"):
            player.current_club_id = data["current_club_id"]

        if data.get("date_of_birth"):
            player.date_of_birth = date.fromisoformat(data["date_of_birth"])

        # Career history
        career_added = 0
        for ch in data.get("career_history", []):
            cid = ch.get("club_id")
            if not cid:
                continue
            existing = session.execute(
                select(PlayerCareer).where(
                    PlayerCareer.player_id == player.id,
                    PlayerCareer.club_id == cid,
                    PlayerCareer.season_start == ch["season_start"],
                )
            ).scalar_one_or_none()
            if not existing:
                session.add(PlayerCareer(
                    player_id=player.id,
                    club_id=cid,
                    season_start=ch["season_start"],
                    season_end=ch.get("season_end"),
                    role="player",
                    source=DATA_SOURCE,
                ))
                career_added += 1

            if ch.get("appearances") is not None or ch.get("goals") is not None:
                existing_ps = session.execute(
                    select(PlayerSeason).where(
                        PlayerSeason.player_id == player.id,
                        PlayerSeason.club_id == cid,
                        PlayerSeason.season == ch["season_start"],
                        PlayerSeason.data_source == DATA_SOURCE,
                    )
                ).scalar_one_or_none()
                if not existing_ps:
                    career_club = session.get(Club, cid)
                    session.add(PlayerSeason(
                        player_id=player.id,
                        club_id=cid,
                        league_id=career_club.league_id if career_club else None,
                        season=ch["season_start"],
                        appearances=ch.get("appearances"),
                        goals=ch.get("goals"),
                        data_source=DATA_SOURCE,
                        confidence_score=1,
                    ))

        # Highlight video
        video_url = data.get("highlight_video_url")
        if video_url:
            existing_media = session.execute(
                select(PlayerMedia).where(
                    PlayerMedia.player_id == player.id,
                    PlayerMedia.url == video_url,
                )
            ).scalar_one_or_none()
            if not existing_media:
                from src.db.models import PlayerMedia
                session.add(PlayerMedia(
                    player_id=player.id,
                    media_type="video",
                    url=video_url,
                    title="Highlight reel",
                    uploaded_by=pu.submitter_email or DATA_SOURCE,
                ))

        reviewer = body.reviewed_by if body else "admin"
        pu.status = "approved"
        pu.reviewed_by = reviewer
        pu.reviewed_at = datetime.now(timezone.utc)
        pu.review_notes = body.notes if body else None

        session.flush()

        logger.info(
            "PendingUpdate %d approved for player %d by %s (+%d career)",
            update_id, player.id, reviewer, career_added,
        )

        return ActionResponse(
            success=True,
            message=f"Update approved and applied to {player.full_name}",
        )


@router.post("/pending-updates/{update_id}/reject", response_model=ActionResponse)
def reject_pending_update(update_id: int, body: ReviewAction | None = None) -> ActionResponse:
    """Reject a pending update — the submitted data is discarded."""
    with get_session() as session:
        pu = session.get(PendingUpdate, update_id)
        if not pu:
            raise HTTPException(status_code=404, detail="Pending update not found")
        if pu.status != "pending":
            raise HTTPException(status_code=409, detail=f"Update already {pu.status}")

        reviewer = body.reviewed_by if body else "admin"
        pu.status = "rejected"
        pu.reviewed_by = reviewer
        pu.reviewed_at = datetime.now(timezone.utc)
        pu.review_notes = body.notes if body else None

        session.flush()

        logger.info("PendingUpdate %d rejected by %s", update_id, reviewer)

        return ActionResponse(
            success=True,
            message=f"Update {update_id} rejected",
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Internal helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _apply_field(player: Player, field: str, data: dict) -> None:
    """Set player.field = data[field] if the submitted value is non-null."""
    val = data.get(field)
    if val is not None:
        setattr(player, field, val)
