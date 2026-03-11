"""Player self-registration endpoints.

POST /api/v1/register/player              – register a new player
POST /api/v1/register/player/{id}/photo   – upload a profile photo
POST /api/v1/register/player/{id}/media   – attach a highlight video
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel, EmailStr, Field, field_validator
from rapidfuzz import fuzz, process
from sqlalchemy import select

from src.db.models import (
    Club,
    League,
    PendingUpdate,
    Player,
    PlayerCareer,
    PlayerMedia,
    PlayerSeason,
)
from src.db.session import get_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/register", tags=["registration"])

UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "data/uploads"))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

CLUB_FUZZY_THRESHOLD = 80
CURRENT_SEASON = "2025-26"
DATA_SOURCE = "self_registration"
ALLOWED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/webp"}
MAX_IMAGE_SIZE = 5 * 1024 * 1024  # 5 MB

VALID_POSITIONS = {"GK", "DEF", "MID", "FWD"}
VALID_FEET = {"L", "R", "B"}
VALID_CONTRACT = {
    "contracted", "out_of_contract", "loan", "trial", "released", "unknown",
}
VALID_AVAILABILITY = {
    "available", "not_available", "open_to_offers", "unknown",
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Request / response schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class CareerHistoryItem(BaseModel):
    club_name: str = Field(..., min_length=1, max_length=200)
    season_start: str = Field(..., pattern=r"^\d{4}-\d{2}$")
    season_end: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}$")
    appearances: int | None = Field(default=None, ge=0)
    goals: int | None = Field(default=None, ge=0)


class PlayerRegistration(BaseModel):
    full_name: str = Field(..., min_length=2, max_length=200)
    email: EmailStr
    phone: str | None = Field(default=None, max_length=30)
    date_of_birth: date
    nationality: str = Field(..., min_length=2, max_length=100)
    position_primary: str = Field(..., description="GK / DEF / MID / FWD")
    position_detail: str | None = Field(default=None, max_length=50)
    height_cm: int | None = Field(default=None, ge=120, le=220)
    preferred_foot: str | None = Field(default=None, description="L / R / B")
    current_club_name: str = Field(..., min_length=1, max_length=200)
    contract_status: str = Field(...)
    availability: str = Field(...)
    bio: str | None = Field(default=None, max_length=2000)
    career_history: list[CareerHistoryItem] = Field(default_factory=list)
    highlight_video_url: str | None = Field(default=None, max_length=500)

    @field_validator("position_primary")
    @classmethod
    def validate_position(cls, v: str) -> str:
        v = v.upper().strip()
        if v not in VALID_POSITIONS:
            raise ValueError(f"position_primary must be one of {VALID_POSITIONS}")
        return v

    @field_validator("preferred_foot")
    @classmethod
    def validate_foot(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.upper().strip()
        if v not in VALID_FEET:
            raise ValueError(f"preferred_foot must be one of {VALID_FEET}")
        return v

    @field_validator("contract_status")
    @classmethod
    def validate_contract(cls, v: str) -> str:
        v = v.lower().strip()
        if v not in VALID_CONTRACT:
            raise ValueError(f"contract_status must be one of {VALID_CONTRACT}")
        return v

    @field_validator("availability")
    @classmethod
    def validate_availability(cls, v: str) -> str:
        v = v.lower().strip()
        if v not in VALID_AVAILABILITY:
            raise ValueError(f"availability must be one of {VALID_AVAILABILITY}")
        return v


class MediaUpload(BaseModel):
    type: str = Field(..., description="video / image / document")
    url: str = Field(..., min_length=5, max_length=500)
    title: str | None = Field(default=None, max_length=200)
    description: str | None = Field(default=None, max_length=1000)


class ClubBrief(BaseModel):
    id: int
    name: str
    logo_url: str | None = None


class LeagueBrief(BaseModel):
    id: int
    name: str
    step: int


class RegistrationResponse(BaseModel):
    id: int
    full_name: str
    email: str | None = None
    date_of_birth: str | None = None
    nationality: str | None = None
    position_primary: str | None = None
    position_detail: str | None = None
    height_cm: int | None = None
    preferred_foot: str | None = None
    contract_status: str | None = None
    availability: str | None = None
    profile_photo_url: str | None = None
    bio: str | None = None
    is_verified: bool = False
    club: ClubBrief | None = None
    league: LeagueBrief | None = None
    career_entries_created: int = 0
    season_entries_created: int = 0
    is_new: bool = True
    pending_update_id: int | None = None


class PhotoResponse(BaseModel):
    player_id: int
    profile_photo_url: str


class MediaResponse(BaseModel):
    id: int
    player_id: int
    media_type: str
    url: str
    title: str | None = None
    description: str | None = None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _fuzzy_match_club(
    name: str,
    club_map: dict[str, int],
    club_names: list[str],
) -> int | None:
    """Return club_id for the best fuzzy match, or None."""
    name_lower = name.strip().lower()

    for db_name, db_id in club_map.items():
        if db_name.lower() == name_lower:
            return db_id

    if not club_names:
        return None

    result = process.extractOne(
        name,
        club_names,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=CLUB_FUZZY_THRESHOLD,
    )
    if result:
        matched_name, score, _ = result
        return club_map.get(matched_name)

    return None


def _build_club_lookup(session: Any) -> tuple[dict[str, int], list[str]]:
    """Return (name->id map, list of names) for fuzzy matching."""
    rows = session.execute(select(Club.id, Club.name)).all()
    club_map = {r.name: r.id for r in rows}
    return club_map, list(club_map.keys())


def _split_name(full_name: str) -> tuple[str, str]:
    """Split 'John Smith' into ('John', 'Smith')."""
    parts = full_name.strip().split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Endpoints
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@router.post("/player", response_model=RegistrationResponse, status_code=201)
def register_player(body: PlayerRegistration) -> RegistrationResponse:
    """Self-register as a player.

    If no existing match is found, a new unverified player record is
    created.  If an existing player matches on name + DOB, the
    submitted data is queued as a ``PendingUpdate`` for human review
    instead of overwriting verified API data.
    """
    with get_session() as session:
        club_map, club_names = _build_club_lookup(session)

        # ── 1. Fuzzy-match current club ──────────────────────────────
        current_club_id = _fuzzy_match_club(
            body.current_club_name, club_map, club_names,
        )
        if current_club_id is None:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Could not match club '{body.current_club_name}' "
                    f"to any club in the database. Check spelling."
                ),
            )

        club_obj = session.get(Club, current_club_id)
        league_obj = (
            session.get(League, club_obj.league_id)
            if club_obj and club_obj.league_id else None
        )

        # ── 2. Check for duplicate (name + DOB) ─────────────────────
        existing = session.execute(
            select(Player).where(
                Player.full_name.ilike(body.full_name.strip()),
                Player.date_of_birth == body.date_of_birth,
                Player.merged_into_id.is_(None),
            )
        ).scalar_one_or_none()

        if existing:
            submitted = _serialize_registration(body, current_club_id, club_map, club_names)

            pending = PendingUpdate(
                player_id=existing.id,
                submitted_data=submitted,
                submitter_email=body.email,
                submitter_phone=body.phone,
            )
            session.add(pending)
            session.flush()

            logger.info(
                "Player %s matched existing id=%d — queued as PendingUpdate %d",
                body.full_name, existing.id, pending.id,
            )

            return RegistrationResponse(
                id=existing.id,
                full_name=existing.full_name,
                email=existing.email,
                date_of_birth=existing.date_of_birth.isoformat() if existing.date_of_birth else None,
                nationality=existing.nationality,
                position_primary=existing.position_primary,
                position_detail=existing.position_detail,
                height_cm=existing.height_cm,
                preferred_foot=existing.preferred_foot,
                contract_status=existing.contract_status,
                availability=existing.availability,
                profile_photo_url=existing.profile_photo_url,
                bio=existing.bio,
                is_verified=existing.is_verified,
                club=ClubBrief(id=club_obj.id, name=club_obj.name, logo_url=club_obj.logo_url) if club_obj else None,
                league=LeagueBrief(id=league_obj.id, name=league_obj.name, step=league_obj.step) if league_obj else None,
                is_new=False,
                pending_update_id=pending.id,
            )

        # ── 3. Create new player ─────────────────────────────────────
        first, last = _split_name(body.full_name)

        player = Player(
            full_name=body.full_name.strip(),
            first_name=first,
            last_name=last,
            email=body.email,
            phone=body.phone,
            date_of_birth=body.date_of_birth,
            nationality=body.nationality,
            position_primary=body.position_primary,
            position_detail=body.position_detail,
            height_cm=body.height_cm,
            preferred_foot=body.preferred_foot,
            current_club_id=current_club_id,
            contract_status=body.contract_status,
            availability=body.availability,
            bio=body.bio,
            is_verified=False,
            overall_confidence=1,
        )
        session.add(player)
        session.flush()

        # ── 4. Career history ────────────────────────────────────────
        career_count = 0
        season_count = 0

        for entry in body.career_history:
            career_club_id = _fuzzy_match_club(
                entry.club_name, club_map, club_names,
            )

            if career_club_id:
                pc = PlayerCareer(
                    player_id=player.id,
                    club_id=career_club_id,
                    season_start=entry.season_start,
                    season_end=entry.season_end,
                    role="player",
                    source=DATA_SOURCE,
                )
                session.add(pc)
                career_count += 1

                if entry.appearances is not None or entry.goals is not None:
                    career_club = session.get(Club, career_club_id)
                    league_id = career_club.league_id if career_club else None

                    ps = PlayerSeason(
                        player_id=player.id,
                        club_id=career_club_id,
                        league_id=league_id,
                        season=entry.season_start,
                        appearances=entry.appearances,
                        goals=entry.goals,
                        data_source=DATA_SOURCE,
                        confidence_score=1,
                    )
                    session.add(ps)
                    season_count += 1
            else:
                logger.warning(
                    "Career club '%s' not matched for player %s",
                    entry.club_name, body.full_name,
                )

        # ── 5. Highlight video ───────────────────────────────────────
        if body.highlight_video_url:
            media = PlayerMedia(
                player_id=player.id,
                media_type="video",
                url=body.highlight_video_url,
                title="Highlight reel",
                uploaded_by=body.email,
            )
            session.add(media)

        session.flush()

        logger.info(
            "New player registered: id=%d name=%s club=%s career=%d stats=%d",
            player.id, player.full_name, club_obj.name if club_obj else "?",
            career_count, season_count,
        )

        return RegistrationResponse(
            id=player.id,
            full_name=player.full_name,
            email=player.email,
            date_of_birth=player.date_of_birth.isoformat() if player.date_of_birth else None,
            nationality=player.nationality,
            position_primary=player.position_primary,
            position_detail=player.position_detail,
            height_cm=player.height_cm,
            preferred_foot=player.preferred_foot,
            contract_status=player.contract_status,
            availability=player.availability,
            profile_photo_url=player.profile_photo_url,
            bio=player.bio,
            is_verified=False,
            club=ClubBrief(id=club_obj.id, name=club_obj.name, logo_url=club_obj.logo_url) if club_obj else None,
            league=LeagueBrief(id=league_obj.id, name=league_obj.name, step=league_obj.step) if league_obj else None,
            career_entries_created=career_count,
            season_entries_created=season_count,
            is_new=True,
        )


@router.post("/player/{player_id}/photo", response_model=PhotoResponse)
async def upload_photo(player_id: int, file: UploadFile = File(...)) -> PhotoResponse:
    """Upload a profile photo for a registered player.

    Accepts JPEG, PNG, or WebP up to 5 MB.  The file is saved to
    the ``data/uploads/`` directory and the URL is stored on the
    player record.
    """
    if file.content_type not in ALLOWED_IMAGE_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"File type must be one of {ALLOWED_IMAGE_TYPES}",
        )

    contents = await file.read()
    if len(contents) > MAX_IMAGE_SIZE:
        raise HTTPException(status_code=400, detail="Image must be under 5 MB")

    ext = file.content_type.split("/")[-1]
    if ext == "jpeg":
        ext = "jpg"
    filename = f"player_{player_id}_{uuid.uuid4().hex[:8]}.{ext}"
    filepath = UPLOAD_DIR / filename

    with open(filepath, "wb") as f:
        f.write(contents)

    photo_url = f"/uploads/{filename}"

    with get_session() as session:
        player = session.get(Player, player_id)
        if not player:
            filepath.unlink(missing_ok=True)
            raise HTTPException(status_code=404, detail="Player not found")

        player.profile_photo_url = photo_url
        session.flush()

    logger.info("Photo uploaded for player %d: %s", player_id, photo_url)
    return PhotoResponse(player_id=player_id, profile_photo_url=photo_url)


@router.post("/player/{player_id}/media", response_model=MediaResponse, status_code=201)
def add_media(player_id: int, body: MediaUpload) -> MediaResponse:
    """Attach a media item (highlight video, image, document) to a player."""
    valid_types = {"video", "image", "document"}
    media_type = body.type.lower().strip()
    if media_type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"type must be one of {valid_types}",
        )

    with get_session() as session:
        player = session.get(Player, player_id)
        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        media = PlayerMedia(
            player_id=player_id,
            media_type=media_type,
            url=body.url,
            title=body.title,
            description=body.description,
            uploaded_by=player.email or "self-registration",
        )
        session.add(media)
        session.flush()

        logger.info("Media added for player %d: %s %s", player_id, media_type, body.url)

        return MediaResponse(
            id=media.id,
            player_id=media.player_id,
            media_type=media.media_type,
            url=media.url,
            title=media.title,
            description=media.description,
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Internal helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _serialize_registration(
    body: PlayerRegistration,
    club_id: int,
    club_map: dict[str, int],
    club_names: list[str],
) -> dict[str, Any]:
    """Convert the registration body to a JSON-safe dict for storage."""
    career = []
    for c in body.career_history:
        career_club_id = _fuzzy_match_club(c.club_name, club_map, club_names)
        career.append({
            "club_name": c.club_name,
            "club_id": career_club_id,
            "season_start": c.season_start,
            "season_end": c.season_end,
            "appearances": c.appearances,
            "goals": c.goals,
        })

    return {
        "full_name": body.full_name,
        "email": body.email,
        "phone": body.phone,
        "date_of_birth": body.date_of_birth.isoformat(),
        "nationality": body.nationality,
        "position_primary": body.position_primary,
        "position_detail": body.position_detail,
        "height_cm": body.height_cm,
        "preferred_foot": body.preferred_foot,
        "current_club_id": club_id,
        "current_club_name": body.current_club_name,
        "contract_status": body.contract_status,
        "availability": body.availability,
        "bio": body.bio,
        "career_history": career,
        "highlight_video_url": body.highlight_video_url,
    }
