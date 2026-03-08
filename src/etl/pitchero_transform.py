"""Transform raw Pitchero data from staging_raw into core tables.

Processes two entity types staged by the Pitchero scraper:

* ``source_entity_type='player'`` — squad-list records with name,
  position, and photo.
* ``source_entity_type='player_profile'`` — richer profile records
  with bio text, birthplace, join date, previous clubs, and
  per-season appearance / goal counts.

For each record the pipeline:

1. Resolves the club by extracting the Pitchero slug from the
   ``profile_url`` field and matching it to ``clubs.pitchero_url``.
2. Cleans the player name and normalises the position to our
   ``PositionPrimary`` enum (GK / DEF / MID / FWD).
3. Matches to an existing player using a three-tier strategy
   (exact name + club → fuzzy name + club → exact name + DOB).
4. Creates or updates the ``Player`` record, filling only NULL
   fields so richer sources are never overwritten.
5. For profile records, upserts a ``PlayerSeason`` (appearances,
   goals; ``confidence_score=3``) and ensures a ``PlayerCareer``
   entry exists for this club/season.
6. Marks the ``staging_raw`` row as ``processed=True``.

Usage::

    python -m src.etl.pitchero_transform
"""

import logging
import re
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urlparse

from rapidfuzz import fuzz, process
from sqlalchemy import case, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.db.models import (
    Club,
    Player,
    PlayerCareer,
    PlayerSeason,
    StagingRaw,
)
from src.db.session import get_session

logger = logging.getLogger(__name__)

# ── constants ────────────────────────────────────────────────────────────

DATA_SOURCE = "pitchero"
SEASON_LABEL = "2024-25"
FUZZY_THRESHOLD = 90
CONFIDENCE_SCORE = 3
COMMIT_EVERY = 50

# ── position normalisation ───────────────────────────────────────────────

_POSITION_MAP: dict[str, str] = {
    "goalkeeper": "GK",
    "keeper": "GK",

    "defender": "DEF",
    "centre back": "DEF",
    "center back": "DEF",
    "central defender": "DEF",
    "left back": "DEF",
    "right back": "DEF",
    "wing back": "DEF",
    "full back": "DEF",

    "midfield": "MID",
    "midfielder": "MID",
    "central midfield": "MID",
    "central midfielder": "MID",
    "attacking midfield": "MID",
    "attacking midfielder": "MID",
    "defensive midfield": "MID",
    "defensive midfielder": "MID",
    "left midfield": "MID",
    "right midfield": "MID",
    "wide midfield": "MID",

    "attacker": "FWD",
    "forward": "FWD",
    "striker": "FWD",
    "centre forward": "FWD",
    "center forward": "FWD",
    "winger": "FWD",
    "right winger": "FWD",
    "left winger": "FWD",
    "inside forward": "FWD",
}

_GROUP_TO_PRIMARY: dict[str, str] = {
    "goalkeepers": "GK",
    "defenders": "DEF",
    "midfielders": "MID",
    "attackers": "FWD",
}


def normalise_position(
    detail: str | None,
    group: str | None = None,
) -> tuple[str | None, str | None]:
    """Return ``(position_primary, position_detail)``."""
    if detail:
        detail_clean = detail.strip()
        primary = _POSITION_MAP.get(detail_clean.lower())
        if primary:
            return primary, detail_clean
        # Partial match on first word
        first_word = detail_clean.split()[0].lower() if detail_clean else ""
        primary = _POSITION_MAP.get(first_word)
        if primary:
            return primary, detail_clean

    if group:
        primary = _GROUP_TO_PRIMARY.get(group.lower().strip())
        if primary:
            return primary, detail.strip() if detail else None

    return None, detail.strip() if detail else None


# ── name cleaning ────────────────────────────────────────────────────────

def clean_name(raw: str) -> str:
    """Strip, collapse whitespace, and title-case a player name."""
    name = raw.strip()
    name = re.sub(r"\s+", " ", name)
    return name.title()


def split_name(full: str) -> tuple[str, str]:
    """'John Smith' → ('John', 'Smith').  Single-word names go to last."""
    parts = full.split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return "", full


def name_from_formal(formal: str) -> str:
    """'Smith, John' → 'John Smith'."""
    if "," in formal:
        parts = [p.strip() for p in formal.split(",", 1)]
        if len(parts) == 2 and parts[1]:
            return f"{parts[1]} {parts[0]}"
    return formal


# ── height parsing ───────────────────────────────────────────────────────

_FT_IN_RE = re.compile(
    r"""(\d+)\s*(?:'|ft|feet)\s*(\d+)?\s*(?:"|in|inches)?""",
    re.IGNORECASE,
)
_CM_RE = re.compile(r"(\d{2,3})\s*cm", re.IGNORECASE)


def parse_height_cm(raw: str | None) -> int | None:
    """Convert various height formats to centimetres.

    Handles ``"5'11"``, ``"6ft 2in"``, ``"180cm"``, ``"180"``.
    """
    if not raw:
        return None
    raw = raw.strip()

    m = _CM_RE.search(raw)
    if m:
        return int(m.group(1))

    m = _FT_IN_RE.search(raw)
    if m:
        feet = int(m.group(1))
        inches = int(m.group(2)) if m.group(2) else 0
        return round(feet * 30.48 + inches * 2.54)

    try:
        val = int(raw)
        if 100 <= val <= 220:
            return val
    except ValueError:
        pass

    return None


# ── date parsing ─────────────────────────────────────────────────────────

_DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%d %B %Y",
    "%d %b %Y",
    "%B %d, %Y",
    "%b %d, %Y",
]


def parse_dob(raw: str | None) -> date | None:
    """Try multiple date formats and return a ``date`` or ``None``."""
    if not raw:
        return None
    raw = raw.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    try:
        return date.fromisoformat(raw)
    except (ValueError, TypeError):
        pass
    return None


# ── club resolution ──────────────────────────────────────────────────────

def _extract_pitchero_slug(profile_url: str) -> str | None:
    """``…/clubs/ashfordunitedfc/teams/…`` → ``ashfordunitedfc``."""
    path = urlparse(profile_url).path
    m = re.search(r"/clubs/([^/]+)", path)
    return m.group(1) if m else None


def _build_club_lookup(session: Session) -> dict[str, Club]:
    """Return ``{pitchero_slug: Club}`` for every club with a pitchero_url."""
    clubs = session.execute(
        select(Club).where(Club.pitchero_url.isnot(None)).where(Club.pitchero_url != "")
    ).scalars().all()

    lookup: dict[str, Club] = {}
    for club in clubs:
        slug = _extract_pitchero_slug(club.pitchero_url)
        if slug:
            lookup[slug] = club
    return lookup


# ── player matching (three-tier) ─────────────────────────────────────────

def _match_player(
    session: Session,
    full_name: str,
    club_id: int | None,
    dob: date | None,
    all_players: list[Player],
    name_list: list[str],
) -> Player | None:
    """Three-tier match: exact+club → fuzzy+club → exact+DOB."""

    # Tier 1: exact name + same club
    for p in all_players:
        if p.full_name == full_name and p.current_club_id == club_id:
            return p

    # Tier 2: fuzzy name + same club (among players at this club)
    if club_id is not None:
        club_players = [p for p in all_players if p.current_club_id == club_id]
        club_names = [p.full_name for p in club_players]
        if club_names:
            result = process.extractOne(
                full_name, club_names,
                scorer=fuzz.WRatio,
                score_cutoff=FUZZY_THRESHOLD,
            )
            if result:
                matched_name, score, idx = result
                logger.debug(
                    "Fuzzy match: '%s' → '%s' (score=%.0f)",
                    full_name, matched_name, score,
                )
                return club_players[idx]

    # Tier 3: exact name + DOB (player transferred between clubs)
    if dob:
        for p in all_players:
            if p.full_name == full_name and p.date_of_birth == dob:
                return p

    return None


# ── record processing ────────────────────────────────────────────────────

def _process_record(
    session: Session,
    record: StagingRaw,
    club_lookup: dict[str, Club],
    all_players: list[Player],
    name_list: list[str],
    counters: dict[str, int],
) -> None:
    """Process a single staging_raw record."""
    raw: dict[str, Any] = record.raw_data
    entity_type = record.source_entity_type

    # ── resolve club ─────────────────────────────────────────────────
    profile_url = raw.get("profile_url", "")
    slug = _extract_pitchero_slug(profile_url)
    club = club_lookup.get(slug) if slug else None

    if club is None:
        record.error_message = f"No club match for slug '{slug}'"
        counters["errors"] += 1
        return

    # ── clean player name ────────────────────────────────────────────
    raw_name = raw.get("name") or ""
    formal = raw.get("formal_name", "")
    if not raw_name and formal:
        raw_name = name_from_formal(formal)
    if not raw_name:
        record.error_message = "Missing player name"
        counters["errors"] += 1
        return

    full_name = clean_name(raw_name)
    first_name, last_name = split_name(full_name)

    # ── normalise position ───────────────────────────────────────────
    pos_detail = raw.get("position")
    pos_group = raw.get("position_group")
    position_primary, position_detail = normalise_position(pos_detail, pos_group)

    # ── parse optional bio fields ────────────────────────────────────
    dob = parse_dob(raw.get("date_of_birth"))
    height_cm = parse_height_cm(raw.get("height"))
    photo_url = raw.get("photo_url")
    bio_text = raw.get("biography")
    birthplace = raw.get("birthplace")

    # ── match or create player ───────────────────────────────────────
    player = _match_player(
        session, full_name, club.id, dob, all_players, name_list,
    )

    if player is None:
        player = Player(
            full_name=full_name,
            first_name=first_name or None,
            last_name=last_name or None,
            date_of_birth=dob,
            nationality=None,
            position_primary=position_primary,
            position_detail=position_detail,
            height_cm=height_cm,
            current_club_id=club.id,
            profile_photo_url=photo_url,
            bio=bio_text,
            contract_status="unknown",
            availability="unknown",
            is_verified=False,
        )
        session.add(player)
        session.flush()
        all_players.append(player)
        name_list.append(full_name)
        counters["created"] += 1
        logger.debug("Created player: %s at %s", full_name, club.name)
    else:
        _fill_nulls(player, {
            "first_name": first_name or None,
            "last_name": last_name or None,
            "date_of_birth": dob,
            "position_primary": position_primary,
            "position_detail": position_detail,
            "height_cm": height_cm,
            "profile_photo_url": photo_url,
            "bio": bio_text,
        })
        player.current_club_id = club.id
        counters["updated"] += 1
        logger.debug("Updated player: %s at %s", full_name, club.name)

    # ── profile-specific enrichment ──────────────────────────────────
    if entity_type == "player_profile":
        stats_map = raw.get("stats_by_season", {})
        for _season_id, stats in stats_map.items():
            apps = stats.get("appearances")
            goals = stats.get("goals")
            if apps is None and goals is None:
                continue
            _upsert_player_season(
                session,
                player_id=player.id,
                club_id=club.id,
                league_id=club.league_id,
                appearances=apps,
                goals=goals,
            )
            counters["stats"] += 1

        _ensure_career_entry(session, player.id, club.id)
        counters["careers"] += 1

    counters["processed"] += 1


def _fill_nulls(player: Player, values: dict[str, Any]) -> None:
    """Set attributes on *player* only where the current value is None."""
    for attr, val in values.items():
        if val is not None and getattr(player, attr, None) is None:
            setattr(player, attr, val)


def _upsert_player_season(
    session: Session,
    *,
    player_id: int,
    club_id: int,
    league_id: int | None,
    appearances: int | None,
    goals: int | None,
) -> None:
    """Insert or update a PlayerSeason row (keyed on the unique constraint)."""
    values = dict(
        player_id=player_id,
        club_id=club_id,
        league_id=league_id,
        season=SEASON_LABEL,
        appearances=appearances,
        goals=goals,
        data_source=DATA_SOURCE,
        confidence_score=CONFIDENCE_SCORE,
    )
    stmt = pg_insert(PlayerSeason).values(**values)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_player_club_season_source",
        set_={
            "league_id": stmt.excluded.league_id,
            "appearances": stmt.excluded.appearances,
            "goals": stmt.excluded.goals,
            "confidence_score": stmt.excluded.confidence_score,
            "updated_at": func.now(),
        },
    )
    session.execute(stmt)


def _ensure_career_entry(
    session: Session,
    player_id: int,
    club_id: int,
) -> None:
    """Create a PlayerCareer row if one doesn't already exist for this
    player/club/season combination."""
    exists = session.execute(
        select(PlayerCareer.id).where(
            PlayerCareer.player_id == player_id,
            PlayerCareer.club_id == club_id,
            PlayerCareer.season_start == SEASON_LABEL,
        )
    ).scalar_one_or_none()

    if exists is None:
        career = PlayerCareer(
            player_id=player_id,
            club_id=club_id,
            season_start=SEASON_LABEL,
            role="player",
            source=DATA_SOURCE,
        )
        session.add(career)


# ══════════════════════════════════════════════════════════════════════════
# Public entry point
# ══════════════════════════════════════════════════════════════════════════

def transform_pitchero() -> dict[str, int]:
    """Process all unprocessed Pitchero records from staging_raw.

    Returns a counters dict: processed, created, updated, stats,
    careers, errors.
    """
    counters: dict[str, int] = {
        "processed": 0,
        "created": 0,
        "updated": 0,
        "stats": 0,
        "careers": 0,
        "errors": 0,
    }

    with get_session() as session:
        club_lookup = _build_club_lookup(session)
        if not club_lookup:
            logger.warning("No clubs with pitchero_url — nothing to transform")
            return counters

        logger.info("Loaded %d clubs with Pitchero URLs", len(club_lookup))

        # Load all existing players for matching
        all_players = session.execute(select(Player)).scalars().all()
        name_list = [p.full_name for p in all_players]
        logger.info("Loaded %d existing players for matching", len(all_players))

        # Profile records first (richer data), then squad records
        records = (
            session.query(StagingRaw)
            .filter(
                StagingRaw.source == DATA_SOURCE,
                StagingRaw.processed.is_(False),
                StagingRaw.source_entity_type.in_(["player", "player_profile"]),
            )
            .order_by(
                case(
                    (StagingRaw.source_entity_type == "player_profile", 0),
                    else_=1,
                )
            )
            .all()
        )

        if not records:
            logger.info("No unprocessed Pitchero records in staging_raw")
            return counters

        logger.info("Processing %d staging records…", len(records))

        for i, record in enumerate(records, 1):
            try:
                _process_record(
                    session, record, club_lookup,
                    all_players, name_list, counters,
                )
            except Exception as exc:
                record.error_message = str(exc)[:500]
                counters["errors"] += 1
                logger.warning(
                    "Error on staging_raw id=%d: %s", record.id, exc,
                )

            record.processed = True
            record.processed_at = datetime.now(timezone.utc)

            if i % COMMIT_EVERY == 0:
                session.commit()
                logger.info("  Progress: %d / %d", i, len(records))

    logger.info(
        "Pitchero transform complete: %d processed, %d created, "
        "%d updated, %d stats, %d careers, %d errors",
        counters["processed"], counters["created"],
        counters["updated"], counters["stats"],
        counters["careers"], counters["errors"],
    )
    return counters


# ══════════════════════════════════════════════════════════════════════════
# CLI:  python -m src.etl.pitchero_transform
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    results = transform_pitchero()

    print()
    print("=" * 55)
    print("  Pitchero Transform Summary")
    print("=" * 55)
    print(f"  Records processed:    {results['processed']}")
    print(f"  New players created:  {results['created']}")
    print(f"  Players updated:      {results['updated']}")
    print(f"  Stats records:        {results['stats']}")
    print(f"  Career entries:       {results['careers']}")
    print(f"  Errors:               {results['errors']}")
    print("=" * 55)
