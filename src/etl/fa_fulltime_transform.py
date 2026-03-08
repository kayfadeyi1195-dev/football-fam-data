"""Transform raw FA Full-Time player data from staging_raw into core tables.

Processes ``staging_raw`` records where ``source='fa_fulltime'`` and
``source_entity_type='player'``.  Each record contains a player name,
club name, appearance count, and goal count scraped from the FA
Full-Time team pages.

For each record the pipeline:

1. Resolves the club by fuzzy-matching ``club_name`` against the
   ``clubs`` table.
2. Cleans the player name and — where available — normalises the
   position to our ``PositionPrimary`` enum.
3. Matches to an existing player using a three-tier strategy
   (exact name + club, fuzzy name + club, exact name + DOB).
4. Creates or updates the ``Player`` record, filling only NULL
   fields so richer data sources are never overwritten.
5. Upserts a ``PlayerSeason`` (appearances, goals;
   ``confidence_score=3``, ``data_source='fa_fulltime'``) and
   ensures a ``PlayerCareer`` entry exists.
6. Marks the ``staging_raw`` row as ``processed=True``.

Usage::

    python -m src.etl.fa_fulltime_transform
"""

import logging
import re
from datetime import date, datetime, timezone
from typing import Any

from rapidfuzz import fuzz, process
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.db.models import (
    Club,
    League,
    Player,
    PlayerCareer,
    PlayerSeason,
    StagingRaw,
)
from src.db.session import get_session

logger = logging.getLogger(__name__)

DATA_SOURCE = "fa_fulltime"
SEASON_LABEL = "2024-25"
FUZZY_THRESHOLD = 90
CLUB_FUZZY_THRESHOLD = 80
CONFIDENCE_SCORE = 3
COMMIT_EVERY = 50

# Regex that strips common suffixes from club names before comparison.
# Order matters — longer suffixes first so "Town FC" is removed before "FC".
_CLUB_SUFFIX_RE = re.compile(
    r"\s+(?:Town\s+FC|United\s+FC|City\s+FC|"
    r"A\.?F\.?C\.?|C\.?F\.?C\.?|F\.?C\.?)\s*$",
    re.IGNORECASE,
)


def strip_club_suffix(name: str) -> str:
    """Remove common trailing suffixes like 'FC', 'AFC', 'Town FC'.

    Applied to *both* sides of the comparison so that
    'Horsham FC' and 'Horsham' match on exact check, and the
    fuzzy scorer isn't distracted by boilerplate words.
    """
    return _CLUB_SUFFIX_RE.sub("", name.strip()).strip()

# ── position normalisation ────────────────────────────────────────────────
# FA Full-Time rarely provides positions, but some records do.

_POSITION_MAP: dict[str, str] = {
    "goalkeeper": "GK", "keeper": "GK", "gk": "GK",
    "defender": "DEF", "defence": "DEF", "def": "DEF",
    "centre back": "DEF", "center back": "DEF",
    "left back": "DEF", "right back": "DEF",
    "full back": "DEF", "wing back": "DEF",
    "midfield": "MID", "midfielder": "MID", "mid": "MID",
    "central midfield": "MID", "attacking midfield": "MID",
    "defensive midfield": "MID",
    "attacker": "FWD", "forward": "FWD", "fwd": "FWD",
    "striker": "FWD", "winger": "FWD",
    "centre forward": "FWD", "center forward": "FWD",
}


def normalise_position(raw: str | None) -> tuple[str | None, str | None]:
    """Return ``(position_primary, position_detail)``."""
    if not raw:
        return None, None
    clean = raw.strip()
    primary = _POSITION_MAP.get(clean.lower())
    if primary:
        return primary, clean
    first_word = clean.split()[0].lower() if clean else ""
    primary = _POSITION_MAP.get(first_word)
    if primary:
        return primary, clean
    return None, clean


# ── name cleaning ─────────────────────────────────────────────────────────

def clean_name(raw: str) -> str:
    """Strip, collapse whitespace, and title-case a player name."""
    name = raw.strip()
    name = re.sub(r"\s+", " ", name)
    return name.title()


def split_name(full: str) -> tuple[str, str]:
    """'John Smith' -> ('John', 'Smith').  Single-word names go to last."""
    parts = full.split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return "", full


# ── club resolution ───────────────────────────────────────────────────────

def _build_club_lookup(
    session: Session,
) -> tuple[dict[str, Club], dict[str, Club], list[str]]:
    """Build club-matching data structures.

    Returns:
        exact_lookup: ``{lowercase_name: Club}`` — includes both the
            original name and the suffix-stripped variant.
        stripped_lookup: ``{stripped_lowercase_name: Club}`` — for the
            fuzzy-match candidate list.
        stripped_names: sorted list of stripped lowercase names for
            rapidfuzz.
    """
    clubs = session.execute(
        select(Club).where(Club.is_active.is_(True))
    ).scalars().all()

    exact_lookup: dict[str, Club] = {}
    stripped_lookup: dict[str, Club] = {}

    for c in clubs:
        lower = c.name.lower()
        exact_lookup[lower] = c

        stripped = strip_club_suffix(c.name).lower()
        exact_lookup[stripped] = c
        stripped_lookup[stripped] = c

    stripped_names = sorted(stripped_lookup.keys())
    return exact_lookup, stripped_lookup, stripped_names


def _resolve_club(
    club_name: str,
    exact_lookup: dict[str, Club],
    stripped_lookup: dict[str, Club],
    stripped_names: list[str],
) -> Club | None:
    """Match an FA Full-Time team name to our clubs table.

    Strategy:
    1. Exact match on the full lowercase name.
    2. Exact match on the suffix-stripped lowercase name.
    3. Fuzzy match (WRatio) on suffix-stripped names.
    """
    key = club_name.lower().strip()
    hit = exact_lookup.get(key)
    if hit:
        return hit

    stripped_key = strip_club_suffix(club_name).lower()
    hit = exact_lookup.get(stripped_key)
    if hit:
        return hit

    if not stripped_names:
        return None

    result = process.extractOne(
        stripped_key,
        stripped_names,
        scorer=fuzz.WRatio,
        score_cutoff=CLUB_FUZZY_THRESHOLD,
    )
    if result:
        matched_name, score, _ = result
        logger.debug(
            "Club fuzzy match: '%s' -> '%s' (score=%.0f)",
            club_name, matched_name, score,
        )
        return stripped_lookup.get(matched_name)

    return None


# ── player matching (three-tier) ──────────────────────────────────────────

def _match_player(
    session: Session,
    full_name: str,
    club_id: int | None,
    dob: date | None,
    all_players: list[Player],
) -> Player | None:
    """Three-tier match: exact name+club -> fuzzy name+club -> exact name+DOB."""

    # Tier 1: exact name + same club
    for p in all_players:
        if p.full_name == full_name and p.current_club_id == club_id:
            return p

    # Tier 2: fuzzy name + same club
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
                    "Player fuzzy match: '%s' -> '%s' (score=%.0f)",
                    full_name, matched_name, score,
                )
                return club_players[idx]

    # Tier 3: exact name + DOB (player transferred between clubs)
    if dob:
        for p in all_players:
            if p.full_name == full_name and p.date_of_birth == dob:
                return p

    return None


# ── record processing ─────────────────────────────────────────────────────

def _fill_nulls(player: Player, values: dict[str, Any]) -> None:
    """Set attributes on *player* only where the current value is None."""
    for attr, val in values.items():
        if val is not None and getattr(player, attr, None) is None:
            setattr(player, attr, val)


def _process_record(
    session: Session,
    record: StagingRaw,
    exact_lookup: dict[str, Club],
    stripped_lookup: dict[str, Club],
    stripped_names: list[str],
    all_players: list[Player],
    counters: dict[str, int],
) -> None:
    """Process a single FA Full-Time player staging record."""
    raw: dict[str, Any] = record.raw_data

    # ── resolve club ──────────────────────────────────────────────────
    raw_club_name = raw.get("club_name") or raw.get("team_name") or ""
    if not raw_club_name:
        record.error_message = "Missing club/team name"
        counters["errors"] += 1
        return

    club = _resolve_club(raw_club_name, exact_lookup, stripped_lookup, stripped_names)
    if club is None:
        record.error_message = f"No club match for '{raw_club_name}'"
        counters["errors"] += 1
        return

    # ── clean player name ─────────────────────────────────────────────
    raw_name = raw.get("player_name", "")
    if not raw_name:
        record.error_message = "Missing player_name"
        counters["errors"] += 1
        return

    full_name = clean_name(raw_name)
    first_name, last_name = split_name(full_name)

    # ── normalise position (if present) ───────────────────────────────
    position_primary, position_detail = normalise_position(
        raw.get("position")
    )

    # ── match or create player ────────────────────────────────────────
    player = _match_player(
        session, full_name, club.id, None, all_players,
    )

    if player is None:
        player = Player(
            full_name=full_name,
            first_name=first_name or None,
            last_name=last_name or None,
            position_primary=position_primary,
            position_detail=position_detail,
            current_club_id=club.id,
            contract_status="unknown",
            availability="unknown",
            is_verified=False,
        )
        session.add(player)
        session.flush()
        all_players.append(player)
        counters["created"] += 1
        logger.debug("Created player: %s at %s", full_name, club.name)
    else:
        _fill_nulls(player, {
            "first_name": first_name or None,
            "last_name": last_name or None,
            "position_primary": position_primary,
            "position_detail": position_detail,
        })
        player.current_club_id = club.id
        counters["updated"] += 1
        logger.debug("Updated player: %s at %s", full_name, club.name)

    # ── upsert player_seasons ─────────────────────────────────────────
    apps = raw.get("appearances")
    goals = raw.get("goals")
    if apps is not None or goals is not None:
        _upsert_player_season(
            session,
            player_id=player.id,
            club_id=club.id,
            league_id=club.league_id,
            appearances=_safe_int(apps),
            goals=_safe_int(goals),
        )
        counters["stats"] += 1

    # ── ensure career entry ───────────────────────────────────────────
    _ensure_career_entry(session, player.id, club.id)
    counters["careers"] += 1

    counters["processed"] += 1


def _safe_int(val: Any) -> int | None:
    """Convert to int, returning None on failure."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


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
    """Create a PlayerCareer row if one doesn't already exist."""
    exists = session.execute(
        select(PlayerCareer.id).where(
            PlayerCareer.player_id == player_id,
            PlayerCareer.club_id == club_id,
            PlayerCareer.season_start == SEASON_LABEL,
        )
    ).scalar_one_or_none()

    if exists is None:
        session.add(PlayerCareer(
            player_id=player_id,
            club_id=club_id,
            season_start=SEASON_LABEL,
            role="player",
            source=DATA_SOURCE,
        ))


# ══════════════════════════════════════════════════════════════════════════
# Public entry point
# ══════════════════════════════════════════════════════════════════════════

def transform_fa_fulltime() -> dict[str, int]:
    """Process all unprocessed FA Full-Time player records from staging_raw.

    Returns a counters dict with keys: processed, created, updated,
    stats, careers, errors, skipped_no_club.
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
        # Build club lookup
        exact_lookup, stripped_lookup, stripped_names = _build_club_lookup(session)
        if not exact_lookup:
            logger.warning("No active clubs in database — nothing to transform")
            return counters

        logger.info(
            "Loaded %d club name variants for matching",
            len(exact_lookup),
        )

        # Load all existing players for matching
        all_players: list[Player] = list(
            session.execute(select(Player)).scalars().all()
        )
        logger.info("Loaded %d existing players for matching", len(all_players))

        # Fetch unprocessed FA Full-Time player records
        records: list[StagingRaw] = (
            session.query(StagingRaw)
            .filter(
                StagingRaw.source == DATA_SOURCE,
                StagingRaw.source_entity_type == "player",
                StagingRaw.processed.is_(False),
            )
            .all()
        )

        if not records:
            logger.info("No unprocessed FA Full-Time player records")
            return counters

        logger.info("Processing %d staging records…", len(records))

        for i, record in enumerate(records, 1):
            try:
                _process_record(
                    session, record,
                    exact_lookup, stripped_lookup, stripped_names,
                    all_players, counters,
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
        "FA Full-Time transform complete: %d processed, %d created, "
        "%d updated, %d stats, %d careers, %d errors",
        counters["processed"], counters["created"],
        counters["updated"], counters["stats"],
        counters["careers"], counters["errors"],
    )
    return counters


# ══════════════════════════════════════════════════════════════════════════
# CLI:  python -m src.etl.fa_fulltime_transform
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    results = transform_fa_fulltime()

    print()
    print("=" * 55)
    print("  FA Full-Time Transform Summary")
    print("=" * 55)
    print(f"  Records processed:    {results['processed']}")
    print(f"  New players created:  {results['created']}")
    print(f"  Players updated:      {results['updated']}")
    print(f"  Stats records:        {results['stats']}")
    print(f"  Career entries:       {results['careers']}")
    print(f"  Errors:               {results['errors']}")
    print("=" * 55)
