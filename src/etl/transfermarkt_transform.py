"""Transform raw Transfermarkt data from staging_raw into core tables.

Processes three entity types staged by the scraper:

* ``source_entity_type='club_squad'`` — club records with nested
  player lists from competition scraping.
* ``source_entity_type='enrichment'`` — individual player search
  results with profile, stats, transfers, and market value data.
* ``source_entity_type='player_stats'`` — per-player season stats.
* ``source_entity_type='player_transfers'`` — transfer history.
* ``source_entity_type='market_value'`` — market value history
  (stored for future use, not transformed into a dedicated table).

For each player record the pipeline:

1. Fuzzy-matches the club name to our ``clubs`` table.
2. Cleans the player name and normalises the position.
3. Matches to an existing player using three-tier matching
   (exact name + club → fuzzy name + club → exact name + DOB).
4. Fills NULL fields on the ``Player`` record — Transfermarkt is
   especially good for ``height_cm``, ``preferred_foot``,
   ``contract_status``, ``nationality``, and ``date_of_birth``.
5. Upserts ``PlayerSeason`` records with real stats (appearances,
   goals, assists, cards, minutes) from the stats pages.
6. Creates ``PlayerCareer`` entries from transfer history, with
   ``season_start``, ``season_end``, and ``role`` (player/loan).
7. Marks the ``staging_raw`` row as processed.

Usage::

    python -m src.etl.transfermarkt_transform
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
    Player,
    PlayerCareer,
    PlayerSeason,
    StagingRaw,
)
from src.db.session import get_session

logger = logging.getLogger(__name__)

DATA_SOURCE = "transfermarkt"
SEASON_LABEL = "2024-25"
FUZZY_THRESHOLD = 85
CONFIDENCE_SCORE = 4
COMMIT_EVERY = 50

_POSITION_MAP: dict[str, str] = {
    "goalkeeper": "GK", "keeper": "GK",
    "centre-back": "DEF", "left-back": "DEF", "right-back": "DEF",
    "defender": "DEF",
    "defensive midfield": "MID", "central midfield": "MID",
    "attacking midfield": "MID", "left midfield": "MID",
    "right midfield": "MID", "midfielder": "MID",
    "left winger": "FWD", "right winger": "FWD",
    "second striker": "FWD", "centre-forward": "FWD",
    "forward": "FWD", "striker": "FWD", "attack": "FWD",
}

_DATE_FORMATS = [
    "%b %d, %Y",
    "%B %d, %Y",
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%d %B %Y",
    "%d %b %Y",
]


# ── helpers ───────────────────────────────────────────────────────────────

def _normalise_position(raw: str | None) -> tuple[str | None, str | None]:
    if not raw:
        return None, None
    detail = raw.strip()
    primary = _POSITION_MAP.get(detail.lower())
    if primary:
        return primary, detail
    first_word = detail.split()[0].lower() if detail else ""
    primary = _POSITION_MAP.get(first_word)
    if primary:
        return primary, detail
    for keyword, pos in _POSITION_MAP.items():
        if keyword in detail.lower():
            return pos, detail
    return None, detail


def _parse_dob(raw: str | None) -> date | None:
    if not raw:
        return None
    raw = raw.strip()
    raw = re.sub(r"\s*\(\d+\)\s*$", "", raw)
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    try:
        return date.fromisoformat(raw)
    except (ValueError, TypeError):
        return None


def _parse_height(raw: str | None) -> int | None:
    if not raw:
        return None
    raw = raw.strip()
    try:
        val = float(raw.replace(",", ".").rstrip("m").strip())
        if val < 3:
            return int(val * 100)
        return int(val)
    except (ValueError, TypeError):
        return None


def _clean_name(raw: str) -> str:
    name = raw.strip()
    name = re.sub(r"\s+", " ", name)
    return name


def _split_name(full: str) -> tuple[str, str]:
    parts = full.split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return "", full


def _infer_contract_status(raw: str | None) -> str | None:
    """Map Transfermarkt contract info to our ContractStatus enum."""
    if not raw:
        return None
    lower = raw.lower()
    if "free agent" in lower or "without club" in lower:
        return "out_of_contract"
    if "loan" in lower:
        return "loan"
    try:
        expiry = _parse_dob(raw)
        if expiry:
            return "contracted"
    except Exception:
        pass
    return None


def _date_to_season(d: date | None) -> str:
    """Convert a date to the ``YYYY-YY`` season label it falls in.

    Football seasons run roughly Aug-May.  A date in July 2024
    maps to ``"2024-25"``; a date in January 2024 maps to ``"2023-24"``.
    """
    if not d:
        return SEASON_LABEL
    year = d.year
    if d.month >= 7:
        return f"{year}-{str(year + 1)[-2:]}"
    return f"{year - 1}-{str(year)[-2:]}"


def _parse_season_raw(raw: str | None) -> str | None:
    """Parse Transfermarkt season strings like '24/25' or '2024/2025'."""
    if not raw:
        return None
    raw = raw.strip()
    m = re.match(r"(\d{2,4})/(\d{2,4})", raw)
    if m:
        start = m.group(1)
        end = m.group(2)
        if len(start) == 2:
            start = "20" + start if int(start) < 50 else "19" + start
        if len(end) == 2:
            end_full = start[:2] + end
        else:
            end_full = end
        return f"{start}-{end_full[-2:]}"
    m2 = re.match(r"(\d{4})", raw)
    if m2:
        year = int(m2.group(1))
        return f"{year}-{str(year + 1)[-2:]}"
    return None


# ── club resolution ───────────────────────────────────────────────────────

def _build_club_lookup(
    session: Session,
) -> tuple[dict[str, int], list[str]]:
    rows = session.execute(select(Club.id, Club.name)).all()
    club_map = {r.name: r.id for r in rows}
    return club_map, list(club_map.keys())


def _resolve_club(
    raw_name: str,
    club_map: dict[str, int],
    club_names: list[str],
) -> int | None:
    if not raw_name:
        return None
    if raw_name in club_map:
        return club_map[raw_name]
    result = process.extractOne(
        raw_name, club_names,
        scorer=fuzz.WRatio,
        score_cutoff=80,
    )
    if result:
        return club_map[result[0]]
    return None


# ── player matching ───────────────────────────────────────────────────────

def _match_player(
    full_name: str,
    club_id: int | None,
    dob: date | None,
    all_players: list[Player],
) -> Player | None:
    for p in all_players:
        if p.full_name == full_name and p.current_club_id == club_id:
            return p

    if club_id is not None:
        club_players = [p for p in all_players if p.current_club_id == club_id]
        club_names = [p.full_name for p in club_players]
        if club_names:
            result = process.extractOne(
                full_name, club_names,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=FUZZY_THRESHOLD,
            )
            if result:
                return club_players[result[2]]

    if dob:
        for p in all_players:
            if p.full_name == full_name and p.date_of_birth == dob:
                return p

    return None


# ── record processing (club_squad / enrichment) ──────────────────────────

def _process_record(
    session: Session,
    record: StagingRaw,
    club_map: dict[str, int],
    club_names: list[str],
    all_players: list[Player],
    counters: dict[str, int],
) -> None:
    raw: dict[str, Any] = record.raw_data
    entity_type = record.source_entity_type

    if entity_type == "market_value":
        counters["processed"] += 1
        return

    if entity_type == "player_stats":
        _process_stats_record(session, raw, club_map, club_names, all_players, counters)
        counters["processed"] += 1
        return

    if entity_type == "player_transfers":
        _process_transfers_record(session, raw, club_map, club_names, all_players, counters)
        counters["processed"] += 1
        return

    # club_squad or enrichment — may contain nested player lists
    players_to_process: list[dict[str, Any]] = []
    if "players" in raw and isinstance(raw["players"], list):
        for p in raw["players"]:
            p.setdefault("_club_name", raw.get("name", ""))
            p.setdefault("_competition_id", raw.get("competition_id", ""))
            players_to_process.append(p)
    else:
        players_to_process.append(raw)

    for p_data in players_to_process:
        _process_single_player(
            session, p_data, club_map, club_names, all_players, counters,
        )

    # If enrichment record has stats/transfers nested, process those too
    if entity_type == "enrichment":
        player_name = _clean_name(raw.get("name", ""))
        dob = _parse_dob(raw.get("date_of_birth"))
        club_name = raw.get("current_club_name", "")
        club_id = _resolve_club(club_name, club_map, club_names) if club_name else None
        player = _match_player(player_name, club_id, dob, all_players) if player_name else None

        if player and raw.get("stats"):
            for stat_row in raw["stats"]:
                _upsert_season_from_stats(
                    session, player.id, stat_row, club_map, club_names, counters,
                )

        if player and raw.get("transfers"):
            _create_careers_from_transfers(
                session, player.id, raw["transfers"], club_map, club_names, counters,
            )

    counters["processed"] += 1


def _process_single_player(
    session: Session,
    raw: dict[str, Any],
    club_map: dict[str, int],
    club_names: list[str],
    all_players: list[Player],
    counters: dict[str, int],
) -> None:
    raw_name = raw.get("name", "").strip()
    if not raw_name:
        return

    full_name = _clean_name(raw_name)
    first_name, last_name = _split_name(full_name)

    dob = _parse_dob(raw.get("date_of_birth"))
    height_cm = raw.get("height_cm") or _parse_height(raw.get("height_raw"))
    position_primary, position_detail = _normalise_position(raw.get("position_detail"))
    nationality = raw.get("nationality")
    preferred_foot = raw.get("preferred_foot")
    photo_url = raw.get("photo_url")
    contract_expiry = raw.get("contract_expiry")
    contract_status = _infer_contract_status(contract_expiry)

    club_name = raw.get("_club_name") or raw.get("current_club_name", "")
    club_id = _resolve_club(club_name, club_map, club_names) if club_name else None

    player = _match_player(full_name, club_id, dob, all_players)

    if player is None:
        player = Player(
            full_name=full_name,
            first_name=first_name or None,
            last_name=last_name or None,
            date_of_birth=dob,
            nationality=nationality,
            position_primary=position_primary,
            position_detail=position_detail,
            height_cm=height_cm,
            preferred_foot=preferred_foot,
            current_club_id=club_id,
            profile_photo_url=photo_url,
            contract_status=contract_status or "unknown",
            availability="unknown",
            is_verified=False,
        )
        session.add(player)
        session.flush()
        all_players.append(player)
        counters["created"] += 1
    else:
        if not player.date_of_birth and dob:
            player.date_of_birth = dob
        if not player.nationality and nationality:
            player.nationality = nationality
        if not player.position_primary and position_primary:
            player.position_primary = position_primary
        if not player.position_detail and position_detail:
            player.position_detail = position_detail
        if not player.height_cm and height_cm:
            player.height_cm = height_cm
        if not player.preferred_foot and preferred_foot:
            player.preferred_foot = preferred_foot
        if not player.profile_photo_url and photo_url:
            player.profile_photo_url = photo_url
        if not player.first_name and first_name:
            player.first_name = first_name
        if not player.last_name and last_name:
            player.last_name = last_name
        if contract_status and player.contract_status in (None, "unknown"):
            player.contract_status = contract_status
        if club_id:
            player.current_club_id = club_id
        counters["updated"] += 1

    if club_id:
        _upsert_season_basic(session, player.id, club_id, counters)
        _ensure_career(session, player.id, club_id, SEASON_LABEL)
        counters["careers"] += 1


# ── stats processing ─────────────────────────────────────────────────────

def _process_stats_record(
    session: Session,
    raw: dict[str, Any],
    club_map: dict[str, int],
    club_names: list[str],
    all_players: list[Player],
    counters: dict[str, int],
) -> None:
    """Process a standalone player_stats staging record."""
    player_name = _clean_name(raw.get("player_name", ""))
    if not player_name:
        return
    dob = _parse_dob(raw.get("date_of_birth"))
    club_name = raw.get("club_name", "")
    club_id = _resolve_club(club_name, club_map, club_names) if club_name else None
    player = _match_player(player_name, club_id, dob, all_players)
    if not player:
        return

    stats_list = raw.get("stats", [])
    for stat_row in stats_list:
        _upsert_season_from_stats(
            session, player.id, stat_row, club_map, club_names, counters,
        )


def _upsert_season_from_stats(
    session: Session,
    player_id: int,
    stat_row: dict[str, Any],
    club_map: dict[str, int],
    club_names: list[str],
    counters: dict[str, int],
) -> None:
    """Upsert a PlayerSeason from a Transfermarkt stats row."""
    season = _parse_season_raw(stat_row.get("season_raw"))
    if not season:
        return

    club_name = stat_row.get("club_name", "")
    club_id = _resolve_club(club_name, club_map, club_names) if club_name else None
    if not club_id:
        return

    values: dict[str, Any] = {
        "player_id": player_id,
        "club_id": club_id,
        "season": season,
        "data_source": DATA_SOURCE,
        "confidence_score": CONFIDENCE_SCORE,
    }

    if stat_row.get("appearances") is not None:
        values["appearances"] = stat_row["appearances"]
    if stat_row.get("goals") is not None:
        values["goals"] = stat_row["goals"]
    if stat_row.get("assists") is not None:
        values["assists"] = stat_row["assists"]
    if stat_row.get("yellow_cards") is not None:
        values["yellow_cards"] = stat_row["yellow_cards"]
    if stat_row.get("red_cards") is not None:
        values["red_cards"] = stat_row["red_cards"]
    if stat_row.get("minutes_played") is not None:
        values["minutes_played"] = stat_row["minutes_played"]

    stmt = pg_insert(PlayerSeason).values(**values)

    update_set: dict[str, Any] = {
        "confidence_score": stmt.excluded.confidence_score,
        "updated_at": func.now(),
    }
    for field in ("appearances", "goals", "assists", "yellow_cards", "red_cards", "minutes_played"):
        if field in values:
            update_set[field] = getattr(stmt.excluded, field)

    stmt = stmt.on_conflict_do_update(
        constraint="uq_player_club_season_source",
        set_=update_set,
    )
    session.execute(stmt)
    counters["stats"] += 1


def _upsert_season_basic(
    session: Session,
    player_id: int,
    club_id: int,
    counters: dict[str, int],
) -> None:
    """Upsert a minimal PlayerSeason (no stats yet)."""
    stmt = pg_insert(PlayerSeason).values(
        player_id=player_id,
        club_id=club_id,
        season=SEASON_LABEL,
        data_source=DATA_SOURCE,
        confidence_score=CONFIDENCE_SCORE,
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_player_club_season_source",
        set_={
            "confidence_score": stmt.excluded.confidence_score,
            "updated_at": func.now(),
        },
    )
    session.execute(stmt)
    counters["stats"] += 1


# ── transfer / career processing ──────────────────────────────────────────

def _process_transfers_record(
    session: Session,
    raw: dict[str, Any],
    club_map: dict[str, int],
    club_names: list[str],
    all_players: list[Player],
    counters: dict[str, int],
) -> None:
    """Process a standalone player_transfers staging record."""
    player_name = _clean_name(raw.get("player_name", ""))
    if not player_name:
        return
    dob = _parse_dob(raw.get("date_of_birth"))
    club_name = raw.get("club_name", "")
    club_id = _resolve_club(club_name, club_map, club_names) if club_name else None
    player = _match_player(player_name, club_id, dob, all_players)
    if not player:
        return

    transfers = raw.get("transfers", [])
    _create_careers_from_transfers(
        session, player.id, transfers, club_map, club_names, counters,
    )


def _create_careers_from_transfers(
    session: Session,
    player_id: int,
    transfers: list[dict[str, Any]],
    club_map: dict[str, int],
    club_names: list[str],
    counters: dict[str, int],
) -> None:
    """Convert a transfer history list into PlayerCareer entries.

    Each transfer defines when a player *arrived* at a club.  We
    infer ``season_end`` from the next transfer's date (or leave it
    ``None`` for the current stint).
    """
    parsed: list[dict[str, Any]] = []
    for t in transfers:
        transfer_date = _parse_dob(t.get("transfer_date"))
        to_club_name = t.get("to_club", "")
        to_club_id = _resolve_club(to_club_name, club_map, club_names) if to_club_name else None
        transfer_type = t.get("transfer_type", "transfer")

        if transfer_type == "end_of_loan":
            role = "player"
        elif transfer_type == "loan":
            role = "loan"
        else:
            role = "player"

        parsed.append({
            "date": transfer_date,
            "club_id": to_club_id,
            "club_name": to_club_name,
            "role": role,
            "season_start": _date_to_season(transfer_date),
        })

    for i, entry in enumerate(parsed):
        if not entry["club_id"]:
            continue

        season_end = None
        if i + 1 < len(parsed) and parsed[i + 1].get("date"):
            season_end = _date_to_season(parsed[i + 1]["date"])
            if season_end == entry["season_start"]:
                season_end = None

        exists = session.execute(
            select(PlayerCareer.id).where(
                PlayerCareer.player_id == player_id,
                PlayerCareer.club_id == entry["club_id"],
                PlayerCareer.season_start == entry["season_start"],
                PlayerCareer.source == DATA_SOURCE,
            )
        ).scalar_one_or_none()

        if exists is None:
            session.add(PlayerCareer(
                player_id=player_id,
                club_id=entry["club_id"],
                season_start=entry["season_start"],
                season_end=season_end,
                role=entry["role"],
                source=DATA_SOURCE,
            ))
            counters["careers"] += 1
        else:
            if season_end:
                session.execute(
                    select(PlayerCareer)
                    .where(PlayerCareer.id == exists)
                ).scalar_one().season_end = season_end


def _ensure_career(
    session: Session,
    player_id: int,
    club_id: int,
    season_start: str,
) -> None:
    exists = session.execute(
        select(PlayerCareer.id).where(
            PlayerCareer.player_id == player_id,
            PlayerCareer.club_id == club_id,
            PlayerCareer.season_start == season_start,
        )
    ).scalar_one_or_none()

    if exists is None:
        session.add(PlayerCareer(
            player_id=player_id,
            club_id=club_id,
            season_start=season_start,
            role="player",
            source=DATA_SOURCE,
        ))


# ══════════════════════════════════════════════════════════════════════════
# Public entry point
# ══════════════════════════════════════════════════════════════════════════

def transform_transfermarkt() -> dict[str, int]:
    """Process all unprocessed Transfermarkt records from staging_raw."""
    counters: dict[str, int] = {
        "processed": 0,
        "created": 0,
        "updated": 0,
        "stats": 0,
        "careers": 0,
        "errors": 0,
    }

    with get_session() as session:
        club_map, club_names = _build_club_lookup(session)
        if not club_map:
            logger.warning("No clubs in database — nothing to transform")
            return counters

        all_players = session.execute(
            select(Player).where(Player.merged_into_id.is_(None))
        ).scalars().all()
        logger.info(
            "Loaded %d clubs, %d players for matching",
            len(club_map), len(all_players),
        )

        records = (
            session.query(StagingRaw)
            .filter(
                StagingRaw.source == DATA_SOURCE,
                StagingRaw.processed.is_(False),
            )
            .all()
        )

        if not records:
            logger.info("No unprocessed Transfermarkt records")
            return counters

        logger.info("Processing %d staging records…", len(records))

        for i, record in enumerate(records, 1):
            try:
                _process_record(
                    session, record, club_map, club_names,
                    all_players, counters,
                )
            except Exception as exc:
                record.error_message = str(exc)[:500]
                counters["errors"] += 1
                logger.warning("Error on staging id=%d: %s", record.id, exc)

            record.processed = True
            record.processed_at = datetime.now(timezone.utc)

            if i % COMMIT_EVERY == 0:
                session.commit()
                logger.info("  Progress: %d / %d", i, len(records))

    logger.info(
        "Transfermarkt transform: %d processed, %d created, "
        "%d updated, %d stats, %d careers, %d errors",
        counters["processed"], counters["created"],
        counters["updated"], counters["stats"],
        counters["careers"], counters["errors"],
    )
    return counters


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    results = transform_transfermarkt()

    print()
    print("=" * 55)
    print("  Transfermarkt Transform Summary")
    print("=" * 55)
    print(f"  Records processed:    {results['processed']}")
    print(f"  New players created:  {results['created']}")
    print(f"  Players updated:      {results['updated']}")
    print(f"  Stats records:        {results['stats']}")
    print(f"  Career entries:       {results['careers']}")
    print(f"  Errors:               {results['errors']}")
    print("=" * 55)
