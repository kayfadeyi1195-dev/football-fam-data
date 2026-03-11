"""Orchestrate the full API-Football data pull for English non-league football.

Five phases run in sequence:

  1. **DISCOVER**  — identify non-league league IDs via the API
  2. **MATCH**     — fuzzy-match API teams to clubs in our database
  3. **SQUADS**    — load player rosters, upsert player records
  4. **STATS**     — pull per-player season statistics
  5. **LOG**       — record the pipeline execution in ``data_source_runs``

The script is **idempotent**: running it twice updates existing records
rather than creating duplicates.

Usage::

    python scripts/run_api_football.py                        # current season (2025-26)
    python scripts/run_api_football.py --no-stats             # skip per-player stats
    python scripts/run_api_football.py --season 2024          # 2024-25 season only
    python scripts/run_api_football.py --seasons 2023,2024    # multiple historical
"""

import argparse
import logging
import sys
import time
from datetime import date, datetime, timezone
from typing import Any

from rapidfuzz import fuzz, process
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.api_clients.api_football import ApiFootballClient
from src.db.models import (
    Club,
    DataSourceRun,
    League,
    Player,
    PlayerSeason,
    RunStatus,
    StagingRaw,
)
from src.db.session import get_session

logger = logging.getLogger(__name__)

# ── constants ────────────────────────────────────────────────────────────

DATA_SOURCE = "api_football"
DEFAULT_API_SEASON = 2025
FUZZY_THRESHOLD = 85


def _season_label(api_year: int) -> str:
    """Convert an API start-year to our ``'YYYY-YY'`` label."""
    return f"{api_year}-{str(api_year + 1)[-2:]}"

_POSITION_MAP: dict[str, str] = {
    "Goalkeeper": "GK",
    "Defender": "DEF",
    "Midfielder": "MID",
    "Attacker": "FWD",
}


# ── value-parsing helpers ────────────────────────────────────────────────

def _parse_cm(raw: str | None) -> int | None:
    if not raw:
        return None
    try:
        return int(raw.replace("cm", "").strip())
    except (ValueError, AttributeError):
        return None


def _parse_kg(raw: str | None) -> int | None:
    if not raw:
        return None
    try:
        return int(raw.replace("kg", "").strip())
    except (ValueError, AttributeError):
        return None


def _parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ── staging helper ───────────────────────────────────────────────────────

def _stage_raw(
    session: Session,
    entity_type: str,
    external_id: str,
    raw_data: dict,
    *,
    processed: bool = False,
) -> None:
    """Upsert a record into staging_raw for audit trail."""
    stmt = pg_insert(StagingRaw).values(
        source=DATA_SOURCE,
        source_entity_type=entity_type,
        external_id=external_id,
        raw_data=raw_data,
        processed=processed,
        processed_at=func.now() if processed else None,
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_staging_source_type_extid",
        set_={
            "raw_data": stmt.excluded.raw_data,
            "processed": stmt.excluded.processed,
            "processed_at": stmt.excluded.processed_at,
            "updated_at": func.now(),
        },
    )
    session.execute(stmt)


# ── player-ID lookup from previous runs ──────────────────────────────────

def _build_player_id_lookup(session: Session) -> dict[str, int]:
    """Load api_player_id → our_player_id from previously staged records.

    We embed ``_matched_player_id`` inside the raw_data JSONB when we
    first process a player, so subsequent runs can skip fuzzy matching.
    """
    rows = session.execute(
        select(StagingRaw.external_id, StagingRaw.raw_data)
        .where(StagingRaw.source == DATA_SOURCE)
        .where(StagingRaw.source_entity_type == "player")
        .where(StagingRaw.processed.is_(True))
    ).all()

    lookup: dict[str, int] = {}
    for ext_id, raw_data in rows:
        pid = raw_data.get("_matched_player_id") if isinstance(raw_data, dict) else None
        if ext_id and pid:
            lookup[ext_id] = pid
    return lookup


# ══════════════════════════════════════════════════════════════════════════
# Phase 1 — DISCOVER
# ══════════════════════════════════════════════════════════════════════════

def phase_discover(
    session: Session,
    client: ApiFootballClient,
    season_label: str,
) -> tuple[dict[str, int], dict[int, int]]:
    """Find non-league English competitions and map them to our leagues table.

    Returns:
        api_league_map:  ``{api_league_name: api_league_id}``
        api_to_our_league:  ``{api_league_id: our_league.id}``
    """
    logger.info("═══ Phase 1: DISCOVER leagues ═══")
    api_league_map = client.discover_english_nonleague()

    if not api_league_map:
        logger.error("No non-league competitions found — check your API key")
        return {}, {}

    # Stage raw league data
    all_leagues_raw = client.get_leagues(country="England")
    for entry in all_leagues_raw:
        lid = entry.get("league", {}).get("id")
        if lid is not None:
            _stage_raw(session, "league", str(lid), entry)
    session.flush()

    # Match API league names to our leagues table by fuzzy name.
    # Try the requested season first; fall back to any season if
    # the requested one doesn't exist yet (historical pulls).
    our_leagues = session.execute(
        select(League).where(League.season == season_label)
    ).scalars().all()
    if not our_leagues:
        our_leagues = session.execute(select(League)).scalars().all()

    our_league_names = [lg.name for lg in our_leagues]
    our_league_by_name = {lg.name: lg for lg in our_leagues}

    api_to_our_league: dict[int, int] = {}
    for api_name, api_id in api_league_map.items():
        result = process.extractOne(
            api_name, our_league_names,
            scorer=fuzz.WRatio,
            score_cutoff=75,
        )
        if result:
            match_name, score, _ = result
            api_to_our_league[api_id] = our_league_by_name[match_name].id
            logger.info(
                "  League: %s → %s (score=%.0f)",
                api_name, match_name, score,
            )
        else:
            logger.warning("  League: %s — no match in our DB", api_name)

    session.commit()
    logger.info(
        "Discovered %d API leagues, matched %d to our DB",
        len(api_league_map), len(api_to_our_league),
    )
    return api_league_map, api_to_our_league


# ══════════════════════════════════════════════════════════════════════════
# Phase 2 — MATCH CLUBS
# ══════════════════════════════════════════════════════════════════════════

def phase_match_clubs(
    session: Session,
    client: ApiFootballClient,
    api_league_map: dict[str, int],
    api_season: int,
) -> tuple[dict[int, Club], dict[int, int]]:
    """Fetch teams from API, fuzzy-match to our clubs table, update records.

    Returns:
        matched_clubs:  ``{api_team_id: Club}``
        team_to_api_league:  ``{api_team_id: api_league_id}``
    """
    logger.info("═══ Phase 2: MATCH CLUBS ═══")

    our_clubs = session.execute(select(Club)).scalars().all()
    if not our_clubs:
        logger.error("No clubs in database — run seed scripts first")
        return {}, {}

    club_name_list = [c.name for c in our_clubs]
    club_by_name: dict[str, Club] = {c.name: c for c in our_clubs}

    matched: dict[int, Club] = {}
    team_to_api_league: dict[int, int] = {}
    unmatched: list[str] = []

    for league_name, api_league_id in sorted(api_league_map.items()):
        logger.info("Fetching teams for %s (api_id=%d)…", league_name, api_league_id)
        teams = client.get_teams(api_league_id, api_season)

        for entry in teams:
            api_team = entry.get("team", {})
            venue = entry.get("venue", {})
            api_team_id = api_team.get("id")
            api_team_name = api_team.get("name", "")

            if not api_team_id or not api_team_name:
                continue

            _stage_raw(session, "team", str(api_team_id), entry)

            result = process.extractOne(
                api_team_name, club_name_list,
                scorer=fuzz.WRatio,
                score_cutoff=FUZZY_THRESHOLD,
            )

            if result is None:
                unmatched.append(api_team_name)
                logger.warning("  No match: %s", api_team_name)
                continue

            match_name, score, _ = result
            club = club_by_name[match_name]
            matched[api_team_id] = club
            team_to_api_league[api_team_id] = api_league_id

            # Store the API team ID on the club record
            club.api_football_id = api_team_id

            # Fill NULL fields with venue / logo info from the API
            if not club.ground_name and venue.get("name"):
                club.ground_name = venue["name"]
            if not club.logo_url and api_team.get("logo"):
                club.logo_url = api_team["logo"]

            logger.info(
                "  Matched: %s → %s (score=%.0f)", api_team_name, club.name, score,
            )

        session.flush()

    session.commit()
    logger.info(
        "Matched %d clubs, %d unmatched", len(matched), len(unmatched),
    )
    for name in unmatched:
        logger.info("  Unmatched API team: %s", name)

    return matched, team_to_api_league


# ══════════════════════════════════════════════════════════════════════════
# Phase 3 — FETCH SQUADS
# ══════════════════════════════════════════════════════════════════════════

PlayerContext = dict[str, Any]   # {"player_id", "club_id", "api_league_id"}


def phase_fetch_squads(
    session: Session,
    client: ApiFootballClient,
    matched_clubs: dict[int, Club],
    team_to_api_league: dict[int, int],
    *,
    create_new: bool = True,
) -> dict[int, PlayerContext]:
    """Fetch squad rosters and upsert player records.

    When *create_new* is ``False`` (historical season mode), unmatched
    players are skipped instead of being created.  This prevents
    polluting the database with players who may no longer be active.

    Returns:
        ``{api_player_id: {"player_id": …, "club_id": …, "api_league_id": …}}``
    """
    logger.info("═══ Phase 3: FETCH SQUADS ═══")
    if not create_new:
        logger.info("  (historical mode — will NOT create new players)")

    # Lookup from previous runs so we can skip re-matching
    prev_map = _build_player_id_lookup(session)
    logger.info("Loaded %d player mappings from previous runs", len(prev_map))

    # In-memory index of existing players for name-based matching
    all_players = session.execute(select(Player)).scalars().all()
    players_by_name_club: dict[tuple[str, int | None], Player] = {
        (p.full_name, p.current_club_id): p for p in all_players
    }

    api_players: dict[int, PlayerContext] = {}
    created = 0
    updated = 0
    skipped_loan = 0
    total_clubs = len(matched_clubs)

    for idx, (api_team_id, club) in enumerate(matched_clubs.items(), 1):
        api_league_id = team_to_api_league.get(api_team_id, 0)
        logger.info("Fetching squad for %s (%d/%d)…", club.name, idx, total_clubs)

        squad_entries = client.get_squad(api_team_id)
        if not squad_entries:
            logger.warning("  Empty squad for %s", club.name)
            continue

        for squad_block in squad_entries:
            for p in squad_block.get("players", []):
                api_pid = p.get("id")
                if not api_pid:
                    continue

                # Loan handling — skip if already processed from another squad
                if api_pid in api_players:
                    skipped_loan += 1
                    logger.debug(
                        "  Skipping %s (already mapped — possible loan)",
                        p.get("name"),
                    )
                    continue

                player_name = (p.get("name") or "").strip()
                if not player_name:
                    continue

                position_primary = _POSITION_MAP.get(p.get("position", ""))
                photo_url = p.get("photo")

                # ── try previous-run mapping first ───────────────────────
                prev_pid = prev_map.get(str(api_pid))
                if prev_pid:
                    player = session.get(Player, prev_pid)
                    if player:
                        if not player.position_primary and position_primary:
                            player.position_primary = position_primary
                        if not player.profile_photo_url and photo_url:
                            player.profile_photo_url = photo_url
                        player.current_club_id = club.id
                        api_players[api_pid] = {
                            "player_id": player.id,
                            "club_id": club.id,
                            "api_league_id": api_league_id,
                        }
                        updated += 1
                        _stage_raw_player(session, api_pid, p, player.id, club.id)
                        continue

                # ── try exact name + club match ──────────────────────────
                existing = players_by_name_club.get((player_name, club.id))

                # ── fall back to exact name at any club ──────────────────
                if not existing:
                    for (pname, _), pl in players_by_name_club.items():
                        if pname == player_name:
                            existing = pl
                            break

                if existing:
                    if not existing.position_primary and position_primary:
                        existing.position_primary = position_primary
                    if not existing.profile_photo_url and photo_url:
                        existing.profile_photo_url = photo_url
                    existing.current_club_id = club.id
                    api_players[api_pid] = {
                        "player_id": existing.id,
                        "club_id": club.id,
                        "api_league_id": api_league_id,
                    }
                    updated += 1
                    _stage_raw_player(session, api_pid, p, existing.id, club.id)
                    continue

                # ── create new player (only in current-season mode) ─────
                if not create_new:
                    logger.debug(
                        "  Skipping unmatched %s (historical — no creation)",
                        player_name,
                    )
                    continue

                new_player = Player(
                    full_name=player_name,
                    position_primary=position_primary,
                    current_club_id=club.id,
                    profile_photo_url=photo_url,
                    contract_status="unknown",
                    availability="unknown",
                    is_verified=False,
                )
                session.add(new_player)
                session.flush()

                players_by_name_club[(player_name, club.id)] = new_player
                api_players[api_pid] = {
                    "player_id": new_player.id,
                    "club_id": club.id,
                    "api_league_id": api_league_id,
                }
                created += 1
                _stage_raw_player(session, api_pid, p, new_player.id, club.id)

        session.commit()

    logger.info(
        "Squad phase complete: %d created, %d updated, %d loan-skips, %d total",
        created, updated, skipped_loan, len(api_players),
    )
    return api_players


def _stage_raw_player(
    session: Session,
    api_pid: int,
    raw: dict,
    our_player_id: int,
    our_club_id: int,
) -> None:
    """Stage a player record with our internal mapping embedded."""
    enriched = dict(raw)
    enriched["_matched_player_id"] = our_player_id
    enriched["_matched_club_id"] = our_club_id
    _stage_raw(session, "player", str(api_pid), enriched, processed=True)


# ══════════════════════════════════════════════════════════════════════════
# Phase 4 — FETCH STATS
# ══════════════════════════════════════════════════════════════════════════

STATS_BATCH_SIZE = 25


def phase_fetch_stats(
    client: ApiFootballClient,
    api_players: dict[int, PlayerContext],
    club_id_map: dict[int, int],
    api_to_our_league: dict[int, int],
    api_season: int,
    season_label: str,
) -> int:
    """Fetch per-player stats for a given season, upsert player_seasons.

    Also enriches player records with detailed biographical data
    (name parts, DOB, nationality, height, weight) from the ``/players``
    endpoint.

    To avoid Railway Postgres idle-connection timeouts, the function
    opens a **fresh DB session for every batch of 25 API calls** instead
    of holding a single session for the entire phase.  Each batch is
    committed and closed before the next one begins.
    """
    logger.info("═══ Phase 4: FETCH STATS (season %d / %s) ═══", api_season, season_label)

    # ── Build a deduplicated work list upfront (no DB needed) ─────────
    seen_our_ids: set[int] = set()
    work_items: list[tuple[int, int, int]] = []  # (api_pid, our_player_id, api_league_id)

    for api_pid, ctx in api_players.items():
        our_player_id = ctx["player_id"]
        if our_player_id in seen_our_ids:
            continue
        seen_our_ids.add(our_player_id)
        work_items.append((api_pid, our_player_id, ctx["api_league_id"]))

    total = len(work_items)
    est_minutes = total / 10 + 1
    logger.info(
        "Fetching stats for %d players in batches of %d (est. %.0f min at 10 req/min)",
        total, STATS_BATCH_SIZE, est_minutes,
    )

    stats_created = 0

    # ── Process in batches of STATS_BATCH_SIZE ────────────────────────
    for batch_start in range(0, total, STATS_BATCH_SIZE):
        batch = work_items[batch_start : batch_start + STATS_BATCH_SIZE]
        batch_end = batch_start + len(batch)

        # Fetch all API responses for this batch *before* opening a session.
        # This is where rate-limited waits happen — no DB connection held.
        api_responses: list[tuple[int, int, int, list[dict]]] = []
        for api_pid, our_player_id, api_league_id in batch:
            idx = batch_start + len(api_responses) + 1
            if idx == 1 or idx % 25 == 0 or idx == total:
                logger.info("Stats progress: %d / %d (%.0f%%)", idx, total, idx / total * 100)

            entries = client.get_player_stats(api_pid, api_season, api_league_id)
            if entries:
                api_responses.append((api_pid, our_player_id, api_league_id, entries))

        if not api_responses:
            continue

        # Now open a short-lived session to persist the batch.
        with get_session() as session:
            for api_pid, our_player_id, api_league_id, entries in api_responses:
                for entry in entries:
                    player_info = entry.get("player", {})
                    statistics = entry.get("statistics", [])

                    # ── enrich player record with detailed bio ───────
                    player = session.get(Player, our_player_id)
                    if player:
                        birth = player_info.get("birth", {})
                        if not player.first_name and player_info.get("firstname"):
                            player.first_name = player_info["firstname"]
                        if not player.last_name and player_info.get("lastname"):
                            player.last_name = player_info["lastname"]
                        if not player.date_of_birth and birth.get("date"):
                            player.date_of_birth = _parse_date(birth["date"])
                        if not player.nationality and player_info.get("nationality"):
                            player.nationality = player_info["nationality"]
                        if not player.height_cm:
                            player.height_cm = _parse_cm(player_info.get("height"))
                        if not player.weight_kg:
                            player.weight_kg = _parse_kg(player_info.get("weight"))
                        if not player.profile_photo_url and player_info.get("photo"):
                            player.profile_photo_url = player_info["photo"]

                    # ── create player_seasons for each stat block ────
                    for stat_block in statistics:
                        games = stat_block.get("games", {})
                        goals_data = stat_block.get("goals", {})
                        cards = stat_block.get("cards", {})
                        subs = stat_block.get("substitutes", {})
                        stat_team = stat_block.get("team", {})
                        stat_league = stat_block.get("league", {})

                        stat_team_id = stat_team.get("id")
                        club_id = club_id_map.get(stat_team_id)
                        if not club_id:
                            continue

                        our_league_id = api_to_our_league.get(
                            stat_league.get("id"), api_to_our_league.get(api_league_id),
                        )

                        values = dict(
                            player_id=our_player_id,
                            club_id=club_id,
                            league_id=our_league_id,
                            season=season_label,
                            appearances=_safe_int(games.get("appearences")),  # API typo
                            starts=_safe_int(games.get("lineups")),
                            sub_appearances=_safe_int(subs.get("in")),
                            goals=_safe_int(goals_data.get("total")),
                            assists=_safe_int(goals_data.get("assists")),
                            yellow_cards=_safe_int(cards.get("yellow")),
                            red_cards=_safe_int(cards.get("red")),
                            minutes_played=_safe_int(games.get("minutes")),
                            data_source=DATA_SOURCE,
                            confidence_score=5,
                        )

                        stmt = pg_insert(PlayerSeason).values(**values)
                        stmt = stmt.on_conflict_do_update(
                            constraint="uq_player_club_season_source",
                            set_={
                                "league_id": stmt.excluded.league_id,
                                "appearances": stmt.excluded.appearances,
                                "starts": stmt.excluded.starts,
                                "sub_appearances": stmt.excluded.sub_appearances,
                                "goals": stmt.excluded.goals,
                                "assists": stmt.excluded.assists,
                                "yellow_cards": stmt.excluded.yellow_cards,
                                "red_cards": stmt.excluded.red_cards,
                                "minutes_played": stmt.excluded.minutes_played,
                                "confidence_score": stmt.excluded.confidence_score,
                                "updated_at": func.now(),
                            },
                        )
                        session.execute(stmt)
                        stats_created += 1

                    _stage_raw(session, "player_stats", str(api_pid), entry)

        logger.info(
            "Batch %d–%d committed (%d stats so far)",
            batch_start + 1, batch_end, stats_created,
        )

    logger.info("Stats phase complete: %d player_seasons records upserted", stats_created)
    return stats_created


# ══════════════════════════════════════════════════════════════════════════
# Phase 5 — LOG RUN
# ══════════════════════════════════════════════════════════════════════════

def _start_run(session: Session) -> DataSourceRun:
    run = DataSourceRun(
        source=DATA_SOURCE,
        run_type="full_pull",
        started_at=datetime.now(timezone.utc),
        status=RunStatus.RUNNING,
    )
    session.add(run)
    session.flush()
    return run


def _complete_run(
    session: Session,
    run: DataSourceRun,
    *,
    records_fetched: int = 0,
    records_loaded: int = 0,
    records_errored: int = 0,
    status: RunStatus = RunStatus.COMPLETED,
    error_log: str | None = None,
) -> None:
    run.completed_at = datetime.now(timezone.utc)
    run.records_fetched = records_fetched
    run.records_loaded = records_loaded
    run.records_errored = records_errored
    run.status = status
    run.error_log = error_log
    session.commit()


# ══════════════════════════════════════════════════════════════════════════
# main
# ══════════════════════════════════════════════════════════════════════════

def run_season(
    client: ApiFootballClient,
    api_season: int,
    *,
    no_stats: bool = False,
    historical: bool = False,
) -> dict[str, Any]:
    """Execute the full pipeline for a single season.

    When *historical* is True, the squad phase will not create new
    player records — it only matches against existing players and
    adds ``player_seasons`` / ``player_career`` records for them.

    Returns a summary dict with counts and status.
    """
    season_label = _season_label(api_season)
    logger.info("=" * 60)
    logger.info(
        "API-Football pull — season %d (%s)%s",
        api_season, season_label,
        "  [historical]" if historical else "",
    )
    logger.info("=" * 60)

    league_count = 0
    club_count = 0
    player_count = 0
    stats_count = 0
    error_occurred = False

    with get_session() as session:
        run = _start_run(session)
        run.run_type = f"{'historical' if historical else 'full'}_pull_{season_label}"
        session.commit()

        try:
            api_league_map, api_to_our_league = phase_discover(
                session, client, season_label,
            )
            league_count = len(api_league_map)
            if not api_league_map:
                _complete_run(
                    session, run,
                    status=RunStatus.FAILED,
                    error_log="No non-league competitions discovered",
                )
                return {"season": season_label, "status": "FAILED", "error": "no leagues"}

            matched_clubs, team_to_api_league = phase_match_clubs(
                session, client, api_league_map, api_season,
            )
            club_count = len(matched_clubs)
            if not matched_clubs:
                _complete_run(
                    session, run,
                    status=RunStatus.FAILED,
                    error_log="No clubs matched between API and database",
                )
                return {"season": season_label, "status": "FAILED", "error": "no clubs"}

            api_players = phase_fetch_squads(
                session, client, matched_clubs, team_to_api_league,
                create_new=not historical,
            )
            player_count = len(api_players)

            if no_stats:
                logger.info("═══ Phase 4: SKIPPED (--no-stats) ═══")
            else:
                # Extract plain club IDs before entering the batched stats
                # phase, which manages its own short-lived sessions to avoid
                # Railway Postgres idle-connection timeouts.
                club_id_map = {api_tid: club.id for api_tid, club in matched_clubs.items()}
                session.commit()

                stats_count = phase_fetch_stats(
                    client, api_players,
                    club_id_map, api_to_our_league,
                    api_season, season_label,
                )

            _complete_run(
                session, run,
                records_fetched=player_count + club_count,
                records_loaded=player_count + stats_count,
            )

        except Exception as exc:
            logger.exception("Pipeline failed for season %s", season_label)
            try:
                _complete_run(
                    session, run,
                    status=RunStatus.FAILED,
                    error_log=str(exc)[:2000],
                )
            except Exception:
                logger.exception("Could not update run record after failure")
            error_occurred = True

    return {
        "season": season_label,
        "api_season": api_season,
        "leagues": league_count,
        "clubs": club_count,
        "players": player_count,
        "stats": stats_count,
        "status": "FAILED" if error_occurred else "OK",
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="API-Football data pull for English non-league football",
    )
    parser.add_argument(
        "--no-stats",
        action="store_true",
        help="Skip the (slow) per-player stats phase",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Fetch a single season by start year (e.g. 2023 for 2023-24)",
    )
    parser.add_argument(
        "--seasons",
        type=str,
        default=None,
        help="Comma-separated start years (e.g. 2021,2022,2023)",
    )
    args = parser.parse_args()

    # ── Determine which seasons to run ────────────────────────────
    if args.seasons:
        season_years = [int(y.strip()) for y in args.seasons.split(",")]
    elif args.season is not None:
        season_years = [args.season]
    else:
        season_years = [DEFAULT_API_SEASON]

    started = time.monotonic()
    client = ApiFootballClient()
    results: list[dict[str, Any]] = []

    for api_season in season_years:
        historical = api_season != DEFAULT_API_SEASON
        summary = run_season(
            client, api_season,
            no_stats=args.no_stats,
            historical=historical,
        )
        results.append(summary)

    elapsed = time.monotonic() - started

    # ── Print combined summary ────────────────────────────────────
    print()
    print("=" * 60)
    print("  API-Football Pipeline Summary")
    print("=" * 60)

    for r in results:
        tag = "  [historical]" if r["api_season"] != DEFAULT_API_SEASON else ""
        status = r["status"]
        print(f"  Season {r['season']}{tag}:")
        if status == "FAILED":
            print(f"    Status:  FAILED — {r.get('error', '?')}")
        else:
            print(f"    Leagues:  {r.get('leagues', 0)}")
            print(f"    Clubs:    {r.get('clubs', 0)}")
            print(f"    Players:  {r.get('players', 0)}")
            print(f"    Stats:    {r.get('stats', 0)}")
            print(f"    Status:   OK")

    print(f"\n  Total elapsed: {elapsed:.0f}s ({elapsed / 60:.1f} min)")
    any_failed = any(r["status"] == "FAILED" for r in results)
    print(f"  Overall:       {'SOME FAILED' if any_failed else 'ALL OK'}")
    print("=" * 60)

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
