#!/usr/bin/env python
"""Fetch match fixtures and lineup data from API-Football.

Covers Steps 1-2 of the English football pyramid (National League,
NL North, NL South) where API-Football has the richest per-game data.

The script is **resumable**: it checks which fixtures already exist
in the database and only fetches new or unprocessed ones.  A
``progress.json`` file is written to ``data/`` so the run can be
safely interrupted and resumed.

Pipeline phases:

  1. **DISCOVER** — identify non-league league IDs
  2. **FIXTURES** — fetch all completed fixtures for each league
  3. **LINEUPS**  — for each new fixture, fetch lineup data and
     create ``match`` + ``match_appearances`` records
  4. **LOG**      — record the run in ``data_source_runs``

Usage::

    python scripts/run_api_football_matches.py
    python scripts/run_api_football_matches.py --league "National League"
    python scripts/run_api_football_matches.py --force   # re-fetch all
"""

import argparse
import json
import logging
import sys
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
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
    Match,
    MatchAppearance,
    Player,
    RunStatus,
    StagingRaw,
)
from src.db.session import get_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

DATA_SOURCE = "api_football"
API_SEASON = 2025
SEASON_LABEL = "2025-26"
FUZZY_THRESHOLD = 85

PROGRESS_FILE = Path("data/match_fetch_progress.json")


# ══════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════

def _safe_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except (ValueError, TypeError):
        return None


def _stage_raw(
    session: Session,
    entity_type: str,
    external_id: str,
    raw_data: dict,
) -> None:
    stmt = pg_insert(StagingRaw).values(
        source=DATA_SOURCE,
        source_entity_type=entity_type,
        external_id=external_id,
        raw_data=raw_data,
        processed=True,
        processed_at=func.now(),
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


# ── progress tracking ────────────────────────────────────────────────────

def _load_progress() -> set[int]:
    """Return set of API fixture IDs already fully processed."""
    if PROGRESS_FILE.exists():
        data = json.loads(PROGRESS_FILE.read_text())
        return set(data.get("processed_fixture_ids", []))
    return set()


def _save_progress(processed: set[int]) -> None:
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(json.dumps({
        "processed_fixture_ids": sorted(processed),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }))


# ── club lookup ──────────────────────────────────────────────────────────

def _build_club_lookups(session: Session) -> tuple[
    dict[int, Club],       # api_football_id -> Club
    dict[str, Club],       # name -> Club
    list[str],             # list of names for fuzzy matching
]:
    clubs = session.execute(select(Club)).scalars().all()
    by_api_id: dict[int, Club] = {}
    by_name: dict[str, Club] = {}
    names: list[str] = []
    for c in clubs:
        if c.api_football_id:
            by_api_id[c.api_football_id] = c
        by_name[c.name] = c
        names.append(c.name)
    return by_api_id, by_name, names


def _resolve_club(
    api_team_id: int | None,
    api_team_name: str,
    by_api_id: dict[int, Club],
    by_name: dict[str, Club],
    name_list: list[str],
) -> Club | None:
    """Match an API team to our clubs table."""
    if api_team_id and api_team_id in by_api_id:
        return by_api_id[api_team_id]

    if api_team_name in by_name:
        return by_name[api_team_name]

    result = process.extractOne(
        api_team_name, name_list,
        scorer=fuzz.WRatio,
        score_cutoff=FUZZY_THRESHOLD,
    )
    if result:
        return by_name[result[0]]

    return None


# ── player lookup ────────────────────────────────────────────────────────

def _build_player_lookup(session: Session) -> dict[str, Player]:
    """Build a name→Player lookup for fuzzy matching lineup players."""
    players = session.execute(
        select(Player).where(Player.merged_into_id.is_(None))
    ).scalars().all()
    return {p.full_name: p for p in players}


def _resolve_player(
    api_player_name: str,
    club_id: int | None,
    player_by_name: dict[str, Player],
    session: Session,
) -> Player | None:
    """Match an API lineup player to our players table.

    First tries exact name match, then fuzzy match.  If no match
    exists and we have a club, creates a minimal player record.
    """
    if api_player_name in player_by_name:
        return player_by_name[api_player_name]

    names = list(player_by_name.keys())
    if names:
        result = process.extractOne(
            api_player_name, names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=85,
        )
        if result:
            return player_by_name[result[0]]

    if not club_id:
        return None

    parts = api_player_name.strip().split()
    first = parts[0] if parts else api_player_name
    last = " ".join(parts[1:]) if len(parts) > 1 else ""

    player = Player(
        full_name=api_player_name,
        first_name=first,
        last_name=last,
        current_club_id=club_id,
        is_verified=False,
    )
    session.add(player)
    session.flush()
    player_by_name[api_player_name] = player
    return player


# ══════════════════════════════════════════════════════════════════════════
# Phase 1 — DISCOVER
# ══════════════════════════════════════════════════════════════════════════

def phase_discover(
    client: ApiFootballClient,
    league_filter: str | None,
) -> dict[str, int]:
    """Find non-league competitions. Returns {name: api_league_id}."""
    logger.info("═══ Phase 1: DISCOVER ═══")
    mapping = client.discover_english_nonleague()

    if league_filter:
        key_lower = league_filter.lower()
        mapping = {
            k: v for k, v in mapping.items()
            if key_lower in k.lower()
        }
        logger.info("Filtered to %d league(s) matching %r", len(mapping), league_filter)

    for name, lid in sorted(mapping.items()):
        logger.info("  %s  (api_id=%d)", name, lid)

    return mapping


# ══════════════════════════════════════════════════════════════════════════
# Phase 2 — FIXTURES
# ══════════════════════════════════════════════════════════════════════════

def phase_fixtures(
    client: ApiFootballClient,
    api_leagues: dict[str, int],
) -> list[dict]:
    """Fetch all completed fixtures for the discovered leagues."""
    logger.info("═══ Phase 2: FIXTURES ═══")

    all_fixtures: list[dict] = []
    for league_name, api_league_id in sorted(api_leagues.items()):
        logger.info("Fetching fixtures for %s…", league_name)
        fixtures = client.get_fixtures(api_league_id, API_SEASON)

        completed = [
            f for f in fixtures
            if f.get("fixture", {}).get("status", {}).get("short") in (
                "FT", "AET", "PEN",
            )
        ]

        logger.info(
            "  %s: %d total fixtures, %d completed",
            league_name, len(fixtures), len(completed),
        )
        for f in completed:
            f["_league_name"] = league_name
            f["_api_league_id"] = api_league_id
        all_fixtures.extend(completed)

    logger.info("Total completed fixtures: %d", len(all_fixtures))
    return all_fixtures


# ══════════════════════════════════════════════════════════════════════════
# Phase 3 — LINEUPS + DB records
# ══════════════════════════════════════════════════════════════════════════

def phase_lineups(
    session: Session,
    client: ApiFootballClient,
    fixtures: list[dict],
    processed_ids: set[int],
    force: bool,
) -> dict[str, int]:
    """For each unprocessed fixture, create match + appearance records.

    Returns counters for the summary.
    """
    logger.info("═══ Phase 3: LINEUPS ═══")

    by_api_id, by_name, name_list = _build_club_lookups(session)
    player_by_name = _build_player_lookup(session)

    our_leagues = session.execute(
        select(League).where(League.season == SEASON_LABEL)
    ).scalars().all()
    league_name_list = [lg.name for lg in our_leagues]
    league_by_name = {lg.name: lg for lg in our_leagues}

    counters = {
        "fixtures_total": len(fixtures),
        "fixtures_skipped": 0,
        "fixtures_processed": 0,
        "fixtures_no_club_match": 0,
        "matches_created": 0,
        "appearances_created": 0,
        "players_created": 0,
        "lineup_unavailable": 0,
    }

    batch_count = 0

    for i, fix_data in enumerate(fixtures, 1):
        fix = fix_data.get("fixture", {})
        api_fix_id = fix.get("id")

        if not api_fix_id:
            continue

        if not force and api_fix_id in processed_ids:
            counters["fixtures_skipped"] += 1
            continue

        existing_match = session.execute(
            select(Match.id).where(
                Match.external_id == str(api_fix_id),
                Match.data_source == DATA_SOURCE,
            )
        ).scalar_one_or_none()

        if existing_match and not force:
            processed_ids.add(api_fix_id)
            counters["fixtures_skipped"] += 1
            continue

        # ── resolve clubs ────────────────────────────────────────────
        teams = fix_data.get("teams", {})
        home_api = teams.get("home", {})
        away_api = teams.get("away", {})

        home_club = _resolve_club(
            home_api.get("id"), home_api.get("name", ""),
            by_api_id, by_name, name_list,
        )
        away_club = _resolve_club(
            away_api.get("id"), away_api.get("name", ""),
            by_api_id, by_name, name_list,
        )

        if not home_club or not away_club:
            logger.debug(
                "  Fixture %d: club match failed (home=%s, away=%s)",
                api_fix_id, home_api.get("name"), away_api.get("name"),
            )
            counters["fixtures_no_club_match"] += 1
            processed_ids.add(api_fix_id)
            continue

        # ── resolve our league ───────────────────────────────────────
        api_league_name = fix_data.get("_league_name", "")
        our_league_id = None
        if api_league_name:
            lr = process.extractOne(
                api_league_name, league_name_list,
                scorer=fuzz.WRatio, score_cutoff=75,
            )
            if lr:
                our_league_id = league_by_name[lr[0]].id

        # ── create / update match ────────────────────────────────────
        goals = fix_data.get("goals", {})
        score = fix_data.get("score", {})
        match_date = _parse_date(fix.get("date"))

        venue_name = fix.get("venue", {}).get("name") if isinstance(fix.get("venue"), dict) else None

        if existing_match:
            match_id = existing_match
            session.execute(
                Match.__table__.update()
                .where(Match.id == match_id)
                .values(
                    home_score=_safe_int(goals.get("home")),
                    away_score=_safe_int(goals.get("away")),
                    attendance=_safe_int(fix.get("attendance")),
                )
            )
        else:
            match = Match(
                league_id=our_league_id,
                home_club_id=home_club.id,
                away_club_id=away_club.id,
                match_date=match_date,
                home_score=_safe_int(goals.get("home")),
                away_score=_safe_int(goals.get("away")),
                attendance=_safe_int(fix.get("attendance")),
                venue=venue_name,
                external_id=str(api_fix_id),
                data_source=DATA_SOURCE,
            )
            session.add(match)
            session.flush()
            match_id = match.id
            counters["matches_created"] += 1

        _stage_raw(session, "fixture", str(api_fix_id), fix_data)

        # ── fetch lineups ────────────────────────────────────────────
        lineups = client.get_fixture_lineups(api_fix_id)

        if not lineups:
            counters["lineup_unavailable"] += 1
            processed_ids.add(api_fix_id)
            counters["fixtures_processed"] += 1

            if i % 25 == 0:
                session.flush()
                _save_progress(processed_ids)
                logger.info("  Progress: %d / %d fixtures", i, len(fixtures))
            continue

        _stage_raw(session, "lineup", str(api_fix_id), {"lineups": lineups})

        for team_lineup in lineups:
            api_team = team_lineup.get("team", {})
            api_team_id = api_team.get("id")
            lineup_club = _resolve_club(
                api_team_id, api_team.get("name", ""),
                by_api_id, by_name, name_list,
            )
            if not lineup_club:
                continue

            starters = team_lineup.get("startXI", [])
            subs = team_lineup.get("substitutes", [])

            for player_entry in starters:
                p = player_entry.get("player", {})
                _create_appearance(
                    session, p, match_id, lineup_club.id,
                    started=True, player_by_name=player_by_name,
                    counters=counters,
                )

            for player_entry in subs:
                p = player_entry.get("player", {})
                _create_appearance(
                    session, p, match_id, lineup_club.id,
                    started=False, player_by_name=player_by_name,
                    counters=counters,
                )

        processed_ids.add(api_fix_id)
        counters["fixtures_processed"] += 1

        batch_count += 1
        if batch_count % 25 == 0:
            session.flush()
            _save_progress(processed_ids)
            logger.info("  Progress: %d / %d fixtures", i, len(fixtures))

    session.flush()
    _save_progress(processed_ids)
    return counters


def _create_appearance(
    session: Session,
    player_data: dict,
    match_id: int,
    club_id: int,
    *,
    started: bool,
    player_by_name: dict[str, Player],
    counters: dict[str, int],
) -> None:
    """Create a single match_appearances record."""
    name = player_data.get("name")
    if not name:
        return

    player = _resolve_player(name, club_id, player_by_name, session)
    if not player:
        return

    if player.id and not hasattr(player, "_was_existing"):
        pass

    existing = session.execute(
        select(MatchAppearance.id).where(
            MatchAppearance.match_id == match_id,
            MatchAppearance.player_id == player.id,
        )
    ).scalar_one_or_none()

    if existing:
        return

    stats = player_data.get("statistics")
    goals = None
    assists = None
    minutes = None
    yellow = None
    red = None
    rating_val = None

    if isinstance(stats, list) and stats:
        s = stats[0]
        games = s.get("games", {}) or {}
        goals_d = s.get("goals", {}) or {}
        cards = s.get("cards", {}) or {}

        minutes = _safe_int(games.get("minutes"))
        goals = _safe_int(goals_d.get("total"))
        assists = _safe_int(goals_d.get("assists"))
        yellow = bool(cards.get("yellow"))
        red = bool(cards.get("red"))

        raw_rating = games.get("rating")
        if raw_rating:
            try:
                rating_val = Decimal(str(raw_rating))
            except Exception:
                pass

    appearance = MatchAppearance(
        player_id=player.id,
        match_id=match_id,
        club_id=club_id,
        started=started,
        minutes_played=minutes,
        goals=goals,
        assists=assists,
        yellow_card=yellow,
        red_card=red,
        rating=rating_val,
        data_source=DATA_SOURCE,
    )
    session.add(appearance)
    counters["appearances_created"] += 1


# ══════════════════════════════════════════════════════════════════════════
# Phase 4 — LOG RUN
# ══════════════════════════════════════════════════════════════════════════

def phase_log_run(
    session: Session,
    started_at: datetime,
    counters: dict[str, int],
    error: str | None = None,
) -> None:
    logger.info("═══ Phase 4: LOG RUN ═══")
    status = RunStatus.FAILED if error else RunStatus.COMPLETED

    run = DataSourceRun(
        source=DATA_SOURCE,
        run_type="matches",
        started_at=started_at,
        completed_at=datetime.now(timezone.utc),
        records_fetched=counters.get("fixtures_total", 0),
        records_loaded=counters.get("matches_created", 0),
        records_errored=counters.get("fixtures_no_club_match", 0),
        status=status,
        error_log=error,
    )
    session.add(run)


# ══════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch match fixtures and lineups from API-Football",
    )
    parser.add_argument(
        "--league", type=str, default=None,
        help="Only process leagues matching this name (substring match)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-fetch all fixtures, even already processed ones",
    )
    args = parser.parse_args()

    started_at = datetime.now(timezone.utc)
    client = ApiFootballClient()

    logger.info("=" * 60)
    logger.info("API-Football Match Fetcher — started")
    logger.info("=" * 60)

    # Phase 1
    api_leagues = phase_discover(client, args.league)
    if not api_leagues:
        logger.error("No leagues discovered — exiting")
        sys.exit(1)

    # Phase 2
    fixtures = phase_fixtures(client, api_leagues)
    if not fixtures:
        logger.warning("No completed fixtures found — nothing to do")

    # Phase 3
    processed_ids = set() if args.force else _load_progress()
    new_count = sum(
        1 for f in fixtures
        if f.get("fixture", {}).get("id") not in processed_ids
    )
    logger.info(
        "Fixtures to process: %d new of %d total (%d already done)",
        new_count, len(fixtures), len(processed_ids),
    )

    counters: dict[str, int] = {}
    error_msg: str | None = None

    try:
        with get_session() as session:
            counters = phase_lineups(
                session, client, fixtures, processed_ids, args.force,
            )
            phase_log_run(session, started_at, counters)
    except Exception as exc:
        error_msg = str(exc)
        logger.exception("Pipeline failed")
        try:
            with get_session() as session:
                phase_log_run(session, started_at, counters, error=error_msg)
        except Exception:
            logger.exception("Failed to log the failed run")

    # ── Summary ──────────────────────────────────────────────────────

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()

    print()
    print("=" * 60)
    print("  API-FOOTBALL MATCH FETCHER — SUMMARY")
    print("=" * 60)
    print(f"  Leagues discovered:        {len(api_leagues)}")
    print(f"  Total fixtures found:      {counters.get('fixtures_total', 0):,}")
    print(f"  Already processed:         {counters.get('fixtures_skipped', 0):,}")
    print(f"  Processed this run:        {counters.get('fixtures_processed', 0):,}")
    print(f"  Club match failures:       {counters.get('fixtures_no_club_match', 0):,}")
    print(f"  Lineup unavailable:        {counters.get('lineup_unavailable', 0):,}")
    print(f"  Matches created:           {counters.get('matches_created', 0):,}")
    print(f"  Appearances created:       {counters.get('appearances_created', 0):,}")
    print(f"  Runtime:                   {elapsed:.0f}s ({elapsed/60:.1f} min)")
    if error_msg:
        print(f"  ERROR: {error_msg[:100]}")
    print("=" * 60)
    print()

    sys.exit(1 if error_msg else 0)


if __name__ == "__main__":
    main()
