#!/usr/bin/env python
"""Enrich player data from FBref via the soccerdata package.

FBref provides advanced statistics (xG, shots, key passes, progressive
carries) that no other source in our pipeline offers.  Coverage is
limited to Step 1 (National League) — and *possibly* NL North / South
— but the data quality is high (confidence_score=4).

soccerdata does not ship with English non-league competitions in its
default league list.  This script writes a ``league_dict.json`` config
file into ``~/soccerdata/config/`` so soccerdata picks them up.

Usage::

    python scripts/run_fbref.py
    python scripts/run_fbref.py --season 2024
    python scripts/run_fbref.py --no-shooting   # skip shooting stats

Phases:

  1. Ensure soccerdata config includes our custom leagues
  2. For each supported competition, pull ``standard`` and
     ``shooting`` player-season stats
  3. Fuzzy-match each player+club to our database
  4. Upsert ``player_seasons`` records (data_source='fbref')
  5. Store raw data in ``staging_raw``
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from rapidfuzz import fuzz, process
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

DATA_SOURCE = "fbref"
SEASON_LABEL = "2025-26"
FUZZY_THRESHOLD = 82

# Entries we need in soccerdata's league_dict.json.
# The "FBref" value is the *Competition Name* from fbref.com/en/comps/.
CUSTOM_LEAGUE_ENTRIES: dict[str, dict[str, str]] = {
    "ENG-National League": {
        "FBref": "National League",
        "season_start": "Aug",
        "season_end": "May",
    },
    "ENG-National League North": {
        "FBref": "National League North",
        "season_start": "Aug",
        "season_end": "Apr",
    },
    "ENG-National League South": {
        "FBref": "National League South",
        "season_start": "Aug",
        "season_end": "Apr",
    },
}

LEAGUES_TO_TRY: list[str] = list(CUSTOM_LEAGUE_ENTRIES.keys())

OUR_LEAGUE_NAMES: dict[str, str] = {
    "ENG-National League": "National League",
    "ENG-National League North": "National League North",
    "ENG-National League South": "National League South",
}


# ══════════════════════════════════════════════════════════════════════════
# soccerdata config-file setup
# ══════════════════════════════════════════════════════════════════════════

def _ensure_league_config() -> None:
    """Write our custom leagues into soccerdata's league_dict.json.

    soccerdata reads ``~/soccerdata/config/league_dict.json`` on import
    and merges its contents with the built-in league list.  We create or
    update that file with our non-league entries.
    """
    config_dir = Path.home() / "soccerdata" / "config"
    config_file = config_dir / "league_dict.json"

    existing: dict[str, Any] = {}
    if config_file.exists():
        try:
            existing = json.loads(config_file.read_text())
        except (json.JSONDecodeError, OSError):
            logger.warning("Corrupt league_dict.json — will overwrite")

    changed = False
    for league_id, entry in CUSTOM_LEAGUE_ENTRIES.items():
        if league_id not in existing:
            existing[league_id] = entry
            changed = True
            logger.info("Adding %s to league_dict.json", league_id)
        else:
            logger.debug("Already in league_dict.json: %s", league_id)

    if changed:
        config_dir.mkdir(parents=True, exist_ok=True)
        config_file.write_text(json.dumps(existing, indent=2) + "\n")
        logger.info("Wrote %s", config_file)

    # After writing the config, reload soccerdata so it picks up changes.
    # soccerdata reads league_dict.json once at import time, so we
    # re-import if possible.
    import importlib
    try:
        import soccerdata
        importlib.reload(soccerdata)
    except Exception:
        pass


def _get_available_leagues() -> list[str]:
    """Return soccerdata leagues, log them, and find our targets."""
    import soccerdata as sd

    available = list(sd.FBref.available_leagues())
    logger.info("soccerdata available leagues (%d):", len(available))
    for lg in sorted(available):
        marker = " ← target" if lg in LEAGUES_TO_TRY else ""
        logger.info("  %s%s", lg, marker)

    return available


def _try_read_stats(
    league_id: str,
    season: int,
    stat_type: str,
) -> pd.DataFrame | None:
    """Attempt to read player-season stats; return None on failure."""
    import soccerdata as sd

    try:
        fbref = sd.FBref(leagues=league_id, seasons=season)
        df = fbref.read_player_season_stats(stat_type=stat_type)
        if df is not None and not df.empty:
            logger.info(
                "  %s / %s: %d rows fetched",
                league_id, stat_type, len(df),
            )
            return df
        logger.info("  %s / %s: no data returned", league_id, stat_type)
        return None
    except Exception as exc:
        logger.warning(
            "  %s / %s: failed — %s", league_id, stat_type, exc,
        )
        return None


# ══════════════════════════════════════════════════════════════════════════
# Club + player matching
# ══════════════════════════════════════════════════════════════════════════

def _build_club_lookup(
    session: Any,
) -> tuple[dict[str, int], list[str]]:
    rows = session.execute(select(Club.id, Club.name)).all()
    club_map = {r.name: r.id for r in rows}
    return club_map, list(club_map.keys())


def _resolve_club(
    fbref_team_name: str,
    club_map: dict[str, int],
    club_names: list[str],
) -> int | None:
    if fbref_team_name in club_map:
        return club_map[fbref_team_name]

    result = process.extractOne(
        fbref_team_name, club_names,
        scorer=fuzz.WRatio,
        score_cutoff=FUZZY_THRESHOLD,
    )
    if result:
        return club_map[result[0]]
    return None


def _build_player_lookup(
    session: Any,
) -> tuple[dict[str, int], list[str]]:
    rows = session.execute(
        select(Player.id, Player.full_name)
        .where(Player.merged_into_id.is_(None))
    ).all()
    player_map = {r.full_name: r.id for r in rows}
    return player_map, list(player_map.keys())


def _resolve_player(
    fbref_player_name: str,
    club_id: int | None,
    player_map: dict[str, int],
    player_names: list[str],
    session: Any,
) -> int | None:
    """Match an FBref player to our DB. Creates a minimal record if unmatched."""
    if fbref_player_name in player_map:
        return player_map[fbref_player_name]

    result = process.extractOne(
        fbref_player_name, player_names,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=FUZZY_THRESHOLD,
    )
    if result:
        return player_map[result[0]]

    if not club_id:
        return None

    parts = fbref_player_name.strip().split()
    first = parts[0] if parts else fbref_player_name
    last = " ".join(parts[1:]) if len(parts) > 1 else ""

    player = Player(
        full_name=fbref_player_name,
        first_name=first,
        last_name=last,
        current_club_id=club_id,
        is_verified=False,
    )
    session.add(player)
    session.flush()
    player_map[fbref_player_name] = player.id
    player_names.append(fbref_player_name)
    return player.id


# ══════════════════════════════════════════════════════════════════════════
# DataFrame → DB records
# ══════════════════════════════════════════════════════════════════════════

def _safe_int(val: Any) -> int | None:
    if pd.isna(val):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> float | None:
    if pd.isna(val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _process_standard(
    df: pd.DataFrame,
    sd_league: str,
    our_league_id: int | None,
    session: Any,
    club_map: dict[str, int],
    club_names: list[str],
    player_map: dict[str, int],
    player_names: list[str],
    counters: dict[str, int],
) -> None:
    """Process a 'standard' stat DataFrame into player_seasons records."""

    df = df.reset_index()

    for _, row in df.iterrows():
        fbref_player = str(row.get("player", ""))
        fbref_team = str(row.get("team", ""))

        if not fbref_player or not fbref_team:
            continue

        club_id = _resolve_club(fbref_team, club_map, club_names)
        if not club_id:
            counters["club_miss"] += 1
            continue

        player_id = _resolve_player(
            fbref_player, club_id, player_map, player_names, session,
        )
        if not player_id:
            counters["player_miss"] += 1
            continue

        cols = row.to_dict()

        appearances = _safe_int(cols.get(("Playing Time", "MP"), cols.get("MP")))
        starts = _safe_int(cols.get(("Playing Time", "Starts"), cols.get("Starts")))
        minutes = _safe_int(cols.get(("Playing Time", "Min"), cols.get("Min")))
        goals = _safe_int(cols.get(("Performance", "Gls"), cols.get("Gls")))
        assists = _safe_int(cols.get(("Performance", "Ast"), cols.get("Ast")))
        yellows = _safe_int(cols.get(("Performance", "CrdY"), cols.get("CrdY")))
        reds = _safe_int(cols.get(("Performance", "CrdR"), cols.get("CrdR")))

        sub_apps = None
        if appearances is not None and starts is not None:
            sub_apps = max(0, appearances - starts)

        stmt = pg_insert(PlayerSeason).values(
            player_id=player_id,
            club_id=club_id,
            league_id=our_league_id,
            season=SEASON_LABEL,
            appearances=appearances,
            starts=starts,
            sub_appearances=sub_apps,
            goals=goals,
            assists=assists,
            yellow_cards=yellows,
            red_cards=reds,
            minutes_played=minutes,
            data_source=DATA_SOURCE,
            confidence_score=4,
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_player_club_season_source",
            set_={
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
        counters["seasons_upserted"] += 1

        # Stage raw record
        raw_data = {
            k: (None if pd.isna(v) else v)
            for k, v in cols.items()
            if isinstance(k, str)
        }
        raw_data["_stat_type"] = "standard"
        raw_data["_sd_league"] = sd_league
        _stage_raw(
            session, "player",
            f"fbref_std_{sd_league}_{fbref_team}_{fbref_player}",
            raw_data,
        )

    session.flush()


def _process_shooting(
    df: pd.DataFrame,
    sd_league: str,
    our_league_id: int | None,
    session: Any,
    club_map: dict[str, int],
    club_names: list[str],
    player_map: dict[str, int],
    player_names: list[str],
    counters: dict[str, int],
) -> None:
    """Process a 'shooting' stat DataFrame.

    Shooting data enriches existing player_seasons records with
    additional detail stored in staging_raw (we don't have xG /
    shots columns in player_seasons, but we stage the raw data so
    it's available for future use).
    """
    df = df.reset_index()

    for _, row in df.iterrows():
        fbref_player = str(row.get("player", ""))
        fbref_team = str(row.get("team", ""))

        if not fbref_player or not fbref_team:
            continue

        club_id = _resolve_club(fbref_team, club_map, club_names)
        player_id = None
        if club_id:
            player_id = _resolve_player(
                fbref_player, club_id, player_map, player_names, session,
            )

        cols = row.to_dict()
        raw_data = {
            k: (None if pd.isna(v) else v)
            for k, v in cols.items()
            if isinstance(k, str)
        }
        raw_data["_stat_type"] = "shooting"
        raw_data["_sd_league"] = sd_league
        raw_data["_matched_player_id"] = player_id
        raw_data["_matched_club_id"] = club_id

        _stage_raw(
            session, "player",
            f"fbref_shoot_{sd_league}_{fbref_team}_{fbref_player}",
            raw_data,
        )
        counters["shooting_staged"] += 1

    session.flush()


def _stage_raw(
    session: Any,
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


# ══════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich player data from FBref via soccerdata",
    )
    parser.add_argument(
        "--season", type=int, default=2025,
        help="Season start year (e.g. 2025 for 2025-26). Default: 2025",
    )
    parser.add_argument(
        "--no-shooting", action="store_true",
        help="Skip shooting stats (faster)",
    )
    args = parser.parse_args()

    started_at = datetime.now(timezone.utc)
    season_year = args.season

    logger.info("=" * 60)
    logger.info("FBref Enrichment — season %d (%s)", season_year, SEASON_LABEL)
    logger.info("=" * 60)

    # ── 1. Ensure config & discover available leagues ────────────────
    _ensure_league_config()

    available = _get_available_leagues()
    target_leagues = [lg for lg in LEAGUES_TO_TRY if lg in available]

    if not target_leagues:
        logger.warning(
            "None of our target leagues are in soccerdata's registry. "
            "Available: %s",
            available,
        )
        print(
            "\n  No target leagues found in soccerdata.  Check that\n"
            "  ~/soccerdata/config/league_dict.json was written and\n"
            "  restart the script (soccerdata reads config at import).\n"
        )
        sys.exit(1)

    logger.info("Target leagues: %s", target_leagues)

    # ── 2. Discover which leagues have data ──────────────────────────

    leagues_with_data: dict[str, dict[str, pd.DataFrame]] = {}

    for sd_league in target_leagues:
        logger.info("Probing %s…", sd_league)
        standard = _try_read_stats(sd_league, season_year, "standard")
        if standard is None:
            logger.info("  %s: no standard data — skipping", sd_league)
            continue

        data: dict[str, pd.DataFrame] = {"standard": standard}

        if not args.no_shooting:
            time.sleep(4)
            shooting = _try_read_stats(sd_league, season_year, "shooting")
            if shooting is not None:
                data["shooting"] = shooting

        leagues_with_data[sd_league] = data
        time.sleep(4)

    if not leagues_with_data:
        logger.warning("No FBref data found for any non-league competition")
        print("\n  No data available. FBref may not cover these leagues yet.\n")
        sys.exit(0)

    logger.info(
        "Data available for %d league(s): %s",
        len(leagues_with_data), list(leagues_with_data.keys()),
    )

    # ── 3. Process into database ─────────────────────────────────────

    counters: dict[str, int] = {
        "seasons_upserted": 0,
        "shooting_staged": 0,
        "club_miss": 0,
        "player_miss": 0,
        "players_created": 0,
    }

    error_msg: str | None = None

    try:
        with get_session() as session:
            club_map, club_names = _build_club_lookup(session)
            player_map, player_names = _build_player_lookup(session)

            initial_player_count = len(player_map)

            our_leagues = session.execute(
                select(League.id, League.name)
                .where(League.season == SEASON_LABEL)
            ).all()
            our_league_map = {lg.name: lg.id for lg in our_leagues}

            for sd_league, data in leagues_with_data.items():
                our_name = OUR_LEAGUE_NAMES.get(sd_league, "")
                our_league_id = our_league_map.get(our_name)

                logger.info("Processing %s → %s (id=%s)", sd_league, our_name, our_league_id)

                if "standard" in data:
                    _process_standard(
                        data["standard"], sd_league, our_league_id,
                        session, club_map, club_names,
                        player_map, player_names, counters,
                    )

                if "shooting" in data:
                    _process_shooting(
                        data["shooting"], sd_league, our_league_id,
                        session, club_map, club_names,
                        player_map, player_names, counters,
                    )

            counters["players_created"] = len(player_map) - initial_player_count

            run = DataSourceRun(
                source=DATA_SOURCE,
                run_type="enrichment",
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                records_fetched=sum(
                    len(df) for data in leagues_with_data.values()
                    for df in data.values()
                ),
                records_loaded=counters["seasons_upserted"],
                records_errored=counters["club_miss"] + counters["player_miss"],
                status=RunStatus.COMPLETED,
            )
            session.add(run)

    except Exception as exc:
        error_msg = str(exc)
        logger.exception("FBref enrichment failed")
        try:
            with get_session() as session:
                run = DataSourceRun(
                    source=DATA_SOURCE,
                    run_type="enrichment",
                    started_at=started_at,
                    completed_at=datetime.now(timezone.utc),
                    records_fetched=0,
                    records_loaded=counters.get("seasons_upserted", 0),
                    records_errored=0,
                    status=RunStatus.FAILED,
                    error_log=error_msg[:500] if error_msg else None,
                )
                session.add(run)
        except Exception:
            logger.exception("Failed to log the failed run")

    # ── Summary ──────────────────────────────────────────────────────

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()

    print()
    print("=" * 60)
    print("  FBREF ENRICHMENT — SUMMARY")
    print("=" * 60)
    print(f"  Leagues with data:       {len(leagues_with_data)}")
    for name in leagues_with_data:
        types = list(leagues_with_data[name].keys())
        rows = sum(len(df) for df in leagues_with_data[name].values())
        print(f"    {name:<35s}  {rows:>4} rows  ({', '.join(types)})")
    print(f"  Season records upserted: {counters.get('seasons_upserted', 0):,}")
    print(f"  Shooting records staged: {counters.get('shooting_staged', 0):,}")
    print(f"  Players created:         {counters.get('players_created', 0):,}")
    print(f"  Club match failures:     {counters.get('club_miss', 0):,}")
    print(f"  Player match failures:   {counters.get('player_miss', 0):,}")
    print(f"  Runtime:                 {elapsed:.0f}s")
    if error_msg:
        print(f"  ERROR: {error_msg[:120]}")
    print("=" * 60)
    print()

    sys.exit(1 if error_msg else 0)


if __name__ == "__main__":
    main()
