"""Scrape FA Full-Time league tables, results, and player data.

Loops through all known Step 4-6 league IDs from
``KNOWN_LEAGUE_IDS``, scrapes each league (divisions, tables,
results, player appearances), stages all records via
``stage_records()``, and logs a ``data_source_runs`` record.

Usage::

    python scripts/run_fa_fulltime.py                       # all known leagues
    python scripts/run_fa_fulltime.py --league "Wessex Football League"
    python scripts/run_fa_fulltime.py --league 274386        # by FA Full-Time ID
    python scripts/run_fa_fulltime.py --discover             # discover only, no scrape
"""

import argparse
import logging
import time
from datetime import datetime, timezone

from src.db.models import DataSourceRun, RunStatus
from src.db.session import get_session
from src.etl.staging import stage_records
from src.scrapers.fa_fulltime import KNOWN_LEAGUE_IDS, FAFullTimeScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

DATA_SOURCE = "fa_fulltime"


def _resolve_league_arg(league_arg: str) -> dict[str, str]:
    """Turn a --league argument into a {name: league_id} dict.

    Accepts either a league name (fuzzy-matched against KNOWN_LEAGUE_IDS
    keys) or a numeric FA Full-Time league ID.
    """
    if league_arg.isdigit():
        name = next(
            (n for n, lid in KNOWN_LEAGUE_IDS.items() if lid == league_arg),
            f"Unknown ({league_arg})",
        )
        return {name: league_arg}

    key_lower = league_arg.lower()
    for name, lid in KNOWN_LEAGUE_IDS.items():
        if key_lower in name.lower():
            return {name: lid}

    logger.error(
        "League '%s' not found in KNOWN_LEAGUE_IDS.  Available:\n  %s",
        league_arg,
        "\n  ".join(f"{n}  (ID {lid})" for n, lid in sorted(KNOWN_LEAGUE_IDS.items())),
    )
    raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="FA Full-Time scraper for Steps 4-6 leagues",
    )
    parser.add_argument(
        "--league",
        type=str,
        default=None,
        help="Scrape a single league by name (substring match) or numeric ID",
    )
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Run league discovery only — print matches and exit",
    )
    parser.add_argument(
        "--no-players",
        action="store_true",
        help="Skip per-team player scraping (much faster)",
    )
    args = parser.parse_args()

    scraper = FAFullTimeScraper()

    # ── discovery mode ────────────────────────────────────────────────
    if args.discover:
        logger.info("=== FA Full-Time league discovery ===")
        leagues = scraper.discover_nonleague_leagues()
        print(f"\nFound {len(leagues)} non-league leagues:\n")
        for lg in leagues:
            print(f"  {lg['name']:<55s}  league={lg['league_id']}")
        return

    # ── determine which leagues to scrape ─────────────────────────────
    if args.league:
        leagues_to_scrape = _resolve_league_arg(args.league)
    else:
        leagues_to_scrape = dict(KNOWN_LEAGUE_IDS)

    logger.info(
        "=== FA Full-Time scraper: %d league(s) ===", len(leagues_to_scrape),
    )
    for name, lid in leagues_to_scrape.items():
        logger.info("  %s  (ID %s)", name, lid)

    # ── start data_source_runs record ─────────────────────────────────
    run_type = "single_league" if args.league else "full_pull"
    with get_session() as session:
        run = DataSourceRun(
            source=DATA_SOURCE,
            run_type=run_type,
            started_at=datetime.now(timezone.utc),
            status=RunStatus.RUNNING,
        )
        session.add(run)
        session.flush()
        run_id = run.id

    # ── scrape loop ───────────────────────────────────────────────────
    started = time.monotonic()

    totals = {
        "leagues_scraped": 0,
        "divisions": 0,
        "table_records": 0,
        "match_records": 0,
        "player_records": 0,
        "errors": 0,
    }
    error_messages: list[str] = []

    for league_name, league_id in leagues_to_scrape.items():
        logger.info("━━━ Scraping: %s (ID %s) ━━━", league_name, league_id)

        try:
            league_data = scraper.scrape_league(league_id)
        except Exception as exc:
            msg = f"{league_name}: {exc}"
            logger.error("Failed to scrape league: %s", msg)
            error_messages.append(msg)
            totals["errors"] += 1
            continue

        divisions = league_data.get("divisions", [])
        if not divisions:
            logger.warning("No divisions returned for %s", league_name)
            continue

        totals["leagues_scraped"] += 1
        totals["divisions"] += len(divisions)

        # If --no-players, strip player data before staging
        if args.no_players:
            for div in divisions:
                div["teams"] = {}

        # Build staging records
        staging = scraper.build_staging_records(league_data)

        # Stage league_table records
        table_recs = staging.get("league_table", [])
        if table_recs:
            staged = stage_records(DATA_SOURCE, "league_table", table_recs)
            totals["table_records"] += staged

        # Stage match records
        match_recs = staging.get("match", [])
        if match_recs:
            staged = stage_records(DATA_SOURCE, "match", match_recs)
            totals["match_records"] += staged

        # Stage player records
        player_recs = staging.get("player", [])
        if player_recs:
            staged = stage_records(DATA_SOURCE, "player", player_recs)
            totals["player_records"] += staged

        # Per-league summary
        logger.info(
            "  %s: %d div(s), %d table rows, %d results, %d players staged",
            league_name,
            len(divisions),
            len(table_recs),
            len(match_recs),
            len(player_recs),
        )

    elapsed = time.monotonic() - started

    # ── complete data_source_runs record ──────────────────────────────
    total_fetched = (
        totals["table_records"] + totals["match_records"] + totals["player_records"]
    )
    final_status = RunStatus.COMPLETED if not error_messages else RunStatus.FAILED

    with get_session() as session:
        run = session.get(DataSourceRun, run_id)
        if run:
            run.completed_at = datetime.now(timezone.utc)
            run.records_fetched = total_fetched
            run.records_loaded = total_fetched
            run.records_errored = totals["errors"]
            run.status = final_status
            if error_messages:
                run.error_log = "\n".join(error_messages)

    # ── summary ───────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  FA Full-Time Scraper — Summary")
    print("=" * 60)
    print(f"  Leagues scraped:   {totals['leagues_scraped']} / {len(leagues_to_scrape)}")
    print(f"  Divisions:         {totals['divisions']}")
    print(f"  Table rows staged: {totals['table_records']}")
    print(f"  Match results:     {totals['match_records']}")
    print(f"  Player records:    {totals['player_records']}")
    print(f"  Errors:            {totals['errors']}")
    print(f"  Elapsed:           {elapsed:.1f}s")
    print(f"  Status:            {final_status.value}")
    if error_messages:
        print(f"\n  Errors:")
        for msg in error_messages:
            print(f"    - {msg}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
