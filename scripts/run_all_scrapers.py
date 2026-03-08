#!/usr/bin/env python
"""Orchestrate all scraping and enrichment in sequence with progress tracking.

Runs five stages in order:

  1. Pitchero scraper  → Pitchero ETL transform
  2. FA Full-Time scraper → FA Full-Time ETL transform
  3. FBref enrichment (Steps 1-2 via soccerdata)
  4. Club website URL discovery
  5. Club website squad scraper

After each stage, prints a progress snapshot showing total players,
players per pyramid step, average confidence, and data sources.

Usage::

    python scripts/run_all_scrapers.py
    python scripts/run_all_scrapers.py --step 3
    python scripts/run_all_scrapers.py --step 4,5,6
    python scripts/run_all_scrapers.py --resume
    python scripts/run_all_scrapers.py --dry-run
"""

import argparse
import json
import logging
import sys
import time
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import func, select

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("run_all_scrapers")

PROGRESS_FILE = Path(__file__).resolve().parent.parent / "data" / "scraper_progress.json"

SOURCE_STEPS: dict[str, set[int]] = {
    "pitchero":       {3, 4, 5},
    "fa_fulltime":    {4, 5, 6},
    "fbref":          {1, 2},
    "club_discovery": {1, 2, 3, 4, 5, 6},
    "club_websites":  {3, 4, 5, 6},
}


# ══════════════════════════════════════════════════════════════════════════
# Progress snapshots
# ══════════════════════════════════════════════════════════════════════════

def _print_progress(label: str) -> None:
    """Print a database health snapshot after a scraping stage."""
    from src.db.models import Club, League, Player, PlayerSeason, StagingRaw
    from src.db.session import get_session

    print()
    print(f"  ┌──────────────────────────────────────────────────────")
    print(f"  │  PROGRESS — after {label}")
    print(f"  ├──────────────────────────────────────────────────────")

    try:
        with get_session() as session:
            total_players = session.scalar(
                select(func.count(Player.id))
                .where(Player.merged_into_id.is_(None))
            ) or 0

            step_counts = session.execute(
                select(League.step, func.count(Player.id))
                .join(Club, Club.id == Player.current_club_id)
                .join(League, League.id == Club.league_id)
                .where(Player.merged_into_id.is_(None))
                .group_by(League.step)
                .order_by(League.step)
            ).all()

            avg_conf = session.scalar(
                select(func.avg(Player.overall_confidence))
                .where(Player.merged_into_id.is_(None))
                .where(Player.overall_confidence.isnot(None))
            )

            source_counts = session.execute(
                select(PlayerSeason.data_source, func.count(PlayerSeason.id))
                .group_by(PlayerSeason.data_source)
                .order_by(func.count(PlayerSeason.id).desc())
            ).all()

            staging_unprocessed = session.scalar(
                select(func.count(StagingRaw.id))
                .where(StagingRaw.processed.is_(False))
            ) or 0

        print(f"  │  Total players:  {total_players:,}")
        if step_counts:
            for step, count in step_counts:
                print(f"  │    Step {step}: {count:,}")
        else:
            print(f"  │    (no step data — clubs may lack league assignments)")

        if avg_conf is not None:
            print(f"  │  Avg confidence: {float(avg_conf):.2f}")
        else:
            print(f"  │  Avg confidence: —")

        if source_counts:
            print(f"  │  Data sources:")
            for source, count in source_counts:
                print(f"  │    {source or '(none)':<25s}  {count:,} season records")
        print(f"  │  Staging queue:  {staging_unprocessed:,} unprocessed")

    except Exception as exc:
        print(f"  │  (snapshot failed: {exc})")

    print(f"  └──────────────────────────────────────────────────────")
    print()


# ══════════════════════════════════════════════════════════════════════════
# Stage runner
# ══════════════════════════════════════════════════════════════════════════

class StageResult:
    __slots__ = ("name", "ok", "elapsed_s", "detail", "error")

    def __init__(
        self,
        name: str,
        ok: bool,
        elapsed_s: float,
        detail: dict[str, Any] | None = None,
        error: str | None = None,
    ):
        self.name = name
        self.ok = ok
        self.elapsed_s = elapsed_s
        self.detail = detail or {}
        self.error = error


def _run_stage(
    name: str,
    fn: Callable[[], dict[str, Any]],
    *,
    dry_run: bool = False,
) -> StageResult:
    """Execute a stage, catch errors, and measure time."""
    log.info("=" * 60)
    log.info("STAGE: %s", name)
    log.info("=" * 60)

    if dry_run:
        log.info("  [DRY RUN] Would run: %s", name)
        return StageResult(name, ok=True, elapsed_s=0, detail={"dry_run": True})

    t0 = time.monotonic()
    try:
        detail = fn() or {}
        elapsed = time.monotonic() - t0
        log.info("  %s completed in %.1fs", name, elapsed)
        return StageResult(name, ok=True, elapsed_s=elapsed, detail=detail)
    except Exception:
        elapsed = time.monotonic() - t0
        tb = traceback.format_exc()
        log.error("  %s FAILED after %.1fs:\n%s", name, elapsed, tb)
        return StageResult(name, ok=False, elapsed_s=elapsed, error=tb[-500:])


# ══════════════════════════════════════════════════════════════════════════
# Resume tracking
# ══════════════════════════════════════════════════════════════════════════

def _load_completed_stages() -> set[str]:
    """Load the set of stage names completed during this run date."""
    if not PROGRESS_FILE.exists():
        return set()
    try:
        data = json.loads(PROGRESS_FILE.read_text())
        if data.get("date") == date.today().isoformat():
            return set(data.get("completed", []))
    except (json.JSONDecodeError, OSError):
        pass
    return set()


def _save_completed_stage(stage_name: str) -> None:
    """Mark a stage as completed for today."""
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    completed = _load_completed_stages()
    completed.add(stage_name)
    PROGRESS_FILE.write_text(json.dumps({
        "date": date.today().isoformat(),
        "completed": sorted(completed),
    }, indent=2) + "\n")


# ══════════════════════════════════════════════════════════════════════════
# Individual stage functions
# ══════════════════════════════════════════════════════════════════════════

def _stage_pitchero_scrape() -> dict[str, Any]:
    """Run the Pitchero scraper for clubs with pitchero_url."""
    from src.db.models import Club
    from src.db.session import get_session
    from src.etl.staging import stage_records
    from src.scrapers.pitchero import PitcheroScraper

    with get_session() as session:
        clubs = session.execute(
            select(Club.name, Club.pitchero_url)
            .where(Club.pitchero_url.isnot(None))
            .where(Club.pitchero_url != "")
        ).all()

    if not clubs:
        log.info("No clubs with pitchero_url — skipping")
        return {"clubs": 0, "players": 0}

    log.info("Pitchero: %d clubs to scrape", len(clubs))
    scraper = PitcheroScraper()
    total_players = 0
    total_profiles = 0
    errors = 0

    for club_name, pitchero_url in clubs:
        try:
            result = scraper.scrape_club(pitchero_url, include_profiles=False)
            squad = result.get("squad", [])
            if squad:
                stage_records("pitchero", "player", squad, id_field="id")
                total_players += len(squad)
            for err in result.get("errors", []):
                log.warning("  %s: %s", club_name, err)
        except Exception as exc:
            errors += 1
            log.warning("  %s: error — %s", club_name, exc)

    return {"clubs": len(clubs), "players": total_players, "errors": errors}


def _stage_pitchero_etl() -> dict[str, Any]:
    """Run the Pitchero ETL transform on staged records."""
    from src.etl.pitchero_transform import transform_pitchero
    return transform_pitchero()


def _stage_fa_fulltime_scrape() -> dict[str, Any]:
    """Run the FA Full-Time scraper for Step 4-6 leagues."""
    from src.scrapers.fa_fulltime import KNOWN_LEAGUE_IDS, FAFullTimeScraper
    from src.etl.staging import stage_records

    scraper = FAFullTimeScraper()
    totals: dict[str, int] = {
        "leagues": 0, "table_records": 0,
        "match_records": 0, "player_records": 0, "errors": 0,
    }

    for league_name, league_id in KNOWN_LEAGUE_IDS.items():
        log.info("  FA Full-Time: %s (ID %s)", league_name, league_id)
        try:
            league_data = scraper.scrape_league(league_id)
            divisions = league_data.get("divisions", [])
            if not divisions:
                continue
            totals["leagues"] += 1

            staging = scraper.build_staging_records(league_data)
            for entity_type in ("league_table", "match", "player"):
                recs = staging.get(entity_type, [])
                if recs:
                    staged = stage_records("fa_fulltime", entity_type, recs)
                    totals[f"{entity_type.replace('league_', '')}records"] = (
                        totals.get(f"{entity_type.replace('league_', '')}records", 0) + staged
                    )
        except Exception as exc:
            totals["errors"] += 1
            log.warning("  FA Full-Time: %s failed — %s", league_name, exc)

    return totals


def _stage_fa_fulltime_etl() -> dict[str, Any]:
    """Run the FA Full-Time ETL transform on staged records."""
    from src.etl.fa_fulltime_transform import transform_fa_fulltime
    return transform_fa_fulltime()


def _stage_fbref() -> dict[str, Any]:
    """Run FBref enrichment for Steps 1-2."""
    from scripts.run_fbref import (
        _ensure_league_config,
        _get_available_leagues,
        _try_read_stats,
        _process_standard,
        _process_shooting,
        _build_club_lookup,
        _build_player_lookup,
        _stage_raw,
        CUSTOM_LEAGUE_ENTRIES,
        LEAGUES_TO_TRY,
        OUR_LEAGUE_NAMES,
        SEASON_LABEL,
        DATA_SOURCE,
    )
    from src.db.models import DataSourceRun, League, RunStatus
    from src.db.session import get_session
    import pandas as pd

    _ensure_league_config()
    available = _get_available_leagues()
    target_leagues = [lg for lg in LEAGUES_TO_TRY if lg in available]

    if not target_leagues:
        log.warning("FBref: no target leagues available in soccerdata")
        return {"skipped": True, "reason": "no leagues available"}

    leagues_with_data: dict[str, dict[str, pd.DataFrame]] = {}
    for sd_league in target_leagues:
        standard = _try_read_stats(sd_league, 2024, "standard")
        if standard is None:
            continue
        data: dict[str, pd.DataFrame] = {"standard": standard}
        import time as _time
        _time.sleep(4)
        shooting = _try_read_stats(sd_league, 2024, "shooting")
        if shooting is not None:
            data["shooting"] = shooting
        leagues_with_data[sd_league] = data
        _time.sleep(4)

    if not leagues_with_data:
        return {"leagues": 0, "reason": "no data on FBref"}

    counters: dict[str, int] = {
        "seasons_upserted": 0, "shooting_staged": 0,
        "club_miss": 0, "player_miss": 0, "players_created": 0,
    }

    with get_session() as session:
        club_map, club_names = _build_club_lookup(session)
        player_map, player_names = _build_player_lookup(session)
        initial_count = len(player_map)

        our_leagues = session.execute(
            select(League.id, League.name).where(League.season == SEASON_LABEL)
        ).all()
        our_league_map = {lg.name: lg.id for lg in our_leagues}

        for sd_league, data in leagues_with_data.items():
            our_name = OUR_LEAGUE_NAMES.get(sd_league, "")
            our_league_id = our_league_map.get(our_name)
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

        counters["players_created"] = len(player_map) - initial_count

    return {
        "leagues": len(leagues_with_data),
        **counters,
    }


def _stage_club_discovery() -> dict[str, Any]:
    """Discover website URLs for clubs with no URL."""
    from src.scrapers.club_websites import ClubWebsiteScraper
    scraper = ClubWebsiteScraper()
    urls = scraper.discover_urls()
    return {"urls_discovered": len(urls)}


def _stage_club_websites_scrape() -> dict[str, Any]:
    """Scrape squad pages from non-Pitchero club websites."""
    from src.scrapers.club_websites import ClubWebsiteScraper
    scraper = ClubWebsiteScraper()
    return scraper.scrape_all_squads()


# ══════════════════════════════════════════════════════════════════════════
# Stage definitions
# ══════════════════════════════════════════════════════════════════════════

STAGES: list[tuple[str, str, Callable[[], dict[str, Any]]]] = [
    ("pitchero_scrape",      "pitchero",       _stage_pitchero_scrape),
    ("pitchero_etl",         "pitchero",       _stage_pitchero_etl),
    ("fa_fulltime_scrape",   "fa_fulltime",    _stage_fa_fulltime_scrape),
    ("fa_fulltime_etl",      "fa_fulltime",    _stage_fa_fulltime_etl),
    ("fbref_enrichment",     "fbref",          _stage_fbref),
    ("club_url_discovery",   "club_discovery", _stage_club_discovery),
    ("club_website_scrape",  "club_websites",  _stage_club_websites_scrape),
]


def _is_relevant(source_key: str, step_filter: set[int] | None) -> bool:
    if step_filter is None:
        return True
    source_steps = SOURCE_STEPS.get(source_key, set())
    return bool(source_steps & step_filter)


# ══════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orchestrate all scrapers with progress tracking",
    )
    parser.add_argument(
        "--step", type=str, default=None,
        help="Only run scrapers for specific steps (e.g. '3' or '4,5,6')",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Skip stages already completed today",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log what would be scraped without making HTTP requests",
    )
    args = parser.parse_args()

    step_filter: set[int] | None = None
    if args.step:
        step_filter = {int(s.strip()) for s in args.step.split(",")}
        log.info("Step filter: %s", step_filter)

    completed_stages = _load_completed_stages() if args.resume else set()
    if completed_stages:
        log.info("Resuming — already completed today: %s", completed_stages)

    started_at = datetime.now(timezone.utc)
    results: list[StageResult] = []

    _print_progress("START")

    for stage_name, source_key, stage_fn in STAGES:
        if not _is_relevant(source_key, step_filter):
            log.info("Skipping %s (not relevant to step filter %s)", stage_name, step_filter)
            continue

        if args.resume and stage_name in completed_stages:
            log.info("Skipping %s (already completed today)", stage_name)
            results.append(StageResult(stage_name, ok=True, elapsed_s=0, detail={"resumed": True}))
            continue

        result = _run_stage(stage_name, stage_fn, dry_run=args.dry_run)
        results.append(result)

        if result.ok and not args.dry_run:
            _save_completed_stage(stage_name)

        _print_progress(stage_name)

    # ── Final summary ────────────────────────────────────────────────

    total_elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()

    print()
    print("=" * 60)
    print("  ALL SCRAPERS — FINAL SUMMARY")
    print("=" * 60)
    print(f"  Total runtime:  {total_elapsed:.0f}s ({total_elapsed / 60:.1f}m)")
    print()

    max_name_len = max((len(r.name) for r in results), default=10)
    for r in results:
        status = "OK" if r.ok else "FAIL"
        detail_str = ""
        if r.detail:
            interesting = {
                k: v for k, v in r.detail.items()
                if k not in ("dry_run", "resumed") and v
            }
            if interesting:
                detail_str = "  " + ", ".join(f"{k}={v}" for k, v in interesting.items())
            elif r.detail.get("dry_run"):
                detail_str = "  (dry run)"
            elif r.detail.get("resumed"):
                detail_str = "  (resumed — skipped)"

        print(f"  {r.name:<{max_name_len}s}  {status:<4s}  {r.elapsed_s:>6.1f}s{detail_str}")
        if r.error:
            first_line = r.error.strip().split("\n")[-1][:80]
            print(f"  {'':>{max_name_len}s}        └─ {first_line}")

    failed = [r for r in results if not r.ok]
    print()
    if failed:
        print(f"  {len(failed)} stage(s) FAILED — see logs above for details")
    else:
        print("  All stages completed successfully")
    print("=" * 60)
    print()

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
