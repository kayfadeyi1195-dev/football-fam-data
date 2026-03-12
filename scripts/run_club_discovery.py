#!/usr/bin/env python3
"""Targeted club website discovery and squad scraping.

Finds clubs with **zero players** and **no website URL**, prioritises
Steps 3-4 (most likely to have discoverable websites), runs URL
discovery, then scrapes squads from any newly found URLs.

Usage::

    python scripts/run_club_discovery.py
    python scripts/run_club_discovery.py --step 3,4        # Steps 3-4 only
    python scripts/run_club_discovery.py --step 5           # Step 5 only
    python scripts/run_club_discovery.py --limit 50         # cap at 50 clubs
    python scripts/run_club_discovery.py --discover-only    # skip squad scraping
"""

import argparse
import logging
import time
from datetime import datetime, timezone

from sqlalchemy import func, select

from src.db.models import (
    Club,
    DataSourceRun,
    League,
    Player,
    RunStatus,
)
from src.db.session import get_session
from src.etl.staging import stage_records
from src.etl.transform import transform_players
from src.scrapers.club_websites import ClubWebsiteScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


def _find_target_clubs(
    step_filter: set[int] | None = None,
    limit: int | None = None,
) -> list[dict]:
    """Find clubs with no website_url, no pitchero_url, and 0 players.

    Returns a list of dicts with club metadata, sorted so Steps 3-4
    come first (highest chance of having a website).
    """
    with get_session() as session:
        # Subquery: player count per club
        player_count_sq = (
            select(
                Player.current_club_id,
                func.count(Player.id).label("cnt"),
            )
            .where(Player.merged_into_id.is_(None))
            .group_by(Player.current_club_id)
            .subquery()
        )

        query = (
            select(
                Club.id,
                Club.name,
                League.name.label("league_name"),
                League.step,
            )
            .outerjoin(League, League.id == Club.league_id)
            .outerjoin(player_count_sq, player_count_sq.c.current_club_id == Club.id)
            .where(Club.website_url.is_(None))
            .where(Club.pitchero_url.is_(None))
            .where(Club.is_active.is_(True))
            .where(
                (player_count_sq.c.cnt.is_(None))
                | (player_count_sq.c.cnt == 0)
            )
        )

        if step_filter:
            query = query.where(League.step.in_(step_filter))

        # Prioritise Steps 3-4 by sorting them first
        query = query.order_by(
            League.step.asc().nullslast(),
            Club.name,
        )

        if limit:
            query = query.limit(limit)

        rows = session.execute(query).all()

    return [
        {
            "club_id": r.id,
            "club_name": r.name,
            "league_name": r.league_name or "Unknown",
            "step": r.step,
        }
        for r in rows
    ]


def run(
    step_filter: set[int] | None = None,
    limit: int | None = None,
    discover_only: bool = False,
) -> dict:
    started = time.monotonic()
    scraper = ClubWebsiteScraper()

    targets = _find_target_clubs(step_filter=step_filter, limit=limit)

    if not targets:
        print("\nNo clubs found matching the criteria (0 players, no URL).")
        return {"targets": 0}

    # Group by step for the report
    by_step: dict[int | None, int] = {}
    for t in targets:
        by_step[t["step"]] = by_step.get(t["step"], 0) + 1

    steps_desc = ", ".join(
        f"Step {s}: {c}" for s, c in sorted(by_step.items(), key=lambda x: (x[0] or 99))
    )
    logger.info("Target clubs: %d (%s)", len(targets), steps_desc)

    # ── Phase 1: URL Discovery ───────────────────────────────────
    logger.info("═══ Phase 1: URL DISCOVERY ═══")

    urls_found: dict[int, str] = {}
    urls_attempted = 0

    with get_session() as session:
        run_record = DataSourceRun(
            source="club_website",
            run_type="discovery_and_scrape",
            started_at=datetime.now(timezone.utc),
            status=RunStatus.RUNNING,
            records_fetched=0,
            records_loaded=0,
            records_errored=0,
        )
        session.add(run_record)
        session.commit()
        run_id = run_record.id

    for t in targets:
        urls_attempted += 1
        club_id = t["club_id"]
        club_name = t["club_name"]

        url = scraper._try_candidate_urls(club_name)
        if url is None:
            url = scraper._search_for_url(club_name)

        if url:
            urls_found[club_id] = url
            # Persist the URL immediately
            with get_session() as session:
                club = session.get(Club, club_id)
                if club:
                    club.website_url = url
                    session.commit()
            logger.info(
                "[%d/%d] %s → %s  (Step %s)",
                urls_attempted, len(targets), club_name, url, t["step"],
            )
        else:
            logger.debug(
                "[%d/%d] %s — not found", urls_attempted, len(targets), club_name,
            )

    logger.info(
        "URL discovery: %d attempted, %d found",
        urls_attempted, len(urls_found),
    )

    # ── Phase 2: Squad Scraping ──────────────────────────────────
    players_scraped = 0
    clubs_with_players = 0
    scrape_errors = 0

    if not discover_only and urls_found:
        logger.info("═══ Phase 2: SQUAD SCRAPING ═══")

        all_records: list[dict] = []

        for club_id, website_url in urls_found.items():
            club_name = next(
                t["club_name"] for t in targets if t["club_id"] == club_id
            )
            try:
                players = scraper.scrape_club(club_id, club_name, website_url)
                if players:
                    clubs_with_players += 1
                    players_scraped += len(players)
                    all_records.extend(players)
                    logger.info("  %s: %d players", club_name, len(players))
                else:
                    logger.debug("  %s: no squad page / no players", club_name)
            except Exception as exc:
                scrape_errors += 1
                logger.warning("  %s: scrape error — %s", club_name, exc)

        if all_records:
            stage_records("club_website", "player", all_records, id_field="id")
            logger.info("Staged %d player records", len(all_records))

        # Run the generic transform to mark records processed
        logger.info("═══ Phase 3: TRANSFORM ═══")
        transform_players(source="club_website")
    elif discover_only:
        logger.info("Skipping squad scraping (--discover-only)")

    # ── Log the run ──────────────────────────────────────────────
    with get_session() as session:
        run_record = session.get(DataSourceRun, run_id)
        if run_record:
            run_record.completed_at = datetime.now(timezone.utc)
            run_record.records_fetched = urls_attempted
            run_record.records_loaded = players_scraped
            run_record.records_errored = scrape_errors
            run_record.status = RunStatus.COMPLETED
            session.commit()

    elapsed = time.monotonic() - started

    summary = {
        "targets": len(targets),
        "urls_attempted": urls_attempted,
        "urls_found": len(urls_found),
        "clubs_with_players": clubs_with_players,
        "players_scraped": players_scraped,
        "scrape_errors": scrape_errors,
        "elapsed_secs": elapsed,
    }

    # ── Print summary ────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  Club Discovery & Scraping Summary")
    print("=" * 60)
    print(f"  Target clubs (0 players, no URL): {len(targets)}")
    for step, count in sorted(by_step.items(), key=lambda x: (x[0] or 99)):
        print(f"    Step {step or '?'}: {count}")
    print()
    print(f"  URLs attempted:    {urls_attempted}")
    print(f"  URLs discovered:   {len(urls_found)}")
    if not discover_only:
        print()
        print(f"  Clubs scraped with players:  {clubs_with_players}")
        print(f"  Total players scraped:       {players_scraped}")
        print(f"  Scrape errors:               {scrape_errors}")
    print()
    print(f"  Elapsed: {elapsed:.0f}s ({elapsed / 60:.1f} min)")
    print("=" * 60)

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Club website discovery and squad scraping for clubs with 0 players",
    )
    parser.add_argument(
        "--step", type=str, default=None,
        help="Only target clubs at these pyramid steps (comma-separated, e.g. 3,4)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max clubs to process",
    )
    parser.add_argument(
        "--discover-only", action="store_true",
        help="Only discover URLs, skip squad scraping",
    )
    args = parser.parse_args()

    step_filter = None
    if args.step:
        step_filter = {int(s.strip()) for s in args.step.split(",")}

    run(
        step_filter=step_filter,
        limit=args.limit,
        discover_only=args.discover_only,
    )


if __name__ == "__main__":
    main()
