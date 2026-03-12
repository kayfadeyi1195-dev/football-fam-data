#!/usr/bin/env python3
"""Discover Pitchero URLs for clubs that don't have one yet.

Generates slug variants from each club's name, checks whether
``https://www.pitchero.com/clubs/{slug}`` returns a valid Pitchero
page, and persists any match to the ``clubs`` table.  After
discovery, runs the Pitchero scraper on newly discovered clubs
to immediately pull squad data.

Usage::

    python scripts/discover_pitchero_clubs.py
    python scripts/discover_pitchero_clubs.py --step 3,4
    python scripts/discover_pitchero_clubs.py --step 5 --limit 50
    python scripts/discover_pitchero_clubs.py --discover-only
"""

import argparse
import logging
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from src.db.models import Club, DataSourceRun, League, RunStatus
from src.db.session import get_session
from src.etl.staging import stage_records
from src.scrapers.pitchero import PitcheroScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


def _load_candidates(
    step_filter: set[int] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Return clubs where ``pitchero_url IS NULL``, prioritised by step."""
    with get_session() as session:
        query = (
            select(
                Club.id,
                Club.name,
                League.step,
                League.name.label("league_name"),
            )
            .outerjoin(League, League.id == Club.league_id)
            .where(Club.pitchero_url.is_(None))
            .where(Club.is_active.is_(True))
        )
        if step_filter:
            query = query.where(League.step.in_(step_filter))

        # Steps 3-5 first (most likely to be on Pitchero), then the rest
        query = query.order_by(League.step.asc().nullslast(), Club.name)

        if limit:
            query = query.limit(limit)

        rows = session.execute(query).all()

    return [
        {
            "club_id": r.id,
            "club_name": r.name,
            "step": r.step,
            "league_name": r.league_name or "Unknown",
        }
        for r in rows
    ]


def run(
    step_filter: set[int] | None = None,
    limit: int | None = None,
    discover_only: bool = False,
) -> dict[str, Any]:
    started = time.monotonic()
    scraper = PitcheroScraper()
    candidates = _load_candidates(step_filter=step_filter, limit=limit)

    if not candidates:
        print("\nNo clubs without a Pitchero URL match the criteria.")
        return {"candidates": 0}

    by_step: dict[int | None, int] = {}
    for c in candidates:
        by_step[c["step"]] = by_step.get(c["step"], 0) + 1
    steps_desc = ", ".join(
        f"Step {s}: {n}" for s, n in sorted(by_step.items(), key=lambda x: (x[0] or 99))
    )
    logger.info("Pitchero discovery: %d candidates (%s)", len(candidates), steps_desc)

    # ── log the run ──────────────────────────────────────────────
    with get_session() as session:
        run_record = DataSourceRun(
            source="pitchero",
            run_type="url_discovery",
            started_at=datetime.now(timezone.utc),
            status=RunStatus.RUNNING,
            records_fetched=0,
            records_loaded=0,
            records_errored=0,
        )
        session.add(run_record)
        session.commit()
        run_id = run_record.id

    # ── Phase 1: URL Discovery ───────────────────────────────────
    logger.info("═══ Phase 1: PITCHERO URL DISCOVERY ═══")

    discovered: dict[int, str] = {}  # club_id → pitchero_url
    club_names_by_id: dict[int, str] = {}

    for idx, c in enumerate(candidates, 1):
        club_id = c["club_id"]
        club_name = c["club_name"]
        club_names_by_id[club_id] = club_name

        pitchero_url = scraper.discover_pitchero_url(club_name)

        if pitchero_url:
            discovered[club_id] = pitchero_url
            with get_session() as session:
                club = session.get(Club, club_id)
                if club:
                    club.pitchero_url = pitchero_url
                    session.commit()
            logger.info(
                "[%d/%d] %s → %s  (Step %s)",
                idx, len(candidates), club_name, pitchero_url, c["step"],
            )
        else:
            logger.debug(
                "[%d/%d] %s — not on Pitchero",
                idx, len(candidates), club_name,
            )

    logger.info(
        "Discovery complete: %d checked, %d found",
        len(candidates), len(discovered),
    )

    # ── Phase 2: Scrape newly discovered clubs ───────────────────
    total_players = 0
    total_profiles = 0
    scrape_errors = 0

    if not discover_only and discovered:
        logger.info("═══ Phase 2: SCRAPING %d NEWLY DISCOVERED CLUBS ═══", len(discovered))

        all_records: list[dict[str, Any]] = []

        for club_id, pitchero_url in discovered.items():
            club_name = club_names_by_id[club_id]
            try:
                result = scraper.scrape_club(pitchero_url, include_profiles=False)
                squad = result.get("squad", [])
                if squad:
                    for p in squad:
                        p["club_id"] = club_id
                        p["club_name"] = club_name
                    all_records.extend(squad)
                    total_players += len(squad)
                    logger.info("  %s: %d players", club_name, len(squad))
                else:
                    logger.debug("  %s: no squad data", club_name)

                for err in result.get("errors", []):
                    logger.warning("  %s: %s", club_name, err)
                    scrape_errors += 1
            except Exception as exc:
                scrape_errors += 1
                logger.warning("  %s: scrape error — %s", club_name, exc)

        if all_records:
            stage_records("pitchero", "player", all_records, id_field="id")
            logger.info("Staged %d player records", len(all_records))

        # Run the Pitchero ETL transform on the new records
        logger.info("═══ Phase 3: PITCHERO ETL TRANSFORM ═══")
        try:
            from src.etl.pitchero_transform import transform_pitchero
            transform_pitchero()
        except Exception as exc:
            logger.warning("Transform error: %s", exc)

    elif discover_only:
        logger.info("Skipping scraping (--discover-only)")

    # ── Update run record ────────────────────────────────────────
    with get_session() as session:
        rec = session.get(DataSourceRun, run_id)
        if rec:
            rec.completed_at = datetime.now(timezone.utc)
            rec.records_fetched = len(candidates)
            rec.records_loaded = total_players
            rec.records_errored = scrape_errors
            rec.status = RunStatus.COMPLETED
            session.commit()

    elapsed = time.monotonic() - started

    summary = {
        "candidates": len(candidates),
        "urls_found": len(discovered),
        "players_scraped": total_players,
        "scrape_errors": scrape_errors,
        "elapsed_secs": elapsed,
    }

    # ── Print summary ────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  Pitchero Discovery Summary")
    print("=" * 60)
    print(f"  Clubs checked (no pitchero_url): {len(candidates)}")
    for step, count in sorted(by_step.items(), key=lambda x: (x[0] or 99)):
        print(f"    Step {step or '?'}: {count}")
    print()
    print(f"  Pitchero URLs found:     {len(discovered)}")
    if discovered:
        hit_rate = len(discovered) / len(candidates) * 100
        print(f"  Hit rate:                {hit_rate:.1f}%")
        print()
        print("  Newly discovered:")
        for club_id, url in list(discovered.items())[:20]:
            print(f"    {club_names_by_id[club_id]:<40s}  {url}")
        if len(discovered) > 20:
            print(f"    … and {len(discovered) - 20} more")
    if not discover_only:
        print()
        print(f"  Players scraped:         {total_players}")
        print(f"  Scrape errors:           {scrape_errors}")
    print()
    print(f"  Elapsed: {elapsed:.0f}s ({elapsed / 60:.1f} min)")
    print("=" * 60)

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Discover Pitchero URLs for clubs and scrape squads",
    )
    parser.add_argument(
        "--step", type=str, default=None,
        help="Only check clubs at these pyramid steps (comma-separated, e.g. 3,4,5)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max clubs to check",
    )
    parser.add_argument(
        "--discover-only", action="store_true",
        help="Only discover URLs, skip squad scraping and ETL",
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
