#!/usr/bin/env python3
"""Orchestrate a Transfermarkt data pull for English non-league football.

Two modes:

**Competition scraping** (default)::

    python scripts/run_transfermarkt.py

Scrapes all 3 non-league competitions (Steps 1-2), stages the raw
data, runs the transform, and prints a summary.

**Enrichment mode** (``--enrich``)::

    python scripts/run_transfermarkt.py --enrich

Finds players in the database who are missing key fields, searches
for each on Transfermarkt, and fills in the gaps.  Also pulls per-
season stats, full transfer history, and market value timelines.

Flags::

    --comp NLN6          Scrape a single competition ID only
    --enrich             Run enrichment mode instead of competition scrape
    --enrich-limit 200   Cap enrichment lookups (default: unlimited)
    --step 1             Only enrich players at this pyramid step
    --stats              In competition mode, also scrape per-player stats
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import (
    Club,
    DataSourceRun,
    League,
    Player,
    RunStatus,
)
from src.db.session import get_session
from src.etl.staging import stage_records
from src.etl.transfermarkt_transform import (
    _infer_contract_status,
    _parse_dob,
    transform_transfermarkt,
)
from src.scrapers.transfermarkt import (
    NONLEAGUE_COMPETITIONS,
    TransfermarktScraper,
)

logger = logging.getLogger(__name__)

STEP_FOR_COMP: dict[str, int] = {
    "CNAT": 1,
    "NLN6": 2,
    "NLS6": 2,
}


# ── helpers ───────────────────────────────────────────────────────────────

def _start_run(session: Session, run_type: str) -> DataSourceRun:
    run = DataSourceRun(
        source="transfermarkt",
        run_type=run_type,
        started_at=datetime.now(timezone.utc),
        status=RunStatus.RUNNING,
        records_fetched=0,
        records_loaded=0,
        records_errored=0,
    )
    session.add(run)
    session.commit()
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
# Competition scraping mode
# ══════════════════════════════════════════════════════════════════════════

def run_competitions(
    comp_filter: str | None = None,
    *,
    fetch_stats: bool = False,
) -> dict[str, Any]:
    """Scrape clubs + squads for non-league competitions, stage, transform.

    When *fetch_stats* is True, also scrape per-player stats and
    transfer history for every player found in squads.
    """
    scraper = TransfermarktScraper()
    comps = {comp_filter: NONLEAGUE_COMPETITIONS[comp_filter]} if comp_filter else NONLEAGUE_COMPETITIONS

    all_clubs: list[dict[str, Any]] = []
    total_players = 0
    total_staged = 0
    stats_staged = 0
    transfers_staged = 0
    mv_staged = 0

    with get_session() as session:
        run = _start_run(session, "competition_scrape")

        for comp_id, comp_name in comps.items():
            step = STEP_FOR_COMP.get(comp_id, 0)
            logger.info(
                "═══ Competition: %s (%s) — Step %d ═══",
                comp_name, comp_id, step,
            )

            try:
                clubs = scraper.scrape_competition(comp_id)
            except Exception as exc:
                logger.error("Failed scraping %s: %s", comp_id, exc)
                clubs = []

            for club in clubs:
                players = club.get("players", [])
                total_players += len(players)
                logger.info("  %s — %d players", club["name"], len(players))

                staged = stage_records(
                    source="transfermarkt",
                    entity_type="club_squad",
                    records=[club],
                    id_field="id",
                )
                total_staged += staged

                if fetch_stats:
                    for p in players:
                        pid = p.get("tm_player_id")
                        if not pid:
                            continue
                        try:
                            stats = scraper.scrape_player_stats(pid)
                            if stats:
                                stats_staged += stage_records(
                                    source="transfermarkt",
                                    entity_type="player_stats",
                                    records=[{
                                        "id": f"tm_stats_{pid}",
                                        "tm_player_id": pid,
                                        "player_name": p.get("name", ""),
                                        "club_name": club["name"],
                                        "date_of_birth": p.get("date_of_birth"),
                                        "stats": stats,
                                    }],
                                )

                            transfers = scraper.scrape_player_transfers(pid)
                            if transfers:
                                transfers_staged += stage_records(
                                    source="transfermarkt",
                                    entity_type="player_transfers",
                                    records=[{
                                        "id": f"tm_transfers_{pid}",
                                        "tm_player_id": pid,
                                        "player_name": p.get("name", ""),
                                        "club_name": club["name"],
                                        "date_of_birth": p.get("date_of_birth"),
                                        "transfers": transfers,
                                    }],
                                )

                            mv = scraper.scrape_market_value_history(pid)
                            if mv:
                                mv_staged += stage_records(
                                    source="transfermarkt",
                                    entity_type="market_value",
                                    records=[{
                                        "id": f"tm_mv_{pid}",
                                        "tm_player_id": pid,
                                        "player_name": p.get("name", ""),
                                        "values": mv,
                                    }],
                                )
                        except Exception as exc:
                            logger.warning(
                                "Error fetching details for player %s: %s",
                                pid, exc,
                            )

            all_clubs.extend(clubs)

        _complete_run(
            session, run,
            records_fetched=total_players,
            records_loaded=total_staged + stats_staged + transfers_staged + mv_staged,
        )

    logger.info("Running Transfermarkt transform…")
    transform_results = transform_transfermarkt()

    return {
        "clubs_scraped": len(all_clubs),
        "players_scraped": total_players,
        "records_staged": total_staged,
        "stats_staged": stats_staged,
        "transfers_staged": transfers_staged,
        "mv_staged": mv_staged,
        "transform": transform_results,
    }


# ══════════════════════════════════════════════════════════════════════════
# Enrichment mode
# ══════════════════════════════════════════════════════════════════════════

def run_enrichment(
    limit: int | None = None,
    step_filter: int | None = None,
) -> dict[str, Any]:
    """Search Transfermarkt for players missing key data and fill gaps.

    For each matched player, scrapes the full profile, per-season
    stats, transfer history, and market value timeline.
    """
    scraper = TransfermarktScraper()

    searched = 0
    matched = 0
    updated = 0
    stats_staged = 0
    transfers_staged = 0
    mv_staged = 0
    errors = 0

    with get_session() as session:
        run = _start_run(session, "enrichment")

        query = (
            select(Player)
            .where(Player.merged_into_id.is_(None))
            .where(
                (Player.height_cm.is_(None))
                | (Player.contract_status.in_(["unknown", None]))
                | (Player.preferred_foot.is_(None))
                | (Player.date_of_birth.is_(None))
            )
        )

        if step_filter:
            query = (
                query
                .join(Club, Club.id == Player.current_club_id)
                .join(League, League.id == Club.league_id)
                .where(League.step == step_filter)
            )

        query = query.order_by(Player.full_name)
        players = session.execute(query).scalars().all()

        if limit:
            players = players[:limit]

        logger.info(
            "Enrichment: %d players to search (limit=%s, step=%s)",
            len(players), limit, step_filter,
        )

        for i, player in enumerate(players, 1):
            dob_str = player.date_of_birth.isoformat() if player.date_of_birth else None
            try:
                result = scraper.search_player(player.full_name, dob_str, full=True)
            except Exception as exc:
                logger.warning("Search error for %s: %s", player.full_name, exc)
                errors += 1
                continue

            searched += 1

            if not result:
                logger.debug("  No Transfermarkt match for %s", player.full_name)
                continue

            matched += 1
            fields_before = _count_filled(player)

            # Fill profile fields
            if not player.height_cm and result.get("height_cm"):
                player.height_cm = result["height_cm"]
            if not player.date_of_birth:
                parsed = _parse_dob(result.get("date_of_birth"))
                if parsed:
                    player.date_of_birth = parsed
            if not player.preferred_foot and result.get("preferred_foot"):
                player.preferred_foot = result["preferred_foot"]
            if not player.nationality and result.get("nationality"):
                player.nationality = result["nationality"]
            if not player.profile_photo_url and result.get("photo_url"):
                player.profile_photo_url = result["photo_url"]
            if player.contract_status in (None, "unknown"):
                cs = _infer_contract_status(result.get("contract_expiry"))
                if cs:
                    player.contract_status = cs

            if _count_filled(player) > fields_before:
                updated += 1

            # Stage enrichment data (profile + nested stats/transfers/mv)
            stage_records(
                source="transfermarkt",
                entity_type="enrichment",
                records=[{
                    "id": f"tm_enrich_{player.id}",
                    "player_id": player.id,
                    "player_name": player.full_name,
                    **result,
                }],
            )

            # Stage stats separately for better tracking
            if result.get("stats"):
                stats_staged += stage_records(
                    source="transfermarkt",
                    entity_type="player_stats",
                    records=[{
                        "id": f"tm_stats_enrich_{player.id}",
                        "tm_player_id": result.get("tm_player_id"),
                        "player_name": player.full_name,
                        "club_name": result.get("current_club_name", ""),
                        "date_of_birth": result.get("date_of_birth"),
                        "stats": result["stats"],
                    }],
                )

            # Stage transfers separately
            if result.get("transfers"):
                transfers_staged += stage_records(
                    source="transfermarkt",
                    entity_type="player_transfers",
                    records=[{
                        "id": f"tm_transfers_enrich_{player.id}",
                        "tm_player_id": result.get("tm_player_id"),
                        "player_name": player.full_name,
                        "club_name": result.get("current_club_name", ""),
                        "date_of_birth": result.get("date_of_birth"),
                        "transfers": result["transfers"],
                    }],
                )

            # Stage market value history
            if result.get("market_value_history"):
                mv_staged += stage_records(
                    source="transfermarkt",
                    entity_type="market_value",
                    records=[{
                        "id": f"tm_mv_enrich_{player.id}",
                        "tm_player_id": result.get("tm_player_id"),
                        "player_name": player.full_name,
                        "values": result["market_value_history"],
                    }],
                )

            if i % 20 == 0:
                session.commit()
                logger.info("  Enrichment progress: %d / %d", i, len(players))

        _complete_run(
            session, run,
            records_fetched=searched,
            records_loaded=updated + stats_staged + transfers_staged + mv_staged,
            records_errored=errors,
        )

    logger.info("Running Transfermarkt transform on staged records…")
    transform_results = transform_transfermarkt()

    return {
        "searched": searched,
        "matched": matched,
        "updated": updated,
        "stats_staged": stats_staged,
        "transfers_staged": transfers_staged,
        "mv_staged": mv_staged,
        "errors": errors,
        "transform": transform_results,
    }


def _count_filled(player: Player) -> int:
    fields = [
        player.height_cm, player.date_of_birth, player.preferred_foot,
        player.nationality, player.profile_photo_url,
    ]
    return sum(1 for f in fields if f is not None)


# ══════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Transfermarkt data pull for English non-league football",
    )
    parser.add_argument(
        "--comp",
        type=str,
        default=None,
        help="Scrape a single competition ID (e.g. CNAT, NLN6, NLS6)",
    )
    parser.add_argument(
        "--enrich",
        action="store_true",
        help="Run enrichment mode — search for players missing data",
    )
    parser.add_argument(
        "--enrich-limit",
        type=int,
        default=None,
        help="Max players to search in enrichment mode",
    )
    parser.add_argument(
        "--step",
        type=int,
        default=None,
        help="Only enrich players at this pyramid step",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="In competition mode, also scrape per-player stats + transfers",
    )
    args = parser.parse_args()

    started = time.monotonic()

    if args.enrich:
        summary = run_enrichment(limit=args.enrich_limit, step_filter=args.step)
        elapsed = time.monotonic() - started
        tx = summary.get("transform", {})

        print()
        print("=" * 60)
        print("  Transfermarkt Enrichment Summary")
        print("=" * 60)
        print(f"  Players searched:     {summary['searched']}")
        print(f"  Matches found:        {summary['matched']}")
        print(f"  Profiles updated:     {summary['updated']}")
        print(f"  Stats staged:         {summary['stats_staged']}")
        print(f"  Transfers staged:     {summary['transfers_staged']}")
        print(f"  Market values staged: {summary['mv_staged']}")
        print(f"  Errors:               {summary['errors']}")
        print()
        if tx:
            print("  Transform:")
            print(f"    Processed:          {tx.get('processed', 0)}")
            print(f"    Stats records:      {tx.get('stats', 0)}")
            print(f"    Career entries:     {tx.get('careers', 0)}")
            print(f"    Errors:             {tx.get('errors', 0)}")
            print()
        print(f"  Elapsed:              {elapsed:.0f}s ({elapsed / 60:.1f} min)")
        print("=" * 60)
    else:
        if args.comp and args.comp not in NONLEAGUE_COMPETITIONS:
            print(f"Unknown competition ID: {args.comp}")
            print(f"Valid IDs: {', '.join(NONLEAGUE_COMPETITIONS)}")
            sys.exit(1)

        summary = run_competitions(comp_filter=args.comp, fetch_stats=args.stats)
        elapsed = time.monotonic() - started
        tx = summary["transform"]

        print()
        print("=" * 60)
        print("  Transfermarkt Pipeline Summary")
        print("=" * 60)
        print(f"  Clubs scraped:        {summary['clubs_scraped']}")
        print(f"  Players scraped:      {summary['players_scraped']}")
        print(f"  Records staged:       {summary['records_staged']}")
        if args.stats:
            print(f"  Stats staged:         {summary['stats_staged']}")
            print(f"  Transfers staged:     {summary['transfers_staged']}")
            print(f"  Market values staged: {summary['mv_staged']}")
        print()
        print("  Transform:")
        print(f"    Processed:          {tx['processed']}")
        print(f"    New players:        {tx['created']}")
        print(f"    Players updated:    {tx['updated']}")
        print(f"    Stats records:      {tx['stats']}")
        print(f"    Career entries:     {tx['careers']}")
        print(f"    Errors:             {tx['errors']}")
        print()
        print(f"  Elapsed:              {elapsed:.0f}s ({elapsed / 60:.1f} min)")
        print("=" * 60)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    main()
