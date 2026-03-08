"""Backfill missing clubs from FA Full-Time staging errors.

Reads all unique club names from ``staging_raw`` records where
``source='fa_fulltime'`` and the error message starts with
'No club match'. For each:

1. Attempt matching with the improved suffix-stripping logic.
2. If still unmatched, create a new ``Club`` row using the
   ``fa_fulltime_league_id`` in the staging data to find the
   best-guess ``league_id``.

After running this script, reset errored records so the transform
picks them up again::

    python scripts/backfill_clubs_from_staging.py

Usage::

    python scripts/backfill_clubs_from_staging.py          # run backfill
    python scripts/backfill_clubs_from_staging.py --dry-run # preview only
"""

import argparse
import logging
import re
from collections import Counter
from typing import Any

from rapidfuzz import fuzz, process
from sqlalchemy import distinct, func, select, update

from src.db.models import Club, League, StagingRaw
from src.db.session import get_session
from src.etl.fa_fulltime_transform import (
    CLUB_FUZZY_THRESHOLD,
    strip_club_suffix,
)
from src.scrapers.fa_fulltime import KNOWN_LEAGUE_IDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# Invert KNOWN_LEAGUE_IDS: {fa_fulltime_id: league_display_name}
_FA_ID_TO_NAME: dict[str, str] = {v: k for k, v in KNOWN_LEAGUE_IDS.items()}


def _extract_club_name(error_msg: str) -> str | None:
    """Pull the club name out of "No club match for 'Foo Bar FC'"."""
    m = re.search(r"No club match for '(.+)'", error_msg)
    return m.group(1) if m else None


def _build_league_map(session: Any) -> dict[str, int]:
    """Map FA Full-Time league IDs to our ``leagues.id``.

    Uses fuzzy matching between the ``KNOWN_LEAGUE_IDS`` display names
    and our ``leagues.name`` column.
    """
    leagues = session.execute(select(League)).scalars().all()
    league_names = [lg.name for lg in leagues]
    league_by_name: dict[str, League] = {lg.name: lg for lg in leagues}

    fa_id_to_league_id: dict[str, int] = {}

    for fa_id, display_name in _FA_ID_TO_NAME.items():
        # Exact match first
        if display_name in league_by_name:
            fa_id_to_league_id[fa_id] = league_by_name[display_name].id
            continue

        result = process.extractOne(
            display_name, league_names,
            scorer=fuzz.WRatio,
            score_cutoff=70,
        )
        if result:
            matched, score, _ = result
            fa_id_to_league_id[fa_id] = league_by_name[matched].id
            logger.info(
                "League map: FA '%s' -> '%s' (score=%.0f)",
                display_name, matched, score,
            )

    return fa_id_to_league_id


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill clubs from FA Full-Time staging errors",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be created without writing to the database",
    )
    args = parser.parse_args()

    with get_session() as session:

        # ── 1. Gather unique errored club names ──────────────────────
        error_rows = session.execute(
            select(
                StagingRaw.raw_data,
                StagingRaw.error_message,
            )
            .where(
                StagingRaw.source == "fa_fulltime",
                StagingRaw.source_entity_type == "player",
                StagingRaw.error_message.like("No club match%"),
            )
        ).all()

        if not error_rows:
            print("\nNo club-match errors found in staging_raw. Nothing to do.")
            return

        # Deduplicate by club name, track the fa_fulltime_league_id
        club_info: dict[str, set[str]] = {}  # {club_name: {fa_league_id, ...}}
        error_counts: Counter[str] = Counter()

        for row in error_rows:
            name = _extract_club_name(row.error_message)
            if not name:
                continue
            error_counts[name] += 1
            fa_lid = (row.raw_data or {}).get("fa_fulltime_league_id", "")
            club_info.setdefault(name, set()).add(str(fa_lid))

        print(f"\n  Found {len(club_info)} unique unmatched club names "
              f"across {sum(error_counts.values()):,d} staging records.\n")

        # ── 2. Load existing clubs for matching ──────────────────────
        existing_clubs = session.execute(
            select(Club).where(Club.is_active.is_(True))
        ).scalars().all()

        exact_lookup: dict[str, Club] = {}
        for c in existing_clubs:
            exact_lookup[c.name.lower()] = c
            exact_lookup[strip_club_suffix(c.name).lower()] = c

        stripped_names = [strip_club_suffix(c.name).lower() for c in existing_clubs]

        # ── 3. Build FA league ID -> our league_id map ───────────────
        fa_league_map = _build_league_map(session)
        logger.info("Mapped %d FA Full-Time league IDs to our leagues", len(fa_league_map))

        # ── 4. Attempt matching / create ─────────────────────────────
        matched = 0
        created = 0
        still_unmatched: list[tuple[str, int]] = []

        for club_name in sorted(club_info.keys()):
            count = error_counts[club_name]
            fa_lids = club_info[club_name]

            # Try exact (with suffix stripping)
            key = club_name.lower().strip()
            club = exact_lookup.get(key)
            if not club:
                stripped_key = strip_club_suffix(club_name).lower()
                club = exact_lookup.get(stripped_key)

            # Try fuzzy
            if not club and stripped_names:
                stripped_key = strip_club_suffix(club_name).lower()
                result = process.extractOne(
                    stripped_key,
                    stripped_names,
                    scorer=fuzz.WRatio,
                    score_cutoff=CLUB_FUZZY_THRESHOLD,
                )
                if result:
                    matched_stripped, score, idx = result
                    club = existing_clubs[idx]
                    logger.info(
                        "  Matched: '%s' -> '%s' (score=%.0f, %d records)",
                        club_name, club.name, score, count,
                    )

            if club:
                matched += 1
                continue

            # Still unmatched — determine league_id for the new club
            league_id = None
            for fa_lid in fa_lids:
                if fa_lid in fa_league_map:
                    league_id = fa_league_map[fa_lid]
                    break

            if args.dry_run:
                league_label = f"league_id={league_id}" if league_id else "league_id=?"
                print(f"  [DRY RUN] Would create: {club_name:<45s}  "
                      f"{league_label}  ({count} records)")
                created += 1
                continue

            # Create new club
            short = club_name[:50] if len(club_name) > 50 else None
            new_club = Club(
                name=club_name,
                short_name=short,
                league_id=league_id,
                is_active=True,
            )
            session.add(new_club)
            session.flush()

            # Add to lookup for subsequent iterations
            exact_lookup[club_name.lower()] = new_club
            exact_lookup[strip_club_suffix(club_name).lower()] = new_club
            existing_clubs.append(new_club)
            stripped_names.append(strip_club_suffix(club_name).lower())

            logger.info(
                "  Created: %s (league_id=%s, %d records)",
                club_name, league_id, count,
            )
            created += 1

        # ── 5. Reset errored records for re-processing ───────────────
        if not args.dry_run and created > 0:
            reset_count = session.execute(
                update(StagingRaw)
                .where(
                    StagingRaw.source == "fa_fulltime",
                    StagingRaw.source_entity_type == "player",
                    StagingRaw.error_message.like("No club match%"),
                )
                .values(
                    processed=False,
                    processed_at=None,
                    error_message=None,
                )
            ).rowcount
            logger.info("Reset %d staging records for re-processing", reset_count)
        else:
            reset_count = 0

        # ── 6. Summary ───────────────────────────────────────────────
        print()
        print("=" * 60)
        print("  Club Backfill Summary")
        print("=" * 60)
        print(f"  Unique unmatched club names:   {len(club_info)}")
        print(f"  Now matched (improved logic):  {matched}")
        print(f"  New clubs created:             {created}")
        if not args.dry_run:
            print(f"  Staging records reset:         {reset_count}")
        else:
            print(f"  Mode:                          DRY RUN (no writes)")
        print("=" * 60)

        if not args.dry_run and (matched > 0 or created > 0):
            print("\n  Next step: re-run the transform:")
            print("    python -m src.etl.fa_fulltime_transform\n")


if __name__ == "__main__":
    main()
