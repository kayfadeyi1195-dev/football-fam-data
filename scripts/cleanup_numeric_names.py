#!/usr/bin/env python3
"""Find and fix players whose full_name is a jersey number (purely numeric).

The FA Full-Time scraper sometimes stores jersey numbers as player
names when the HTML table structure varies.  This script:

1. Finds all ``Player`` rows where ``full_name`` is purely digits.
2. For each, checks ``staging_raw`` for the original scraped data to
   see if a real name can be recovered from an adjacent field.
3. If a real name is found, updates the player record.
4. If not, deletes the player and all dependent rows
   (player_seasons, player_career, match_appearances, player_media,
   shortlist_players).

Usage::

    python scripts/cleanup_numeric_names.py              # dry-run (report only)
    python scripts/cleanup_numeric_names.py --apply      # actually fix/delete
"""

import argparse
import logging
import re

from sqlalchemy import delete, select, update

from src.db.models import (
    MatchAppearance,
    MergeCandidate,
    Player,
    PlayerCareer,
    PlayerMedia,
    PlayerSeason,
    ShortlistPlayer,
    StagingRaw,
)
from src.db.session import get_session

logger = logging.getLogger(__name__)

_NUMERIC_RE = re.compile(r"^\d+$")


def _try_recover_name(raw_data: dict) -> str | None:
    """Attempt to extract a real player name from staged raw data.

    FA Full-Time staging records store the scraped fields as-is.
    We look through several plausible keys for a non-numeric string
    that could be the actual player name.
    """
    candidates = []
    for key in ("player_name", "name", "full_name", "Player Name"):
        val = raw_data.get(key)
        if val and isinstance(val, str) and not _NUMERIC_RE.match(val):
            candidates.append(val.strip())

    # Some records embed the club + player in a combined string
    for key in ("club_name", "team_name"):
        val = raw_data.get(key)
        if val and isinstance(val, str) and not _NUMERIC_RE.match(val):
            # Not a name, but useful for logging
            pass

    return candidates[0] if candidates else None


def run(*, apply: bool = False) -> None:
    numeric_players: list[tuple[int, str]] = []

    with get_session() as session:
        with session.no_autoflush:
            # ── Find all players with purely numeric full_name ────
            players = session.execute(
                select(Player).where(Player.full_name.regexp_match(r"^\d+$"))
            ).scalars().all()

        if not players:
            print("No players with purely numeric names found.")
            return

        print(f"\nFound {len(players)} player(s) with numeric names:\n")

        recovered = 0
        deleted = 0
        unmerged = 0
        merge_candidates_removed = 0
        unfixable: list[tuple[int, str, str | None]] = []

        for player in players:
            club_name = ""
            if player.current_club_id:
                from src.db.models import Club
                club = session.get(Club, player.current_club_id)
                club_name = club.name if club else ""

            # ── Search staging_raw for the original record ────────
            with session.no_autoflush:
                staging_rows = session.execute(
                    select(StagingRaw)
                    .where(StagingRaw.source == "fa_fulltime")
                    .where(StagingRaw.source_entity_type == "player")
                    .where(StagingRaw.raw_data["player_name"].as_string() == player.full_name)
                ).scalars().all()

                # Also try external_id patterns that embed the numeric name
                if not staging_rows:
                    staging_rows = session.execute(
                        select(StagingRaw)
                        .where(StagingRaw.source == "fa_fulltime")
                        .where(StagingRaw.source_entity_type == "player")
                        .where(StagingRaw.external_id.contains(f"_{player.full_name}"))
                    ).scalars().all()

            real_name = None
            for sr in staging_rows:
                raw = sr.raw_data or {}
                real_name = _try_recover_name(raw)
                if real_name:
                    break

            if real_name:
                print(
                    f"  [FIX]    id={player.id:>6d}  "
                    f"'{player.full_name}' → '{real_name}'  "
                    f"(club: {club_name})"
                )
                if apply:
                    player.full_name = real_name
                    parts = real_name.split(None, 1)
                    if len(parts) == 2:
                        if not player.first_name:
                            player.first_name = parts[0]
                        if not player.last_name:
                            player.last_name = parts[1]
                recovered += 1
            else:
                # Count dependent records for the report
                with session.no_autoflush:
                    season_count = session.execute(
                        select(PlayerSeason)
                        .where(PlayerSeason.player_id == player.id)
                    ).scalars().all()
                    career_count = session.execute(
                        select(PlayerCareer)
                        .where(PlayerCareer.player_id == player.id)
                    ).scalars().all()

                    # Check for players merged into this one
                    merged_refs = session.execute(
                        select(Player.id)
                        .where(Player.merged_into_id == player.id)
                    ).scalars().all()

                    # Check for merge_candidates referencing this player
                    mc_refs = session.execute(
                        select(MergeCandidate.id).where(
                            (MergeCandidate.player_a_id == player.id)
                            | (MergeCandidate.player_b_id == player.id)
                        )
                    ).scalars().all()

                extra = ""
                if merged_refs:
                    extra += f"  merged_refs={len(merged_refs)}"
                if mc_refs:
                    extra += f"  merge_candidates={len(mc_refs)}"

                print(
                    f"  [DELETE] id={player.id:>6d}  "
                    f"name='{player.full_name}'  "
                    f"club={club_name}  "
                    f"seasons={len(season_count)}  "
                    f"careers={len(career_count)}{extra}"
                )
                unfixable.append((player.id, player.full_name, club_name))

                if apply:
                    # 1. Unmerge: clear merged_into_id on players pointing here
                    if merged_refs:
                        session.execute(
                            update(Player)
                            .where(Player.merged_into_id == player.id)
                            .values(merged_into_id=None)
                        )
                        unmerged += len(merged_refs)

                    # 2. Remove merge_candidates referencing this player
                    if mc_refs:
                        session.execute(
                            delete(MergeCandidate).where(
                                (MergeCandidate.player_a_id == player.id)
                                | (MergeCandidate.player_b_id == player.id)
                            )
                        )
                        merge_candidates_removed += len(mc_refs)

                    # 3. Delete dependent rows (FK order)
                    session.execute(
                        delete(ShortlistPlayer)
                        .where(ShortlistPlayer.player_id == player.id)
                    )
                    session.execute(
                        delete(MatchAppearance)
                        .where(MatchAppearance.player_id == player.id)
                    )
                    session.execute(
                        delete(PlayerMedia)
                        .where(PlayerMedia.player_id == player.id)
                    )
                    session.execute(
                        delete(PlayerSeason)
                        .where(PlayerSeason.player_id == player.id)
                    )
                    session.execute(
                        delete(PlayerCareer)
                        .where(PlayerCareer.player_id == player.id)
                    )

                    # 4. Delete the player itself
                    session.execute(
                        delete(Player).where(Player.id == player.id)
                    )
                deleted += 1

        # ── Also clean up staging_raw records with numeric names ──
        numeric_staging = session.execute(
            select(StagingRaw)
            .where(StagingRaw.source == "fa_fulltime")
            .where(StagingRaw.source_entity_type == "player")
            .where(StagingRaw.raw_data["player_name"].as_string().regexp_match(r"^\d+$"))
        ).scalars().all()

        staging_fixed = 0
        if numeric_staging:
            print(f"\n  Found {len(numeric_staging)} staging_raw records with numeric player names")
            if apply:
                for sr in numeric_staging:
                    sr.processed = False
                    sr.error_message = "numeric_name_detected"
                    staging_fixed += 1

        if apply:
            session.commit()

        # ── Summary ──────────────────────────────────────────────
        print()
        print("=" * 60)
        print("  Numeric Name Cleanup Summary")
        print("=" * 60)
        print(f"  Total numeric-name players found:  {len(players)}")
        print(f"  Recoverable (name found in raw):   {recovered}")
        print(f"  Unfixable (will be deleted):        {deleted}")
        if unmerged:
            print(f"  Players unmerged (merged_into_id):  {unmerged}")
        if merge_candidates_removed:
            print(f"  Merge candidates removed:           {merge_candidates_removed}")
        if numeric_staging:
            print(f"  Staging records flagged:            {len(numeric_staging)}")
        print()
        if apply:
            print(f"  Changes APPLIED: {recovered} fixed, {deleted} deleted, "
                  f"{unmerged} unmerged, {staging_fixed} staging records flagged")
        else:
            print("  DRY RUN — no changes made. Re-run with --apply to commit.")
        print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find and fix players with purely numeric names (jersey numbers)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually apply fixes/deletions (default is dry-run)",
    )
    args = parser.parse_args()

    run(apply=args.apply)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    main()
