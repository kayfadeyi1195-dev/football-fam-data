#!/usr/bin/env python
"""Backfill clubs that have zero player data.

For each club with 0 active players the script tries three discovery
strategies in order, stopping as soon as one finds squad data:

1. **Pitchero** — attempt to discover the club on Pitchero and scrape
   its squad page.
2. **Club website** — if the club has a ``website_url``, look for a
   squad page on it.
3. **FA Full-Time** — if the club belongs to a league with a known
   FA Full-Time ID, scrape that league for player data.

Steps 3-4 are prioritised because they're the most likely to have
discoverable data. Steps 5-6 come next. Steps 1-2 are usually
covered by API sources already.

Usage::

    python scripts/backfill_gaps.py                 # full backfill
    python scripts/backfill_gaps.py --step 4        # only Step 4 clubs
    python scripts/backfill_gaps.py --dry-run       # preview without scraping
    python scripts/backfill_gaps.py --limit 20      # cap at 20 clubs
"""

import argparse
import logging
import sys
import time
from typing import Any

from sqlalchemy import and_, func, select

from src.db.models import Club, League, Player
from src.db.session import get_session
from src.etl.staging import stage_records

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════
# Find empty clubs
# ══════════════════════════════════════════════════════════════════════════

def _find_empty_clubs(
    step_filter: int | None = None,
) -> list[dict[str, Any]]:
    """Return clubs with 0 active players, ordered by priority.

    Priority: Step 3 first, then 4, then 5, then 6, then 1-2.
    """
    active_filter = Player.merged_into_id.is_(None)

    with get_session() as session:
        rows = session.execute(
            select(
                Club.id,
                Club.name,
                Club.website_url,
                Club.pitchero_url,
                League.id.label("league_id"),
                League.name.label("league_name"),
                League.step,
            )
            .join(League, Club.league_id == League.id)
            .outerjoin(
                Player,
                and_(
                    Player.current_club_id == Club.id,
                    active_filter,
                ),
            )
            .where(Club.is_active.is_(True))
            .group_by(
                Club.id, Club.name, Club.website_url, Club.pitchero_url,
                League.id, League.name, League.step,
            )
            .having(func.count(Player.id) == 0)
            .order_by(League.step, League.name, Club.name)
        ).all()

    clubs = [
        {
            "club_id": r.id,
            "club_name": r.name,
            "website_url": r.website_url,
            "pitchero_url": r.pitchero_url,
            "league_id": r.league_id,
            "league_name": r.league_name,
            "step": r.step,
        }
        for r in rows
        if step_filter is None or r.step == step_filter
    ]

    # Sort by priority: steps 3,4 first (most data-rich), then 5,6, then 1,2
    priority = {3: 0, 4: 1, 5: 2, 6: 3, 1: 4, 2: 5}
    clubs.sort(key=lambda c: (priority.get(c["step"], 9), c["club_name"]))

    return clubs


# ══════════════════════════════════════════════════════════════════════════
# Discovery strategies
# ══════════════════════════════════════════════════════════════════════════

def _try_pitchero(club: dict[str, Any]) -> dict[str, Any]:
    """Attempt to discover and scrape the club on Pitchero."""
    from src.scrapers.pitchero import PitcheroScraper

    scraper = PitcheroScraper()
    pitchero_url = club["pitchero_url"]

    if not pitchero_url:
        discovered = scraper.discover_pitchero_url(club["club_name"])
        if not discovered:
            return {"found": False, "reason": "no Pitchero URL discovered"}
        pitchero_url = discovered

        with get_session() as session:
            db_club = session.get(Club, club["club_id"])
            if db_club:
                db_club.pitchero_url = pitchero_url

        log.info("Discovered Pitchero URL for %s: %s", club["club_name"], pitchero_url)

    try:
        data = scraper.scrape_club(pitchero_url, include_profiles=False)
        players = data.get("players", [])
        if not players:
            return {"found": False, "reason": "squad page empty"}

        records = []
        for p in players:
            ext_id = f"pitchero_{club['club_id']}_{p.get('name', 'unknown')}"
            records.append({
                "id": ext_id,
                "club_id": club["club_id"],
                "club_name": club["club_name"],
                **p,
            })
        staged = stage_records("pitchero", "player", records, id_field="id")
        return {"found": True, "players": len(players), "staged": staged}

    except Exception as e:
        return {"found": False, "reason": str(e)[:120]}


def _try_club_website(club: dict[str, Any]) -> dict[str, Any]:
    """Attempt to scrape squad from the club's own website."""
    from src.scrapers.club_websites import discover_squad_links, scrape_squad_page

    url = club["website_url"]
    if not url:
        return {"found": False, "reason": "no website_url"}

    try:
        squad_links = discover_squad_links(url)
        if not squad_links:
            return {"found": False, "reason": "no squad links found on site"}

        all_players: list[dict] = []
        for link in squad_links[:3]:
            players = scrape_squad_page(link)
            all_players.extend(players)

        if not all_players:
            return {"found": False, "reason": "squad pages empty"}

        records = []
        for p in all_players:
            ext_id = f"cw_{club['club_id']}_{p.get('name', 'unknown')}"
            records.append({
                "id": ext_id,
                "club_id": club["club_id"],
                "club_name": club["club_name"],
                **p,
            })
        staged = stage_records("club_website", "player", records, id_field="id")
        return {"found": True, "players": len(all_players), "staged": staged}

    except Exception as e:
        return {"found": False, "reason": str(e)[:120]}


def _try_fa_fulltime(club: dict[str, Any]) -> dict[str, Any]:
    """Check FA Full-Time for the club's league and scrape player data."""
    from src.scrapers.fa_fulltime import KNOWN_LEAGUE_IDS, FAFullTimeScraper

    league_name = club["league_name"]
    fa_id = None
    for known_name, lid in KNOWN_LEAGUE_IDS.items():
        if known_name.lower() in league_name.lower() or league_name.lower() in known_name.lower():
            fa_id = lid
            break

    if not fa_id:
        return {"found": False, "reason": f"no FA Full-Time ID for {league_name}"}

    try:
        scraper = FAFullTimeScraper()
        data = scraper.scrape_league(fa_id)

        player_records = data.get("player_records", [])
        club_name_lower = club["club_name"].lower()
        relevant = [
            r for r in player_records
            if club_name_lower in r.get("club", "").lower()
            or r.get("club", "").lower() in club_name_lower
        ]

        if not relevant:
            return {"found": False, "reason": "club not found in FA FT league data"}

        staged = stage_records("fa_fulltime", "player", relevant, id_field="id")
        return {"found": True, "players": len(relevant), "staged": staged}

    except Exception as e:
        return {"found": False, "reason": str(e)[:120]}


# ══════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill clubs with 0 players",
    )
    parser.add_argument("--step", type=int, choices=[1, 2, 3, 4, 5, 6], default=None)
    parser.add_argument("--dry-run", action="store_true", help="List empty clubs without scraping")
    parser.add_argument("--limit", type=int, default=None, help="Max clubs to attempt")
    args = parser.parse_args()

    clubs = _find_empty_clubs(step_filter=args.step)
    if args.limit:
        clubs = clubs[:args.limit]

    print()
    print("=" * 72)
    print("  FOOTBALL FAM — BACKFILL GAPS")
    print(f"  Clubs with 0 players: {len(clubs)}")
    if args.step:
        print(f"  Filtered to Step: {args.step}")
    print("=" * 72)

    if not clubs:
        print("\n  No empty clubs found — nothing to do.")
        return

    # Step distribution
    step_counts: dict[int, int] = {}
    for c in clubs:
        step_counts[c["step"]] = step_counts.get(c["step"], 0) + 1
    print("\n  By step:")
    for step in sorted(step_counts):
        print(f"    Step {step}: {step_counts[step]:,} clubs")

    if args.dry_run:
        print("\n  --dry-run: listing clubs (no scraping)\n")
        for c in clubs:
            flags = []
            if c["pitchero_url"]:
                flags.append("pitchero")
            if c["website_url"]:
                flags.append("website")
            flag_str = f"  [{', '.join(flags)}]" if flags else ""
            print(f"    Step {c['step']}  {c['club_name']:<35s}  {c['league_name']}{flag_str}")
        return

    # ── Run discovery strategies ─────────────────────────────────────

    strategies = [
        ("pitchero", _try_pitchero),
        ("club_website", _try_club_website),
        ("fa_fulltime", _try_fa_fulltime),
    ]

    filled = 0
    still_empty: list[dict] = []
    results_by_source: dict[str, int] = {"pitchero": 0, "club_website": 0, "fa_fulltime": 0}

    for i, club in enumerate(clubs, 1):
        log.info(
            "[%d/%d] %s (Step %d, %s)",
            i, len(clubs), club["club_name"], club["step"], club["league_name"],
        )

        found = False
        for strategy_name, fn in strategies:
            result = fn(club)
            if result["found"]:
                log.info(
                    "  -> %s: found %d players",
                    strategy_name, result.get("players", 0),
                )
                filled += 1
                results_by_source[strategy_name] += 1
                found = True
                break
            else:
                log.debug(
                    "  -> %s: %s", strategy_name, result.get("reason", "no data"),
                )

        if not found:
            still_empty.append(club)

    # ── Summary ──────────────────────────────────────────────────────

    print()
    print("=" * 72)
    print("  BACKFILL SUMMARY")
    print("=" * 72)
    print(f"\n  Clubs attempted:    {len(clubs)}")
    print(f"  Clubs filled:       {filled}")
    print(f"  Still empty:        {len(still_empty)}")
    print(f"\n  Filled by source:")
    for src, cnt in results_by_source.items():
        if cnt:
            print(f"    {src}: {cnt}")

    if still_empty:
        print(f"\n  Remaining gaps ({len(still_empty)} clubs):")
        for c in still_empty[:30]:
            print(f"    Step {c['step']}  {c['club_name']:<35s}  {c['league_name']}")
        if len(still_empty) > 30:
            print(f"    … and {len(still_empty) - 30} more")

    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(1)
    except Exception:
        log.exception("Backfill failed")
        sys.exit(2)
