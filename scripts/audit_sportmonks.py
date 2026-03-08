#!/usr/bin/env python
"""Audit Sportmonks coverage for English non-league football.

Checks three things for each discovered competition:

1. Can we get a list of teams (i.e. does the league exist in
   Sportmonks at all)?
2. Can we get squad data (player rosters) for at least one team?
3. Can we get player-level statistics for at least one player?

The output is a coverage table that tells you whether a Sportmonks
subscription is worth it for Steps 3-4 data or whether scraping is
the better option.

Usage::

    python scripts/audit_sportmonks.py
"""

import logging
import sys
import time
from typing import Any

from src.config import SPORTMONKS_API_TOKEN

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger(__name__)


def main() -> None:
    if not SPORTMONKS_API_TOKEN:
        print(
            "\n  SPORTMONKS_API_TOKEN not set.\n"
            "  Set it in .env to run this audit.\n"
        )
        sys.exit(1)

    from src.api_clients.sportmonks import SportmonksClient

    client = SportmonksClient()

    # ── 1. Discover English non-league competitions ──────────────────
    print()
    print("=" * 80)
    print("  SPORTMONKS COVERAGE AUDIT — English Non-League")
    print("=" * 80)

    eng_id = client.england_country_id
    print(f"\n  England country_id: {eng_id}")
    print("  Discovering competitions…")

    comps = client.discover_english_nonleague()

    if not comps:
        print("\n  No English non-league competitions found on Sportmonks.")
        print("  This likely means Sportmonks does not cover these leagues.")
        sys.exit(0)

    print(f"  Found {len(comps)} competition(s)\n")

    # ── 2. Audit each competition ────────────────────────────────────

    results: list[dict[str, Any]] = []

    for comp_name, info in sorted(comps.items()):
        league_id = info.get("league_id")
        season_id = info.get("current_season_id")

        log.info("Auditing: %s (league=%s, season=%s)", comp_name, league_id, season_id)

        entry: dict[str, Any] = {
            "name": comp_name,
            "league_id": league_id,
            "season_id": season_id,
            "has_teams": False,
            "team_count": 0,
            "has_squads": False,
            "squad_size": 0,
            "has_stats": False,
            "sample_team": None,
            "sample_player": None,
        }

        if not season_id:
            log.warning("  No current season for %s — skipping", comp_name)
            results.append(entry)
            continue

        # ── 2a. Check teams ──────────────────────────────────────────
        teams = client.get_teams(season_id)
        entry["has_teams"] = len(teams) > 0
        entry["team_count"] = len(teams)

        if not teams:
            log.info("  No teams returned for %s", comp_name)
            results.append(entry)
            continue

        # Pick the first team for deeper inspection
        sample_team = teams[0]
        team_id = sample_team.get("id")
        team_name = sample_team.get("name", "?")
        entry["sample_team"] = team_name
        log.info("  %d teams found — sampling: %s (id=%s)", len(teams), team_name, team_id)

        # ── 2b. Check squad ──────────────────────────────────────────
        squad = client.get_squad(
            team_id,
            season_id=season_id,
            includes=["player", "details"],
        )
        entry["has_squads"] = len(squad) > 0
        entry["squad_size"] = len(squad)

        if not squad:
            log.info("  No squad data for %s", team_name)
            results.append(entry)
            continue

        log.info("  Squad has %d players", len(squad))

        # ── 2c. Check player stats ───────────────────────────────────
        sample_member = squad[0]
        player_id = (
            sample_member.get("player_id")
            or sample_member.get("id")
        )

        if player_id:
            player_data = client.get_player(
                player_id,
                includes=["statistics.details"],
            )
            stats = player_data.get("statistics", [])
            has_stats = bool(stats)
            entry["has_stats"] = has_stats
            entry["sample_player"] = player_data.get("display_name") or player_data.get("common_name")

            if has_stats:
                log.info(
                    "  Player %s has %d stat record(s)",
                    entry["sample_player"], len(stats),
                )
            else:
                log.info("  Player %s — no statistics found", entry["sample_player"])

        results.append(entry)

    # ── 3. Print coverage report ─────────────────────────────────────

    print()
    print("=" * 80)
    print("  COVERAGE REPORT")
    print("=" * 80)
    print()
    print(
        f"  {'Competition':<45s}  {'Teams':>5s}  "
        f"{'Squads':>6s}  {'Stats':>5s}  Sample"
    )
    print(f"  {'':─<45s}  {'':─>5s}  {'':─>6s}  {'':─>5s}  {'':─<25s}")

    squads_yes = 0
    stats_yes = 0

    for r in results:
        teams_str = str(r["team_count"]) if r["has_teams"] else "—"
        squads_str = f"{r['squad_size']}p" if r["has_squads"] else "—"
        stats_str = "YES" if r["has_stats"] else "—"
        sample = r.get("sample_team", "") or ""

        if r["has_squads"]:
            squads_yes += 1
        if r["has_stats"]:
            stats_yes += 1

        print(
            f"  {r['name']:<45s}  {teams_str:>5s}  "
            f"{squads_str:>6s}  {stats_str:>5s}  {sample}"
        )

    total = len(results)
    print()
    print(f"  {'-' * 75}")
    print(f"  Total competitions:  {total}")
    print(f"  With squad data:     {squads_yes} / {total}")
    print(f"  With player stats:   {stats_yes} / {total}")
    print()

    # ── 4. Recommendation ────────────────────────────────────────────

    if stats_yes >= 3:
        verdict = "WORTH IT — Sportmonks has usable player stats for multiple leagues."
    elif squads_yes >= 3:
        verdict = "PARTIAL — squad rosters available but limited stats. May complement scraping."
    elif squads_yes >= 1:
        verdict = "MINIMAL — some squad data exists but coverage is thin. Scraping is safer."
    else:
        verdict = "NOT WORTH IT — no meaningful non-league data. Rely on scraping instead."

    print(f"  VERDICT: {verdict}")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(1)
    except Exception:
        log.exception("Audit failed")
        sys.exit(2)
