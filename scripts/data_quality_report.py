#!/usr/bin/env python
"""Generate a comprehensive data quality report for the Football Fam database.

Produces five sections:

1. **Coverage Summary** — leagues, clubs, and players vs expected totals.
2. **Data Quality Metrics** — confidence, field completeness, cross-referencing.
3. **Data Source Breakdown** — records per source, last run, error rates.
4. **Freshness** — how recently records were updated.
5. **Top Gaps** — actionable items to focus scraping efforts.

Output is printed to the console *and* saved as JSON to
``data/quality_report_{YYYY-MM-DD}.json`` for historical tracking.

Usage::

    python scripts/data_quality_report.py
"""

import json
import logging
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import and_, case, cast, distinct, func, Integer, select
from sqlalchemy.orm import Session

from src.db.models import (
    Club,
    DataSourceRun,
    League,
    MergeCandidate,
    Player,
    PlayerSeason,
    StagingRaw,
)
from src.db.session import get_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger(__name__)

CURRENT_SEASON = "2025-26"

EXPECTED_LEAGUES: dict[int, int] = {
    1: 1,
    2: 2,
    3: 4,
    4: 8,
    5: 16,
    6: 30,
}
EXPECTED_CLUBS: dict[int, int] = {
    1: 24,
    2: 48,
    3: 88,
    4: 160,
    5: 304,
    6: 540,
}

W = 72  # console column width


# ══════════════════════════════════════════════════════════════════════════
# 1. COVERAGE SUMMARY
# ══════════════════════════════════════════════════════════════════════════

def _coverage_summary(session: Session) -> dict[str, Any]:
    """Leagues, clubs, and players per step vs expectations."""

    # --- leagues per step ---
    league_rows = session.execute(
        select(League.step, func.count(League.id).label("cnt"))
        .group_by(League.step)
        .order_by(League.step)
    ).all()
    leagues_by_step = {r.step: r.cnt for r in league_rows}

    # --- clubs per step ---
    club_rows = session.execute(
        select(League.step, func.count(Club.id).label("cnt"))
        .join(League, Club.league_id == League.id)
        .group_by(League.step)
        .order_by(League.step)
    ).all()
    clubs_by_step = {r.step: r.cnt for r in club_rows}

    # --- active (non-merged) players per step ---
    player_rows = session.execute(
        select(League.step, func.count(Player.id).label("cnt"))
        .join(Club, Player.current_club_id == Club.id)
        .join(League, Club.league_id == League.id)
        .where(Player.merged_into_id.is_(None))
        .group_by(League.step)
        .order_by(League.step)
    ).all()
    players_by_step = {r.step: r.cnt for r in player_rows}

    # --- club coverage: clubs with >=1 player, clubs with >=11 ---
    coverage_rows = session.execute(
        select(
            Club.id,
            League.step,
            func.count(Player.id).label("player_cnt"),
        )
        .join(League, Club.league_id == League.id)
        .outerjoin(
            Player,
            and_(
                Player.current_club_id == Club.id,
                Player.merged_into_id.is_(None),
            ),
        )
        .group_by(Club.id, League.step)
    ).all()

    total_clubs_all = len(coverage_rows)
    clubs_with_any = sum(1 for r in coverage_rows if r.player_cnt > 0)
    clubs_with_squad = sum(1 for r in coverage_rows if r.player_cnt >= 11)

    pct_any = (clubs_with_any / total_clubs_all * 100) if total_clubs_all else 0
    pct_squad = (clubs_with_squad / total_clubs_all * 100) if total_clubs_all else 0

    # per-step club coverage
    step_club_any: dict[int, int] = defaultdict(int)
    step_club_squad: dict[int, int] = defaultdict(int)
    step_club_total: dict[int, int] = defaultdict(int)
    for r in coverage_rows:
        step_club_total[r.step] += 1
        if r.player_cnt > 0:
            step_club_any[r.step] += 1
        if r.player_cnt >= 11:
            step_club_squad[r.step] += 1

    steps_detail: list[dict[str, Any]] = []
    for step in range(1, 7):
        exp_l = EXPECTED_LEAGUES.get(step, 0)
        exp_c = EXPECTED_CLUBS.get(step, 0)
        act_l = leagues_by_step.get(step, 0)
        act_c = clubs_by_step.get(step, 0)
        act_p = players_by_step.get(step, 0)
        tot_c = step_club_total.get(step, 0)
        any_c = step_club_any.get(step, 0)
        sq_c = step_club_squad.get(step, 0)

        steps_detail.append({
            "step": step,
            "leagues_expected": exp_l,
            "leagues_actual": act_l,
            "clubs_expected": exp_c,
            "clubs_actual": act_c,
            "players_active": act_p,
            "clubs_with_any_player": any_c,
            "clubs_with_squad": sq_c,
            "pct_clubs_with_player": round(any_c / tot_c * 100, 1) if tot_c else 0,
            "pct_clubs_with_squad": round(sq_c / tot_c * 100, 1) if tot_c else 0,
        })

    return {
        "steps": steps_detail,
        "totals": {
            "leagues": sum(leagues_by_step.values()),
            "clubs": total_clubs_all,
            "players_active": sum(players_by_step.values()),
            "clubs_with_any_player": clubs_with_any,
            "clubs_with_squad": clubs_with_squad,
            "pct_clubs_with_player": round(pct_any, 1),
            "pct_clubs_with_squad": round(pct_squad, 1),
        },
    }


# ══════════════════════════════════════════════════════════════════════════
# 2. DATA QUALITY METRICS
# ══════════════════════════════════════════════════════════════════════════

def _data_quality(session: Session) -> dict[str, Any]:
    active_filter = Player.merged_into_id.is_(None)

    total_active: int = session.execute(
        select(func.count(Player.id)).where(active_filter)
    ).scalar_one()

    if total_active == 0:
        return {"total_active": 0, "note": "no active players"}

    # --- field completeness ---
    fields = {
        "date_of_birth": Player.date_of_birth,
        "position_primary": Player.position_primary,
        "nationality": Player.nationality,
        "profile_photo_url": Player.profile_photo_url,
        "height_cm": Player.height_cm,
        "current_club_id": Player.current_club_id,
    }
    completeness: dict[str, float] = {}
    for name, col in fields.items():
        cnt: int = session.execute(
            select(func.count(Player.id))
            .where(active_filter, col.isnot(None))
        ).scalar_one()
        completeness[name] = round(cnt / total_active * 100, 1)

    # --- avg confidence by step ---
    conf_rows = session.execute(
        select(
            League.step,
            func.round(func.avg(Player.overall_confidence), 2).label("avg_conf"),
        )
        .join(Club, Player.current_club_id == Club.id)
        .join(League, Club.league_id == League.id)
        .where(active_filter, Player.overall_confidence.isnot(None))
        .group_by(League.step)
        .order_by(League.step)
    ).all()
    avg_conf_by_step = {r.step: float(r.avg_conf) if r.avg_conf else 0 for r in conf_rows}

    # --- avg confidence by data source ---
    src_conf_rows = session.execute(
        select(
            PlayerSeason.data_source,
            func.round(func.avg(cast(PlayerSeason.confidence_score, Integer)), 2).label("avg"),
        )
        .join(Player, PlayerSeason.player_id == Player.id)
        .where(active_filter)
        .group_by(PlayerSeason.data_source)
        .order_by(PlayerSeason.data_source)
    ).all()
    avg_conf_by_source = {
        r.data_source or "unknown": float(r.avg) if r.avg else 0
        for r in src_conf_rows
    }

    # --- % with current-season stats ---
    with_season: int = session.execute(
        select(func.count(distinct(PlayerSeason.player_id)))
        .join(Player, PlayerSeason.player_id == Player.id)
        .where(active_filter, PlayerSeason.season == CURRENT_SEASON)
    ).scalar_one()
    pct_season = round(with_season / total_active * 100, 1)

    # --- % with 2+ data sources (cross-referenced) ---
    multi_src_sub = (
        select(
            PlayerSeason.player_id,
            func.count(distinct(PlayerSeason.data_source)).label("src_cnt"),
        )
        .join(Player, PlayerSeason.player_id == Player.id)
        .where(active_filter)
        .group_by(PlayerSeason.player_id)
        .having(func.count(distinct(PlayerSeason.data_source)) >= 2)
        .subquery()
    )
    multi_src_count: int = session.execute(
        select(func.count()).select_from(multi_src_sub)
    ).scalar_one()
    pct_multi = round(multi_src_count / total_active * 100, 1)

    # --- pending merge candidates ---
    pending_merges: int = session.execute(
        select(func.count(MergeCandidate.id))
        .where(MergeCandidate.status == "pending")
    ).scalar_one()

    return {
        "total_active": total_active,
        "field_completeness_pct": completeness,
        "avg_confidence_by_step": avg_conf_by_step,
        "avg_confidence_by_source": avg_conf_by_source,
        "pct_with_current_season_stats": pct_season,
        "pct_with_multi_sources": pct_multi,
        "multi_source_count": multi_src_count,
        "pending_merge_candidates": pending_merges,
    }


# ══════════════════════════════════════════════════════════════════════════
# 3. DATA SOURCE BREAKDOWN
# ══════════════════════════════════════════════════════════════════════════

def _data_sources(session: Session) -> dict[str, Any]:
    # --- staging records by source ---
    staging_rows = session.execute(
        select(
            StagingRaw.source,
            func.count().label("total"),
            func.count().filter(StagingRaw.processed.is_(True)).label("processed"),
            func.count().filter(StagingRaw.error_message.isnot(None)).label("errored"),
        )
        .group_by(StagingRaw.source)
        .order_by(StagingRaw.source)
    ).all()

    sources: list[dict[str, Any]] = []
    for r in staging_rows:
        error_rate = round(r.errored / r.total * 100, 1) if r.total else 0
        sources.append({
            "source": r.source,
            "total_records": r.total,
            "processed": r.processed,
            "errored": r.errored,
            "error_rate_pct": error_rate,
        })

    # --- player_seasons records by data_source ---
    ps_rows = session.execute(
        select(
            PlayerSeason.data_source,
            func.count().label("cnt"),
        )
        .group_by(PlayerSeason.data_source)
        .order_by(func.count().desc())
    ).all()
    season_records_by_source = {
        r.data_source or "unknown": r.cnt for r in ps_rows
    }

    # --- last run per source ---
    run_rows = session.execute(
        select(
            DataSourceRun.source,
            func.max(DataSourceRun.started_at).label("last_run"),
            DataSourceRun.status,
        )
        .group_by(DataSourceRun.source, DataSourceRun.status)
        .order_by(DataSourceRun.source)
    ).all()

    last_runs: dict[str, dict[str, Any]] = {}
    for r in run_rows:
        src = r.source
        if src not in last_runs:
            last_runs[src] = {}
        last_runs[src][r.status] = r.last_run.isoformat() if r.last_run else None

    return {
        "staging_by_source": sources,
        "season_records_by_source": season_records_by_source,
        "last_runs": last_runs,
    }


# ══════════════════════════════════════════════════════════════════════════
# 4. FRESHNESS
# ══════════════════════════════════════════════════════════════════════════

def _freshness(session: Session) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    d7 = now - timedelta(days=7)
    d30 = now - timedelta(days=30)
    d90 = now - timedelta(days=90)

    active_filter = Player.merged_into_id.is_(None)

    total: int = session.execute(
        select(func.count(Player.id)).where(active_filter)
    ).scalar_one()

    cnt_7: int = session.execute(
        select(func.count(Player.id))
        .where(active_filter, Player.updated_at >= d7)
    ).scalar_one()
    cnt_30: int = session.execute(
        select(func.count(Player.id))
        .where(active_filter, Player.updated_at >= d30)
    ).scalar_one()
    cnt_90: int = session.execute(
        select(func.count(Player.id))
        .where(active_filter, Player.updated_at >= d90)
    ).scalar_one()

    # stalest records by step (oldest updated_at)
    stale_rows = session.execute(
        select(
            League.step,
            func.min(Player.updated_at).label("oldest"),
            func.count(Player.id).filter(Player.updated_at < d90).label("stale_cnt"),
        )
        .join(Club, Player.current_club_id == Club.id)
        .join(League, Club.league_id == League.id)
        .where(active_filter)
        .group_by(League.step)
        .order_by(League.step)
    ).all()

    stalest_by_step = {
        r.step: {
            "oldest_update": r.oldest.isoformat() if r.oldest else None,
            "stale_count_90d": r.stale_cnt,
        }
        for r in stale_rows
    }

    return {
        "total_active": total,
        "updated_last_7d": cnt_7,
        "updated_last_30d": cnt_30,
        "updated_last_90d": cnt_90,
        "stale_90d_plus": total - cnt_90,
        "stalest_by_step": stalest_by_step,
    }


# ══════════════════════════════════════════════════════════════════════════
# 5. TOP GAPS
# ══════════════════════════════════════════════════════════════════════════

def _top_gaps(session: Session) -> dict[str, Any]:
    active_filter = Player.merged_into_id.is_(None)

    # --- clubs with 0 active players ---
    zero_clubs = session.execute(
        select(Club.id, Club.name, League.name.label("league"), League.step)
        .join(League, Club.league_id == League.id)
        .outerjoin(
            Player,
            and_(
                Player.current_club_id == Club.id,
                active_filter,
            ),
        )
        .group_by(Club.id, Club.name, League.name, League.step)
        .having(func.count(Player.id) == 0)
        .order_by(League.step, League.name, Club.name)
    ).all()

    empty_clubs = [
        {"club_id": r.id, "club": r.name, "league": r.league, "step": r.step}
        for r in zero_clubs
    ]

    # --- leagues with <50% club coverage ---
    league_cov_rows = session.execute(
        select(
            League.id,
            League.name,
            League.step,
            func.count(distinct(Club.id)).label("total_clubs"),
            func.count(distinct(Player.current_club_id)).label("clubs_with_players"),
        )
        .outerjoin(Club, Club.league_id == League.id)
        .outerjoin(
            Player,
            and_(
                Player.current_club_id == Club.id,
                active_filter,
            ),
        )
        .group_by(League.id, League.name, League.step)
        .order_by(League.step, League.name)
    ).all()

    low_coverage_leagues = []
    for r in league_cov_rows:
        if r.total_clubs == 0:
            continue
        pct = r.clubs_with_players / r.total_clubs * 100
        if pct < 50:
            low_coverage_leagues.append({
                "league_id": r.id,
                "league": r.name,
                "step": r.step,
                "total_clubs": r.total_clubs,
                "clubs_with_players": r.clubs_with_players,
                "coverage_pct": round(pct, 1),
            })

    # --- steps with lowest avg confidence ---
    step_conf_rows = session.execute(
        select(
            League.step,
            func.round(func.avg(Player.overall_confidence), 2).label("avg"),
        )
        .join(Club, Player.current_club_id == Club.id)
        .join(League, Club.league_id == League.id)
        .where(active_filter, Player.overall_confidence.isnot(None))
        .group_by(League.step)
        .order_by(func.avg(Player.overall_confidence))
    ).all()

    lowest_conf_steps = [
        {"step": r.step, "avg_confidence": float(r.avg) if r.avg else 0}
        for r in step_conf_rows
    ]

    return {
        "clubs_with_zero_players": empty_clubs,
        "clubs_with_zero_count": len(empty_clubs),
        "leagues_below_50pct_coverage": low_coverage_leagues,
        "steps_by_lowest_confidence": lowest_conf_steps,
    }


# ══════════════════════════════════════════════════════════════════════════
# Console output helpers
# ══════════════════════════════════════════════════════════════════════════

def _hline(char: str = "─") -> str:
    return char * W


def _heading(title: str) -> None:
    print()
    print(f"╔{'═' * (W - 2)}╗")
    print(f"║  {title:<{W - 4}}║")
    print(f"╚{'═' * (W - 2)}╝")


def _print_coverage(data: dict[str, Any]) -> None:
    _heading("1 · COVERAGE SUMMARY")
    print()
    print(f"  {'Step':<6} {'Leagues':>10} {'Clubs':>12} {'Players':>10}  {'≥1 plyr':>8} {'≥11 plyr':>9}")
    print(f"  {'':─<6} {'(act/exp)':─>10} {'(act/exp)':─>12} {'':─>10}  {'':─>8} {'':─>9}")
    for s in data["steps"]:
        print(
            f"  Step {s['step']}  "
            f"{s['leagues_actual']:>3}/{s['leagues_expected']:<3}  "
            f"{s['clubs_actual']:>4}/{s['clubs_expected']:<4}   "
            f"{s['players_active']:>7,}  "
            f"{s['pct_clubs_with_player']:>6.1f}%  "
            f"{s['pct_clubs_with_squad']:>6.1f}%"
        )
    t = data["totals"]
    print(f"  {_hline()}")
    print(
        f"  TOTAL   {t['leagues']:>6,}     {t['clubs']:>6,}   "
        f"{t['players_active']:>7,}  "
        f"{t['pct_clubs_with_player']:>6.1f}%  "
        f"{t['pct_clubs_with_squad']:>6.1f}%"
    )


def _print_quality(data: dict[str, Any]) -> None:
    _heading("2 · DATA QUALITY METRICS")
    total = data["total_active"]
    print(f"\n  Active players: {total:,}")

    print("\n  Field completeness:")
    for field, pct in data["field_completeness_pct"].items():
        bar = "█" * int(pct / 5)
        print(f"    {field:<25s} {pct:>5.1f}%  {bar}")

    print("\n  Avg confidence by step:")
    for step, avg in sorted(data["avg_confidence_by_step"].items()):
        bar = "█" * int(float(avg) * 4)
        print(f"    Step {step}:  {float(avg):.2f}  {bar}")

    print("\n  Avg confidence by source:")
    for src, avg in sorted(data["avg_confidence_by_source"].items()):
        print(f"    {src:<25s} {float(avg):.2f}")

    print(f"\n  Current-season stats:  {data['pct_with_current_season_stats']}% of players")
    print(f"  Cross-referenced (2+ sources): {data['pct_with_multi_sources']}% ({data['multi_source_count']:,} players)")
    print(f"  Pending merge candidates: {data['pending_merge_candidates']:,}")


def _print_sources(data: dict[str, Any]) -> None:
    _heading("3 · DATA SOURCE BREAKDOWN")
    print()
    print(f"  {'Source':<20s} {'Total':>8s} {'Processed':>10s} {'Errored':>8s} {'Err%':>6s}")
    print(f"  {'':─<20s} {'':─>8s} {'':─>10s} {'':─>8s} {'':─>6s}")
    for s in data["staging_by_source"]:
        print(
            f"  {s['source']:<20s} "
            f"{s['total_records']:>8,} "
            f"{s['processed']:>10,} "
            f"{s['errored']:>8,} "
            f"{s['error_rate_pct']:>5.1f}%"
        )

    if data["season_records_by_source"]:
        print("\n  Season stats records contributed:")
        for src, cnt in sorted(data["season_records_by_source"].items(), key=lambda x: -x[1]):
            print(f"    {src:<25s} {cnt:>8,}")

    if data["last_runs"]:
        print("\n  Last pipeline runs:")
        for src, statuses in sorted(data["last_runs"].items()):
            parts = [f"{s}={d}" for s, d in statuses.items() if d]
            print(f"    {src:<25s} {', '.join(parts)}")


def _print_freshness(data: dict[str, Any]) -> None:
    _heading("4 · FRESHNESS")
    total = data["total_active"]
    print(f"\n  Updated last  7 days: {data['updated_last_7d']:>7,}  ({data['updated_last_7d']/total*100 if total else 0:.1f}%)")
    print(f"  Updated last 30 days: {data['updated_last_30d']:>7,}  ({data['updated_last_30d']/total*100 if total else 0:.1f}%)")
    print(f"  Updated last 90 days: {data['updated_last_90d']:>7,}  ({data['updated_last_90d']/total*100 if total else 0:.1f}%)")
    print(f"  Stale (90+ days):     {data['stale_90d_plus']:>7,}  ({data['stale_90d_plus']/total*100 if total else 0:.1f}%)")

    if data["stalest_by_step"]:
        print("\n  Stalest records by step:")
        for step, info in sorted(data["stalest_by_step"].items()):
            oldest = info["oldest_update"][:10] if info["oldest_update"] else "n/a"
            print(f"    Step {step}:  oldest update {oldest},  {info['stale_count_90d']:,} stale records")


def _print_gaps(data: dict[str, Any]) -> None:
    _heading("5 · TOP GAPS (actionable)")

    print(f"\n  Clubs with 0 players: {data['clubs_with_zero_count']:,}")
    shown = data["clubs_with_zero_players"][:30]
    if shown:
        for c in shown:
            print(f"    Step {c['step']}  {c['club']:<35s}  ({c['league']})")
        if data["clubs_with_zero_count"] > 30:
            print(f"    … and {data['clubs_with_zero_count'] - 30:,} more")

    low = data["leagues_below_50pct_coverage"]
    print(f"\n  Leagues below 50% club coverage: {len(low)}")
    for lg in low[:20]:
        print(
            f"    Step {lg['step']}  {lg['league']:<45s}  "
            f"{lg['clubs_with_players']}/{lg['total_clubs']} clubs  ({lg['coverage_pct']}%)"
        )
    if len(low) > 20:
        print(f"    … and {len(low) - 20} more")

    steps = data["steps_by_lowest_confidence"]
    if steps:
        print("\n  Steps ranked by confidence (lowest first):")
        for s in steps:
            print(f"    Step {s['step']}:  avg {s['avg_confidence']:.2f}")


# ══════════════════════════════════════════════════════════════════════════
# JSON serialisation helper
# ══════════════════════════════════════════════════════════════════════════

def _json_serialisable(obj: Any) -> Any:
    """Convert non-serialisable types for json.dumps."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if hasattr(obj, "__float__"):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


# ══════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════

def generate_report() -> dict[str, Any]:
    """Run all report sections and return the combined dict."""
    log.info("Generating data quality report…")
    with get_session() as session:
        report: dict[str, Any] = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "season": CURRENT_SEASON,
            "coverage": _coverage_summary(session),
            "quality": _data_quality(session),
            "sources": _data_sources(session),
            "freshness": _freshness(session),
            "gaps": _top_gaps(session),
        }
    return report


def main() -> None:
    report = generate_report()

    # ── Console output ────────────────────────────────────────────────
    print()
    print("=" * W)
    print("  FOOTBALL FAM — DATA QUALITY REPORT")
    print(f"  Generated: {report['generated_at']}")
    print(f"  Season:    {report['season']}")
    print("=" * W)

    _print_coverage(report["coverage"])
    _print_quality(report["quality"])
    _print_sources(report["sources"])
    _print_freshness(report["freshness"])
    _print_gaps(report["gaps"])

    print()
    print("=" * W)

    # ── Save JSON ─────────────────────────────────────────────────────
    data_dir = Path(__file__).resolve().parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    today_str = date.today().isoformat()
    out_path = data_dir / f"quality_report_{today_str}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=_json_serialisable)

    print(f"\n  JSON saved → {out_path}")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(1)
    except Exception:
        log.exception("Report generation failed")
        sys.exit(2)
