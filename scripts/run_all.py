#!/usr/bin/env python
"""Master pipeline orchestrator for Football Fam.

Runs every data source, ETL transform, entity resolution, confidence
scoring, and quality reporting in the correct order.  Each stage is
wrapped so a failure logs the error and moves on to the next stage
rather than crashing the whole pipeline.

Usage::

    python scripts/run_all.py                       # full pipeline
    python scripts/run_all.py --step 5              # only sources relevant to Step 5
    python scripts/run_all.py --source pitchero     # only run the Pitchero scraper
    python scripts/run_all.py --skip-scraping       # APIs + ETL + resolution only
    python scripts/run_all.py --report-only         # just produce the quality report
"""

import argparse
import json
import logging
import os
import sys
import time
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

# ---------------------------------------------------------------------------
# Logging — dual output: console + rotating file
# ---------------------------------------------------------------------------

LOG_DIR = Path(__file__).resolve().parent.parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"pipeline_{date.today().isoformat()}.log"

_root = logging.getLogger()
_root.setLevel(logging.INFO)

_fmt = logging.Formatter(
    "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(_fmt)
_root.addHandler(_console)

_fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setFormatter(_fmt)
_root.addHandler(_fh)

log = logging.getLogger("run_all")


# ══════════════════════════════════════════════════════════════════════════
# Stage registry
# ══════════════════════════════════════════════════════════════════════════

class StageResult:
    """Outcome of a single pipeline stage."""

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

    @property
    def status_icon(self) -> str:
        return "OK" if self.ok else "FAIL"


# Which pyramid steps each data source covers
SOURCE_STEPS: dict[str, set[int]] = {
    "api_football":        {1, 2, 3},
    "football_web_pages":  {1, 2, 3, 4},
    "sportmonks":          {1, 2, 3, 4},
    "pitchero":            {3, 4, 5},
    "fa_fulltime":         {4, 5, 6},
    "fbref":               {1, 2},
    "club_websites":       {3, 4, 5, 6},
}

ALL_SOURCES = list(SOURCE_STEPS.keys())


def _source_relevant(source: str, step_filter: int | None) -> bool:
    """Return True if *source* covers *step_filter* (or no filter set)."""
    if step_filter is None:
        return True
    return step_filter in SOURCE_STEPS.get(source, set())


# ══════════════════════════════════════════════════════════════════════════
# Individual stage functions
#
# Each returns an optional dict of stats that gets attached to the
# StageResult.  They may import lazily so the module loads fast even
# if some deps are missing.
# ══════════════════════════════════════════════════════════════════════════

# ── 1. API-Football ──────────────────────────────────────────────────────

def _run_api_football() -> dict[str, Any]:
    from scripts.run_api_football import main as _main
    _main()
    return {"source": "api_football"}


# ── 2. Football Web Pages ────────────────────────────────────────────────

def _run_football_web_pages() -> dict[str, Any]:
    from src.api_clients.football_web_pages import FootballWebPagesClient

    client = FootballWebPagesClient()
    comps = client.discover_nonleague_competitions()
    log.info("FWP discovered %d competitions", len(comps))

    from src.etl.staging import stage_records

    total_staged = 0
    for comp_name, comp_id in comps.items():
        log.info("FWP — fetching appearances for %s (id=%s)", comp_name, comp_id)
        try:
            rows = client.get_appearances(comp_id)
            if rows:
                records = [
                    {
                        "id": f"fwp_{comp_id}_{i}",
                        "comp_id": comp_id,
                        "comp_name": comp_name,
                        **row,
                    }
                    for i, row in enumerate(rows)
                ]
                staged = stage_records("football_web_pages", "player", records, id_field="id")
                total_staged += staged
        except Exception:
            log.warning("FWP — failed on comp %s", comp_name, exc_info=True)

    return {"competitions": len(comps), "records_staged": total_staged}


# ── 3. Sportmonks ────────────────────────────────────────────────────────

def _run_sportmonks() -> dict[str, Any]:
    from src.config import SPORTMONKS_API_TOKEN

    if not SPORTMONKS_API_TOKEN:
        log.info("Sportmonks — no API token configured, skipping")
        return {"skipped": True, "reason": "no token"}

    from src.api_clients.sportmonks import SportmonksClient
    from src.etl.staging import stage_records

    client = SportmonksClient()
    comps = client.discover_english_nonleague()
    log.info("Sportmonks — %d non-league competitions discovered", len(comps))

    total_staged = 0
    for comp_name, info in comps.items():
        season_id = info.get("current_season_id")
        if not season_id:
            continue
        teams = client.get_teams(season_id)
        for team in teams:
            try:
                squad = client.get_squad(
                    team.get("id", 0),
                    season_id=season_id,
                    includes=["player", "details"],
                )
                if squad:
                    for p in squad:
                        p.setdefault("id", f"sm_{p.get('player_id', p.get('id', 0))}")
                        p["team_name"] = team.get("name")
                        p["competition"] = comp_name
                    staged = stage_records("sportmonks", "player", squad, id_field="id")
                    total_staged += staged
            except Exception:
                log.warning("Sportmonks — error on team %s", team.get("name"), exc_info=True)

    return {"competitions": len(comps), "records_staged": total_staged}


# ── 4. Pitchero scraper ─────────────────────────────────────────────────

def _run_pitchero() -> dict[str, Any]:
    from scripts.run_pitchero_scraper import main as _main
    _main()
    return {"source": "pitchero"}


# ── 5. FA Full-Time scraper ─────────────────────────────────────────────

def _run_fa_fulltime() -> dict[str, Any]:
    from scripts.run_fa_fulltime import main as _main
    _main()
    return {"source": "fa_fulltime"}


# ── 6. FBref enrichment ─────────────────────────────────────────────────

def _run_fbref() -> dict[str, Any]:
    from src.scrapers.fbref import scrape_squad_stats, scrape_league_stats

    results = scrape_league_stats(
        "https://fbref.com/en/comps/34/National-League-Stats"
    )
    if not results:
        log.info("FBref — placeholder; no data returned (not yet implemented)")
        return {"skipped": True, "reason": "placeholder scraper"}
    return {"records": len(results)}


# ── 7. Club website scraper ─────────────────────────────────────────────

def _run_club_websites() -> dict[str, Any]:
    from sqlalchemy import select as sa_select
    from src.db.models import Club
    from src.db.session import get_session
    from src.scrapers.club_websites import scrape_squad_page
    from src.etl.staging import stage_records

    with get_session() as session:
        clubs = session.execute(
            sa_select(Club.id, Club.name, Club.website_url)
            .where(
                Club.website_url.isnot(None),
                Club.website_url != "",
            )
        ).all()

    total_staged = 0
    for club_id, club_name, url in clubs:
        try:
            players = scrape_squad_page(url)
            if players:
                for p in players:
                    p.setdefault("id", f"cw_{club_id}_{p.get('name', '')}")
                    p["club_id"] = club_id
                    p["club_name"] = club_name
                staged = stage_records("club_website", "player", players, id_field="id")
                total_staged += staged
        except Exception:
            log.warning("Club website scrape failed for %s", club_name, exc_info=True)

    if total_staged == 0:
        return {"skipped": True, "reason": "placeholder scraper or no data"}
    return {"clubs_attempted": len(clubs), "records_staged": total_staged}


# ── 8. ETL transforms ───────────────────────────────────────────────────

def _run_transforms() -> dict[str, Any]:
    from src.etl.pitchero_transform import transform_pitchero
    from src.etl.fa_fulltime_transform import transform_fa_fulltime

    pit = transform_pitchero()
    log.info("Pitchero transform: %s", pit)

    fa = transform_fa_fulltime()
    log.info("FA Full-Time transform: %s", fa)

    return {"pitchero": pit, "fa_fulltime": fa}


# ── 9. Entity resolution ────────────────────────────────────────────────

def _run_entity_resolution() -> dict[str, Any]:
    from src.etl.entity_resolution import run_entity_resolution
    return run_entity_resolution()


# ── 10. Confidence scoring ──────────────────────────────────────────────

def _run_confidence() -> dict[str, Any]:
    from src.etl.confidence import recalculate_confidence
    return recalculate_confidence()


# ── 11. Refresh search indexes ──────────────────────────────────────────

def _run_reindex() -> dict[str, Any]:
    from sqlalchemy import text
    from src.db.session import get_session

    with get_session() as session:
        session.execute(text("""
            UPDATE players SET search_vector =
                setweight(to_tsvector('english', coalesce(full_name, '')), 'A') ||
                setweight(to_tsvector('english', coalesce(position_detail, '')), 'B') ||
                setweight(to_tsvector('english', coalesce(nationality, '')), 'B') ||
                setweight(to_tsvector('english', coalesce(bio, '')), 'C')
            WHERE merged_into_id IS NULL
        """))
        result = session.execute(text("SELECT count(*) FROM players WHERE merged_into_id IS NULL"))
        count = result.scalar_one()

    log.info("Search vectors refreshed for %d players", count)
    return {"players_reindexed": count}


# ── 12. Data quality report ─────────────────────────────────────────────

def _run_quality_report() -> dict[str, Any]:
    from scripts.data_quality_report import generate_report, main as _report_main
    _report_main()
    return {"generated": True}


# ══════════════════════════════════════════════════════════════════════════
# Stage runner with error isolation
# ══════════════════════════════════════════════════════════════════════════

def _run_stage(name: str, fn: Callable[[], dict[str, Any]]) -> StageResult:
    """Execute *fn*, catching any exception so later stages still run."""
    log.info("━━━  STAGE: %s  ━━━", name)
    t0 = time.monotonic()
    try:
        detail = fn() or {}
        elapsed = time.monotonic() - t0
        log.info(
            "Stage %s completed in %.1fs — %s",
            name, elapsed, detail,
        )
        return StageResult(name, ok=True, elapsed_s=elapsed, detail=detail)
    except Exception:
        elapsed = time.monotonic() - t0
        tb = traceback.format_exc()
        log.error("Stage %s FAILED after %.1fs:\n%s", name, elapsed, tb)
        return StageResult(name, ok=False, elapsed_s=elapsed, error=tb)


# ══════════════════════════════════════════════════════════════════════════
# Pipeline builder
# ══════════════════════════════════════════════════════════════════════════

def _build_pipeline(args: argparse.Namespace) -> list[tuple[str, Callable]]:
    """Return the ordered list of (stage_name, callable) to execute."""

    if args.report_only:
        return [("quality_report", _run_quality_report)]

    stages: list[tuple[str, Callable]] = []
    step = args.step
    source = args.source
    skip_scraping = args.skip_scraping

    def _want(src_name: str, is_scraper: bool = False) -> bool:
        if source and source != src_name:
            return False
        if skip_scraping and is_scraper:
            return False
        return _source_relevant(src_name, step)

    # ── Data acquisition ─────────────────────────────────────────────
    if _want("api_football"):
        stages.append(("api_football", _run_api_football))
    if _want("football_web_pages"):
        stages.append(("football_web_pages", _run_football_web_pages))
    if _want("sportmonks"):
        stages.append(("sportmonks", _run_sportmonks))
    if _want("pitchero", is_scraper=True):
        stages.append(("pitchero", _run_pitchero))
    if _want("fa_fulltime", is_scraper=True):
        stages.append(("fa_fulltime", _run_fa_fulltime))
    if _want("fbref", is_scraper=True):
        stages.append(("fbref", _run_fbref))
    if _want("club_websites", is_scraper=True):
        stages.append(("club_websites", _run_club_websites))

    # ── Processing (always run unless filtering to a single source) ──
    if not source:
        stages.append(("etl_transforms", _run_transforms))
        stages.append(("entity_resolution", _run_entity_resolution))
        stages.append(("confidence_scoring", _run_confidence))
        stages.append(("search_reindex", _run_reindex))
        stages.append(("quality_report", _run_quality_report))

    return stages


# ══════════════════════════════════════════════════════════════════════════
# Summary printer
# ══════════════════════════════════════════════════════════════════════════

def _print_summary(results: list[StageResult], total_s: float) -> None:
    W = 76
    print()
    print("=" * W)
    print("  FOOTBALL FAM — PIPELINE SUMMARY")
    print("=" * W)

    # stage table
    print()
    print(f"  {'Stage':<28s}  {'Status':<6s}  {'Time':>8s}  Notes")
    print(f"  {'':─<28s}  {'':─<6s}  {'':─>8s}  {'':─<28s}")
    for r in results:
        time_str = f"{r.elapsed_s:.1f}s"
        notes = ""
        if r.error:
            first_line = r.error.strip().splitlines()[-1][:40]
            notes = first_line
        elif r.detail:
            if r.detail.get("skipped"):
                notes = f"skipped: {r.detail.get('reason', '')}"
            else:
                highlights = []
                for k, v in r.detail.items():
                    if isinstance(v, (int, float, str, bool)):
                        highlights.append(f"{k}={v}")
                notes = ", ".join(highlights[:3])
        print(f"  {r.name:<28s}  {r.status_icon:<6s}  {time_str:>8s}  {notes}")

    # totals
    ok_count = sum(1 for r in results if r.ok)
    fail_count = sum(1 for r in results if not r.ok)
    print(f"\n  Stages: {ok_count} passed, {fail_count} failed")
    print(f"  Total runtime: {total_s:.1f}s ({total_s/60:.1f} min)")

    # ── ETL detail if available ──────────────────────────────────────
    etl = next((r for r in results if r.name == "etl_transforms" and r.ok), None)
    if etl and etl.detail:
        print("\n  ETL Transform Detail:")
        for src_name, stats in etl.detail.items():
            if isinstance(stats, dict):
                parts = [f"{k}={v}" for k, v in stats.items()]
                print(f"    {src_name}: {', '.join(parts)}")

    # ── Entity resolution detail ─────────────────────────────────────
    er = next((r for r in results if r.name == "entity_resolution" and r.ok), None)
    if er and er.detail:
        d = er.detail
        print(
            f"\n  Entity Resolution: "
            f"{d.get('candidates', 0)} candidates, "
            f"{d.get('auto_merged', 0)} merged, "
            f"{d.get('queued', 0)} queued, "
            f"{d.get('skipped', 0)} skipped"
        )

    # ── Confidence detail ────────────────────────────────────────────
    conf = next((r for r in results if r.name == "confidence_scoring" and r.ok), None)
    if conf and conf.detail:
        d = conf.detail
        total_scored = d.get("total_scored", 0)
        dist = d.get("distribution", {})
        print(f"\n  Confidence Distribution ({total_scored:,} players):")
        for bucket in range(5, -1, -1):
            cnt = dist.get(bucket, dist.get(str(bucket), 0))
            pct = (cnt / total_scored * 100) if total_scored else 0
            print(f"    {bucket}.xx: {cnt:>6,}  ({pct:5.1f}%)")

        avg_step = d.get("avg_by_step", {})
        if avg_step:
            print("  Avg confidence by step:")
            for step, avg in sorted(avg_step.items(), key=lambda x: int(x[0])):
                print(f"    Step {step}: {avg:.2f}")

    print()
    print("=" * W)
    print(f"  Log file: {LOG_FILE}")
    print("=" * W)
    print()


# ══════════════════════════════════════════════════════════════════════════
# Optional notifications
# ══════════════════════════════════════════════════════════════════════════

def _notify(results: list[StageResult], total_s: float) -> None:
    """Send a Slack or email notification if configured via env vars.

    Set SLACK_WEBHOOK_URL or SMTP_* variables in .env to enable.
    This is fire-and-forget — failures here are logged but ignored.
    """
    # ── Slack ────────────────────────────────────────────────────────
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            import requests as _req

            ok = sum(1 for r in results if r.ok)
            fail = sum(1 for r in results if not r.ok)
            text = (
                f"*Football Fam Pipeline Complete*\n"
                f"Stages: {ok} passed, {fail} failed\n"
                f"Runtime: {total_s:.0f}s"
            )
            _req.post(webhook, json={"text": text}, timeout=10)
            log.info("Slack notification sent")
        except Exception:
            log.warning("Slack notification failed", exc_info=True)

    # ── Email ────────────────────────────────────────────────────────
    smtp_host = os.getenv("SMTP_HOST")
    mail_to = os.getenv("PIPELINE_MAIL_TO")
    if smtp_host and mail_to:
        try:
            import smtplib
            from email.message import EmailMessage

            ok = sum(1 for r in results if r.ok)
            fail = sum(1 for r in results if not r.ok)

            msg = EmailMessage()
            msg["Subject"] = f"Football Fam Pipeline: {ok} ok, {fail} failed"
            msg["From"] = os.getenv("SMTP_FROM", "pipeline@footballfam.com")
            msg["To"] = mail_to
            body_lines = [f"{r.name}: {r.status_icon} ({r.elapsed_s:.1f}s)" for r in results]
            msg.set_content("\n".join(body_lines))

            port = int(os.getenv("SMTP_PORT", "587"))
            with smtplib.SMTP(smtp_host, port) as server:
                user = os.getenv("SMTP_USER")
                pwd = os.getenv("SMTP_PASS")
                if user and pwd:
                    server.starttls()
                    server.login(user, pwd)
                server.send_message(msg)
            log.info("Email notification sent to %s", mail_to)
        except Exception:
            log.warning("Email notification failed", exc_info=True)


# ══════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Football Fam — master data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python scripts/run_all.py                   # full pipeline\n"
            "  python scripts/run_all.py --step 5          # Step-5 sources only\n"
            "  python scripts/run_all.py --source pitchero # single source\n"
            "  python scripts/run_all.py --skip-scraping   # APIs + ETL only\n"
            "  python scripts/run_all.py --report-only     # quality report only\n"
        ),
    )
    p.add_argument(
        "--step", type=int, choices=[1, 2, 3, 4, 5, 6], default=None,
        help="Only run sources that cover this pyramid step",
    )
    p.add_argument(
        "--source", type=str, choices=ALL_SOURCES, default=None,
        help="Only run a specific data source",
    )
    p.add_argument(
        "--skip-scraping", action="store_true",
        help="Skip web scrapers; run API clients + ETL + resolution only",
    )
    p.add_argument(
        "--report-only", action="store_true",
        help="Skip everything and just generate the quality report",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    log.info("=" * 60)
    log.info("FOOTBALL FAM — MASTER PIPELINE STARTED")
    log.info("  step filter:    %s", args.step or "all")
    log.info("  source filter:  %s", args.source or "all")
    log.info("  skip scraping:  %s", args.skip_scraping)
    log.info("  report only:    %s", args.report_only)
    log.info("=" * 60)

    pipeline = _build_pipeline(args)

    if not pipeline:
        log.warning("No stages selected — nothing to do. Check your flags.")
        sys.exit(0)

    log.info("Stages queued: %s", [name for name, _ in pipeline])

    t_start = time.monotonic()
    results: list[StageResult] = []

    for stage_name, fn in pipeline:
        result = _run_stage(stage_name, fn)
        results.append(result)

    total_s = time.monotonic() - t_start

    _print_summary(results, total_s)
    _notify(results, total_s)

    any_failed = any(not r.ok for r in results)
    if any_failed:
        log.warning("Pipeline finished with failures")
        sys.exit(1)
    else:
        log.info("Pipeline finished successfully")
        sys.exit(0)


if __name__ == "__main__":
    main()
