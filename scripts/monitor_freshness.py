#!/usr/bin/env python
"""Health monitor for the Football Fam data pipeline.

Checks four things and prints warnings for anything that looks wrong:

1. **Stale players** — players not updated in 90+ days.
2. **Failed runs** — recent ``data_source_runs`` with status=failed.
3. **Stuck staging** — ``staging_raw`` records that are unprocessed
   and older than 24 hours (should have been transformed by now).
4. **Source gaps** — data sources that haven't run at all in 7+ days.

Optionally sends alerts via Slack webhook or email if the
``SLACK_WEBHOOK_URL`` / ``SMTP_*`` env vars are configured.

Usage::

    python scripts/monitor_freshness.py
    python scripts/monitor_freshness.py --alert   # force send alerts
    python scripts/monitor_freshness.py --json    # machine-readable output
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, distinct, func, select

from src.db.models import (
    Club,
    DataSourceRun,
    League,
    Player,
    StagingRaw,
)
from src.db.session import get_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger(__name__)

W = 72


# ══════════════════════════════════════════════════════════════════════════
# Check functions — each returns (ok: bool, findings: dict)
# ══════════════════════════════════════════════════════════════════════════

def check_stale_players(session: Any) -> tuple[bool, dict[str, Any]]:
    """Players not updated in 90+ days, broken down by step."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=90)
    active_filter = Player.merged_into_id.is_(None)

    total_active: int = session.execute(
        select(func.count(Player.id)).where(active_filter)
    ).scalar_one()

    stale_total: int = session.execute(
        select(func.count(Player.id))
        .where(active_filter, Player.updated_at < cutoff)
    ).scalar_one()

    stale_by_step = session.execute(
        select(
            League.step,
            func.count(Player.id).label("cnt"),
        )
        .join(Club, Player.current_club_id == Club.id)
        .join(League, Club.league_id == League.id)
        .where(active_filter, Player.updated_at < cutoff)
        .group_by(League.step)
        .order_by(League.step)
    ).all()

    pct = (stale_total / total_active * 100) if total_active else 0
    ok = pct < 25  # warning if >25% are stale

    return ok, {
        "total_active": total_active,
        "stale_90d": stale_total,
        "stale_pct": round(pct, 1),
        "by_step": {r.step: r.cnt for r in stale_by_step},
    }


def check_failed_runs(session: Any) -> tuple[bool, dict[str, Any]]:
    """Data source runs that failed in the last 7 days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    rows = session.execute(
        select(
            DataSourceRun.source,
            DataSourceRun.run_type,
            DataSourceRun.started_at,
            DataSourceRun.status,
            DataSourceRun.error_log,
        )
        .where(
            DataSourceRun.status == "failed",
            DataSourceRun.started_at >= cutoff,
        )
        .order_by(DataSourceRun.started_at.desc())
    ).all()

    failures = [
        {
            "source": r.source,
            "run_type": r.run_type,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "error": (r.error_log or "")[:200],
        }
        for r in rows
    ]

    return len(failures) == 0, {"recent_failures": failures, "count": len(failures)}


def check_stuck_staging(session: Any) -> tuple[bool, dict[str, Any]]:
    """Staging records unprocessed for 24+ hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    rows = session.execute(
        select(
            StagingRaw.source,
            StagingRaw.source_entity_type,
            func.count().label("cnt"),
            func.min(StagingRaw.created_at).label("oldest"),
        )
        .where(
            StagingRaw.processed.is_(False),
            StagingRaw.created_at < cutoff,
        )
        .group_by(StagingRaw.source, StagingRaw.source_entity_type)
        .order_by(func.count().desc())
    ).all()

    stuck = [
        {
            "source": r.source,
            "entity_type": r.source_entity_type,
            "count": r.cnt,
            "oldest": r.oldest.isoformat() if r.oldest else None,
        }
        for r in rows
    ]
    total_stuck = sum(s["count"] for s in stuck)

    return total_stuck == 0, {"stuck_records": stuck, "total": total_stuck}


def check_source_gaps(session: Any) -> tuple[bool, dict[str, Any]]:
    """Sources that haven't run successfully in 7+ days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    expected_sources = [
        "api_football", "football_web_pages", "pitchero", "fa_fulltime",
    ]

    rows = session.execute(
        select(
            DataSourceRun.source,
            func.max(DataSourceRun.started_at).label("last_run"),
        )
        .where(DataSourceRun.status == "completed")
        .group_by(DataSourceRun.source)
    ).all()

    last_run_map = {r.source: r.last_run for r in rows}

    gaps = []
    for src in expected_sources:
        last = last_run_map.get(src)
        if last is None:
            gaps.append({"source": src, "last_run": None, "issue": "never run"})
        elif last < cutoff:
            days_ago = (datetime.now(timezone.utc) - last).days
            gaps.append({
                "source": src,
                "last_run": last.isoformat(),
                "issue": f"{days_ago} days ago",
            })

    return len(gaps) == 0, {"gaps": gaps}


# ══════════════════════════════════════════════════════════════════════════
# Alert dispatcher
# ══════════════════════════════════════════════════════════════════════════

def _send_alerts(warnings: list[str]) -> None:
    """Send alerts via Slack and/or email if configured."""
    if not warnings:
        return

    body = "Football Fam Pipeline Monitor\n\n" + "\n".join(
        f"  - {w}" for w in warnings
    )

    # ── Slack ────────────────────────────────────────────────────────
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            import requests
            text = f"*Pipeline Health Warnings ({len(warnings)})*\n```\n{body}\n```"
            requests.post(webhook, json={"text": text}, timeout=10)
            log.info("Slack alert sent")
        except Exception:
            log.warning("Slack alert failed", exc_info=True)

    # ── Email ────────────────────────────────────────────────────────
    smtp_host = os.getenv("SMTP_HOST")
    mail_to = os.getenv("PIPELINE_MAIL_TO")
    if smtp_host and mail_to:
        try:
            import smtplib
            from email.message import EmailMessage

            msg = EmailMessage()
            msg["Subject"] = f"Football Fam: {len(warnings)} pipeline warning(s)"
            msg["From"] = os.getenv("SMTP_FROM", "pipeline@footballfam.com")
            msg["To"] = mail_to
            msg.set_content(body)

            port = int(os.getenv("SMTP_PORT", "587"))
            with smtplib.SMTP(smtp_host, port) as server:
                user = os.getenv("SMTP_USER")
                pwd = os.getenv("SMTP_PASS")
                if user and pwd:
                    server.starttls()
                    server.login(user, pwd)
                server.send_message(msg)
            log.info("Email alert sent to %s", mail_to)
        except Exception:
            log.warning("Email alert failed", exc_info=True)


# ══════════════════════════════════════════════════════════════════════════
# Console output
# ══════════════════════════════════════════════════════════════════════════

def _print_check(name: str, ok: bool, data: dict[str, Any]) -> None:
    icon = "PASS" if ok else "WARN"
    print(f"\n  [{icon}]  {name}")
    for k, v in data.items():
        if isinstance(v, list) and v:
            print(f"         {k}:")
            for item in v[:10]:
                if isinstance(item, dict):
                    parts = [f"{ik}={iv}" for ik, iv in item.items()]
                    print(f"           - {', '.join(parts)}")
                else:
                    print(f"           - {item}")
            if len(v) > 10:
                print(f"           … and {len(v) - 10} more")
        elif isinstance(v, dict) and v:
            parts = [f"{ik}={iv}" for ik, iv in v.items()]
            print(f"         {k}: {', '.join(parts)}")
        else:
            print(f"         {k}: {v}")


# ══════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="Football Fam pipeline health monitor")
    parser.add_argument("--alert", action="store_true", help="Force send alerts even if no env vars")
    parser.add_argument("--json", action="store_true", help="Output as JSON instead of console text")
    args = parser.parse_args()

    checks = [
        ("Stale players (90+ days)", check_stale_players),
        ("Failed pipeline runs (7 days)", check_failed_runs),
        ("Stuck staging records (24+ hrs)", check_stuck_staging),
        ("Data source gaps (7+ days)", check_source_gaps),
    ]

    results: dict[str, Any] = {}
    warnings: list[str] = []

    with get_session() as session:
        for name, fn in checks:
            ok, data = fn(session)
            results[name] = {"ok": ok, **data}
            if not ok:
                warnings.append(name)

    if args.json:
        print(json.dumps(results, indent=2, default=str))
    else:
        print()
        print("=" * W)
        print("  FOOTBALL FAM — PIPELINE HEALTH MONITOR")
        print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        print("=" * W)

        for name, fn in checks:
            r = results[name]
            ok = r.pop("ok")
            _print_check(name, ok, r)

        if warnings:
            print(f"\n  *** {len(warnings)} WARNING(S) ***")
            for w in warnings:
                print(f"    - {w}")
        else:
            print("\n  All checks passed.")

        print()
        print("=" * W)

    if warnings:
        _send_alerts(warnings)

    sys.exit(1 if warnings else 0)


if __name__ == "__main__":
    main()
