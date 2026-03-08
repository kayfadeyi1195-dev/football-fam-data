"""Diagnostic script for staging_raw records.

Prints:
1. Record counts grouped by source, entity type, and processed status.
2. Top 20 most common error messages for FA Full-Time records.

Usage::

    python scripts/check_staging.py
"""

import logging
from sqlalchemy import func, select

from src.db.models import StagingRaw
from src.db.session import get_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)


def main() -> None:
    with get_session() as session:

        # ── 1. Counts by source / entity_type / processed ────────────
        rows = session.execute(
            select(
                StagingRaw.source,
                StagingRaw.source_entity_type,
                StagingRaw.processed,
                func.count().label("cnt"),
            )
            .group_by(
                StagingRaw.source,
                StagingRaw.source_entity_type,
                StagingRaw.processed,
            )
            .order_by(
                StagingRaw.source,
                StagingRaw.source_entity_type,
                StagingRaw.processed,
            )
        ).all()

        print()
        print("=" * 70)
        print("  staging_raw — record counts")
        print("=" * 70)
        print(f"  {'Source':<20s}  {'Entity Type':<20s}  {'Processed':<10s}  {'Count':>7s}")
        print("  " + "-" * 62)

        for r in rows:
            status = "Yes" if r.processed else "No"
            print(f"  {r.source:<20s}  {r.source_entity_type:<20s}  {status:<10s}  {r.cnt:>7,d}")

        # Grand total
        total = sum(r.cnt for r in rows)
        print("  " + "-" * 62)
        print(f"  {'TOTAL':<20s}  {'':<20s}  {'':<10s}  {total:>7,d}")

        # ── 2. Top 20 error messages for fa_fulltime ─────────────────
        error_rows = session.execute(
            select(
                StagingRaw.error_message,
                func.count().label("cnt"),
            )
            .where(
                StagingRaw.source == "fa_fulltime",
                StagingRaw.error_message.isnot(None),
            )
            .group_by(StagingRaw.error_message)
            .order_by(func.count().desc())
            .limit(20)
        ).all()

        print()
        print("=" * 70)
        print("  fa_fulltime — top 20 error messages")
        print("=" * 70)

        if not error_rows:
            print("  (no errors)")
        else:
            total_errors = sum(r.cnt for r in error_rows)
            for i, r in enumerate(error_rows, 1):
                msg = (r.error_message or "")[:80]
                print(f"  {i:2d}. [{r.cnt:>5,d}]  {msg}")
            print("  " + "-" * 62)
            print(f"  Shown: {total_errors:,d} errors across top {len(error_rows)} messages")

            # Also count total errors
            total_err_count: int = session.execute(
                select(func.count()).where(
                    StagingRaw.source == "fa_fulltime",
                    StagingRaw.error_message.isnot(None),
                )
            ).scalar_one()
            print(f"  Total fa_fulltime records with errors: {total_err_count:,d}")

        print()


if __name__ == "__main__":
    main()
