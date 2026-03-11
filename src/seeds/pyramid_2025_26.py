"""Seed the leagues table with the complete English football pyramid.

Covers Steps 1–6 for the 2025-26 season, based on the FA National
League System structure.

The league *structure* (names, steps, regions, parent relationships)
is identical to 2024-25.  What changes between seasons is which
**clubs** sit in each league — that is handled by the club-seeding
scripts, not here.

Known promotion / relegation movements between 2024-25 and 2025-26
(for reference when seeding clubs):

**Step 1  National League**
  - IN:  Scunthorpe United, Brackley Town (from NL North)
         Boreham Wood, Truro City (from NL South)
         Morecambe, Carlisle United (relegated from League Two)
  - OUT: Barnet, Oldham Athletic (promoted to League Two)
         AFC Fylde (relegated to NL North)
         Ebbsfleet United, Maidenhead United,
         Dagenham & Redbridge (relegated to NL South)

**Step 2  NL North**
  - IN:  AFC Fylde (from NL)
         Worksop Town, Macclesfield (from NPL Premier)
         AFC Telford United, Bedford Town (from SLP Central)
         Merthyr Town (from SLP South)
  - OUT: Scunthorpe United, Brackley Town (to NL)
         Rushall Olympic, Warrington Town (to NPL Premier)
         Needham Market (to SLP Central)
         Farsley Celtic (to NCEL Premier — Step 5)

**Step 2  NL South**
  - IN:  Ebbsfleet United, Maidenhead United,
         Dagenham & Redbridge (from NL)
         Dover Athletic, Horsham (from Isthmian Premier)
         AFC Totton (from SLP South)
  - OUT: Boreham Wood, Truro City (to NL)
         Aveley, Welling United, St Albans City (to Isthmian Premier)
         Weymouth (to SLP South)

**Step 3  NPL Premier**
  - IN:  Warrington Town, Rushall Olympic (from NL North)
         Stocksbridge Park Steels, Cleethorpes Town (from NPL D1 East)
         Hednesford Town (from NPL D1 West)
  - OUT: Worksop Town, Macclesfield (to NL North)
         Matlock Town, Blyth Spartans (to NPL D1 East)
         Mickleover, Basford United (to NPL D1 Midlands)

**Step 3  SLP Central**
  - IN:  Needham Market (from NL North)
  - OUT: AFC Telford United, Bedford Town (to NL North)

**Step 3  SLP South**
  - IN:  Weymouth (from NL South)
  - OUT: Merthyr Town (to NL North)
         AFC Totton (to NL South)

**Step 3  Isthmian Premier**
  - IN:  Aveley, Welling United, St Albans City (from NL South)
         Brentwood Town (from Isthmian North)
         Ramsgate, Burgess Hill Town (from Isthmian South East)
  - OUT: Dover Athletic, Horsham (to NL South)
         Bowers & Pitsea (to Isthmian North)
         Hendon, Bognor Regis Town (to Isthmian South Central)
         Hastings United (to Isthmian South East)

The script is **idempotent** — it uses PostgreSQL ``INSERT … ON CONFLICT
DO UPDATE`` keyed on ``(name, season)`` so you can run it repeatedly
without creating duplicates.

Usage::

    python -m src.seeds.pyramid_2025_26
"""

import logging

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db.models import League
from src.db.session import get_session

logger = logging.getLogger(__name__)

SEASON = "2025-26"

# ═══════════════════════════════════════════════════════════════════════════
# League data — Steps 1 to 5
#
# Each tuple: (name, short_name, step, region, division)
# ═══════════════════════════════════════════════════════════════════════════

STEP_1_TO_5: list[tuple[str, str, int, str, str]] = [
    # ── Step 1 (1 league, 24 clubs) ───────────────────────────────────────
    ("National League",
     "NL", 1, "National", "Premier"),

    # ── Step 2 (2 leagues, ~24 clubs each) ────────────────────────────────
    ("National League North",
     "NL North", 2, "North", "North"),
    ("National League South",
     "NL South", 2, "South", "South"),

    # ── Step 3 (4 leagues, ~22 clubs each) ────────────────────────────────
    ("Northern Premier League Premier Division",
     "NPL Prem", 3, "North", "Premier Division"),
    ("Southern League Premier Division Central",
     "SLP Central", 3, "Midlands", "Premier Division Central"),
    ("Southern League Premier Division South",
     "SLP South", 3, "South", "Premier Division South"),
    ("Isthmian League Premier Division",
     "Isthmian Prem", 3, "South", "Premier Division"),

    # ── Step 4 (8 leagues, ~20 clubs each) ────────────────────────────────
    ("Northern Premier League Division One East",
     "NPL D1 East", 4, "North", "Division One East"),
    ("Northern Premier League Division One Midlands",
     "NPL D1 Mids", 4, "Midlands", "Division One Midlands"),
    ("Northern Premier League Division One West",
     "NPL D1 West", 4, "North", "Division One West"),
    ("Southern League Division One Central",
     "SL D1 Central", 4, "Midlands", "Division One Central"),
    ("Southern League Division One South",
     "SL D1 South", 4, "South", "Division One South"),
    ("Isthmian League Division One North",
     "Isthmian D1 N", 4, "South", "Division One North"),
    ("Isthmian League Division One South Central",
     "Isthmian D1 SC", 4, "South", "Division One South Central"),
    ("Isthmian League Division One South East",
     "Isthmian D1 SE", 4, "South", "Division One South East"),

    # ── Step 5 (16 leagues, ~18-20 clubs each) ────────────────────────────
    ("Combined Counties League Premier Division North",
     "CCL Prem N", 5, "South", "Premier Division North"),
    ("Combined Counties League Premier Division South",
     "CCL Prem S", 5, "South", "Premier Division South"),
    ("Eastern Counties Football League Premier Division",
     "ECFL Prem", 5, "South", "Premier Division"),
    ("Essex Senior Football League",
     "Essex Senior", 5, "South", "Premier Division"),
    ("Hellenic Football League Premier Division",
     "Hellenic Prem", 5, "South", "Premier Division"),
    ("Midland Football League Premier Division",
     "MFL Prem", 5, "Midlands", "Premier Division"),
    ("North West Counties Football League Premier Division",
     "NWCFL Prem", 5, "North", "Premier Division"),
    ("Northern Football League Division One",
     "NorFL D1", 5, "North", "Division One"),
    ("Northern Counties East Football League Premier Division",
     "NCEL Prem", 5, "North", "Premier Division"),
    ("Southern Combination Football League Premier Division",
     "SCFL Prem", 5, "South", "Premier Division"),
    ("Southern Counties East Football League Premier Division",
     "SCEFL Prem", 5, "South", "Premier Division"),
    ("Spartan South Midlands Football League Premier Division",
     "SSMFL Prem", 5, "South", "Premier Division"),
    ("United Counties League Premier Division North",
     "UCL Prem N", 5, "Midlands", "Premier Division North"),
    ("United Counties League Premier Division South",
     "UCL Prem S", 5, "Midlands", "Premier Division South"),
    ("Wessex Football League Premier Division",
     "Wessex Prem", 5, "South", "Premier Division"),
    ("Western Football League Premier Division",
     "Western Prem", 5, "South", "Premier Division"),
]

# ═══════════════════════════════════════════════════════════════════════════
# Step 6 — the Division One / Division Two feeders beneath each Step 5
# league.  Each entry also carries the *name* of its Step 5 parent so we
# can look up the parent_league_id after the Step 5 rows exist.
#
# Each tuple: (name, short_name, region, division, parent_step5_name)
# ═══════════════════════════════════════════════════════════════════════════

STEP_6: list[tuple[str, str, str, str, str]] = [
    # ── Combined Counties League ──────────────────────────────────────────
    ("Combined Counties League Division One",
     "CCL D1", "South", "Division One",
     "Combined Counties League Premier Division North"),

    # ── Eastern Counties Football League ──────────────────────────────────
    ("Eastern Counties Football League Division One North",
     "ECFL D1 N", "South", "Division One North",
     "Eastern Counties Football League Premier Division"),
    ("Eastern Counties Football League Division One South",
     "ECFL D1 S", "South", "Division One South",
     "Eastern Counties Football League Premier Division"),

    # ── Hellenic Football League ──────────────────────────────────────────
    ("Hellenic Football League Division One",
     "Hellenic D1", "South", "Division One",
     "Hellenic Football League Premier Division"),
    ("Hellenic Football League Division Two",
     "Hellenic D2", "South", "Division Two",
     "Hellenic Football League Premier Division"),

    # ── Midland Football League ───────────────────────────────────────────
    ("Midland Football League Division One",
     "MFL D1", "Midlands", "Division One",
     "Midland Football League Premier Division"),
    ("Midland Football League Division Two",
     "MFL D2", "Midlands", "Division Two",
     "Midland Football League Premier Division"),

    # ── North West Counties Football League ───────────────────────────────
    ("North West Counties Football League Division One North",
     "NWCFL D1 N", "North", "Division One North",
     "North West Counties Football League Premier Division"),
    ("North West Counties Football League Division One South",
     "NWCFL D1 S", "North", "Division One South",
     "North West Counties Football League Premier Division"),

    # ── Northern Football League ──────────────────────────────────────────
    ("Northern Football League Division Two",
     "NorFL D2", "North", "Division Two",
     "Northern Football League Division One"),

    # ── Northern Counties East Football League ────────────────────────────
    ("Northern Counties East Football League Division One",
     "NCEL D1", "North", "Division One",
     "Northern Counties East Football League Premier Division"),

    # ── Southern Combination Football League ──────────────────────────────
    ("Southern Combination Football League Division One",
     "SCFL D1", "South", "Division One",
     "Southern Combination Football League Premier Division"),
    ("Southern Combination Football League Division Two",
     "SCFL D2", "South", "Division Two",
     "Southern Combination Football League Premier Division"),

    # ── Southern Counties East Football League ────────────────────────────
    ("Southern Counties East Football League Division One",
     "SCEFL D1", "South", "Division One",
     "Southern Counties East Football League Premier Division"),

    # ── Spartan South Midlands Football League ────────────────────────────
    ("Spartan South Midlands Football League Division One",
     "SSMFL D1", "South", "Division One",
     "Spartan South Midlands Football League Premier Division"),
    ("Spartan South Midlands Football League Division Two",
     "SSMFL D2", "South", "Division Two",
     "Spartan South Midlands Football League Premier Division"),

    # ── United Counties League ────────────────────────────────────────────
    ("United Counties League Division One",
     "UCL D1", "Midlands", "Division One",
     "United Counties League Premier Division North"),

    # ── Wessex Football League ────────────────────────────────────────────
    ("Wessex Football League Division One",
     "Wessex D1", "South", "Division One",
     "Wessex Football League Premier Division"),

    # ── Western Football League ───────────────────────────────────────────
    ("Western Football League Division One",
     "Western D1", "South", "Division One",
     "Western Football League Premier Division"),
]


# ═══════════════════════════════════════════════════════════════════════════
# Upsert helpers
# ═══════════════════════════════════════════════════════════════════════════

def _upsert_league(
    session,
    *,
    name: str,
    short_name: str,
    step: int,
    region: str,
    division: str,
    parent_league_id: int | None = None,
) -> None:
    """Insert a league or update it if (name, season) already exists."""
    values = dict(
        name=name,
        short_name=short_name,
        step=step,
        region=region,
        division=division,
        season=SEASON,
        parent_league_id=parent_league_id,
    )
    stmt = pg_insert(League).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["name", "season"],
        set_={
            "short_name": stmt.excluded.short_name,
            "step": stmt.excluded.step,
            "region": stmt.excluded.region,
            "division": stmt.excluded.division,
            "parent_league_id": stmt.excluded.parent_league_id,
            "updated_at": func.now(),
        },
    )
    session.execute(stmt)


def _lookup_parent_ids(session) -> dict[str, int]:
    """Return a {name: id} map for all Step 5 leagues in this season."""
    rows = session.execute(
        select(League.name, League.id).where(
            League.step == 5,
            League.season == SEASON,
        )
    ).all()
    return {name: lid for name, lid in rows}


# ═══════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════

def load_seed_data() -> dict[int, int]:
    """Seed the leagues table with the full 2025-26 English pyramid.

    Returns:
        A dict mapping step number to the count of leagues upserted,
        e.g. ``{1: 1, 2: 2, 3: 4, 4: 8, 5: 16, 6: 19}``.
    """
    counts: dict[int, int] = {s: 0 for s in range(1, 7)}

    with get_session() as session:
        # ── Steps 1–5 (no parent_league_id) ───────────────────────────────
        for name, short_name, step, region, division in STEP_1_TO_5:
            _upsert_league(
                session,
                name=name,
                short_name=short_name,
                step=step,
                region=region,
                division=division,
            )
            counts[step] += 1

        # Flush so Step 5 rows get their IDs assigned
        session.flush()

        # ── Step 6 (with parent_league_id) ────────────────────────────────
        parent_map = _lookup_parent_ids(session)

        for name, short_name, region, division, parent_name in STEP_6:
            parent_id = parent_map.get(parent_name)
            if parent_id is None:
                logger.warning(
                    "Parent league %r not found for %r — inserting without parent",
                    parent_name, name,
                )

            _upsert_league(
                session,
                name=name,
                short_name=short_name,
                step=6,
                region=region,
                division=division,
                parent_league_id=parent_id,
            )
            counts[6] += 1

    # ── Summary log ───────────────────────────────────────────────────────
    total = sum(counts.values())
    logger.info("Pyramid seed complete — %d leagues upserted for season %s", total, SEASON)
    for step in sorted(counts):
        logger.info("  Step %d: %d leagues", step, counts[step])

    return counts


# ═══════════════════════════════════════════════════════════════════════════
# CLI entry point:  python -m src.seeds.pyramid_2025_26
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    counts = load_seed_data()
    total = sum(counts.values())
    print(f"\nDone — {total} leagues seeded across Steps 1-6 for {SEASON}.")
