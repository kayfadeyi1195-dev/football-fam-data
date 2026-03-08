"""Full-text and filtered player search.

Uses PostgreSQL's ``tsvector`` / ``tsquery`` for text search combined
with standard SQL filters — everything runs in a single database
query so filtering is never done in Python.

Usage::

    from src.search import PlayerSearch

    ps = PlayerSearch()
    results = ps.search_players(query="striker left foot", position="FWD", step=5)
    for player in results["results"]:
        print(player["full_name"], player["club_name"], player["appearances"])

CLI smoke-test::

    python -m src.search "fast winger"
    python -m src.search --position DEF --step 5
"""

import logging
import math
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from sqlalchemy import Float, Select, and_, case, cast, desc, func, literal, nulls_last, or_, select, text
from sqlalchemy.orm import Session, aliased

from src.db.models import Club, League, Player, PlayerSeason
from src.db.session import get_session

logger = logging.getLogger(__name__)

CURRENT_SEASON = "2024-25"


@dataclass(frozen=True)
class SearchParams:
    """Validated, immutable holder for every search parameter."""

    query: str | None = None
    position: str | None = None
    step: int | None = None
    league_id: int | None = None
    club_id: int | None = None
    min_age: int | None = None
    max_age: int | None = None
    nationality: str | None = None
    availability: str | None = None
    min_confidence: float | None = None
    min_appearances: int | None = None
    min_goals: int | None = None
    has_photo: bool | None = None
    sort_by: str = "relevance"
    page: int = 1
    per_page: int = 20


class PlayerSearch:
    """Build and execute a single SQL query that combines full-text
    search with structured filters, returning paginated results.
    """

    # Valid sort options and their ORDER BY clauses (built dynamically)
    _SORT_OPTIONS = {"relevance", "name", "age", "confidence"}

    def search_players(self, **kwargs: Any) -> dict[str, Any]:
        """Search and filter players.

        All keyword arguments map 1:1 to :class:`SearchParams` fields.
        Returns::

            {
                "results": [dict, ...],
                "total": int,
                "page": int,
                "pages": int,
            }
        """
        params = self._validate(kwargs)

        with get_session() as session:
            query = self._build_query(params)
            count_query = self._build_count_query(params)

            total: int = session.execute(count_query).scalar_one()
            pages = max(1, math.ceil(total / params.per_page))
            page = min(params.page, pages) if pages else 1
            offset = (page - 1) * params.per_page

            rows = session.execute(
                query.offset(offset).limit(params.per_page)
            ).all()

            results = [self._row_to_dict(row) for row in rows]

        return {
            "results": results,
            "total": total,
            "page": page,
            "pages": pages,
        }

    # ─── validation ───────────────────────────────────────────────────

    @staticmethod
    def _validate(raw: dict[str, Any]) -> SearchParams:
        sort_by = raw.get("sort_by", "relevance")
        if sort_by not in PlayerSearch._SORT_OPTIONS:
            sort_by = "relevance"

        page = max(1, int(raw.get("page", 1)))
        per_page = min(100, max(1, int(raw.get("per_page", 20))))

        position = raw.get("position")
        if position:
            position = position.upper()
            if position not in ("GK", "DEF", "MID", "FWD"):
                position = None

        step = raw.get("step")
        if step is not None:
            step = int(step)
            if step < 1 or step > 6:
                step = None

        return SearchParams(
            query=raw.get("query") or None,
            position=position,
            step=step,
            league_id=raw.get("league_id"),
            club_id=raw.get("club_id"),
            min_age=raw.get("min_age"),
            max_age=raw.get("max_age"),
            nationality=raw.get("nationality"),
            availability=raw.get("availability"),
            min_confidence=raw.get("min_confidence"),
            min_appearances=raw.get("min_appearances"),
            min_goals=raw.get("min_goals"),
            has_photo=raw.get("has_photo"),
            sort_by=sort_by,
            page=page,
            per_page=per_page,
        )

    # ─── query building ───────────────────────────────────────────────

    def _build_base(self, params: SearchParams) -> tuple[list, list, list]:
        """Return (columns, joins-already-applied, where-clauses)
        that are shared between the data query and the count query.
        """
        LatestSeason = aliased(PlayerSeason, flat=True)

        # Subquery: latest season per player (highest confidence, most
        # recent season string, most appearances — deterministic pick)
        latest_sq = (
            select(
                PlayerSeason.player_id,
                func.max(PlayerSeason.id).label("max_ps_id"),
            )
            .where(PlayerSeason.season == CURRENT_SEASON)
            .group_by(PlayerSeason.player_id)
            .subquery("latest_season")
        )

        joins: list[tuple] = []
        wheres: list = []

        # Always LEFT JOIN club and league for display columns
        # (Player -> Club -> League)
        joins.append(("club", Club, Player.current_club_id == Club.id))
        joins.append(("league", League, Club.league_id == League.id))
        joins.append(("latest_sq", latest_sq, Player.id == latest_sq.c.player_id))
        joins.append(("ps", LatestSeason, LatestSeason.id == latest_sq.c.max_ps_id))

        # Exclude merged (soft-deleted) players
        wheres.append(Player.merged_into_id.is_(None))

        # ── text search ───────────────────────────────────────────────
        ts_rank = cast(literal(0), Float)  # default when no query
        if params.query:
            tsquery = func.plainto_tsquery("english", params.query)
            wheres.append(Player.search_vector.op("@@")(tsquery))
            ts_rank = func.ts_rank_cd(Player.search_vector, tsquery)

        # ── structured filters ────────────────────────────────────────
        if params.position:
            wheres.append(Player.position_primary == params.position)

        if params.club_id is not None:
            wheres.append(Player.current_club_id == params.club_id)

        if params.league_id is not None:
            wheres.append(Club.league_id == params.league_id)

        if params.step is not None:
            wheres.append(League.step == params.step)

        if params.nationality:
            wheres.append(func.lower(Player.nationality) == params.nationality.lower())

        if params.availability:
            wheres.append(Player.availability == params.availability)

        if params.has_photo is True:
            wheres.append(Player.profile_photo_url.isnot(None))
            wheres.append(Player.profile_photo_url != "")
        elif params.has_photo is False:
            wheres.append(
                or_(
                    Player.profile_photo_url.is_(None),
                    Player.profile_photo_url == "",
                )
            )

        if params.min_age is not None or params.max_age is not None:
            today = date.today()
            if params.max_age is not None:
                earliest_dob = today - timedelta(days=(params.max_age + 1) * 365.25)
                wheres.append(Player.date_of_birth > earliest_dob)
            if params.min_age is not None:
                latest_dob = today - timedelta(days=params.min_age * 365.25)
                wheres.append(Player.date_of_birth <= latest_dob)
            wheres.append(Player.date_of_birth.isnot(None))

        if params.min_confidence is not None:
            wheres.append(LatestSeason.confidence_score >= params.min_confidence)

        if params.min_appearances is not None:
            wheres.append(LatestSeason.appearances >= params.min_appearances)

        if params.min_goals is not None:
            wheres.append(LatestSeason.goals >= params.min_goals)

        return ts_rank, joins, wheres, LatestSeason

    def _apply_joins(self, stmt: Select, joins: list) -> Select:
        """Apply LEFT JOINs in the correct order."""
        for tag, target, onclause in joins:
            stmt = stmt.outerjoin(target, onclause)
        return stmt

    def _build_query(self, params: SearchParams) -> Select:
        """Build the paginated data query returning all display columns."""
        ts_rank, joins, wheres, LatestSeason = self._build_base(params)

        # Age calculation (years from DOB to today)
        age_expr = func.extract(
            "year",
            func.age(func.current_date(), Player.date_of_birth),
        )

        stmt = (
            select(
                Player.id,
                Player.full_name,
                Player.first_name,
                Player.last_name,
                Player.date_of_birth,
                age_expr.label("age"),
                Player.nationality,
                Player.position_primary,
                Player.position_detail,
                Player.height_cm,
                Player.weight_kg,
                Player.preferred_foot,
                Player.contract_status,
                Player.availability,
                Player.profile_photo_url,
                Player.is_verified,
                Club.id.label("club_id"),
                Club.name.label("club_name"),
                Club.logo_url.label("club_logo_url"),
                League.id.label("league_id"),
                League.name.label("league_name"),
                League.step.label("league_step"),
                LatestSeason.season.label("season"),
                LatestSeason.appearances,
                LatestSeason.starts,
                LatestSeason.goals,
                LatestSeason.assists,
                LatestSeason.yellow_cards,
                LatestSeason.red_cards,
                LatestSeason.clean_sheets,
                LatestSeason.minutes_played,
                LatestSeason.confidence_score,
                ts_rank.label("relevance"),
            )
        )

        stmt = self._apply_joins(stmt, joins)

        if wheres:
            stmt = stmt.where(and_(*wheres))

        # ── ordering ──────────────────────────────────────────────────
        if params.sort_by == "relevance" and params.query:
            stmt = stmt.order_by(desc("relevance"), Player.full_name)
        elif params.sort_by == "name":
            stmt = stmt.order_by(Player.full_name)
        elif params.sort_by == "age":
            stmt = stmt.order_by(nulls_last(Player.date_of_birth.asc()))
        elif params.sort_by == "confidence":
            stmt = stmt.order_by(
                nulls_last(desc(LatestSeason.confidence_score)),
                Player.full_name,
            )
        else:
            stmt = stmt.order_by(Player.full_name)

        return stmt

    def _build_count_query(self, params: SearchParams) -> Select:
        """Build a lightweight COUNT(*) query with the same filters."""
        _, joins, wheres, _ = self._build_base(params)

        stmt = select(func.count(Player.id.distinct()))
        stmt = self._apply_joins(stmt, joins)

        if wheres:
            stmt = stmt.where(and_(*wheres))

        return stmt

    # ─── result mapping ───────────────────────────────────────────────

    @staticmethod
    def _row_to_dict(row: Any) -> dict[str, Any]:
        """Map a result row to a clean JSON-serialisable dict."""
        def _val(v: Any) -> Any:
            if isinstance(v, date):
                return v.isoformat()
            return v

        return {
            "id": row.id,
            "full_name": row.full_name,
            "first_name": row.first_name,
            "last_name": row.last_name,
            "date_of_birth": _val(row.date_of_birth),
            "age": int(row.age) if row.age is not None else None,
            "nationality": row.nationality,
            "position_primary": row.position_primary,
            "position_detail": row.position_detail,
            "height_cm": row.height_cm,
            "weight_kg": row.weight_kg,
            "preferred_foot": row.preferred_foot,
            "contract_status": row.contract_status,
            "availability": row.availability,
            "profile_photo_url": row.profile_photo_url,
            "is_verified": row.is_verified,
            "club": {
                "id": row.club_id,
                "name": row.club_name,
                "logo_url": row.club_logo_url,
            } if row.club_id else None,
            "league": {
                "id": row.league_id,
                "name": row.league_name,
                "step": row.league_step,
            } if row.league_id else None,
            "season_stats": {
                "season": row.season,
                "appearances": row.appearances,
                "starts": row.starts,
                "goals": row.goals,
                "assists": row.assists,
                "yellow_cards": row.yellow_cards,
                "red_cards": row.red_cards,
                "clean_sheets": row.clean_sheets,
                "minutes_played": row.minutes_played,
                "confidence_score": row.confidence_score,
            } if row.season else None,
            "relevance": float(row.relevance) if row.relevance else None,
        }


# ─── CLI ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import json

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    parser = argparse.ArgumentParser(description="Search Football Fam players")
    parser.add_argument("query", nargs="?", default=None, help="Free-text search query")
    parser.add_argument("--position", type=str, help="GK / DEF / MID / FWD")
    parser.add_argument("--step", type=int, help="Pyramid step (1-6)")
    parser.add_argument("--league-id", type=int, help="League ID")
    parser.add_argument("--club-id", type=int, help="Club ID")
    parser.add_argument("--min-age", type=int, help="Minimum age")
    parser.add_argument("--max-age", type=int, help="Maximum age")
    parser.add_argument("--nationality", type=str, help="Nationality filter")
    parser.add_argument("--availability", type=str, help="Availability status")
    parser.add_argument("--min-confidence", type=float, help="Minimum confidence score")
    parser.add_argument("--min-appearances", type=int, help="Minimum appearances")
    parser.add_argument("--min-goals", type=int, help="Minimum goals")
    parser.add_argument("--has-photo", action="store_true", default=None, help="Only with photo")
    parser.add_argument("--sort", type=str, default="relevance",
                        choices=["relevance", "name", "age", "confidence"])
    parser.add_argument("--page", type=int, default=1)
    parser.add_argument("--per-page", type=int, default=10)

    args = parser.parse_args()

    search_kwargs: dict[str, Any] = {
        "query": args.query,
        "position": args.position,
        "step": args.step,
        "league_id": args.league_id,
        "club_id": args.club_id,
        "min_age": args.min_age,
        "max_age": args.max_age,
        "nationality": args.nationality,
        "availability": args.availability,
        "min_confidence": args.min_confidence,
        "min_appearances": args.min_appearances,
        "min_goals": args.min_goals,
        "has_photo": True if args.has_photo else None,
        "sort_by": args.sort,
        "page": args.page,
        "per_page": args.per_page,
    }
    # Remove None values so defaults apply
    search_kwargs = {k: v for k, v in search_kwargs.items() if v is not None}

    ps = PlayerSearch()
    result = ps.search_players(**search_kwargs)

    print(f"\n  Total: {result['total']}  |  Page {result['page']}/{result['pages']}\n")

    for p in result["results"]:
        club = p["club"]["name"] if p["club"] else "—"
        step = f"Step {p['league']['step']}" if p["league"] else "—"
        age = f"age {p['age']}" if p["age"] else ""
        pos = p["position_primary"] or ""
        stats = ""
        if p["season_stats"]:
            s = p["season_stats"]
            stats = f"  Apps:{s['appearances'] or 0}  Goals:{s['goals'] or 0}"
        rel = f"  rel:{p['relevance']:.3f}" if p.get("relevance") else ""

        print(f"  {p['full_name']:<30s}  {pos:<3s}  {age:<8s}  "
              f"{club:<25s}  {step}{stats}{rel}")

    if not result["results"]:
        print("  No players found.")

    print()
