"""Recalculate overall confidence scores for every active player.

The score is a composite 0.00–5.00 value stored on the player record,
built from six factors:

1. **Source diversity** — how many distinct data sources contributed
   season data for this player.
2. **Data completeness** — percentage of key biographical fields that
   are non-null.
3. **Data freshness** — how recently the player record was updated.
4. **Career history** — whether at least one ``player_career`` entry
   exists.
5. **Season stats** — whether at least one ``player_seasons`` entry
   exists.
6. **Photo** — whether a profile photo URL is set.

A JSONB breakdown is stored alongside the final score for
transparency and debugging.

Usage::

    from src.etl.confidence import recalculate_confidence
    stats = recalculate_confidence()

CLI::

    python scripts/run_confidence.py
"""

import logging
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from sqlalchemy import distinct, func, select
from sqlalchemy.orm import Session

from src.db.models import (
    Club,
    League,
    Player,
    PlayerCareer,
    PlayerSeason,
)
from src.db.session import get_session

logger = logging.getLogger(__name__)

COMMIT_EVERY = 200

# ── key fields for completeness check ─────────────────────────────────────

_KEY_FIELDS = [
    "full_name",
    "position_primary",
    "current_club_id",
    "date_of_birth",
    "nationality",
    "height_cm",
]


# ══════════════════════════════════════════════════════════════════════════
# Scoring helpers
# ══════════════════════════════════════════════════════════════════════════

def _score_source_diversity(
    source_count: int,
    best_source_confidence: int,
) -> tuple[float, dict[str, Any]]:
    """Factor 1: number of distinct data sources.

    Returns ``(points, detail)`` where ``points`` is the base
    confidence plus a multi-source bonus.
    """
    base = float(min(best_source_confidence, 5))

    if source_count >= 3:
        bonus = 1.0
    elif source_count == 2:
        bonus = 0.5
    else:
        bonus = 0.0

    score = min(base + bonus, 5.0)
    return score, {
        "source_count": source_count,
        "best_source_confidence": best_source_confidence,
        "bonus": bonus,
        "score": round(score, 2),
    }


def _score_completeness(player: Player) -> tuple[float, dict[str, Any]]:
    """Factor 2: percentage of key fields that are non-null."""
    filled = 0
    field_status: dict[str, bool] = {}
    for field in _KEY_FIELDS:
        val = getattr(player, field, None)
        present = val is not None
        field_status[field] = present
        if present:
            filled += 1

    pct = filled / len(_KEY_FIELDS) if _KEY_FIELDS else 0.0

    if pct >= 1.0:
        bonus = 0.5
    elif pct >= 0.5:
        bonus = 0.25
    else:
        bonus = 0.0

    return bonus, {
        "filled": filled,
        "total": len(_KEY_FIELDS),
        "pct": round(pct * 100, 1),
        "bonus": bonus,
        "fields": field_status,
    }


def _score_freshness(player: Player) -> tuple[float, dict[str, Any]]:
    """Factor 3: how recently the record was updated."""
    now = datetime.now(timezone.utc)
    updated = player.updated_at
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=timezone.utc)

    age_days = (now - updated).days

    if age_days <= 30:
        bonus = 0.25
        label = "fresh"
    elif age_days <= 90:
        bonus = 0.0
        label = "recent"
    else:
        bonus = -0.5
        label = "stale"

    return bonus, {
        "days_since_update": age_days,
        "label": label,
        "bonus": bonus,
    }


def _score_career(has_career: bool) -> tuple[float, dict[str, Any]]:
    """Factor 4: has at least one career history entry."""
    bonus = 0.25 if has_career else 0.0
    return bonus, {"has_career": has_career, "bonus": bonus}


def _score_stats(has_stats: bool) -> tuple[float, dict[str, Any]]:
    """Factor 5: has at least one season-stats record."""
    bonus = 0.25 if has_stats else 0.0
    return bonus, {"has_stats": has_stats, "bonus": bonus}


def _score_photo(player: Player) -> tuple[float, dict[str, Any]]:
    """Factor 6: has a profile photo."""
    has = bool(player.profile_photo_url)
    bonus = 0.25 if has else 0.0
    return bonus, {"has_photo": has, "bonus": bonus}


# ══════════════════════════════════════════════════════════════════════════
# Batch pre-loading (avoids N+1 queries)
# ══════════════════════════════════════════════════════════════════════════

def _preload_source_info(
    session: Session,
) -> tuple[dict[int, int], dict[int, int]]:
    """Return per-player source count and best confidence score.

    Returns:
        source_counts: ``{player_id: number_of_distinct_data_sources}``
        best_confidence: ``{player_id: max_confidence_score}``
    """
    rows = session.execute(
        select(
            PlayerSeason.player_id,
            func.count(distinct(PlayerSeason.data_source)).label("src_cnt"),
            func.max(PlayerSeason.confidence_score).label("best_conf"),
        )
        .group_by(PlayerSeason.player_id)
    ).all()

    source_counts: dict[int, int] = {}
    best_confidence: dict[int, int] = {}
    for r in rows:
        source_counts[r.player_id] = r.src_cnt
        best_confidence[r.player_id] = r.best_conf or 1

    return source_counts, best_confidence


def _preload_has_career(session: Session) -> set[int]:
    """Return set of player_ids that have at least one career entry."""
    rows = session.execute(
        select(distinct(PlayerCareer.player_id))
    ).scalars().all()
    return set(rows)


def _preload_has_stats(session: Session) -> set[int]:
    """Return set of player_ids that have at least one season record."""
    rows = session.execute(
        select(distinct(PlayerSeason.player_id))
    ).scalars().all()
    return set(rows)


def _preload_step_map(session: Session) -> dict[int, int]:
    """Return ``{club_id: step}`` for step-level reporting."""
    rows = session.execute(
        select(Club.id, League.step)
        .outerjoin(League, Club.league_id == League.id)
    ).all()
    return {r.id: r.step for r in rows if r.step is not None}


# ══════════════════════════════════════════════════════════════════════════
# Public entry point
# ══════════════════════════════════════════════════════════════════════════

def recalculate_confidence() -> dict[str, Any]:
    """Recalculate ``overall_confidence`` for every active player.

    Returns a stats dict suitable for the CLI summary.
    """
    with get_session() as session:
        # Pre-load aggregate data to avoid N+1
        source_counts, best_confidence = _preload_source_info(session)
        career_set = _preload_has_career(session)
        stats_set = _preload_has_stats(session)
        step_map = _preload_step_map(session)

        players: list[Player] = list(
            session.execute(
                select(Player).where(Player.merged_into_id.is_(None))
            ).scalars().all()
        )

        logger.info("Scoring %d active players…", len(players))

        distribution: Counter[int] = Counter()
        step_scores: dict[int, list[float]] = defaultdict(list)
        lowest: list[tuple[float, int, str, str]] = []

        for i, player in enumerate(players, 1):
            pid = player.id

            # Factor 1 — source diversity
            src_cnt = source_counts.get(pid, 0)
            best_conf = best_confidence.get(pid, 1)
            base_score, d_source = _score_source_diversity(src_cnt, best_conf)

            # Factors 2–6
            b_complete, d_complete = _score_completeness(player)
            b_fresh, d_fresh = _score_freshness(player)
            b_career, d_career = _score_career(pid in career_set)
            b_stats, d_stats = _score_stats(pid in stats_set)
            b_photo, d_photo = _score_photo(player)

            raw_total = base_score + b_complete + b_fresh + b_career + b_stats + b_photo
            final = max(0.0, min(raw_total, 5.0))
            final_dec = Decimal(str(final)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP,
            )

            detail = {
                "source_diversity": d_source,
                "completeness": d_complete,
                "freshness": d_fresh,
                "career_history": d_career,
                "season_stats": d_stats,
                "photo": d_photo,
                "raw_total": round(raw_total, 2),
                "final": float(final_dec),
            }

            player.overall_confidence = final_dec
            player.confidence_detail = detail

            # Tracking
            bucket = int(final_dec)
            distribution[bucket] += 1

            club_id = player.current_club_id
            if club_id and club_id in step_map:
                step_scores[step_map[club_id]].append(float(final_dec))

            lowest.append((float(final_dec), pid, player.full_name, ""))

            if i % COMMIT_EVERY == 0:
                session.flush()
                logger.info("  Progress: %d / %d", i, len(players))

    # Sort lowest for reporting
    lowest.sort(key=lambda x: x[0])

    avg_by_step: dict[int, float] = {}
    for step, scores in sorted(step_scores.items()):
        avg_by_step[step] = round(sum(scores) / len(scores), 2) if scores else 0.0

    result = {
        "total_scored": len(players),
        "distribution": dict(sorted(distribution.items())),
        "avg_by_step": avg_by_step,
        "lowest_20": [
            {"score": s, "player_id": pid, "name": name}
            for s, pid, name, _ in lowest[:20]
        ],
    }

    logger.info(
        "Confidence scoring complete: %d players scored", len(players),
    )
    return result
