"""Player deduplication and entity-resolution pipeline.

Finds duplicate player records across data sources and either
auto-merges them (high confidence) or queues them for human review
(borderline confidence).

Three-phase pipeline
--------------------
1. **Candidate generation** — uses blocking (same club, then same
   league) plus fuzzy name matching to find plausible pairs.
2. **Scoring** — each pair gets a numeric score based on name
   similarity, club, position, DOB, and nationality.
3. **Decision** — ``>= 90`` → auto-merge, ``70-89`` → review queue,
   ``< 70`` → skip.

The merge operation reassigns all child records (``player_seasons``,
``player_career``, ``match_appearances``, ``player_media``,
``shortlist_players``) from the secondary player to the primary,
fills NULL fields on the primary, and sets
``secondary.merged_into_id = primary.id``.

Usage::

    from src.etl.entity_resolution import run_entity_resolution
    stats = run_entity_resolution()

CLI::

    python scripts/run_entity_resolution.py
"""

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from rapidfuzz import fuzz
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from src.db.models import (
    MatchAppearance,
    MergeCandidate,
    MergeStatus,
    Player,
    PlayerCareer,
    PlayerMedia,
    PlayerSeason,
    ShortlistPlayer,
)
from src.db.session import get_session

logger = logging.getLogger(__name__)

# ── thresholds ────────────────────────────────────────────────────────────

NAME_BLOCK_THRESHOLD = 70
AUTO_MERGE_THRESHOLD = 90
REVIEW_THRESHOLD = 70
COMMIT_EVERY = 100

# ── scoring weights ───────────────────────────────────────────────────────

W_NAME = 1.0        # 0-100 from token_sort_ratio
W_SAME_CLUB = 30
W_SAME_POSITION = 15
W_DOB_MATCH = 40
W_DOB_CONFLICT = -100
W_SAME_NATIONALITY = 10


# ══════════════════════════════════════════════════════════════════════════
# 1. Candidate generation
# ══════════════════════════════════════════════════════════════════════════

def _load_active_players(session: Session) -> list[Player]:
    """Load all players that haven't been merged into another record."""
    return list(
        session.execute(
            select(Player).where(Player.merged_into_id.is_(None))
        ).scalars().all()
    )


def _load_existing_pairs(session: Session) -> set[tuple[int, int]]:
    """Return all (player_a_id, player_b_id) pairs already evaluated."""
    rows = session.execute(
        select(MergeCandidate.player_a_id, MergeCandidate.player_b_id)
    ).all()
    pairs: set[tuple[int, int]] = set()
    for a, b in rows:
        pairs.add((min(a, b), max(a, b)))
    return pairs


def generate_candidates(
    players: list[Player],
    existing_pairs: set[tuple[int, int]],
) -> list[tuple[Player, Player]]:
    """Find plausible duplicate pairs using blocking + fuzzy names.

    Blocking strategy:
    - Block 1: players at the **same club**.
    - Block 2: players in the **same league** (catches transfers
      within a league that created separate records).

    Within each block, only pairs whose ``token_sort_ratio`` on
    ``full_name`` exceeds ``NAME_BLOCK_THRESHOLD`` are kept.
    """
    candidates: dict[tuple[int, int], tuple[Player, Player]] = {}

    # Index players by club and by league
    by_club: dict[int | None, list[Player]] = defaultdict(list)
    by_league: dict[int | None, list[Player]] = defaultdict(list)

    for p in players:
        by_club[p.current_club_id].append(p)

    # Build league index (need club -> league mapping)
    club_league: dict[int, int | None] = {}
    for p in players:
        if p.current_club_id and p.current_club is not None:
            lid = getattr(p.current_club, "league_id", None)
            club_league[p.current_club_id] = lid
            if lid is not None:
                by_league[lid].append(p)

    def _add_block(block: list[Player]) -> None:
        for i in range(len(block)):
            for j in range(i + 1, len(block)):
                a, b = block[i], block[j]
                key = (min(a.id, b.id), max(a.id, b.id))
                if key in existing_pairs or key in candidates:
                    continue

                score = fuzz.token_sort_ratio(a.full_name, b.full_name)
                if score >= NAME_BLOCK_THRESHOLD:
                    candidates[key] = (a, b)

    # Block 1 — same club
    for club_id, group in by_club.items():
        if club_id is None or len(group) < 2:
            continue
        _add_block(group)

    # Block 2 — same league (but different clubs)
    for league_id, group in by_league.items():
        if league_id is None or len(group) < 2:
            continue
        _add_block(group)

    logger.info(
        "Generated %d candidate pairs from %d players",
        len(candidates), len(players),
    )
    return list(candidates.values())


# ══════════════════════════════════════════════════════════════════════════
# 2. Scoring
# ══════════════════════════════════════════════════════════════════════════

def score_pair(a: Player, b: Player) -> tuple[int, dict[str, Any]]:
    """Compute a similarity score and a reasons dict for a pair.

    Returns ``(score, reasons)`` where ``reasons`` captures each
    contributing factor for audit/display purposes.
    """
    reasons: dict[str, Any] = {}

    # Name similarity
    name_score = fuzz.token_sort_ratio(a.full_name, b.full_name)
    reasons["name_similarity"] = name_score
    total = name_score * W_NAME

    # Same club
    if (
        a.current_club_id is not None
        and a.current_club_id == b.current_club_id
    ):
        total += W_SAME_CLUB
        reasons["same_club"] = True
    else:
        reasons["same_club"] = False

    # Same position
    if (
        a.position_primary is not None
        and a.position_primary == b.position_primary
    ):
        total += W_SAME_POSITION
        reasons["same_position"] = True
    else:
        reasons["same_position"] = False

    # DOB
    if a.date_of_birth and b.date_of_birth:
        if a.date_of_birth == b.date_of_birth:
            total += W_DOB_MATCH
            reasons["dob"] = "match"
        else:
            total += W_DOB_CONFLICT
            reasons["dob"] = "conflict"
    else:
        reasons["dob"] = "unknown"

    # Nationality
    if (
        a.nationality
        and b.nationality
        and a.nationality.lower() == b.nationality.lower()
    ):
        total += W_SAME_NATIONALITY
        reasons["same_nationality"] = True
    else:
        reasons["same_nationality"] = False

    final_score = max(0, min(int(total), 200))
    reasons["total_score"] = final_score
    return final_score, reasons


# ══════════════════════════════════════════════════════════════════════════
# 3. Decision + Merge
# ══════════════════════════════════════════════════════════════════════════

def _pick_primary(a: Player, b: Player, session: Session) -> tuple[Player, Player]:
    """Choose which player record to keep.

    Prefers the record with the highest-confidence season data.
    Falls back to the record with more non-null fields, then lower ID.
    """
    def _best_confidence(player_id: int) -> int:
        row = session.execute(
            select(PlayerSeason.confidence_score)
            .where(PlayerSeason.player_id == player_id)
            .order_by(PlayerSeason.confidence_score.desc().nulls_last())
            .limit(1)
        ).scalar_one_or_none()
        return row or 0

    conf_a = _best_confidence(a.id)
    conf_b = _best_confidence(b.id)
    if conf_a != conf_b:
        return (a, b) if conf_a >= conf_b else (b, a)

    def _field_count(p: Player) -> int:
        fields = [
            p.full_name, p.first_name, p.last_name, p.date_of_birth,
            p.nationality, p.position_primary, p.position_detail,
            p.height_cm, p.weight_kg, p.preferred_foot, p.bio,
            p.profile_photo_url,
        ]
        return sum(1 for f in fields if f is not None)

    fc_a = _field_count(a)
    fc_b = _field_count(b)
    if fc_a != fc_b:
        return (a, b) if fc_a >= fc_b else (b, a)

    return (a, b) if a.id <= b.id else (b, a)


def merge_players(
    primary: Player,
    secondary: Player,
    session: Session,
) -> None:
    """Merge *secondary* into *primary*.

    - Fills NULL fields on primary from secondary.
    - Reassigns all child rows to primary.
    - Sets ``secondary.merged_into_id = primary.id``.
    """
    # Fill NULLs on primary
    merge_fields = [
        "first_name", "last_name", "date_of_birth", "nationality",
        "position_primary", "position_detail", "height_cm", "weight_kg",
        "preferred_foot", "profile_photo_url", "bio",
    ]
    for field in merge_fields:
        if getattr(primary, field) is None and getattr(secondary, field) is not None:
            setattr(primary, field, getattr(secondary, field))

    sid = secondary.id
    pid = primary.id

    # Reassign player_seasons (handle unique-constraint conflicts)
    _reassign_seasons(session, sid, pid)

    # Reassign player_career
    session.execute(
        update(PlayerCareer)
        .where(PlayerCareer.player_id == sid)
        .values(player_id=pid)
    )

    # Reassign match_appearances
    session.execute(
        update(MatchAppearance)
        .where(MatchAppearance.player_id == sid)
        .values(player_id=pid)
    )

    # Reassign player_media
    session.execute(
        update(PlayerMedia)
        .where(PlayerMedia.player_id == sid)
        .values(player_id=pid)
    )

    # Reassign shortlist_players (skip conflicts)
    _reassign_shortlist_entries(session, sid, pid)

    # Mark secondary as merged
    secondary.merged_into_id = pid

    logger.info(
        "Merged player #%d (%s) into #%d (%s)",
        sid, secondary.full_name, pid, primary.full_name,
    )


def _reassign_seasons(session: Session, old_id: int, new_id: int) -> None:
    """Move season records, skipping rows that would violate the
    unique constraint ``(player_id, club_id, season, data_source)``."""
    existing = session.execute(
        select(
            PlayerSeason.club_id,
            PlayerSeason.season,
            PlayerSeason.data_source,
        ).where(PlayerSeason.player_id == new_id)
    ).all()
    existing_keys = {(r.club_id, r.season, r.data_source) for r in existing}

    to_move = session.execute(
        select(PlayerSeason).where(PlayerSeason.player_id == old_id)
    ).scalars().all()

    for ps in to_move:
        key = (ps.club_id, ps.season, ps.data_source)
        if key in existing_keys:
            session.delete(ps)
        else:
            ps.player_id = new_id
            existing_keys.add(key)


def _reassign_shortlist_entries(
    session: Session, old_id: int, new_id: int,
) -> None:
    """Move shortlist entries, deleting any that would cause a
    unique-constraint violation."""
    existing = session.execute(
        select(ShortlistPlayer.shortlist_id)
        .where(ShortlistPlayer.player_id == new_id)
    ).scalars().all()
    existing_sids = set(existing)

    to_move = session.execute(
        select(ShortlistPlayer).where(ShortlistPlayer.player_id == old_id)
    ).scalars().all()

    for sp in to_move:
        if sp.shortlist_id in existing_sids:
            session.delete(sp)
        else:
            sp.player_id = new_id
            existing_sids.add(sp.shortlist_id)


# ══════════════════════════════════════════════════════════════════════════
# Public entry point
# ══════════════════════════════════════════════════════════════════════════

def run_entity_resolution() -> dict[str, int]:
    """Execute the full entity-resolution pipeline.

    Returns a counters dict with keys:
    ``candidates``, ``auto_merged``, ``queued``, ``skipped``.
    """
    counters = {
        "candidates": 0,
        "auto_merged": 0,
        "queued": 0,
        "skipped": 0,
    }

    with get_session() as session:
        players = _load_active_players(session)
        logger.info("Loaded %d active players", len(players))

        existing_pairs = _load_existing_pairs(session)
        logger.info("Found %d already-evaluated pairs", len(existing_pairs))

        pairs = generate_candidates(players, existing_pairs)
        counters["candidates"] = len(pairs)

        if not pairs:
            logger.info("No new candidate pairs found")
            return counters

        for i, (a, b) in enumerate(pairs, 1):
            score, reasons = score_pair(a, b)

            # Normalise key so player_a_id < player_b_id
            pa, pb = (a, b) if a.id < b.id else (b, a)

            if score >= AUTO_MERGE_THRESHOLD:
                primary, secondary = _pick_primary(pa, pb, session)
                merge_players(primary, secondary, session)

                mc = MergeCandidate(
                    player_a_id=pa.id,
                    player_b_id=pb.id,
                    score=score,
                    match_reasons=reasons,
                    status=MergeStatus.MERGED,
                    reviewed_by="auto",
                    reviewed_at=datetime.now(timezone.utc),
                )
                session.add(mc)
                counters["auto_merged"] += 1

            elif score >= REVIEW_THRESHOLD:
                mc = MergeCandidate(
                    player_a_id=pa.id,
                    player_b_id=pb.id,
                    score=score,
                    match_reasons=reasons,
                    status=MergeStatus.PENDING,
                )
                session.add(mc)
                counters["queued"] += 1

            else:
                counters["skipped"] += 1

            if i % COMMIT_EVERY == 0:
                session.flush()
                logger.info("  Progress: %d / %d pairs", i, len(pairs))

    logger.info(
        "Entity resolution complete: %d candidates, %d auto-merged, "
        "%d queued for review, %d skipped",
        counters["candidates"], counters["auto_merged"],
        counters["queued"], counters["skipped"],
    )
    return counters
