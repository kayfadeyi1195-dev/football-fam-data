"""Data transformation and cleaning.

Reads un-processed rows from ``staging_raw``, normalises names,
fills missing fields, and upserts into the core tables (players, clubs,
etc.).
"""

import logging
import re
from typing import Any

import pandas as pd

from src.db.models import StagingRaw, Player, Club
from src.db.session import get_session

logger = logging.getLogger(__name__)


def normalise_name(raw: str) -> str:
    """Lowercase, strip whitespace, and collapse multiple spaces."""
    name = raw.strip().lower()
    name = re.sub(r"\s+", " ", name)
    return name


def transform_players(source: str | None = None) -> int:
    """Process staged player records into the players table.

    Args:
        source: Optionally limit to records from a single source.

    Returns:
        Number of player records processed.
    """
    processed = 0

    with get_session() as session:
        query = session.query(StagingRaw).filter(
            StagingRaw.source_entity_type == "player",
            StagingRaw.processed == False,  # noqa: E712
        )
        if source:
            query = query.filter(StagingRaw.source == source)

        for row in query.all():
            data: dict[str, Any] = row.raw_data

            # TODO: Map source-specific field names to Player columns.
            logger.debug("Transforming player record id=%s source=%s", row.id, row.source)

            row.processed = True
            processed += 1

    logger.info("Transformed %d player records", processed)
    return processed


def transform_clubs(source: str | None = None) -> int:
    """Process staged club records into the clubs table.

    Args:
        source: Optionally limit to records from a single source.

    Returns:
        Number of club records processed.
    """
    processed = 0

    with get_session() as session:
        query = session.query(StagingRaw).filter(
            StagingRaw.source_entity_type == "club",
            StagingRaw.processed == False,  # noqa: E712
        )
        if source:
            query = query.filter(StagingRaw.source == source)

        for row in query.all():
            data: dict[str, Any] = row.raw_data

            # TODO: Map source-specific field names to Club columns.
            logger.debug("Transforming club record id=%s source=%s", row.id, row.source)

            row.processed = True
            processed += 1

    logger.info("Transformed %d club records", processed)
    return processed
