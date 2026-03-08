"""Raw data staging.

Functions in this module take data straight from an API client or
scraper and insert it into the ``staging_raw`` table as JSONB.
This gives us a durable copy of every raw payload so we can always
re-process from scratch.

Uses ``INSERT … ON CONFLICT DO UPDATE`` so re-runs replace the
stored ``raw_data`` and reset ``processed`` to ``False``, ensuring
the record will be picked up by the next transform pass.
"""

import logging
from typing import Any

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db.models import StagingRaw
from src.db.session import get_session

logger = logging.getLogger(__name__)


def stage_records(
    source: str,
    entity_type: str,
    records: list[dict[str, Any]],
    id_field: str = "id",
) -> int:
    """Upsert a batch of raw records into the staging table.

    On conflict (same *source*, *entity_type*, *external_id*) the
    existing row's ``raw_data`` is replaced with the new payload and
    ``processed`` is reset to ``False`` so transforms pick it up again.

    Args:
        source: Identifier for the data source (e.g. ``"pitchero"``).
        entity_type: What the records represent (e.g. ``"player"``).
        records: List of raw dicts as returned by the source.
        id_field: Key within each dict that holds the external ID.

    Returns:
        The number of records staged.
    """
    staged = 0
    with get_session() as session:
        for record in records:
            values = dict(
                source=source,
                source_entity_type=entity_type,
                external_id=str(record.get(id_field, "")),
                raw_data=record,
                processed=False,
            )
            stmt = pg_insert(StagingRaw).values(**values)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_staging_source_type_extid",
                set_={
                    "raw_data": stmt.excluded.raw_data,
                    "processed": False,
                    "processed_at": None,
                    "error_message": None,
                    "updated_at": func.now(),
                },
            )
            session.execute(stmt)
            staged += 1

    logger.info("Staged %d %s records from %s", staged, entity_type, source)
    return staged
