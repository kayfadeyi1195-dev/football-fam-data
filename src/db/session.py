"""Database engine and session management.

Provides three things the rest of the codebase needs:

* ``get_engine()`` — returns a shared (singleton) SQLAlchemy ``Engine``.
* ``get_session()`` — a context manager that yields a ``Session`` which
  auto-commits on success and rolls back on error.
* ``init_db()``     — creates every table defined in ``models.py``
  (handy for quick local development; in production use Alembic instead).

Usage::

    from src.db.session import get_session

    with get_session() as session:
        clubs = session.query(Club).all()
"""

import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import Engine, create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from src.config import DATABASE_URL

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Singleton engine
# ---------------------------------------------------------------------------

_engine: Engine | None = None


def get_engine() -> Engine:
    """Return the shared SQLAlchemy engine, creating it on first call.

    The engine is stored in a module-level variable so every part of the
    application re-uses the same connection pool.
    """
    global _engine
    if _engine is None:
        logger.info("Creating database engine for %s…", DATABASE_URL[:25])
        _engine = create_engine(
            DATABASE_URL,
            echo=False,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        logger.info("Database engine created")
    return _engine


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

def _session_factory() -> sessionmaker[Session]:
    """Build a sessionmaker bound to the singleton engine."""
    return sessionmaker(bind=get_engine(), expire_on_commit=False)


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Yield a database session that commits on success, rolls back on error.

    Example::

        with get_session() as session:
            session.add(some_object)
            # auto-commits when the block exits normally
    """
    factory = _session_factory()
    session = factory()
    logger.debug("Database session opened")
    try:
        yield session
        session.commit()
        logger.debug("Database session committed")
    except Exception:
        session.rollback()
        logger.exception("Database session rolled back due to error")
        raise
    finally:
        session.close()
        logger.debug("Database session closed")


# ---------------------------------------------------------------------------
# Quick dev helper
# ---------------------------------------------------------------------------

def init_db() -> list[str]:
    """Create all tables defined in models.py and return their names.

    This is a convenience for local development.  In production you
    should use Alembic migrations (``alembic upgrade head``) instead.
    """
    from src.db.models import Base  # imported here to avoid circular imports

    engine = get_engine()
    logger.info("Creating all tables…")
    Base.metadata.create_all(bind=engine)

    table_names = inspect(engine).get_table_names()
    logger.info("Tables in database: %s", table_names)
    return table_names
