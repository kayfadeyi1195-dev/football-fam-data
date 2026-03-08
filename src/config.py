"""Application configuration.

Loads environment variables from a ``.env`` file (via python-dotenv)
and exposes them as typed Python constants.  Every other module in the
project should import settings from here rather than reading ``os.environ``
directly.

Usage::

    from src.config import DATABASE_URL, API_FOOTBALL_KEY

Required variables will cause the application to exit immediately with
a clear error message if they are missing.
"""

import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _require(name: str) -> str:
    """Return the value of an environment variable or exit with an error."""
    value = os.getenv(name)
    if not value:
        sys.exit(
            f"ERROR: Required environment variable {name!r} is not set. "
            f"Copy .env.example to .env and fill it in."
        )
    return value


# ── Required ──────────────────────────────────────────────────────────────

DATABASE_URL: str = _require("DATABASE_URL")
API_FOOTBALL_KEY: str = _require("API_FOOTBALL_KEY")

# ── Optional ──────────────────────────────────────────────────────────────

SPORTMONKS_API_TOKEN: str = os.getenv("SPORTMONKS_API_TOKEN", "")
FWP_API_KEY: str = os.getenv("FWP_API_KEY", "")
SCRAPE_DELAY_SECONDS: float = float(os.getenv("SCRAPE_DELAY_SECONDS", "2.0"))
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

# ── Bootstrap logging ────────────────────────────────────────────────────

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger.debug("Config loaded — DATABASE_URL=%s…", DATABASE_URL[:25])
logger.debug("Config loaded — LOG_LEVEL=%s", LOG_LEVEL)
