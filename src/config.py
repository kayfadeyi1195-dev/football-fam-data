"""Application configuration.

Loads environment variables from a ``.env`` file (via python-dotenv)
and exposes them as typed Python constants.  Every other module in the
project should import settings from here rather than reading ``os.environ``
directly.

Usage::

    from src.config import DATABASE_URL, API_FOOTBALL_KEY

``DATABASE_URL`` raises ``RuntimeError`` if missing so the traceback
is visible in Railway / Render logs (``sys.exit`` swallows the message).
All other variables default to empty strings or sensible values.
"""

import logging
import os

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── DATABASE_URL (raises on missing so the error shows in logs) ──────────

DATABASE_URL: str = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    raise RuntimeError(
        "Required environment variable 'DATABASE_URL' is not set. "
        "Set it in .env or as a platform environment variable."
    )

# ── API keys (optional — features degrade gracefully without them) ───────

API_FOOTBALL_KEY: str = os.getenv("API_FOOTBALL_KEY", "")
SPORTMONKS_API_TOKEN: str = os.getenv("SPORTMONKS_API_TOKEN", "")
FWP_API_KEY: str = os.getenv("FWP_API_KEY", "")

# ── Tuning ───────────────────────────────────────────────────────────────

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
if not API_FOOTBALL_KEY:
    logger.warning("API_FOOTBALL_KEY is not set — API-Football calls will fail")
