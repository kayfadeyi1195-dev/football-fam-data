"""Scrape squad and player data from Pitchero-powered club websites.

Reads clubs with a ``pitchero_url`` from the database, scrapes each
club's squad page (and optionally player profiles), and stages the
raw data in ``staging_raw`` for later transformation.

Usage::

    python scripts/run_pitchero_scraper.py               # squads only
    python scripts/run_pitchero_scraper.py --profiles     # squads + player profiles
"""

import argparse
import logging

from sqlalchemy import select

from src.db.models import Club
from src.db.session import get_session
from src.etl.staging import stage_records
from src.scrapers.pitchero import PitcheroScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _load_pitchero_clubs() -> list[tuple[str, str]]:
    """Return (club_name, pitchero_url) for every club that has one."""
    with get_session() as session:
        clubs = session.execute(
            select(Club.name, Club.pitchero_url)
            .where(Club.pitchero_url.isnot(None))
            .where(Club.pitchero_url != "")
        ).all()
    return [(name, url) for name, url in clubs]


def main() -> None:
    parser = argparse.ArgumentParser(description="Pitchero squad scraper")
    parser.add_argument(
        "--profiles",
        action="store_true",
        help="Also scrape individual player profile pages (slow)",
    )
    args = parser.parse_args()

    logger.info("=== Pitchero scraper started ===")

    clubs = _load_pitchero_clubs()
    if not clubs:
        logger.warning("No clubs with pitchero_url in database — nothing to scrape")
        return

    logger.info("Found %d clubs with Pitchero URLs", len(clubs))

    scraper = PitcheroScraper()
    total_players = 0
    total_profiles = 0

    for club_name, pitchero_url in clubs:
        logger.info("Scraping %s (%s)", club_name, pitchero_url)

        result = scraper.scrape_club(
            pitchero_url,
            include_profiles=args.profiles,
        )

        squad = result.get("squad", [])
        if squad:
            stage_records("pitchero", "player", squad, id_field="id")
            total_players += len(squad)

        profiles = result.get("profiles", {})
        if profiles:
            stage_records(
                "pitchero", "player_profile",
                list(profiles.values()),
                id_field="player_id",
            )
            total_profiles += len(profiles)

        for err in result.get("errors", []):
            logger.warning("  %s: %s", club_name, err)

    logger.info(
        "=== Pitchero scraper finished: %d players, %d profiles across %d clubs ===",
        total_players, total_profiles, len(clubs),
    )


if __name__ == "__main__":
    main()
