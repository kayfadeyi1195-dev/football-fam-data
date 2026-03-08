"""Scraper for FBref (https://fbref.com).

FBref provides detailed advanced statistics for many football leagues.
Where available for lower-league competitions, this scraper pulls
player-level stats (goals, assists, xG, progressive passes, etc.).
"""

import logging
import time

import requests
from bs4 import BeautifulSoup

from src.config import SCRAPE_DELAY_SECONDS

logger = logging.getLogger(__name__)

BASE_URL = "https://fbref.com/en"

# FBref asks for a 3-second crawl delay in robots.txt
FBREF_DELAY = max(SCRAPE_DELAY_SECONDS, 3.0)


def fetch_page(url: str) -> BeautifulSoup:
    """Download a page and return a BeautifulSoup parse tree."""
    logger.info("Fetching %s", url)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    time.sleep(FBREF_DELAY)
    return BeautifulSoup(response.text, "html.parser")


def scrape_squad_stats(squad_url: str) -> list[dict]:
    """Scrape player stats from an FBref squad page.

    Returns a list of dicts with keys like ``player``, ``appearances``,
    ``goals``, ``assists``, ``minutes``.
    """
    soup = fetch_page(squad_url)
    players: list[dict] = []

    # TODO: Implement selectors once target FBref pages are identified.
    logger.warning("fbref.scrape_squad_stats is a placeholder — implement selectors")

    return players


def scrape_league_stats(league_url: str) -> list[dict]:
    """Scrape aggregated league-level player stats from FBref."""
    soup = fetch_page(league_url)
    stats: list[dict] = []

    # TODO: Implement selectors once target FBref pages are identified.
    logger.warning("fbref.scrape_league_stats is a placeholder — implement selectors")

    return stats
