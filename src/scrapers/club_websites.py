"""Generic scraper for individual club websites (non-Pitchero).

Most lower-league clubs run a simple site on WordPress, Squarespace,
Wix, or raw HTML.  This module has two jobs:

1. **URL discovery** — find website URLs for clubs that have none.
2. **Squad scraping** — extract player names (and positions / photos
   where possible) from whatever structure the site uses.

Because every club site is different, the extraction is heuristic:
we try several common squad-page paths, then hunt for player data
in ``<table>`` rows, ``<li>`` items, ``<div>`` cards, and ``<article>``
blocks.  Even just names are valuable for Steps 4-6 where we have
very little data.

Usage::

    from src.scrapers.club_websites import ClubWebsiteScraper

    scraper = ClubWebsiteScraper()

    # Part 1 — discover missing URLs
    discoveries = scraper.discover_urls()

    # Part 2 — scrape squads from known websites
    results = scraper.scrape_all_squads()

CLI::

    python -m src.scrapers.club_websites            # both parts
    python -m src.scrapers.club_websites --discover  # URL discovery only
    python -m src.scrapers.club_websites --scrape    # squad scraping only
"""

import logging
import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag
from sqlalchemy import select

from src.config import SCRAPE_DELAY_SECONDS
from src.db.models import Club
from src.db.session import get_session
from src.etl.staging import stage_records

logger = logging.getLogger(__name__)

# ── constants ────────────────────────────────────────────────────────────

DATA_SOURCE = "club_website"
CONFIDENCE_SCORE = 3

MAX_RETRIES = 2
INITIAL_BACKOFF_SECS = 2.0
REQUEST_TIMEOUT = 20

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
]

SQUAD_PATHS = [
    "/squad",
    "/first-team",
    "/teams/first-team",
    "/the-squad",
    "/players",
    "/team",
    "/teams/first-team/squad",
    "/first-team/squad",
    "/senior-squad",
]

_POSITION_KEYWORDS: dict[str, str] = {
    "goalkeeper": "GK", "goalkeepers": "GK", "gk": "GK", "keeper": "GK",
    "defender": "DEF", "defenders": "DEF", "defence": "DEF",
    "def": "DEF", "back": "DEF", "full-back": "DEF", "centre-back": "DEF",
    "midfielder": "MID", "midfielders": "MID", "midfield": "MID", "mid": "MID",
    "forward": "FWD", "forwards": "FWD", "striker": "FWD", "strikers": "FWD",
    "fwd": "FWD", "attack": "FWD", "attackers": "FWD", "winger": "FWD",
}


# ══════════════════════════════════════════════════════════════════════════
# Scraper class
# ══════════════════════════════════════════════════════════════════════════

class ClubWebsiteScraper:
    """Find club URLs and extract squad data from generic club websites."""

    def __init__(self, delay_secs: float = SCRAPE_DELAY_SECONDS) -> None:
        self._session = requests.Session()
        self._delay = max(delay_secs, 2.0)
        self._last_request_at: float = 0.0
        self._ua_index = 0

    # ── low-level helpers ─────────────────────────────────────────────

    def _next_ua(self) -> str:
        ua = _USER_AGENTS[self._ua_index % len(_USER_AGENTS)]
        self._ua_index += 1
        return ua

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self._delay:
            time.sleep(self._delay - elapsed)

    def _get(self, url: str, *, allow_redirects: bool = True) -> requests.Response | None:
        """GET with throttle, retry, and User-Agent rotation."""
        backoff = INITIAL_BACKOFF_SECS
        for attempt in range(1, MAX_RETRIES + 1):
            self._throttle()
            self._last_request_at = time.monotonic()
            headers = {"User-Agent": self._next_ua()}
            try:
                resp = self._session.get(
                    url, headers=headers, timeout=REQUEST_TIMEOUT,
                    allow_redirects=allow_redirects,
                )
            except requests.RequestException as exc:
                logger.debug("Request error for %s: %s", url, exc)
                if attempt == MAX_RETRIES:
                    return None
                time.sleep(backoff)
                backoff *= 2
                continue

            if resp.status_code == 200:
                return resp
            if resp.status_code in {301, 302, 303, 307, 308} and not allow_redirects:
                return resp
            if resp.status_code in {429, 500, 502, 503, 504} and attempt < MAX_RETRIES:
                time.sleep(backoff)
                backoff *= 2
                continue
            return None
        return None

    def _url_is_live(self, url: str) -> bool:
        """Return True if the URL returns 200 (or a redirect to 200)."""
        resp = self._get(url)
        return resp is not None

    # ══════════════════════════════════════════════════════════════════
    # PART 1 — URL Discovery
    # ══════════════════════════════════════════════════════════════════

    @staticmethod
    def _slugify(name: str) -> str:
        """Turn 'Barnet Town FC' into 'barnettown'."""
        slug = name.lower()
        for suffix in (
            " football club", " fc", " f.c.", " afc", " a.f.c.",
            " cfc", " town", " city", " united", " wanderers",
            " rovers", " athletic", " ath",
        ):
            slug = slug.replace(suffix, "")
        slug = re.sub(r"[^a-z0-9]", "", slug)
        return slug

    @staticmethod
    def _slug_variants(name: str) -> list[str]:
        """Generate plausible URL slugs from a club name."""
        base = ClubWebsiteScraper._slugify(name)
        lower = name.lower()
        has_town = "town" in lower
        has_city = "city" in lower

        variants = [base]
        if base not in (base + "fc",):
            variants.append(base + "fc")
        variants.append(base + "afc")
        if has_town:
            town_slug = re.sub(r"[^a-z0-9]", "", lower.split("town")[0])
            variants.append(town_slug + "town")
            variants.append(town_slug + "townfc")
        if has_city:
            city_slug = re.sub(r"[^a-z0-9]", "", lower.split("city")[0])
            variants.append(city_slug + "city")
            variants.append(city_slug + "cityfc")

        return list(dict.fromkeys(variants))

    def _try_candidate_urls(self, name: str) -> str | None:
        """Try common domain patterns for a club name."""
        slugs = self._slug_variants(name)
        tlds = [".co.uk", ".com", ".net", ".org.uk"]

        for slug in slugs:
            for tld in tlds:
                candidate = f"https://www.{slug}{tld}"
                logger.debug("Trying %s", candidate)
                resp = self._get(candidate)
                if resp is not None:
                    text_lower = resp.text[:5000].lower()
                    if _looks_like_football_site(text_lower, name):
                        logger.info("Discovered %s → %s", name, candidate)
                        return candidate
        return None

    _SKIP_DOMAINS = {
        "google.com", "youtube.com", "facebook.com", "twitter.com",
        "instagram.com", "wikipedia.org", "bbc.co.uk", "bbc.com",
        "pitchero.com", "reddit.com", "linkedin.com", "duckduckgo.com",
    }

    def _extract_search_links(self, html: str) -> list[str]:
        """Pull candidate URLs from search-engine HTML."""
        soup = BeautifulSoup(html, "html.parser")
        urls: list[str] = []
        for a_tag in soup.find_all("a", href=True):
            href: str = a_tag["href"]
            # Google wraps links in /url?q=…
            if href.startswith("/url?q="):
                href = href.split("/url?q=")[1].split("&")[0]
            # DuckDuckGo wraps links in //duckduckgo.com/l/?uddg=…
            if "uddg=" in href:
                from urllib.parse import unquote
                href = unquote(href.split("uddg=")[1].split("&")[0])
            if not href.startswith("http"):
                continue
            domain = urlparse(href).netloc.lower().lstrip("www.")
            if any(skip in domain for skip in self._SKIP_DOMAINS):
                continue
            urls.append(href)
        return urls

    def _try_google_search(self, name: str) -> str | None:
        """Search Google for the club and pick the best result.

        Returns ``None`` and sets ``_google_blocked`` if Google
        returns 429 or a captcha page so the caller can fall back
        to DuckDuckGo.
        """
        query = f"{name} football club official website"
        url = "https://www.google.com/search"
        params = {"q": query, "num": 5}
        headers = {"User-Agent": self._next_ua()}

        self._throttle()
        self._last_request_at = time.monotonic()

        try:
            resp = self._session.get(
                url, params=params, headers=headers, timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException:
            return None

        if resp.status_code in {429, 503}:
            logger.warning("Google blocked (HTTP %d) — will use DuckDuckGo fallback", resp.status_code)
            self._google_blocked = True
            return None

        if resp.status_code != 200:
            return None

        # Google sometimes returns 200 with a captcha page
        if "captcha" in resp.text.lower() or "unusual traffic" in resp.text.lower():
            logger.warning("Google captcha detected — will use DuckDuckGo fallback")
            self._google_blocked = True
            return None

        for href in self._extract_search_links(resp.text):
            if self._url_is_live(href):
                logger.info("Google discovered %s → %s", name, href)
                return href

        return None

    def _try_duckduckgo_search(self, name: str) -> str | None:
        """Fallback search using DuckDuckGo's HTML-only interface."""
        query = f"{name} football club official website"
        url = "https://html.duckduckgo.com/html/"
        headers = {"User-Agent": self._next_ua()}

        self._throttle()
        self._last_request_at = time.monotonic()

        try:
            resp = self._session.post(
                url,
                data={"q": query},
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as exc:
            logger.debug("DuckDuckGo request error: %s", exc)
            return None

        if resp.status_code != 200:
            logger.debug("DuckDuckGo HTTP %d", resp.status_code)
            return None

        for href in self._extract_search_links(resp.text):
            if self._url_is_live(href):
                logger.info("DuckDuckGo discovered %s → %s", name, href)
                return href

        return None

    def _search_for_url(self, name: str) -> str | None:
        """Try Google first, fall back to DuckDuckGo if blocked."""
        if not getattr(self, "_google_blocked", False):
            result = self._try_google_search(name)
            if result:
                return result

        return self._try_duckduckgo_search(name)

    def discover_urls(
        self,
        limit: int | None = None,
        step_filter: set[int] | None = None,
    ) -> dict[str, str]:
        """Find website URLs for clubs that have ``website_url IS NULL``.

        *step_filter* restricts discovery to clubs whose league is at
        the given pyramid steps (e.g. ``{3, 4}``).

        Returns a mapping of club name → discovered URL and updates
        the ``clubs`` table in-place.
        """
        from src.db.models import League

        self._google_blocked = False
        discovered: dict[str, str] = {}
        attempted = 0

        with get_session() as session:
            query = (
                select(Club)
                .where(Club.website_url.is_(None))
                .where(Club.pitchero_url.is_(None))
                .where(Club.is_active.is_(True))
            )
            if step_filter:
                query = (
                    query
                    .join(League, League.id == Club.league_id)
                    .where(League.step.in_(step_filter))
                )
            query = query.order_by(Club.id)
            if limit:
                query = query.limit(limit)
            clubs = session.execute(query).scalars().all()

            steps_desc = ",".join(str(s) for s in sorted(step_filter)) if step_filter else "all"
            logger.info(
                "URL discovery: %d clubs without website_url (steps=%s)",
                len(clubs), steps_desc,
            )

            for club in clubs:
                attempted += 1
                url = self._try_candidate_urls(club.name)

                if url is None:
                    url = self._search_for_url(club.name)

                if url:
                    club.website_url = url
                    discovered[club.name] = url
                    logger.info("[%d/%d] %s → %s", attempted, len(clubs), club.name, url)
                else:
                    logger.debug("[%d/%d] %s — not found", attempted, len(clubs), club.name)

        logger.info(
            "URL discovery complete: %d attempted, %d discovered",
            attempted, len(discovered),
        )
        return discovered

    # ══════════════════════════════════════════════════════════════════
    # PART 2 — Squad Page Scraping
    # ══════════════════════════════════════════════════════════════════

    def _find_squad_page(self, base_url: str) -> tuple[str | None, requests.Response | None]:
        """Try common squad page paths until one returns 200."""
        base = base_url.rstrip("/")
        for path in SQUAD_PATHS:
            url = base + path
            resp = self._get(url)
            if resp is not None:
                logger.info("Squad page found: %s", url)
                return url, resp
        return None, None

    def _scrape_squad_from_html(
        self,
        html: str,
        base_url: str,
    ) -> list[dict[str, Any]]:
        """Extract player data from an arbitrary squad page.

        Tries multiple extraction strategies in priority order and
        returns the first one that finds players.
        """
        soup = BeautifulSoup(html, "html.parser")
        players: list[dict[str, Any]] = []

        strategies = [
            self._extract_from_tables,
            self._extract_from_cards,
            self._extract_from_lists,
            self._extract_from_headings,
        ]
        for strategy in strategies:
            players = strategy(soup, base_url)
            if players:
                logger.debug(
                    "  Strategy %s found %d players",
                    strategy.__name__, len(players),
                )
                break

        return players

    # ── extraction strategies ─────────────────────────────────────────

    @staticmethod
    def _extract_from_tables(
        soup: BeautifulSoup,
        base_url: str,
    ) -> list[dict[str, Any]]:
        """Look for player data in HTML tables."""
        players: list[dict[str, Any]] = []

        for table in soup.find_all("table"):
            headers = [
                th.get_text(strip=True).lower()
                for th in table.find_all("th")
            ]
            name_col = _find_column_index(headers, ["name", "player", "players"])
            pos_col = _find_column_index(headers, ["position", "pos", "pos."])

            if name_col is None:
                continue

            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) <= name_col:
                    continue
                raw_name = cells[name_col].get_text(strip=True)
                if not raw_name or raw_name.lower() in headers:
                    continue
                player: dict[str, Any] = {"name": _clean_name(raw_name)}

                if pos_col is not None and len(cells) > pos_col:
                    player["position"] = cells[pos_col].get_text(strip=True)

                img = cells[name_col].find("img")
                if img and img.get("src"):
                    player["photo_url"] = _abs_url(img["src"], base_url)

                players.append(player)

        return players

    @staticmethod
    def _extract_from_cards(
        soup: BeautifulSoup,
        base_url: str,
    ) -> list[dict[str, Any]]:
        """Look for player cards (common in modern CMS themes)."""
        players: list[dict[str, Any]] = []
        current_position: str | None = None

        card_selectors = [
            {"class_": re.compile(r"player", re.I)},
            {"class_": re.compile(r"squad", re.I)},
            {"class_": re.compile(r"team-member", re.I)},
            {"class_": re.compile(r"staff-member", re.I)},
        ]

        cards: list[Tag] = []
        for sel in card_selectors:
            cards = soup.find_all(["div", "article", "li", "a"], **sel)
            if cards:
                break

        if not cards:
            return []

        for card in cards:
            text = card.get_text(" ", strip=True)
            if len(text) < 2 or len(text) > 200:
                continue

            heading = card.find(re.compile(r"h[1-6]"))
            if heading:
                heading_text = heading.get_text(strip=True)
                if _is_position_heading(heading_text):
                    current_position = heading_text
                    continue
                raw_name = heading_text
            else:
                name_el = card.find(
                    class_=re.compile(r"(name|title|player)", re.I),
                )
                raw_name = name_el.get_text(strip=True) if name_el else ""

            if not raw_name or len(raw_name) < 2:
                continue

            if not _looks_like_person_name(raw_name):
                continue

            player: dict[str, Any] = {"name": _clean_name(raw_name)}

            pos_el = card.find(
                class_=re.compile(r"(position|role|pos)", re.I),
            )
            if pos_el:
                player["position"] = pos_el.get_text(strip=True)
            elif current_position:
                player["position"] = current_position

            img = card.find("img")
            if img and img.get("src"):
                src = img["src"]
                if not _is_generic_placeholder(src):
                    player["photo_url"] = _abs_url(src, base_url)

            players.append(player)

        return players

    @staticmethod
    def _extract_from_lists(
        soup: BeautifulSoup,
        base_url: str,
    ) -> list[dict[str, Any]]:
        """Look for players in ``<ul>`` / ``<ol>`` lists."""
        players: list[dict[str, Any]] = []

        for ul in soup.find_all(["ul", "ol"]):
            parent_class = " ".join(ul.get("class", []))
            parent_text = (ul.parent.get_text(" ", strip=True)[:100] if ul.parent else "").lower()

            if not any(
                kw in parent_class.lower() or kw in parent_text
                for kw in ("squad", "player", "team", "roster")
            ):
                continue

            items = ul.find_all("li")
            if len(items) < 5:
                continue

            for li in items:
                text = li.get_text(strip=True)
                if not _looks_like_person_name(text):
                    continue
                player: dict[str, Any] = {"name": _clean_name(text)}
                img = li.find("img")
                if img and img.get("src"):
                    player["photo_url"] = _abs_url(img["src"], base_url)
                players.append(player)

        return players

    @staticmethod
    def _extract_from_headings(
        soup: BeautifulSoup,
        base_url: str,
    ) -> list[dict[str, Any]]:
        """Fall back to looking for position headings followed by names."""
        players: list[dict[str, Any]] = []
        current_position: str | None = None

        for el in soup.find_all(re.compile(r"h[1-6]|p|div|span")):
            text = el.get_text(strip=True)
            if _is_position_heading(text):
                current_position = text
                continue
            if current_position and _looks_like_person_name(text) and len(text) < 60:
                players.append({
                    "name": _clean_name(text),
                    "position": current_position,
                })

        return players

    # ── main squad scraping entry point ───────────────────────────────

    def scrape_club(self, club_id: int, club_name: str, website_url: str) -> list[dict[str, Any]]:
        """Scrape a single club's website for squad data."""
        squad_url, resp = self._find_squad_page(website_url)
        if not resp:
            logger.debug("No squad page found for %s (%s)", club_name, website_url)
            return []

        players = self._scrape_squad_from_html(resp.text, squad_url or website_url)

        for p in players:
            p["club_id"] = club_id
            p["club_name"] = club_name
            p["source_url"] = squad_url
            if "position" in p:
                p["position_normalised"] = _normalise_position(p["position"])
            p["id"] = f"cw_{club_id}_{_slugify_name(p['name'])}"

        return players

    def scrape_all_squads(
        self,
        limit: int | None = None,
    ) -> dict[str, int]:
        """Scrape squads from all non-Pitchero clubs with a known URL.

        Returns a summary dict with counts.
        """
        totals: dict[str, int] = {
            "clubs_attempted": 0,
            "clubs_with_players": 0,
            "players_found": 0,
            "errors": 0,
        }

        with get_session() as session:
            query = (
                select(Club.id, Club.name, Club.website_url)
                .where(Club.website_url.isnot(None))
                .where(Club.pitchero_url.is_(None))
                .where(Club.is_active.is_(True))
                .order_by(Club.id)
            )
            if limit:
                query = query.limit(limit)
            clubs = session.execute(query).all()

        logger.info("Squad scraping: %d clubs to try", len(clubs))

        all_records: list[dict[str, Any]] = []

        for club_id, club_name, website_url in clubs:
            totals["clubs_attempted"] += 1
            try:
                players = self.scrape_club(club_id, club_name, website_url)
                if players:
                    totals["clubs_with_players"] += 1
                    totals["players_found"] += len(players)
                    all_records.extend(players)
                    logger.info(
                        "  %s: %d players found", club_name, len(players),
                    )
                else:
                    logger.debug("  %s: no players found", club_name)
            except Exception as exc:
                totals["errors"] += 1
                logger.warning("  %s: error — %s", club_name, exc)

            if len(all_records) >= 200:
                stage_records(DATA_SOURCE, "player", all_records, id_field="id")
                all_records = []

        if all_records:
            stage_records(DATA_SOURCE, "player", all_records, id_field="id")

        logger.info(
            "Squad scraping complete: %d attempted, %d with players, "
            "%d total players, %d errors",
            totals["clubs_attempted"],
            totals["clubs_with_players"],
            totals["players_found"],
            totals["errors"],
        )
        return totals


# ══════════════════════════════════════════════════════════════════════════
# Helper functions
# ══════════════════════════════════════════════════════════════════════════

def _find_column_index(
    headers: list[str],
    candidates: list[str],
) -> int | None:
    for i, h in enumerate(headers):
        if h in candidates:
            return i
    return None


def _clean_name(raw: str) -> str:
    """Strip numbers, parenthetical info, and normalise whitespace."""
    name = re.sub(r"^\d+[\.\)]\s*", "", raw)
    name = re.sub(r"\(.*?\)", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _slugify_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "_", name.lower()).strip("_")[:60]


def _abs_url(href: str, base: str) -> str:
    if href.startswith(("http://", "https://")):
        return href
    return urljoin(base, href)


def _is_generic_placeholder(src: str) -> bool:
    """Return True for common placeholder/avatar image paths."""
    lower = src.lower()
    return any(
        kw in lower
        for kw in ("placeholder", "avatar", "default", "noimage", "no-image", "blank")
    )


def _looks_like_person_name(text: str) -> bool:
    """Rough heuristic: 2-4 words, mostly alpha, not too long."""
    if not text or len(text) > 80 or len(text) < 3:
        return False
    words = text.split()
    if len(words) < 1 or len(words) > 5:
        return False
    alpha_ratio = sum(c.isalpha() or c == "-" or c == "'" for c in text) / len(text)
    if alpha_ratio < 0.7:
        return False
    lower = text.lower()
    if any(kw in lower for kw in ("click", "view", "read", "more", "http", "www", "page")):
        return False
    return True


def _is_position_heading(text: str) -> bool:
    """Return True if text is a positional group heading."""
    return text.strip().lower().rstrip("s") in {
        "goalkeeper", "defender", "midfielder", "forward",
        "striker", "attacker",
        "gk", "def", "mid", "fwd",
    } or text.strip().lower() in _POSITION_KEYWORDS


def _normalise_position(raw: str) -> str:
    """Map a free-text position to GK / DEF / MID / FWD."""
    lower = raw.strip().lower()
    if lower in _POSITION_KEYWORDS:
        return _POSITION_KEYWORDS[lower]
    for keyword, pos in _POSITION_KEYWORDS.items():
        if keyword in lower:
            return pos
    return raw.strip().upper()[:10]


def _looks_like_football_site(html_snippet: str, club_name: str) -> bool:
    """Check whether a page looks like it belongs to a football club."""
    club_parts = club_name.lower().split()
    core_words = [
        w for w in club_parts
        if w not in {"fc", "f.c.", "afc", "a.f.c.", "the", "of", "cfc"}
    ]
    name_match = any(w in html_snippet for w in core_words if len(w) > 2)

    football_kws = (
        "football", "squad", "fixtures", "results", "league",
        "matchday", "first team", "1st team",
    )
    football_match = any(kw in html_snippet for kw in football_kws)

    return name_match and football_match


# ══════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Club website URL discovery and squad scraping",
    )
    parser.add_argument(
        "--discover", action="store_true",
        help="Run URL discovery only",
    )
    parser.add_argument(
        "--scrape", action="store_true",
        help="Run squad scraping only",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max clubs to process (for testing)",
    )
    parser.add_argument(
        "--step", type=str, default=None,
        help="Only discover URLs for these pyramid steps (comma-separated, e.g. 3,4)",
    )
    args = parser.parse_args()

    step_filter = None
    if args.step:
        step_filter = {int(s.strip()) for s in args.step.split(",")}

    run_both = not args.discover and not args.scrape

    scraper = ClubWebsiteScraper()

    if args.discover or run_both:
        print("\n── URL DISCOVERY ─────────────────────────────────")
        urls = scraper.discover_urls(limit=args.limit, step_filter=step_filter)
        print(f"  Discovered {len(urls)} URLs")
        for name, url in list(urls.items())[:20]:
            print(f"    {name:<40s}  {url}")
        if len(urls) > 20:
            print(f"    … and {len(urls) - 20} more")

    if args.scrape or run_both:
        print("\n── SQUAD SCRAPING ────────────────────────────────")
        totals = scraper.scrape_all_squads(limit=args.limit)
        print(f"  Clubs attempted:     {totals['clubs_attempted']}")
        print(f"  Clubs with players:  {totals['clubs_with_players']}")
        print(f"  Players found:       {totals['players_found']}")
        print(f"  Errors:              {totals['errors']}")

    print()
