"""Scraper for Transfermarkt player and club data.

Transfermarkt provides rich biographical and contract data that is
hard to find elsewhere: market values, contract expiry dates, agent
names, preferred foot, and exact heights.  This is particularly
valuable for Steps 1-3 where Transfermarkt has good coverage.

**URL patterns**::

    Competition clubs:  /…/startseite/wettbewerb/{comp_id}
    Club squad:         /…/startseite/verein/{club_id}/saison_id/{season}
    Player profile:     /…/profil/spieler/{player_id}
    Player stats:       /…/leistungsdaten/spieler/{id}/plus/0?saison={season}
    Player transfers:   /…/transfers/spieler/{player_id}
    Market value chart: /…/marktwertverlauf/spieler/{player_id}
    Player search:      /…/schnellsuche/ergebnis/schnellsuche?query={name}

**Rate limiting**: Transfermarkt aggressively blocks automated
requests.  We use a 5-second minimum delay, realistic browser
headers, and User-Agent rotation.

Usage::

    from src.scrapers.transfermarkt import TransfermarktScraper

    scraper = TransfermarktScraper()
    clubs = scraper.scrape_competition("CNAT")
    profile = scraper.scrape_player_profile(player_id=123456)
    stats = scraper.scrape_player_stats(player_id=123456, season=2024)
    transfers = scraper.scrape_player_transfers(player_id=123456)
    mv_history = scraper.scrape_market_value_history(player_id=123456)

CLI smoke-test::

    python -m src.scrapers.transfermarkt CNAT
"""

import json
import logging
import re
import time
from typing import Any
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup, Tag

from src.config import SCRAPE_DELAY_SECONDS

logger = logging.getLogger(__name__)

BASE_URL = "https://www.transfermarkt.co.uk"

TM_DELAY = max(SCRAPE_DELAY_SECONDS, 5.0)
MAX_RETRIES = 3
INITIAL_BACKOFF = 5.0
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

NONLEAGUE_COMPETITIONS: dict[str, str] = {
    "CNAT": "National League",
    "NLN6": "National League North",
    "NLS6": "National League South",
}

# Slug used in Transfermarkt URLs for each competition
_COMP_SLUGS: dict[str, str] = {
    "CNAT": "national-league",
    "NLN6": "national-league-north",
    "NLS6": "national-league-south",
}

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) "
    "Gecko/20100101 Firefox/128.0",
]

_POSITION_MAP: dict[str, str] = {
    "goalkeeper": "GK",
    "keeper": "GK",
    "centre-back": "DEF",
    "left-back": "DEF",
    "right-back": "DEF",
    "defender": "DEF",
    "defensive midfield": "MID",
    "central midfield": "MID",
    "attacking midfield": "MID",
    "left midfield": "MID",
    "right midfield": "MID",
    "midfielder": "MID",
    "left winger": "FWD",
    "right winger": "FWD",
    "second striker": "FWD",
    "centre-forward": "FWD",
    "forward": "FWD",
    "striker": "FWD",
    "attack": "FWD",
}


class TransfermarktScraper:
    """Scrape squad and player data from Transfermarkt."""

    def __init__(self, delay_secs: float = TM_DELAY) -> None:
        self._session = requests.Session()
        self._delay = max(delay_secs, 5.0)
        self._last_request_at: float = 0.0
        self._ua_index = 0

    # ── low-level helpers ─────────────────────────────────────────

    def _next_ua(self) -> str:
        ua = _USER_AGENTS[self._ua_index % len(_USER_AGENTS)]
        self._ua_index += 1
        return ua

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": self._next_ua(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": f"{BASE_URL}/",
            "Connection": "keep-alive",
            "DNT": "1",
        }

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self._delay:
            time.sleep(self._delay - elapsed)

    def _get(self, url: str) -> requests.Response | None:
        """GET with throttle, retry, User-Agent rotation."""
        backoff = INITIAL_BACKOFF
        for attempt in range(1, MAX_RETRIES + 1):
            self._throttle()
            self._last_request_at = time.monotonic()
            try:
                resp = self._session.get(
                    url, headers=self._headers(), timeout=30,
                )
            except requests.RequestException as exc:
                logger.warning("Request error: %s", exc)
                if attempt == MAX_RETRIES:
                    return None
                time.sleep(backoff)
                backoff *= 2
                continue

            if resp.status_code == 200:
                return resp

            if resp.status_code in {403, 451}:
                logger.warning(
                    "Blocked by Transfermarkt (%d) — backing off %.0fs",
                    resp.status_code, backoff * 2,
                )
                time.sleep(backoff * 2)
                backoff *= 3
                if attempt == MAX_RETRIES:
                    return None
                continue

            if resp.status_code == 404:
                return None

            if resp.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
                time.sleep(backoff)
                backoff *= 2
                continue

            logger.warning("Unexpected status %d for %s", resp.status_code, url)
            return None
        return None

    # ══════════════════════════════════════════════════════════════
    # Competition / club scraping
    # ══════════════════════════════════════════════════════════════

    def scrape_competition(
        self,
        comp_id: str,
        season: int = 2024,
    ) -> list[dict[str, Any]]:
        """Scrape all clubs and their squads for a competition.

        Returns a list of club dicts, each containing a ``players``
        list with per-player data.
        """
        comp_name = NONLEAGUE_COMPETITIONS.get(comp_id, comp_id)
        comp_slug = _COMP_SLUGS.get(comp_id, comp_id.lower())
        logger.info("Scraping competition %s (%s), season %d", comp_id, comp_name, season)

        url = (
            f"{BASE_URL}/{comp_slug}/startseite/wettbewerb/{comp_id}"
            f"/saison_id/{season}"
        )
        resp = self._get(url)
        if not resp:
            logger.error("Failed to load competition page: %s", comp_id)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        club_links = self._extract_club_links(soup)
        logger.info("  Found %d clubs in %s", len(club_links), comp_id)

        clubs: list[dict[str, Any]] = []
        for club_name, club_url, tm_club_id in club_links:
            squad = self.scrape_club_squad(club_url, season)
            clubs.append({
                "id": f"tm_club_{tm_club_id}",
                "tm_club_id": tm_club_id,
                "name": club_name,
                "url": club_url,
                "competition_id": comp_id,
                "competition_name": comp_name,
                "season": season,
                "players": squad,
            })

        return clubs

    @staticmethod
    def _extract_club_links(
        soup: BeautifulSoup,
    ) -> list[tuple[str, str, str]]:
        """Extract (club_name, club_url, tm_club_id) from a competition page."""
        results: list[tuple[str, str, str]] = []
        seen: set[str] = set()

        for td in soup.find_all("td", class_="hauptlink no-border-links"):
            a = td.find("a", href=True)
            if not a:
                continue
            href: str = a["href"]
            if "/verein/" not in href:
                continue
            m = re.search(r"/verein/(\d+)", href)
            if not m:
                continue
            tm_id = m.group(1)
            if tm_id in seen:
                continue
            seen.add(tm_id)
            name = a.get_text(strip=True)
            full_url = BASE_URL + href if href.startswith("/") else href
            results.append((name, full_url, tm_id))

        return results

    def scrape_club_squad(
        self,
        club_url: str,
        season: int = 2024,
    ) -> list[dict[str, Any]]:
        """Scrape the squad page for a single club."""
        m = re.search(r"/verein/(\d+)", club_url)
        if not m:
            return []

        tm_club_id = m.group(1)
        slug = club_url.split("/startseite/")[0].rsplit("/", 1)[-1] if "/startseite/" in club_url else ""
        url = (
            f"{BASE_URL}/{slug}/kader/verein/{tm_club_id}"
            f"/saison_id/{season}/plus/1"
        )
        resp = self._get(url)
        if not resp:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        players: list[dict[str, Any]] = []
        table = soup.find("table", class_="items")
        if not table:
            return []

        rows = table.find_all("tr", class_=["odd", "even"])
        for row in rows:
            player = self._parse_squad_row(row)
            if player:
                player["tm_club_id"] = tm_club_id
                players.append(player)

        logger.info("    %s: %d players", slug or tm_club_id, len(players))
        return players

    @staticmethod
    def _parse_squad_row(row: Tag) -> dict[str, Any] | None:
        """Parse a single <tr> from the detailed squad table."""
        cells = row.find_all("td", recursive=False)
        if len(cells) < 4:
            return None

        player: dict[str, Any] = {}

        # Player name + link
        name_cell = row.find("td", class_="hauptlink")
        if not name_cell:
            return None
        a = name_cell.find("a", href=True)
        if not a:
            return None
        player["name"] = a.get_text(strip=True)
        href: str = a["href"]
        player["tm_url"] = BASE_URL + href if href.startswith("/") else href

        m = re.search(r"/spieler/(\d+)", href)
        player["tm_player_id"] = m.group(1) if m else None
        player["id"] = f"tm_{player['tm_player_id']}" if player["tm_player_id"] else None

        # Position
        pos_cells = row.find_all("td", class_="posrela")
        if pos_cells:
            pos_text = pos_cells[0].find("tr")
            if pos_text:
                inner = pos_text.find_all("td")
                if len(inner) >= 2:
                    player["position_detail"] = inner[-1].get_text(strip=True)

        if not player.get("position_detail"):
            for td in cells:
                text = td.get_text(strip=True).lower()
                if text in _POSITION_MAP:
                    player["position_detail"] = td.get_text(strip=True)
                    break

        # DOB + age
        for td in cells:
            text = td.get_text(strip=True)
            m_dob = re.match(r"(\w+ \d+, \d{4})\s*\((\d+)\)", text)
            if m_dob:
                player["date_of_birth"] = m_dob.group(1)
                player["age"] = int(m_dob.group(2))
                break

        # Nationality (flag images)
        flag_imgs = row.find_all("img", class_="flaggenrahmen")
        nationalities = []
        for img in flag_imgs:
            nat = img.get("title", "").strip()
            if nat and nat not in nationalities:
                nationalities.append(nat)
        if nationalities:
            player["nationality"] = nationalities[0]
            player["nationalities"] = nationalities

        # Height
        for td in cells:
            text = td.get_text(strip=True)
            if text.endswith("m") and "," in text:
                player["height_raw"] = text
                try:
                    player["height_cm"] = int(
                        float(text.replace(",", ".").rstrip("m").strip()) * 100
                    )
                except ValueError:
                    pass
                break

        # Preferred foot
        for td in cells:
            text = td.get_text(strip=True).lower()
            if text in ("right", "left", "both"):
                player["preferred_foot"] = text[0].upper()
                break

        # Market value
        for td in cells:
            text = td.get_text(strip=True)
            if "£" in text or "€" in text or "$" in text:
                player["market_value_raw"] = text
                break

        # Contract expiry
        for td in cells:
            text = td.get_text(strip=True)
            if re.match(r"\w+ \d+, \d{4}$", text) and "date_of_birth" in player and text != player.get("date_of_birth"):
                player["contract_expiry"] = text
                break

        return player

    # ══════════════════════════════════════════════════════════════
    # Individual player profile
    # ══════════════════════════════════════════════════════════════

    def scrape_player_profile(
        self,
        player_id: str | int,
    ) -> dict[str, Any] | None:
        """Scrape a full player profile page."""
        url = f"{BASE_URL}/any/profil/spieler/{player_id}"
        resp = self._get(url)
        if not resp:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        profile: dict[str, Any] = {"tm_player_id": str(player_id)}

        header = soup.find("h1", class_="data-header__headline-wrapper")
        if header:
            profile["name"] = header.get_text(strip=True)

        info_table = soup.find("div", class_="info-table")
        if info_table:
            for row in info_table.find_all("span", class_="info-table__content"):
                label_el = row.find_previous_sibling("span", class_="info-table__content--regular")
                if not label_el:
                    label_el = row.find_previous("span", class_="info-table__content--regular")
                label = label_el.get_text(strip=True).rstrip(":").lower() if label_el else ""
                value = row.get_text(strip=True)

                if "date of birth" in label:
                    profile["date_of_birth"] = value
                elif "height" in label:
                    profile["height_raw"] = value
                    try:
                        profile["height_cm"] = int(
                            float(value.replace(",", ".").rstrip("m").strip()) * 100
                        )
                    except ValueError:
                        pass
                elif "citizenship" in label or "nationality" in label:
                    profile["nationality"] = value
                elif "position" in label:
                    profile["position_detail"] = value
                elif "foot" in label:
                    foot = value.lower()
                    if foot in ("right", "left", "both"):
                        profile["preferred_foot"] = foot[0].upper()
                elif "agent" in label:
                    profile["agent"] = value
                elif "contract" in label and "expir" in label:
                    profile["contract_expiry"] = value
                elif "current club" in label:
                    profile["current_club_name"] = value

        mv_el = soup.find("div", class_="tm-player-market-value-development__current-value")
        if mv_el:
            profile["market_value_raw"] = mv_el.get_text(strip=True)

        img = soup.find("img", class_="data-header__profile-image")
        if img and img.get("src"):
            src = img["src"]
            if "default" not in src.lower():
                profile["photo_url"] = src

        return profile

    # ══════════════════════════════════════════════════════════════
    # Player season stats (leistungsdaten)
    # ══════════════════════════════════════════════════════════════

    def scrape_player_stats(
        self,
        player_id: str | int,
        season: int | None = None,
    ) -> list[dict[str, Any]]:
        """Scrape per-season performance stats for a player.

        If *season* is given (e.g. ``2024``), fetches that single
        season's detailed breakdown.  Otherwise fetches the career
        overview which has one row per season/competition.

        Returns a list of dicts, one per competition/season row.
        """
        pid = str(player_id)
        if season is not None:
            url = (
                f"{BASE_URL}/any/leistungsdaten/spieler/{pid}"
                f"/plus/0?saison={season}"
            )
        else:
            url = f"{BASE_URL}/any/leistungsdaten/spieler/{pid}/plus/0"

        resp = self._get(url)
        if not resp:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        seasons: list[dict[str, Any]] = []

        for table in soup.find_all("table", class_="items"):
            header_cells = table.find_all("th")
            col_names = [th.get_text(strip=True).lower() for th in header_cells]

            for row in table.find_all("tr", class_=["odd", "even"]):
                cells = row.find_all("td", recursive=False)
                if len(cells) < 5:
                    continue
                entry = self._parse_stats_row(cells, col_names, pid)
                if entry:
                    if season is not None:
                        entry["api_season"] = season
                    seasons.append(entry)

        logger.info(
            "  Stats for player %s: %d season/comp rows", pid, len(seasons),
        )
        return seasons

    @staticmethod
    def _parse_stats_row(
        cells: list[Tag],
        col_names: list[str],
        player_id: str,
    ) -> dict[str, Any] | None:
        """Extract stats from a single row of the performance table."""
        entry: dict[str, Any] = {"tm_player_id": player_id}

        def _cell_int(idx: int) -> int | None:
            if idx >= len(cells):
                return None
            text = cells[idx].get_text(strip=True).replace(".", "")
            if text == "-" or not text:
                return None
            try:
                return int(text)
            except ValueError:
                return None

        def _find_col(keyword: str) -> int:
            for i, name in enumerate(col_names):
                if keyword in name:
                    return i
            return -1

        # Season / competition from first cells
        season_text = cells[0].get_text(strip=True) if cells else ""
        if season_text:
            entry["season_raw"] = season_text

        comp_link = None
        for c in cells[:3]:
            a = c.find("a", href=True)
            if a and "/wettbewerb/" in a.get("href", ""):
                comp_link = a
                break
        if comp_link:
            entry["competition"] = comp_link.get_text(strip=True)
            entry["competition_url"] = comp_link["href"]

        club_link = None
        for c in cells[:5]:
            a = c.find("a", href=True)
            if a and "/verein/" in a.get("href", ""):
                club_link = a
                break
        if club_link:
            entry["club_name"] = club_link.get_text(strip=True)

        # Numeric stat columns — Transfermarkt tables vary, so we
        # try both column-name lookup and positional fallback.
        stat_fields = [
            ("appearances", "appear", "match"),
            ("goals", "goal", None),
            ("assists", "assist", None),
            ("yellow_cards", "yellow", None),
            ("second_yellows", "second", "2nd"),
            ("red_cards", "red", None),
            ("minutes_played", "minut", None),
            ("goals_per_match", "per match", "per game"),
        ]

        numeric_cells = []
        for c in cells:
            text = c.get_text(strip=True).replace(".", "")
            if text == "-" or re.match(r"^[\d',]+$", text.replace(".", "")):
                numeric_cells.append(c)

        for idx, (field, kw1, kw2) in enumerate(stat_fields):
            col_idx = _find_col(kw1)
            if col_idx < 0 and kw2:
                col_idx = _find_col(kw2)

            if col_idx >= 0:
                val = _cell_int(col_idx)
            elif idx < len(numeric_cells):
                text = numeric_cells[idx].get_text(strip=True).replace(".", "").replace("'", "")
                if text == "-" or not text:
                    val = None
                else:
                    try:
                        val = int(text)
                    except ValueError:
                        val = None
            else:
                val = None

            entry[field] = val

        # Minutes can have tick marks like 1'234
        for c in cells:
            text = c.get_text(strip=True)
            if "'" in text and re.match(r"[\d'.]+$", text):
                cleaned = text.replace("'", "").replace(".", "")
                try:
                    entry["minutes_played"] = int(cleaned)
                except ValueError:
                    pass
                break

        if not entry.get("appearances") and not entry.get("goals"):
            return None

        return entry

    # ══════════════════════════════════════════════════════════════
    # Player transfer history
    # ══════════════════════════════════════════════════════════════

    def scrape_player_transfers(
        self,
        player_id: str | int,
    ) -> list[dict[str, Any]]:
        """Scrape the full transfer history for a player.

        Returns a list of transfer dicts with date, from_club,
        to_club, fee, market_value_at_time, etc.
        """
        pid = str(player_id)
        url = f"{BASE_URL}/any/transfers/spieler/{pid}"
        resp = self._get(url)
        if not resp:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        transfers: list[dict[str, Any]] = []

        grid = soup.find("div", class_="grid")
        if not grid:
            tables = soup.find_all("table", class_="items")
            for table in tables:
                for row in table.find_all("tr", class_=["odd", "even"]):
                    t = self._parse_transfer_row(row, pid)
                    if t:
                        transfers.append(t)
        else:
            for box in soup.find_all("div", class_="grid__cell"):
                t = self._parse_transfer_box(box, pid)
                if t:
                    transfers.append(t)

        if not transfers:
            for table in soup.find_all("table"):
                for row in table.find_all("tr"):
                    t = self._parse_transfer_row(row, pid)
                    if t:
                        transfers.append(t)

        logger.info(
            "  Transfers for player %s: %d entries", pid, len(transfers),
        )
        return transfers

    @staticmethod
    def _parse_transfer_row(
        row: Tag,
        player_id: str,
    ) -> dict[str, Any] | None:
        """Parse a transfer from table row format."""
        cells = row.find_all("td")
        if len(cells) < 4:
            return None

        entry: dict[str, Any] = {"tm_player_id": player_id}

        # Transfer date
        for td in cells:
            text = td.get_text(strip=True)
            if re.match(r"\w+ \d+, \d{4}", text):
                entry["transfer_date"] = text
                break

        # From / to clubs
        club_links = []
        for td in cells:
            for a in td.find_all("a", href=re.compile(r"/verein/\d+")):
                name = a.get_text(strip=True)
                if name and name not in [c["name"] for c in club_links]:
                    club_links.append({"name": name, "url": a["href"]})

        if len(club_links) >= 2:
            entry["from_club"] = club_links[0]["name"]
            entry["to_club"] = club_links[1]["name"]
        elif len(club_links) == 1:
            entry["to_club"] = club_links[0]["name"]

        # Fee / loan info
        for td in cells:
            text = td.get_text(strip=True).lower()
            if any(kw in text for kw in ("£", "€", "$", "free", "loan", "fee", "undisclosed")):
                entry["transfer_fee_raw"] = td.get_text(strip=True)
                if "free" in text:
                    entry["transfer_type"] = "free_transfer"
                elif "loan" in text and "end" in text:
                    entry["transfer_type"] = "end_of_loan"
                elif "loan" in text:
                    entry["transfer_type"] = "loan"
                else:
                    entry["transfer_type"] = "transfer"
                break

        # Market value at time
        for td in cells:
            cls = td.get("class", [])
            if "rechts" in cls or "right" in cls:
                text = td.get_text(strip=True)
                if text and ("£" in text or "€" in text or "k" in text.lower() or "m" in text.lower()):
                    if "transfer_fee_raw" not in entry or text != entry.get("transfer_fee_raw"):
                        entry["market_value_at_time"] = text

        if not entry.get("transfer_date") and not entry.get("to_club"):
            return None

        return entry

    @staticmethod
    def _parse_transfer_box(
        box: Tag,
        player_id: str,
    ) -> dict[str, Any] | None:
        """Parse a transfer from the newer grid/box layout."""
        entry: dict[str, Any] = {"tm_player_id": player_id}

        date_el = box.find("span", class_="tm-player-transfer-history-grid__date")
        if date_el:
            entry["transfer_date"] = date_el.get_text(strip=True)

        old_club = box.find("span", class_="tm-player-transfer-history-grid__old-club")
        if old_club:
            a = old_club.find("a")
            entry["from_club"] = a.get_text(strip=True) if a else old_club.get_text(strip=True)

        new_club = box.find("span", class_="tm-player-transfer-history-grid__new-club")
        if new_club:
            a = new_club.find("a")
            entry["to_club"] = a.get_text(strip=True) if a else new_club.get_text(strip=True)

        fee_el = box.find("span", class_="tm-player-transfer-history-grid__fee")
        if fee_el:
            fee_text = fee_el.get_text(strip=True)
            entry["transfer_fee_raw"] = fee_text
            lower = fee_text.lower()
            if "free" in lower:
                entry["transfer_type"] = "free_transfer"
            elif "loan" in lower and "end" in lower:
                entry["transfer_type"] = "end_of_loan"
            elif "loan" in lower:
                entry["transfer_type"] = "loan"
            else:
                entry["transfer_type"] = "transfer"

        mv_el = box.find("span", class_="tm-player-transfer-history-grid__market-value")
        if mv_el:
            entry["market_value_at_time"] = mv_el.get_text(strip=True)

        if not entry.get("transfer_date") and not entry.get("to_club"):
            return None

        return entry

    # ══════════════════════════════════════════════════════════════
    # Player market value history
    # ══════════════════════════════════════════════════════════════

    def scrape_market_value_history(
        self,
        player_id: str | int,
    ) -> list[dict[str, Any]]:
        """Scrape the market value history chart data.

        Transfermarkt embeds the chart data as a JavaScript object
        in the page.  We try to extract it from the embedded JSON
        first, then fall back to parsing the ``highcharts`` config.

        Returns a list of ``{date, value, club}`` dicts.
        """
        pid = str(player_id)
        url = f"{BASE_URL}/any/marktwertverlauf/spieler/{pid}"
        resp = self._get(url)
        if not resp:
            return []

        values: list[dict[str, Any]] = []

        # Transfermarkt often embeds chart data in a <script> with
        # "series" or "Highcharts" containing the value points.
        for script in BeautifulSoup(resp.text, "html.parser").find_all("script"):
            text = script.string or ""
            if "series" not in text and "Highcharts" not in text:
                continue

            # Look for the data array in the Highcharts config
            data_match = re.search(
                r"'data'\s*:\s*(\[.*?\])\s*[,}]", text, re.DOTALL,
            )
            if data_match:
                try:
                    data = json.loads(data_match.group(1))
                    for point in data:
                        if isinstance(point, dict):
                            values.append({
                                "date": point.get("datum_mw"),
                                "value_raw": point.get("mw"),
                                "value_formatted": point.get("y"),
                                "club": point.get("verein"),
                                "age": point.get("age"),
                            })
                except (json.JSONDecodeError, TypeError):
                    pass

            # Alternative embedded JSON format
            json_match = re.search(
                r"var\s+chart_data\s*=\s*(\[.*?\])\s*;", text, re.DOTALL,
            )
            if json_match and not values:
                try:
                    data = json.loads(json_match.group(1))
                    for point in data:
                        if isinstance(point, dict):
                            values.append({
                                "date": point.get("x") or point.get("datum_mw"),
                                "value_raw": point.get("y") or point.get("mw"),
                                "club": point.get("verein"),
                            })
                except (json.JSONDecodeError, TypeError):
                    pass

        # Fallback: parse a visible table if the JS extraction failed
        if not values:
            soup = BeautifulSoup(resp.text, "html.parser")
            for row in soup.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue
                date_text = cells[0].get_text(strip=True)
                val_text = cells[-1].get_text(strip=True)
                if date_text and ("£" in val_text or "€" in val_text or "k" in val_text.lower()):
                    values.append({
                        "date": date_text,
                        "value_raw": val_text,
                    })

        logger.info(
            "  Market value history for player %s: %d data points",
            pid, len(values),
        )
        return values

    # ══════════════════════════════════════════════════════════════
    # Full player deep-scrape (profile + stats + transfers + MV)
    # ══════════════════════════════════════════════════════════════

    def scrape_player_full(
        self,
        player_id: str | int,
        season: int | None = None,
    ) -> dict[str, Any] | None:
        """Scrape all available data for a single player.

        Combines profile, stats, transfers, and market value history
        into one dict.  Used by enrichment mode.
        """
        profile = self.scrape_player_profile(player_id)
        if not profile:
            return None

        profile["stats"] = self.scrape_player_stats(player_id, season)
        profile["transfers"] = self.scrape_player_transfers(player_id)
        profile["market_value_history"] = self.scrape_market_value_history(player_id)

        return profile

    # ══════════════════════════════════════════════════════════════
    # Search / enrichment
    # ══════════════════════════════════════════════════════════════

    def search_player(
        self,
        name: str,
        dob: str | None = None,
        *,
        full: bool = True,
    ) -> dict[str, Any] | None:
        """Search Transfermarkt for a player and return best match.

        If *dob* is provided (``YYYY-MM-DD``), the result whose DOB
        is closest is preferred.

        When *full* is ``True`` (the default), the matched player's
        profile, stats, transfers, and market value history are all
        scraped and included in the returned dict.
        """
        url = (
            f"{BASE_URL}/schnellsuche/ergebnis/schnellsuche"
            f"?query={quote_plus(name)}&Spieler_Spieler=1"
        )
        resp = self._get(url)
        if not resp:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        results_table = soup.find("table", class_="items")
        if not results_table:
            return None

        best: dict[str, Any] | None = None
        for row in results_table.find_all("tr", class_=["odd", "even"]):
            a = row.find("a", href=re.compile(r"/spieler/\d+"))
            if not a:
                continue

            href = a["href"]
            m = re.search(r"/spieler/(\d+)", href)
            if not m:
                continue

            result_name = a.get_text(strip=True)
            player_id = m.group(1)

            result_dob = None
            for td in row.find_all("td"):
                text = td.get_text(strip=True)
                if re.match(r"\w+ \d+, \d{4}\s*\(\d+\)", text):
                    result_dob = re.match(r"(\w+ \d+, \d{4})", text)
                    if result_dob:
                        result_dob = result_dob.group(1)
                    break

            candidate = {
                "name": result_name,
                "tm_player_id": player_id,
                "date_of_birth": result_dob,
            }

            if best is None:
                best = candidate

            if dob and result_dob and dob in str(result_dob):
                best = candidate
                break

        if best and best.get("tm_player_id"):
            pid = best["tm_player_id"]
            if full:
                full_data = self.scrape_player_full(pid)
                if full_data:
                    best.update(full_data)
            else:
                profile = self.scrape_player_profile(pid)
                if profile:
                    best.update(profile)

        return best


# ═══════════════════════════════════════════════════════════════════════════
# CLI:  python -m src.scrapers.transfermarkt [comp_id]
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    comp = sys.argv[1] if len(sys.argv) > 1 else "CNAT"
    print(f"\nTransfermarkt scraper — competition {comp}")
    print("=" * 60)

    scraper = TransfermarktScraper()
    clubs = scraper.scrape_competition(comp)

    total_players = 0
    for club in clubs:
        players = club.get("players", [])
        total_players += len(players)
        print(f"  {club['name']:<35s}  {len(players)} players")

    print(f"\n  Total: {len(clubs)} clubs, {total_players} players")
    print("=" * 60)
