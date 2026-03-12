"""Scraper for FA Full-Time (https://fulltime.thefa.com).

FA Full-Time is the FA's official results service.  Steps 4-6 leagues
(and many grassroots / county leagues) publish tables, results, and
per-team player appearance stats here.

**Site structure (server-rendered HTML, no JS framework required):**

League landing page::

    /index.html?league={league_id}
      → season & division dropdowns with numeric IDs

League table::

    /table.html?league={lid}&selectedSeason={sid}&selectedDivision={did}
                &selectedCompetition=0&selectedFixtureGroupKey={fgk}
      → ``<table class="cell-dividers">``  POS | Team | P | W | D | L | F | A | GD | PTS
      → team links: ``/displayTeam.html?divisionseason={ds}&teamID={tid}``

Results page::

    /results.html?league={lid}&selectedSeason={sid}&selectedDivision={did}
                  &selectedCompetition=0&selectedFixtureGroupKey={fgk}
      → div-based grid with fixture IDs, teams, scores, dates

Fixture detail::

    /displayFixture.html?id={fixture_id}
      → home/away team names, score, date/time, venue, attendance
      → **NO lineups** for adult or youth leagues

Team page::

    /displayTeam.html?divisionseason={ds}&teamID={tid}
      → tabs: Results | Fixtures | Player Season Totals | … | Players
      → "Player Season Totals" has per-match appearance data with player names

League discovery::

    /home/mostVisitedLeagues.html   — ~1000 leagues
    /home/leagues.html              — A–Z directory (paginated, ~30/page)
    /home/search.html               — POST search by name

Usage::

    from src.scrapers.fa_fulltime import FAFullTimeScraper

    scraper = FAFullTimeScraper()

    # discover leagues relevant to our pyramid
    leagues = scraper.discover_nonleague_leagues()

    # scrape a league: table + results + team player data
    scraper.scrape_league(league_id="840602727")

CLI smoke-test::

    python -m src.scrapers.fa_fulltime [league_id]
"""

import logging
import re
import sys
import time
from dataclasses import dataclass, field
from html import unescape
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import requests
from bs4 import BeautifulSoup, Tag

from src.config import SCRAPE_DELAY_SECONDS

logger = logging.getLogger(__name__)

BASE_URL = "https://fulltime.thefa.com"

MAX_RETRIES = 3
INITIAL_BACKOFF_SECS = 2.0
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
]

# FA Full-Time league IDs for Step 4-6 leagues we care about.
# Discovered from /home/mostVisitedLeagues.html
KNOWN_LEAGUE_IDS: dict[str, str] = {
    "Southern Combination Football League": "840602727",
    "United Counties Football League": "1625657",
    "Spartan South Midlands Football League": "522238936",
    "Hellenic League": "646734134",
    "Combined Counties Football League": "3956158",
    "Midland Football League": "6125369",
    "Western Football League": "3355283",
    "Wessex Football League": "274386",
    "Southern Counties East Football League": "9431449",
    "Essex Senior Football League": "2829940",
    "North West Counties League": "7727970",
    "Northern Premier League": "3891769",
}


@dataclass
class DivisionInfo:
    """Metadata for a single division within a league on FA Full-Time."""

    league_id: str
    season_id: str
    division_id: str
    fixture_group_key: str
    division_name: str = ""
    season_name: str = ""


@dataclass
class TableRow:
    """A single row from a league table."""

    position: int
    team_name: str
    team_id: str = ""
    division_season: str = ""
    played: int = 0
    won: int = 0
    drawn: int = 0
    lost: int = 0
    goals_for: int = 0
    goals_against: int = 0
    goal_difference: int = 0
    points: int = 0


@dataclass
class MatchResult:
    """A single match result scraped from the results page."""

    fixture_id: str
    match_type: str = ""
    date: str = ""
    time: str = ""
    home_team: str = ""
    away_team: str = ""
    home_score: int | None = None
    away_score: int | None = None
    competition: str = ""


@dataclass
class FixtureDetail:
    """Detailed fixture data from the fixture detail page."""

    fixture_id: str
    home_team: str = ""
    away_team: str = ""
    home_score: int | None = None
    away_score: int | None = None
    date: str = ""
    time: str = ""
    venue: str = ""
    attendance: int | None = None
    home_logo_url: str = ""
    away_logo_url: str = ""


@dataclass
class PlayerAppearance:
    """Per-player season summary from the team page."""

    player_name: str
    team_name: str = ""
    team_id: str = ""
    appearances: int = 0
    goals: int = 0
    position: str = ""
    match_data: list[dict[str, Any]] = field(default_factory=list)


class FAFullTimeScraper:
    """Scrape league tables, results, and player data from FA Full-Time."""

    def __init__(self, delay_secs: float = SCRAPE_DELAY_SECONDS) -> None:
        self._session = requests.Session()
        self._delay = delay_secs
        self._last_request_at: float = 0.0
        self._ua_index = 0

    def _next_ua(self) -> str:
        ua = _USER_AGENTS[self._ua_index % len(_USER_AGENTS)]
        self._ua_index += 1
        return ua

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self._delay:
            time.sleep(self._delay - elapsed)

    def _get(self, url: str) -> requests.Response | None:
        """GET with throttle, retry, and User-Agent rotation."""
        backoff = INITIAL_BACKOFF_SECS

        for attempt in range(1, MAX_RETRIES + 1):
            self._throttle()
            self._last_request_at = time.monotonic()

            headers = {"User-Agent": self._next_ua()}
            logger.info("FA Full-Time GET %s  (attempt %d/%d)", url, attempt, MAX_RETRIES)

            try:
                resp = self._session.get(url, headers=headers, timeout=30)
            except requests.RequestException as exc:
                logger.warning("Request error: %s", exc)
                if attempt == MAX_RETRIES:
                    return None
                time.sleep(backoff)
                backoff *= 2
                continue

            logger.info("FA Full-Time %d  %s", resp.status_code, url)

            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                return None
            if resp.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
                logger.warning("Retryable %d — backing off %.1fs", resp.status_code, backoff)
                time.sleep(backoff)
                backoff *= 2
                continue
            return None

        return None

    def _soup(self, url: str) -> BeautifulSoup | None:
        resp = self._get(url)
        if resp is None:
            return None
        return BeautifulSoup(resp.text, "html.parser")

    # ─── league discovery ─────────────────────────────────────────────────

    def discover_all_leagues(self) -> list[dict[str, str]]:
        """Fetch the ~1000 most-visited leagues from FA Full-Time.

        Returns a list of ``{"name": ..., "league_id": ...}`` dicts.
        """
        soup = self._soup(f"{BASE_URL}/home/mostVisitedLeagues.html")
        if soup is None:
            logger.error("Failed to fetch most-visited leagues page")
            return []

        leagues: list[dict[str, str]] = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "league=" not in href:
                continue
            name = link.get_text(strip=True)
            if not name:
                continue
            match = re.search(r"league=(\d+)", href)
            if match:
                leagues.append({"name": name, "league_id": match.group(1)})

        logger.info("Discovered %d leagues from mostVisitedLeagues", len(leagues))
        return leagues

    def discover_nonleague_leagues(self) -> list[dict[str, str]]:
        """Filter discovered leagues to those relevant to Steps 4-6.

        Uses keyword matching against league names.  Returns a subset
        of ``discover_all_leagues()`` output.
        """
        keywords = [
            "combined counties", "eastern counties", "essex senior",
            "hellenic", "midland football league", "north west counties",
            "northern league", "northern counties east",
            "northern premier", "southern combination",
            "southern counties east", "southern league",
            "spartan south", "united counties", "wessex football",
            "western football league", "isthmian",
        ]
        all_leagues = self.discover_all_leagues()
        matches: list[dict[str, str]] = []
        for league in all_leagues:
            lname = league["name"].lower()
            if any(kw in lname for kw in keywords):
                # Skip youth / women's / development / cup-only variants
                if any(
                    skip in lname
                    for skip in ("youth", "women", "u18", "u23", "u16", "u14", "u12",
                                 "development", "invitation", "cup")
                ):
                    continue
                matches.append(league)

        logger.info("Found %d non-league leagues matching our pyramid", len(matches))
        return matches

    def search_leagues(self, query: str) -> list[dict[str, str]]:
        """Search FA Full-Time for leagues by name (POST form)."""
        self._throttle()
        self._last_request_at = time.monotonic()

        url = f"{BASE_URL}/home/search.html"
        try:
            resp = self._session.post(
                url,
                data={"searchString": query},
                headers={"User-Agent": self._next_ua()},
                timeout=30,
            )
        except requests.RequestException as exc:
            logger.warning("Search request error: %s", exc)
            return []

        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        results: list[dict[str, str]] = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "league=" not in href:
                continue
            name = link.get_text(strip=True)
            match = re.search(r"league=(\d+)", href)
            if match and name:
                results.append({"name": name, "league_id": match.group(1)})

        logger.info("Search '%s' returned %d leagues", query, len(results))
        return results

    # ─── league metadata (seasons / divisions) ────────────────────────────

    def get_divisions(self, league_id: str) -> list[DivisionInfo]:
        """Get available seasons, divisions, and fixture group keys for a league.

        Parses the league landing page dropdowns to extract the IDs
        needed to build table/results URLs.
        """
        soup = self._soup(f"{BASE_URL}/index.html?league={league_id}")
        if soup is None:
            logger.error("Failed to load league landing page for %s", league_id)
            return []

        # Extract the default parameters from the first table/results link
        table_link = soup.find("a", href=re.compile(r"table\.html"))
        if not table_link:
            logger.warning("No table link found for league %s", league_id)
            return []

        href = table_link.get("href", "")
        parsed = urlparse(href)
        params = parse_qs(parsed.query)

        season_id = params.get("selectedSeason", [""])[0]
        division_id = params.get("selectedDivision", [""])[0]
        fgk = params.get("selectedFixtureGroupKey", [""])[0]

        if not season_id:
            logger.warning("Could not extract season ID for league %s", league_id)
            return []

        # Parse season and division dropdowns for all options
        divisions: list[DivisionInfo] = []
        season_options: list[tuple[str, str]] = []
        division_options: list[tuple[str, str]] = []

        for select in soup.find_all("select"):
            select_name = select.get("name", "")
            for option in select.find_all("option"):
                val = option.get("value", "")
                text = option.get_text(strip=True)
                if not val or not text:
                    continue
                if "Season" in select_name or "season" in select_name:
                    season_options.append((val, text))
                elif "Division" in select_name or "division" in select_name:
                    if val.isdigit() and len(val) > 4:
                        division_options.append((val, text))

        if division_options:
            for div_id, div_name in division_options:
                # Skip age-group / youth divisions
                lname = div_name.lower()
                if any(s in lname for s in ("u18", "u23", "u16", "u14", "u12")):
                    continue
                divisions.append(DivisionInfo(
                    league_id=league_id,
                    season_id=season_id,
                    division_id=div_id,
                    fixture_group_key=fgk,
                    division_name=div_name,
                    season_name=next(
                        (s[1] for s in season_options if s[0] == season_id), ""
                    ),
                ))
        else:
            divisions.append(DivisionInfo(
                league_id=league_id,
                season_id=season_id,
                division_id=division_id,
                fixture_group_key=fgk,
            ))

        logger.info(
            "League %s: %d division(s), season %s",
            league_id, len(divisions), season_id,
        )
        return divisions

    # ─── league table ─────────────────────────────────────────────────────

    def _table_url(self, div: DivisionInfo) -> str:
        params = {
            "league": div.league_id,
            "selectedSeason": div.season_id,
            "selectedDivision": div.division_id,
            "selectedCompetition": "0",
            "selectedFixtureGroupKey": div.fixture_group_key,
        }
        return f"{BASE_URL}/table.html?{urlencode(params)}"

    def scrape_league_table(self, div: DivisionInfo) -> list[TableRow]:
        """Scrape the league table for a given division."""
        soup = self._soup(self._table_url(div))
        if soup is None:
            return []

        table = soup.find("table", class_="cell-dividers")
        if not table:
            logger.warning("No cell-dividers table found")
            return []

        rows: list[TableRow] = []
        for tr in table.find_all("tr"):  # type: ignore[union-attr]
            cells = tr.find_all("td")
            if len(cells) < 7:
                continue

            # Extract team name and IDs from the link
            team_name = ""
            team_id = ""
            division_season = ""
            team_link = tr.find("a", href=re.compile(r"displayTeam"))
            if team_link:
                team_name = team_link.get_text(strip=True)
                href = team_link.get("href", "")
                tid_match = re.search(r"teamID=(\d+)", href)
                ds_match = re.search(r"divisionseason=(\d+)", href)
                if tid_match:
                    team_id = tid_match.group(1)
                if ds_match:
                    division_season = ds_match.group(1)
            else:
                left_cell = tr.find("td", class_="left")
                if left_cell:
                    team_name = left_cell.get_text(strip=True)

            if not team_name:
                continue

            def _int(cell: Tag) -> int:
                text = cell.get_text(strip=True)
                try:
                    return int(text)
                except (ValueError, TypeError):
                    return 0

            rows.append(TableRow(
                position=_int(cells[0]),
                team_name=team_name,
                team_id=team_id,
                division_season=division_season,
                played=_int(cells[2]) if len(cells) > 2 else 0,
                won=_int(cells[3]) if len(cells) > 3 else 0,
                drawn=_int(cells[4]) if len(cells) > 4 else 0,
                lost=_int(cells[5]) if len(cells) > 5 else 0,
                goals_for=_int(cells[6]) if len(cells) > 6 else 0,
                goals_against=_int(cells[7]) if len(cells) > 7 else 0,
                goal_difference=_int(cells[8]) if len(cells) > 8 else 0,
                points=_int(cells[9]) if len(cells) > 9 else 0,
            ))

        logger.info("Scraped %d teams from table (division %s)", len(rows), div.division_name)
        return rows

    # ─── results ──────────────────────────────────────────────────────────

    def _results_url(self, div: DivisionInfo) -> str:
        params = {
            "league": div.league_id,
            "selectedSeason": div.season_id,
            "selectedDivision": div.division_id,
            "selectedCompetition": "0",
            "selectedFixtureGroupKey": div.fixture_group_key,
        }
        return f"{BASE_URL}/results.html?{urlencode(params)}"

    def scrape_results(self, div: DivisionInfo) -> list[MatchResult]:
        """Scrape all match results for a division.

        The results page uses a ``<div>``-based grid (not ``<table>``).
        Each fixture block has ``id="fixture-{id}"`` with child divs
        for type, date, home/away teams, and score.
        """
        soup = self._soup(self._results_url(div))
        if soup is None:
            return []

        results: list[MatchResult] = []

        for fixture_div in soup.find_all("div", id=re.compile(r"^fixture-\d+")):
            fid_match = re.search(r"fixture-(\d+)", fixture_div.get("id", ""))
            if not fid_match:
                continue
            fixture_id = fid_match.group(1)

            type_div = fixture_div.find("div", class_="type-col")
            datetime_div = fixture_div.find("div", class_="datetime-col")
            home_div = fixture_div.find("div", class_="home-team-col")
            score_div = fixture_div.find("div", class_="score-col")
            away_div = fixture_div.find("div", class_="road-team-col")
            comp_div = fixture_div.find("div", class_="fg-col")

            match_type = type_div.get_text(strip=True) if type_div else ""

            date_str = ""
            time_str = ""
            if datetime_div:
                dt_text = datetime_div.get_text(strip=True)
                dt_parts = re.match(r"(\d{2}/\d{2}/\d{2})\s*(\d{2}:\d{2})?", dt_text)
                if dt_parts:
                    date_str = dt_parts.group(1)
                    time_str = dt_parts.group(2) or ""

            home_team = ""
            if home_div:
                home_link = home_div.find("a")
                home_team = (home_link or home_div).get_text(strip=True)
                home_team = unescape(home_team)

            away_team = ""
            if away_div:
                away_link = away_div.find("a")
                away_team = (away_link or away_div).get_text(strip=True)
                away_team = unescape(away_team)

            home_score = None
            away_score = None
            if score_div:
                score_text = score_div.get_text(strip=True)
                score_match = re.search(r"(\d+)\s*-\s*(\d+)", score_text)
                if score_match:
                    home_score = int(score_match.group(1))
                    away_score = int(score_match.group(2))

            competition = comp_div.get_text(strip=True) if comp_div else ""

            results.append(MatchResult(
                fixture_id=fixture_id,
                match_type=match_type,
                date=date_str,
                time=time_str,
                home_team=home_team,
                away_team=away_team,
                home_score=home_score,
                away_score=away_score,
                competition=competition,
            ))

        logger.info("Scraped %d results for division %s", len(results), div.division_name)
        return results

    # ─── fixture detail ───────────────────────────────────────────────────

    def scrape_fixture(self, fixture_id: str) -> FixtureDetail | None:
        """Scrape a single fixture detail page.

        Returns team names, score, date, venue, and attendance.
        **Lineups are not available** on FA Full-Time fixture pages.
        """
        soup = self._soup(f"{BASE_URL}/displayFixture.html?id={fixture_id}")
        if soup is None:
            return None

        detail = FixtureDetail(fixture_id=fixture_id)

        # Teams
        home_h2 = soup.select_one(".home-team .team-name h2")
        away_h2 = soup.select_one(".road-team .team-name h2")
        if home_h2:
            detail.home_team = unescape(home_h2.get_text(strip=True))
        if away_h2:
            detail.away_team = unescape(away_h2.get_text(strip=True))

        # Logos
        home_img = soup.select_one(".home-team .team-logo img")
        away_img = soup.select_one(".road-team .team-logo img")
        if home_img:
            detail.home_logo_url = home_img.get("src", "")
        if away_img:
            detail.away_logo_url = away_img.get("src", "")

        # Score
        score_div = soup.select_one(".score.played .score-container")
        if score_div:
            score_ps = score_div.find_all("p")
            if len(score_ps) >= 2:
                try:
                    detail.home_score = int(score_ps[0].get_text(strip=True))
                    detail.away_score = int(score_ps[1].get_text(strip=True))
                except ValueError:
                    pass

        # Date / time / venue
        dt_div = soup.select_one(".fixture-date-time")
        if dt_div:
            ps = dt_div.find_all("p")
            for p in ps:
                text = p.get_text(strip=True)
                if re.match(r"\d{2}/\d{2}/\d{2}", text):
                    detail.date = text
                elif re.match(r"\d{2}:\d{2}", text):
                    detail.time = text
                elif text and text not in ("FT",):
                    detail.venue = text

        # Attendance
        score_section = soup.select_one(".score.played")
        if score_section:
            att_text = score_section.get_text()
            att_match = re.search(r"Attendance:\s*(\d+)", att_text)
            if att_match:
                detail.attendance = int(att_match.group(1))

        logger.info(
            "Fixture %s: %s %s-%s %s",
            fixture_id, detail.home_team,
            detail.home_score, detail.away_score,
            detail.away_team,
        )
        return detail

    # ─── team page / player data ──────────────────────────────────────────

    def scrape_team_players(
        self, division_season: str, team_id: str, team_name: str = "",
    ) -> list[PlayerAppearance]:
        """Scrape player appearance data from the team page.

        The "Player Season Totals" tab (tab-2) on each team page
        contains a table with player names and per-match appearances.
        This is the closest FA Full-Time gets to squad lists.
        """
        url = (
            f"{BASE_URL}/displayTeam.html?"
            f"divisionseason={division_season}&teamID={team_id}"
        )
        soup = self._soup(url)
        if soup is None:
            return []

        # Resolve team name from the page title if not provided
        if not team_name:
            title_tag = soup.find("title")
            if title_tag:
                title_text = title_tag.get_text(strip=True)
                # Title format: "Team Name | Season | Full-Time"
                team_name = title_text.split("|")[0].strip()

        players: list[PlayerAppearance] = []

        # Find all tables and look for the one with "Player Name" header
        for table in soup.find_all("table"):
            headers = [th.get_text(strip=True) for th in table.find_all("th")]
            if "Player Name" not in headers:
                continue

            pn_idx = headers.index("Player Name")

            for tr in table.find_all("tr"):
                cells = tr.find_all("td")
                if len(cells) <= pn_idx:
                    continue

                # Player name is in a link inside the cell
                name_cell = cells[pn_idx]
                name_link = name_cell.find("a")
                player_name = (name_link or name_cell).get_text(strip=True)

                if not player_name:
                    continue

                # Jersey numbers sometimes land in the name column.
                # If the value is purely numeric, try adjacent cells
                # for the real name before giving up.
                if player_name.isdigit():
                    recovered = False
                    for offset in (1, -1):
                        adj = pn_idx + offset
                        if 0 <= adj < len(cells):
                            candidate = cells[adj].get_text(strip=True)
                            if candidate and not candidate.isdigit():
                                logger.debug(
                                    "Recovered name %r from adjacent cell "
                                    "(was jersey %s)",
                                    candidate, player_name,
                                )
                                player_name = candidate
                                recovered = True
                                break
                    if not recovered:
                        logger.debug(
                            "Skipping purely numeric 'name' %s for team %s",
                            player_name, team_name,
                        )
                        continue

                # Count non-empty numbered columns as appearances
                appearances = 0
                goals = 0
                for i, header in enumerate(headers):
                    if not header.isdigit():
                        continue
                    if i < len(cells):
                        val = cells[i].get_text(strip=True)
                        if val:
                            appearances += 1
                            # Goal values might be encoded differently;
                            # for now we just count appearances

                # Look for "Appearances" and "Goals" columns if they exist
                if "Appearances" in headers:
                    app_idx = headers.index("Appearances")
                    if app_idx < len(cells):
                        try:
                            appearances = int(cells[app_idx].get_text(strip=True))
                        except ValueError:
                            pass

                if "Overall Goals" in headers:
                    g_idx = headers.index("Overall Goals")
                    if g_idx < len(cells):
                        try:
                            goals = int(cells[g_idx].get_text(strip=True))
                        except ValueError:
                            pass
                elif "Goals" in headers:
                    g_idx = headers.index("Goals")
                    if g_idx < len(cells):
                        try:
                            goals = int(cells[g_idx].get_text(strip=True))
                        except ValueError:
                            pass

                players.append(PlayerAppearance(
                    player_name=player_name,
                    team_name=team_name,
                    team_id=team_id,
                    appearances=appearances,
                    goals=goals,
                ))

            break  # only process the first player table

        logger.info(
            "Team %s (%s): %d players found",
            team_name, team_id, len(players),
        )
        return players

    # ─── high-level orchestration ─────────────────────────────────────────

    def scrape_league(self, league_id: str) -> dict[str, Any]:
        """Scrape a complete league: divisions, tables, results, and player data.

        Returns a dict structured for staging::

            {
                "league_id": "840602727",
                "divisions": [
                    {
                        "division_name": "Premier",
                        "table": [TableRow dicts],
                        "results": [MatchResult dicts],
                        "teams": {
                            "team_id": {
                                "team_name": "...",
                                "players": [PlayerAppearance dicts],
                            }
                        },
                    }
                ],
            }
        """
        logger.info("═══ Scraping league %s ═══", league_id)
        output: dict[str, Any] = {"league_id": league_id, "divisions": []}

        divisions = self.get_divisions(league_id)
        if not divisions:
            logger.warning("No divisions found for league %s", league_id)
            return output

        for div in divisions:
            logger.info("── Division: %s ──", div.division_name)
            div_data: dict[str, Any] = {
                "division_name": div.division_name,
                "season_name": div.season_name,
                "table": [],
                "results": [],
                "teams": {},
            }

            # Scrape table
            table_rows = self.scrape_league_table(div)
            div_data["table"] = [
                {
                    "position": r.position,
                    "team_name": r.team_name,
                    "team_id": r.team_id,
                    "division_season": r.division_season,
                    "played": r.played,
                    "won": r.won,
                    "drawn": r.drawn,
                    "lost": r.lost,
                    "goals_for": r.goals_for,
                    "goals_against": r.goals_against,
                    "goal_difference": r.goal_difference,
                    "points": r.points,
                }
                for r in table_rows
            ]

            # Scrape results
            results = self.scrape_results(div)
            div_data["results"] = [
                {
                    "fixture_id": r.fixture_id,
                    "match_type": r.match_type,
                    "date": r.date,
                    "time": r.time,
                    "home_team": r.home_team,
                    "away_team": r.away_team,
                    "home_score": r.home_score,
                    "away_score": r.away_score,
                    "competition": r.competition,
                }
                for r in results
            ]

            # Scrape player data from each team
            for row in table_rows:
                if not row.team_id or not row.division_season:
                    continue
                players = self.scrape_team_players(
                    division_season=row.division_season,
                    team_id=row.team_id,
                    team_name=row.team_name,
                )
                div_data["teams"][row.team_id] = {
                    "team_name": row.team_name,
                    "players": [
                        {
                            "player_name": p.player_name,
                            "team_name": p.team_name,
                            "team_id": p.team_id,
                            "appearances": p.appearances,
                            "goals": p.goals,
                        }
                        for p in players
                    ],
                }

            output["divisions"].append(div_data)

        # Summary
        total_teams = sum(
            len(d["table"]) for d in output["divisions"]
        )
        total_players = sum(
            len(t["players"])
            for d in output["divisions"]
            for t in d["teams"].values()
        )
        total_results = sum(len(d["results"]) for d in output["divisions"])

        logger.info(
            "League %s complete: %d division(s), %d teams, "
            "%d results, %d players",
            league_id, len(output["divisions"]),
            total_teams, total_results, total_players,
        )
        return output

    def build_staging_records(self, league_data: dict[str, Any]) -> dict[str, list[dict]]:
        """Convert scrape output into records ready for ``stage_records()``.

        Returns a dict with three keys:
        - ``"league_table"`` — one record per team row
        - ``"match"`` — one record per match result
        - ``"player"`` — one record per player appearance
        """
        league_id = league_data["league_id"]
        table_records: list[dict] = []
        match_records: list[dict] = []
        player_records: list[dict] = []

        for div in league_data.get("divisions", []):
            div_name = div.get("division_name", "")

            for row in div.get("table", []):
                row["id"] = f"{league_id}_{row.get('team_id', '')}"
                row["fa_fulltime_league_id"] = league_id
                row["division"] = div_name
                table_records.append(row)

            for result in div.get("results", []):
                result["id"] = result.get("fixture_id", "")
                result["fa_fulltime_league_id"] = league_id
                result["division"] = div_name
                match_records.append(result)

            for team_data in div.get("teams", {}).values():
                team_name = team_data.get("team_name", "")
                for player in team_data.get("players", []):
                    player["id"] = (
                        f"{league_id}_{player.get('team_id', '')}_"
                        f"{player.get('player_name', '')}"
                    )
                    player["fa_fulltime_league_id"] = league_id
                    player["division"] = div_name
                    player["club_name"] = team_name
                    player_records.append(player)

        return {
            "league_table": table_records,
            "match": match_records,
            "player": player_records,
        }


# ─── CLI ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    scraper = FAFullTimeScraper()

    if len(sys.argv) > 1:
        # Scrape a specific league
        lid = sys.argv[1]
        print(f"\nScraping league {lid}...")
        data = scraper.scrape_league(lid)
        for div in data["divisions"]:
            print(f"\n  Division: {div['division_name']}")
            print(f"    Table: {len(div['table'])} teams")
            print(f"    Results: {len(div['results'])} matches")
            players_total = sum(len(t["players"]) for t in div["teams"].values())
            print(f"    Players: {players_total} across {len(div['teams'])} teams")
            if div["table"]:
                print("    Top 5:")
                for row in div["table"][:5]:
                    print(f"      {row['position']:2d}. {row['team_name']:<30s}  "
                          f"P{row['played']}  W{row['won']}  PTS{row['points']}")
    else:
        # Discovery mode
        print("\n=== FA Full-Time Non-League Discovery ===\n")
        leagues = scraper.discover_nonleague_leagues()
        for league in leagues:
            print(f"  {league['name']:<55s}  league={league['league_id']}")
        print(f"\n  Total: {len(leagues)} leagues found")

        # Also show known league IDs
        print("\n=== Known Step 4-6 League IDs ===\n")
        for name, lid in sorted(KNOWN_LEAGUE_IDS.items()):
            print(f"  {name:<55s}  league={lid}")
