"""Scraper for Pitchero-powered club websites.

Pitchero is a Next.js application.  Every page embeds a
``<script id="__NEXT_DATA__">`` tag containing the full page state
as JSON, so we can extract structured data without parsing HTML.

**Squad data** lives at::

    __NEXT_DATA__
      .props.initialReduxState.teams.teamSquad.players.{teamId}
        [ {name: "Goalkeepers", members: [{name, id, position, avatar}, …]}, … ]

**Player profile** data (bio, stats) lives at::

    __NEXT_DATA__
      .props.initialReduxState.teams.teamSquad.profiles.{teamId}-p{playerId}
        {name, id, position, avatar, biography, birthplace, joined,
         previousClubs, occupation, …}

      .props.initialReduxState.teams.teamSquad.profileStats.{teamId}-p{playerId}-{seasonId}
        {featured: [{label: "Appearances", value: 12}, …]}

The **club homepage** embeds team listings at::

    __NEXT_DATA__
      .props.pageProps.club.sections[].teams[]
        {id, name, gender, navigationItems, …}

Usage::

    from src.scrapers.pitchero import PitcheroScraper

    scraper = PitcheroScraper()
    squad   = scraper.scrape_squad("https://www.pitchero.com/clubs/ashfordunitedfc")
    profile = scraper.scrape_player_profile(
        "https://www.pitchero.com/clubs/ashfordunitedfc",
        team_id=229937, player_id=2921610, player_slug="lanre-azeez",
    )

CLI smoke-test::

    python -m src.scrapers.pitchero ashfordunitedfc
"""

import json
import logging
import re
import time
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from src.config import SCRAPE_DELAY_SECONDS

logger = logging.getLogger(__name__)

# ── constants ────────────────────────────────────────────────────────────

PITCHERO_BASE = "https://www.pitchero.com"

MAX_RETRIES = 3
INITIAL_BACKOFF_SECS = 2.0
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
]

# Team-page URL suffixes to try (clubs label their first team differently)
_SQUAD_SUFFIXES = [
    "/the-team",
    "",
]

# First-team names to look for when picking the right team
_FIRST_TEAM_NAMES = {
    "first team", "1st team", "senior squad", "senior team",
    "first xi", "1st xi", "men's first team", "mens first team",
}


class PitcheroScraper:
    """Extract squad and player data from Pitchero club sites."""

    def __init__(self, delay_secs: float = SCRAPE_DELAY_SECONDS) -> None:
        self._session = requests.Session()
        self._delay = delay_secs
        self._last_request_at: float = 0.0
        self._ua_index = 0

    # ─── low-level helpers ───────────────────────────────────────────────

    def _next_ua(self) -> str:
        ua = _USER_AGENTS[self._ua_index % len(_USER_AGENTS)]
        self._ua_index += 1
        return ua

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self._delay:
            time.sleep(self._delay - elapsed)

    def _get(self, url: str) -> requests.Response | None:
        """GET with throttle, retry, and User-Agent rotation.

        Returns the Response on success or ``None`` on any error
        (including 404 — many clubs lack certain pages).
        """
        backoff = INITIAL_BACKOFF_SECS

        for attempt in range(1, MAX_RETRIES + 1):
            self._throttle()
            self._last_request_at = time.monotonic()

            headers = {"User-Agent": self._next_ua()}
            logger.info(
                "Pitchero GET %s  (attempt %d/%d)", url, attempt, MAX_RETRIES,
            )

            try:
                resp = self._session.get(url, headers=headers, timeout=30)
            except requests.RequestException as exc:
                logger.warning("Request error: %s", exc)
                if attempt == MAX_RETRIES:
                    return None
                time.sleep(backoff)
                backoff *= 2
                continue

            logger.info("Pitchero %d  %s", resp.status_code, url)

            if resp.status_code == 200:
                return resp

            if resp.status_code == 404:
                return None

            if resp.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
                logger.warning(
                    "Retryable %d — backing off %.1fs", resp.status_code, backoff,
                )
                time.sleep(backoff)
                backoff *= 2
                continue

            return None

        return None

    @staticmethod
    def _extract_next_data(html: str) -> dict | None:
        """Pull the ``__NEXT_DATA__`` JSON blob from a Pitchero page."""
        m = re.search(
            r'<script\s+id="__NEXT_DATA__"\s+type="application/json">(.*?)</script>',
            html,
            re.DOTALL,
        )
        if not m:
            return None
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            logger.warning("Failed to parse __NEXT_DATA__ JSON")
            return None

    @staticmethod
    def _normalise_club_url(raw_url: str) -> str:
        """Ensure the URL starts with the Pitchero base and has no trailing slash."""
        url = raw_url.rstrip("/")
        if not url.startswith("http"):
            url = f"{PITCHERO_BASE}/clubs/{url}"
        return url

    @staticmethod
    def _club_slug(club_url: str) -> str:
        """Extract the club slug from a Pitchero URL.

        ``https://www.pitchero.com/clubs/ashfordunitedfc`` → ``ashfordunitedfc``
        """
        path = urlparse(club_url).path.rstrip("/")
        parts = path.split("/")
        for i, part in enumerate(parts):
            if part == "clubs" and i + 1 < len(parts):
                return parts[i + 1]
        return parts[-1]

    # ─── team discovery ──────────────────────────────────────────────────

    def discover_teams(self, club_url: str) -> list[dict]:
        """Return all teams listed on a club's homepage.

        Each dict has ``id``, ``name``, ``gender``, etc.
        """
        url = self._normalise_club_url(club_url)
        resp = self._get(url)
        if not resp:
            return []

        data = self._extract_next_data(resp.text)
        if not data:
            return []

        teams: list[dict] = []
        props = data.get("props") or {}
        page_props = props.get("pageProps") or {}
        club_data = page_props.get("club") or {}
        sections = club_data.get("sections") or []

        for section in sections:
            if not isinstance(section, dict):
                continue
            for team in (section.get("teams") or []):
                teams.append(team)

        logger.info("Found %d teams for %s", len(teams), url)
        return teams

    def find_first_team_id(self, club_url: str) -> int | None:
        """Find the Pitchero team-ID for the first team / senior squad."""
        teams = self.discover_teams(club_url)
        if not teams:
            return None

        for team in teams:
            if team.get("name", "").lower().strip() in _FIRST_TEAM_NAMES:
                return team["id"]

        # Fallback: if there's only one male team, use that
        male_teams = [t for t in teams if t.get("gender") == "male"]
        if len(male_teams) == 1:
            return male_teams[0]["id"]

        # Last resort: first team in the list
        if teams:
            logger.warning(
                "Could not identify first team — using first listed: %s",
                teams[0].get("name"),
            )
            return teams[0]["id"]

        return None

    # ─── squad scraping ──────────────────────────────────────────────────

    def scrape_squad(
        self,
        club_url: str,
        team_id: int | None = None,
    ) -> list[dict]:
        """Scrape the squad page and return a flat list of player dicts.

        Each dict contains: ``name``, ``id``, ``position``,
        ``position_group``, ``photo_url``, ``profile_url``,
        ``profile_type``.

        If *team_id* is not given, the scraper auto-detects the first
        team via the club homepage.
        """
        url = self._normalise_club_url(club_url)
        slug = self._club_slug(url)

        if team_id is None:
            team_id = self.find_first_team_id(url)
            if team_id is None:
                logger.error("Cannot determine first-team ID for %s", url)
                return []

        squad_data = self._fetch_squad_json(url, slug, team_id)
        if squad_data is None:
            return []

        players: list[dict] = []
        for group in squad_data:
            group_name = group.get("name", "Unknown")
            for member in group.get("members", []):
                if member.get("profile_type") != "player":
                    continue

                pid = member.get("id")
                name = member.get("name", "")
                avatar = member.get("avatar", "")
                is_placeholder = "holders" in avatar

                player_slug = self._make_player_slug(name)
                profile_path = f"/clubs/{slug}/teams/{team_id}/player/{player_slug}-{pid}"

                players.append({
                    "name": name,
                    "formal_name": member.get("formalName", ""),
                    "id": pid,
                    "position": member.get("position"),
                    "position_group": group_name,
                    "photo_url": None if is_placeholder else avatar,
                    "profile_url": f"{PITCHERO_BASE}{profile_path}",
                    "profile_type": member.get("profile_type"),
                })

        logger.info(
            "Scraped squad for %s (team %d): %d players",
            slug, team_id, len(players),
        )
        return players

    def _fetch_squad_json(
        self,
        club_url: str,
        slug: str,
        team_id: int,
    ) -> list[dict] | None:
        """Try multiple URL patterns to find the squad JSON."""
        for suffix in _SQUAD_SUFFIXES:
            page_url = f"{club_url}/teams/{team_id}{suffix}"
            resp = self._get(page_url)
            if not resp:
                continue

            data = self._extract_next_data(resp.text)
            if not data:
                continue

            props = data.get("props") or {}
            redux = props.get("initialReduxState") or {}
            teams_state = redux.get("teams") or {}
            team_squad = teams_state.get("teamSquad") or {}
            players_map = team_squad.get("players") or {}
            squad = players_map.get(str(team_id)) or []
            if squad:
                return squad

        logger.warning("No squad data found for %s team %d", slug, team_id)
        return None

    # ─── player profile scraping ─────────────────────────────────────────

    def scrape_player_profile(
        self,
        club_url: str,
        *,
        team_id: int,
        player_id: int,
        player_slug: str,
    ) -> dict:
        """Scrape a single player's profile page.

        Returns a dict with bio fields and season stats, or ``{}``
        on failure.
        """
        url = self._normalise_club_url(club_url)
        slug = self._club_slug(url)
        profile_url = f"{url}/teams/{team_id}/player/{player_slug}-{player_id}"

        resp = self._get(profile_url)
        if not resp:
            return {}

        data = self._extract_next_data(resp.text)
        if not data:
            return {}

        props = data.get("props") or {}
        logger.debug(
            "Profile __NEXT_DATA__ top-level keys: %s  |  "
            "pageProps keys: %s",
            list(data.keys()),
            list((props.get("pageProps") or {}).keys()),
        )

        redux_state = props.get("initialReduxState") or {}
        teams_state = redux_state.get("teams") or {}
        team_squad = teams_state.get("teamSquad") or {}

        profile_key = f"{team_id}-p{player_id}"

        profiles = team_squad.get("profiles") or {}
        profile = profiles.get(profile_key) or {}

        if not profile:
            logger.debug(
                "No profile data at key '%s'. Available keys: %s",
                profile_key, list(profiles.keys())[:10],
            )

        # Collect stats across all available seasons
        stats_by_season: dict[str, dict] = {}
        profile_stats = team_squad.get("profileStats") or {}
        for key, stat_obj in profile_stats.items():
            if not key.startswith(profile_key):
                continue
            if not isinstance(stat_obj, dict):
                continue
            season_id = key.split("-")[-1]
            featured: dict[str, int] = {}
            for entry in (stat_obj.get("featured") or []):
                if isinstance(entry, dict) and "label" in entry:
                    featured[entry["label"]] = entry.get("value")
            stats_by_season[season_id] = {
                "appearances": featured.get("Appearances"),
                "goals": featured.get("Goals scored"),
                "fixtures_summary": stat_obj.get("fixturesSummary"),
            }

        page_props = props.get("pageProps") or {}
        season_id = page_props.get("seasonId")

        result = {
            "player_id": player_id,
            "name": profile.get("name"),
            "position": profile.get("position"),
            "photo_url": profile.get("avatar"),
            "biography": profile.get("biography"),
            "birthplace": profile.get("birthplace"),
            "joined": profile.get("joined"),
            "previous_clubs": profile.get("previousClubs"),
            "occupation": profile.get("occupation"),
            "season_id": season_id,
            "stats_by_season": stats_by_season,
            "profile_url": profile_url,
        }

        logger.info(
            "Scraped profile for %s (id=%d): %d season(s) of stats",
            result.get("name", "?"), player_id, len(stats_by_season),
        )
        return result

    def scrape_club(
        self,
        club_url: str,
        *,
        include_profiles: bool = True,
    ) -> dict:
        """Full scrape of a club: squad list plus optional player profiles.

        Returns::

            {
                "club_url": "…",
                "club_slug": "…",
                "team_id": 229937,
                "squad": [ {player dict}, … ],
                "profiles": { player_id: {profile dict}, … },
                "errors": [ "…", … ],
            }
        """
        url = self._normalise_club_url(club_url)
        slug = self._club_slug(url)
        result: dict[str, Any] = {
            "club_url": url,
            "club_slug": slug,
            "team_id": None,
            "squad": [],
            "profiles": {},
            "errors": [],
        }

        team_id = self.find_first_team_id(url)
        if team_id is None:
            result["errors"].append("Could not find first-team ID")
            return result
        result["team_id"] = team_id

        squad = self.scrape_squad(url, team_id=team_id)
        result["squad"] = squad

        if include_profiles and squad:
            for player in squad:
                pid = player.get("id")
                if not pid:
                    continue
                try:
                    pslug = self._make_player_slug(player.get("name", ""))
                    profile = self.scrape_player_profile(
                        url,
                        team_id=team_id,
                        player_id=pid,
                        player_slug=pslug,
                    )
                    if profile:
                        result["profiles"][pid] = profile
                except Exception as exc:
                    msg = f"Error scraping player {pid}: {exc}"
                    logger.warning(msg)
                    result["errors"].append(msg)

        logger.info(
            "Club scrape complete: %s — %d players, %d profiles, %d errors",
            slug, len(squad), len(result["profiles"]), len(result["errors"]),
        )
        return result

    # ─── discovery helpers ───────────────────────────────────────────────

    @staticmethod
    def _make_player_slug(name: str) -> str:
        """'Lanre Azeez' → 'lanre-azeez'"""
        return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

    @staticmethod
    def guess_club_url(club_name: str) -> str:
        """Guess a Pitchero URL from a club name.

        ``'Ashford United FC'`` → ``https://www.pitchero.com/clubs/ashfordunitedfc``
        """
        slug = re.sub(r"[^a-z0-9]+", "", club_name.lower())
        return f"{PITCHERO_BASE}/clubs/{slug}"

    def discover_pitchero_url(self, club_name: str) -> str | None:
        """Try common slug patterns to find a club on Pitchero.

        Returns the URL if found, else ``None``.
        """
        candidates = _slug_candidates(club_name)
        for slug in candidates:
            url = f"{PITCHERO_BASE}/clubs/{slug}"
            resp = self._get(url)
            if resp and resp.status_code == 200:
                data = self._extract_next_data(resp.text)
                if data:
                    logger.info("Found Pitchero site: %s → %s", club_name, url)
                    return url
        logger.info("No Pitchero site found for %s", club_name)
        return None


def _slug_candidates(club_name: str) -> list[str]:
    """Generate plausible Pitchero slugs for a club name.

    ``'Ashford United FC'`` might be listed under ``ashfordunitedfc``,
    ``ashfordunited``, ``ashfordunitedfootballclub``, etc.
    """
    lower = club_name.lower()
    stripped = re.sub(r"[^a-z0-9 ]", "", lower).strip()
    words = stripped.split()

    # Remove common suffixes to build a "core" name
    suffixes_to_strip = {"fc", "afc", "town", "city"}
    core = [w for w in words if w not in suffixes_to_strip]

    no_spaces = "".join(words)
    core_joined = "".join(core)

    slugs: list[str] = [no_spaces]
    if core_joined != no_spaces:
        slugs.append(core_joined)
    slugs.append(no_spaces + "fc" if "fc" not in words else no_spaces)
    slugs.append(core_joined + "fc")
    slugs.append(core_joined + "footballclub")

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for s in slugs:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    return unique


# ═══════════════════════════════════════════════════════════════════════════
# CLI smoke-test:  python -m src.scrapers.pitchero <club_slug>
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    slug = sys.argv[1] if len(sys.argv) > 1 else "ashfordunitedfc"
    club_url = f"{PITCHERO_BASE}/clubs/{slug}"

    print("=" * 65)
    print(f"Pitchero scraper — {club_url}")
    print("=" * 65)

    scraper = PitcheroScraper()

    teams = scraper.discover_teams(club_url)
    print(f"\nTeams found: {len(teams)}")
    for t in teams:
        print(f"  {t.get('name', '?'):<30s}  id={t.get('id')}")

    print("\nScraping squad…")
    squad = scraper.scrape_squad(club_url)
    print(f"\n{len(squad)} players found:\n")
    for p in squad:
        pos = p.get("position") or "?"
        print(f"  {p['name']:<30s}  {pos:<25s}  id={p['id']}")

    if squad and len(sys.argv) > 2 and sys.argv[2] == "--profiles":
        print("\nScraping profiles (this takes a while)…\n")
        team_id = scraper.find_first_team_id(club_url)
        for p in squad[:3]:
            pslug = scraper._make_player_slug(p["name"])
            profile = scraper.scrape_player_profile(
                club_url,
                team_id=team_id,
                player_id=p["id"],
                player_slug=pslug,
            )
            if profile:
                print(f"  {profile.get('name', '?')}")
                print(f"    Position:    {profile.get('position')}")
                print(f"    Birthplace:  {profile.get('birthplace')}")
                print(f"    Joined:      {profile.get('joined')}")
                print(f"    Prev clubs:  {profile.get('previous_clubs')}")
                bio = profile.get("biography") or ""
                if bio:
                    print(f"    Bio:         {bio[:80]}…" if len(bio) > 80 else f"    Bio:         {bio}")
                for sid, stats in profile.get("stats_by_season", {}).items():
                    print(f"    Season {sid}: apps={stats.get('appearances')}, goals={stats.get('goals')}")
                print()

    print("=" * 65)
