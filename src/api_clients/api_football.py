"""Client for the API-Football v3 REST API.

Hosted at ``v3.football.api-sports.io`` (accessed via RapidAPI).

Features:

* **Rate limiting** — the free tier allows 10 requests per minute.
  This client tracks timestamps and sleeps automatically when the
  budget is exhausted.
* **Retry with exponential back-off** — transient HTTP errors (429,
  500, 502, 503, 504) are retried up to 3 times.
* **Pagination** — the ``/players`` endpoint paginates; the client
  fetches every page and merges the results.
* **Graceful error handling** — on failure the methods log the error
  and return an empty list so the pipeline never crashes.

Usage::

    from src.api_clients.api_football import ApiFootballClient

    client = ApiFootballClient()
    leagues = client.get_leagues()
    teams  = client.get_teams(league_id=199, season=2024)

CLI smoke-test::

    python -m src.api_clients.api_football
"""

import logging
import time
from collections import deque
from typing import Any

import requests

from src.config import API_FOOTBALL_KEY

logger = logging.getLogger(__name__)

# ── API constants ────────────────────────────────────────────────────────

BASE_URL = "https://v3.football.api-sports.io"

MAX_RETRIES = 3
INITIAL_BACKOFF_SECS = 2.0

# Free-tier rate limit: 10 requests per 60 seconds
RATE_LIMIT_CALLS = 10
RATE_LIMIT_WINDOW_SECS = 60.0

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Keywords that identify English non-league competitions in the API's
# league names.  Ordered broadest → narrowest so the first match wins.
_NONLEAGUE_KEYWORDS: list[str] = [
    "Non League",
    "National League",
    "Northern Premier",
    "Southern League",
    "Isthmian",
    "FA Trophy",
    "FA Vase",
]


class ApiFootballClient:
    """Thin wrapper around the API-Football v3 REST API."""

    def __init__(self, api_key: str = API_FOOTBALL_KEY) -> None:
        self._session = requests.Session()
        self._session.headers.update({
            "x-apisports-key": api_key,
        })
        # Sliding window of request timestamps for rate limiting
        self._call_times: deque[float] = deque()

    # ─── low-level helpers ───────────────────────────────────────────────

    def _wait_for_rate_limit(self) -> None:
        """Block until we are allowed to make another request."""
        now = time.monotonic()

        # Drop timestamps older than the rate-limit window
        while self._call_times and (now - self._call_times[0]) > RATE_LIMIT_WINDOW_SECS:
            self._call_times.popleft()

        if len(self._call_times) >= RATE_LIMIT_CALLS:
            oldest = self._call_times[0]
            sleep_for = RATE_LIMIT_WINDOW_SECS - (now - oldest) + 0.5
            if sleep_for > 0:
                logger.info("Rate limit reached — sleeping %.1fs", sleep_for)
                time.sleep(sleep_for)

        self._call_times.append(time.monotonic())

    def _get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> dict:
        """Send a GET request with rate limiting and retry logic.

        Returns the full parsed JSON body (caller extracts
        ``response`` or ``paging`` as needed).

        Raises ``requests.HTTPError`` only after all retries are
        exhausted.
        """
        url = f"{BASE_URL}/{endpoint}"
        backoff = INITIAL_BACKOFF_SECS

        for attempt in range(1, MAX_RETRIES + 1):
            self._wait_for_rate_limit()

            logger.info(
                "API-Football  GET /%s  params=%s  (attempt %d/%d)",
                endpoint, params, attempt, MAX_RETRIES,
            )

            try:
                resp = self._session.get(url, params=params, timeout=30)
            except requests.RequestException as exc:
                logger.warning("Request failed: %s", exc)
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(backoff)
                backoff *= 2
                continue

            logger.info(
                "API-Football  %d  %s  remaining=%s",
                resp.status_code,
                endpoint,
                resp.headers.get("x-ratelimit-requests-remaining", "?"),
            )

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
                logger.warning(
                    "Retryable %d from /%s — backing off %.1fs",
                    resp.status_code, endpoint, backoff,
                )
                time.sleep(backoff)
                backoff *= 2
                continue

            resp.raise_for_status()

        return {}  # unreachable, but keeps the type checker happy

    def _get_response(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict]:
        """GET and return the ``response`` array, or ``[]`` on error."""
        try:
            body = self._get(endpoint, params)
        except Exception:
            logger.exception("API-Football request failed for /%s", endpoint)
            return []

        errors = body.get("errors")
        if errors:
            logger.error("API-Football returned errors for /%s: %s", endpoint, errors)
            return []

        return body.get("response", [])

    def _get_all_pages(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict]:
        """Fetch every page of a paginated endpoint and merge results."""
        params = dict(params or {})
        params.setdefault("page", 1)

        all_items: list[dict] = []

        while True:
            try:
                body = self._get(endpoint, params)
            except Exception:
                logger.exception(
                    "API-Football paginated request failed for /%s page %s",
                    endpoint, params.get("page"),
                )
                break

            errors = body.get("errors")
            if errors:
                logger.error("API-Football errors on /%s: %s", endpoint, errors)
                break

            all_items.extend(body.get("response", []))

            paging = body.get("paging", {})
            current_page = paging.get("current", 1)
            total_pages = paging.get("total", 1)

            if current_page >= total_pages:
                break

            params["page"] = current_page + 1

        return all_items

    # ─── public methods ──────────────────────────────────────────────────

    def get_leagues(self, country: str = "England") -> list[dict]:
        """Return all leagues / cups for a country.

        Each item contains ``league`` and ``country`` sub-dicts.
        """
        return self._get_response("leagues", {"country": country})

    def get_teams(self, league_id: int, season: int) -> list[dict]:
        """Return every team in a league for a given season.

        Each item contains ``team`` and ``venue`` sub-dicts.
        """
        return self._get_response("teams", {
            "league": league_id,
            "season": season,
        })

    def get_squad(self, team_id: int) -> list[dict]:
        """Return the squad for a team.

        Each item contains a ``players`` list of dicts with ``id``,
        ``name``, ``age``, ``number``, and ``position``.
        """
        return self._get_response("players/squads", {"team": team_id})

    def get_player_stats(
        self,
        player_id: int,
        season: int,
        league_id: int,
    ) -> list[dict]:
        """Return detailed stats for a single player in a season/league.

        Uses pagination because the ``/players`` endpoint pages at 20
        results.
        """
        return self._get_all_pages("players", {
            "id": player_id,
            "season": season,
            "league": league_id,
        })

    def get_fixtures(self, league_id: int, season: int) -> list[dict]:
        """Return every fixture for a league and season."""
        return self._get_response("fixtures", {
            "league": league_id,
            "season": season,
        })

    def get_fixture_lineups(self, fixture_id: int) -> list[dict]:
        """Return the lineups for a single fixture.

        Each item contains ``team``, ``coach``, ``formation``, and
        ``startXI`` / ``substitutes`` lists.
        """
        return self._get_response("fixtures/lineups", {
            "fixture": fixture_id,
        })

    # ─── discovery helper ────────────────────────────────────────────────

    def discover_english_nonleague(self) -> dict[str, int]:
        """Find non-league English competitions and return a name→id map.

        Calls ``get_leagues(country='England')`` and keeps any league
        whose name matches one of the known non-league keywords.

        Returns:
            ``{"National League": 199, "National League North": 200, …}``
        """
        all_leagues = self.get_leagues(country="England")

        mapping: dict[str, int] = {}
        for entry in all_leagues:
            league = entry.get("league", {})
            name: str = league.get("name", "")
            league_id: int | None = league.get("id")

            if not name or league_id is None:
                continue

            for keyword in _NONLEAGUE_KEYWORDS:
                if keyword.lower() in name.lower():
                    mapping[name] = league_id
                    break

        logger.info(
            "Discovered %d English non-league competitions", len(mapping),
        )
        for name, lid in sorted(mapping.items()):
            logger.info("  %-40s  id=%d", name, lid)

        return mapping


# ═══════════════════════════════════════════════════════════════════════════
# CLI smoke-test:  python -m src.api_clients.api_football
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    client = ApiFootballClient()

    print("=" * 60)
    print("API-Football — Discovering English non-league competitions")
    print("=" * 60)

    mapping = client.discover_english_nonleague()

    if not mapping:
        print("\nNo competitions found (check your API key).")
    else:
        print(f"\n{len(mapping)} competitions found:\n")
        for name, lid in sorted(mapping.items()):
            print(f"  {name:<45s}  league_id={lid}")

    print("\n" + "=" * 60)
