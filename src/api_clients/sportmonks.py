"""Client for the Sportmonks Football API v3 (https://docs.sportmonks.com).

Sportmonks uses an ``api_token`` query parameter **or** an
``Authorization`` header for authentication.  This client uses the
header approach to keep URLs clean in logs.

Key Sportmonks concepts:

* **Includes** — enrichment parameters that nest related data inside
  a response (e.g. ``?include=player;statistics``).  Up to 2 levels
  of nesting are supported.
* **Pagination** — most list endpoints paginate with ``page`` and
  ``per_page`` parameters.  Team-squad endpoints do *not* paginate.
* **Filters** — narrow results with ``filters=`` (e.g.
  ``filters=countryIds:462``).
* **Country IDs** — Sportmonks splits the UK into separate football
  nations.  462 = "United Kingdom" (not England!).  The correct
  England ID must be looked up via ``/v3/core/countries/search/England``.

Usage::

    from src.api_clients.sportmonks import SportmonksClient

    client = SportmonksClient()
    leagues = client.get_leagues(country_id=client.england_country_id)

CLI smoke-test::

    python -m src.api_clients.sportmonks
"""

import logging
import time
from collections import deque
from typing import Any

import requests

from src.config import SPORTMONKS_API_TOKEN

logger = logging.getLogger(__name__)

BASE_URL = "https://api.sportmonks.com/v3/football"
CORE_URL = "https://api.sportmonks.com/v3/core"

MAX_RETRIES = 3
INITIAL_BACKOFF_SECS = 2.0

RATE_LIMIT_CALLS = 180
RATE_LIMIT_WINDOW_SECS = 60.0

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

_NONLEAGUE_KEYWORDS: list[str] = [
    "Non League",
    "National League",
    "Northern Premier",
    "Southern Premier",
    "Southern League",
    "Isthmian",
    "NPL",
    "Step",
]


class SportmonksClient:
    """Thin wrapper around the Sportmonks Football API v3."""

    def __init__(self, api_token: str = SPORTMONKS_API_TOKEN) -> None:
        if not api_token:
            logger.warning("SPORTMONKS_API_TOKEN is empty — all API calls will fail")

        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": api_token,
            "Accept": "application/json",
        })
        self._call_times: deque[float] = deque()
        self._england_id: int | None = None

    # ── low-level helpers ─────────────────────────────────────────────────

    def _wait_for_rate_limit(self) -> None:
        """Block until the sliding-window rate limit has capacity."""
        now = time.monotonic()
        while len(self._call_times) >= RATE_LIMIT_CALLS:
            oldest = self._call_times[0]
            elapsed = now - oldest
            if elapsed >= RATE_LIMIT_WINDOW_SECS:
                self._call_times.popleft()
            else:
                wait = RATE_LIMIT_WINDOW_SECS - elapsed + 0.1
                logger.debug("Rate limit: sleeping %.1fs", wait)
                time.sleep(wait)
                now = time.monotonic()

    def _get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        *,
        base_url: str = BASE_URL,
    ) -> dict[str, Any]:
        """GET a Sportmonks endpoint with retries and rate limiting.

        Returns the full JSON body (with ``data``, ``pagination``, etc.).
        On unrecoverable error returns ``{"data": []}``.
        """
        url = f"{base_url}/{endpoint}"
        params = dict(params or {})

        backoff = INITIAL_BACKOFF_SECS
        for attempt in range(1, MAX_RETRIES + 1):
            self._wait_for_rate_limit()
            self._call_times.append(time.monotonic())

            safe_params = {
                k: v for k, v in params.items() if k != "api_token"
            }
            logger.info(
                "Sportmonks GET %s params=%s (attempt %d/%d)",
                endpoint, safe_params, attempt, MAX_RETRIES,
            )

            try:
                resp = self._session.get(url, params=params, timeout=30)
            except requests.RequestException as exc:
                logger.warning("Request error: %s", exc)
                if attempt < MAX_RETRIES:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                return {"data": []}

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
                logger.warning(
                    "Sportmonks %d on %s — retrying in %.1fs",
                    resp.status_code, endpoint, backoff,
                )
                time.sleep(backoff)
                backoff *= 2
                continue

            logger.error(
                "Sportmonks %d on %s: %s",
                resp.status_code, endpoint, resp.text[:300],
            )
            return {"data": []}

        return {"data": []}

    def _get_all_pages(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Auto-paginate through a Sportmonks list endpoint.

        Sportmonks pagination uses ``page`` and reports
        ``pagination.has_more``.
        """
        params = dict(params or {})
        params.setdefault("per_page", 100)
        params["page"] = 1

        all_data: list[dict[str, Any]] = []

        while True:
            body = self._get(endpoint, params)
            data = body.get("data")
            if not data:
                break

            if isinstance(data, list):
                all_data.extend(data)
            else:
                all_data.append(data)
                break

            pagination = body.get("pagination", {})
            has_more = pagination.get("has_more", False)
            if not has_more:
                break

            params["page"] += 1

        return all_data

    # ── country lookup ─────────────────────────────────────────────────────

    def lookup_country_id(self, name: str = "England") -> int | None:
        """Search the Core API for a country and return its ID.

        Sportmonks splits the UK into football nations (England,
        Scotland, Wales, Northern Ireland).  This method calls
        ``/v3/core/countries/search/{name}`` and returns the first
        match, or ``None`` if nothing is found.
        """
        body = self._get(
            f"countries/search/{name}",
            base_url=CORE_URL,
        )
        data = body.get("data", [])
        if not data:
            logger.warning("Country search for %r returned no results", name)
            return None

        results = data if isinstance(data, list) else [data]
        for entry in results:
            entry_name = entry.get("name", "")
            if entry_name.lower() == name.lower():
                cid = entry["id"]
                logger.info(
                    "Sportmonks country lookup: %s -> id=%d", name, cid,
                )
                return cid

        cid = results[0]["id"]
        logger.info(
            "Sportmonks country lookup: %r -> id=%d (first result, "
            "exact match not found)",
            name, cid,
        )
        return cid

    @property
    def england_country_id(self) -> int:
        """Return England's Sportmonks country ID (cached after first lookup)."""
        if self._england_id is not None:
            return self._england_id

        looked_up = self.lookup_country_id("England")
        if looked_up is not None:
            self._england_id = looked_up
            return self._england_id

        logger.warning(
            "Could not look up England country ID — "
            "falling back to leagues endpoint with country search"
        )
        self._england_id = 0
        return 0

    # ── public methods ────────────────────────────────────────────────────

    def get_leagues(
        self,
        country_id: int | None = None,
        includes: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return leagues, optionally filtered by country.

        Args:
            country_id: Sportmonks country ID. Use
                ``client.england_country_id`` for England.
            includes: Optional enrichment includes (e.g. ``['season']``).
        """
        params: dict[str, Any] = {}
        if country_id:
            params["filters"] = f"countryIds:{country_id}"
        if includes:
            params["include"] = ";".join(includes)

        return self._get_all_pages("leagues", params)

    def get_teams(
        self,
        season_id: int,
        includes: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return all teams for a given season.

        Args:
            season_id: The Sportmonks season ID.
            includes: e.g. ``['players', 'coach']``.
        """
        params: dict[str, Any] = {}
        if includes:
            params["include"] = ";".join(includes)

        return self._get_all_pages(f"teams/seasons/{season_id}", params)

    def get_squad(
        self,
        team_id: int,
        season_id: int | None = None,
        includes: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return squad members for a team.

        If ``season_id`` is given, returns the historical squad for that
        season.  Otherwise returns the current domestic squad.

        The squad endpoint does **not** paginate — all players come in
        one response.

        Common includes: ``['player', 'details', 'position']``.
        """
        if season_id:
            endpoint = f"squads/teams/{team_id}/seasons/{season_id}"
        else:
            endpoint = f"squads/teams/{team_id}"

        params: dict[str, Any] = {}
        if includes:
            params["include"] = ";".join(includes)

        body = self._get(endpoint, params)
        data = body.get("data", [])
        return data if isinstance(data, list) else [data] if data else []

    def get_player(
        self,
        player_id: int,
        includes: list[str] | None = None,
    ) -> dict[str, Any]:
        """Return detailed info for a single player.

        Common includes: ``['statistics', 'position', 'nationality',
        'teams', 'transfers']``.
        """
        params: dict[str, Any] = {}
        if includes:
            params["include"] = ";".join(includes)

        body = self._get(f"players/{player_id}", params)
        return body.get("data", {})

    def get_seasons(
        self,
        league_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return seasons, optionally filtered by league."""
        params: dict[str, Any] = {}
        if league_id:
            params["filters"] = f"leagueIds:{league_id}"
        return self._get_all_pages("seasons", params)

    def get_standings(
        self,
        season_id: int,
        includes: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return league standings for a season."""
        params: dict[str, Any] = {}
        if includes:
            params["include"] = ";".join(includes)

        body = self._get(f"standings/seasons/{season_id}", params)
        data = body.get("data", [])
        return data if isinstance(data, list) else [data] if data else []

    def get_transfers_by_team(self, team_id: int) -> list[dict[str, Any]]:
        """Return transfer records for a team."""
        return self._get_all_pages(f"transfers/teams/{team_id}")

    # ── discovery ─────────────────────────────────────────────────────────

    def discover_english_nonleague(
        self,
    ) -> dict[str, dict[str, Any]]:
        """Find English non-league competitions and their seasons.

        Looks up the correct country ID for England dynamically via
        the Core API, then fetches all leagues for that country and
        filters for non-league keywords.

        Returns a dict keyed by league name, each value containing
        ``league_id``, ``current_season_id``, and ``category``.
        """
        country_id = self.england_country_id

        if country_id:
            all_leagues = self.get_leagues(
                country_id=country_id,
                includes=["currentSeason"],
            )
        else:
            logger.warning(
                "England country ID unknown — fetching ALL leagues "
                "and filtering by name (slower)"
            )
            all_leagues = self.get_leagues(includes=["currentSeason"])

        results: dict[str, dict[str, Any]] = {}
        for lg in all_leagues:
            name = lg.get("name", "")
            lg_id = lg.get("id")

            is_nonleague = any(
                kw.lower() in name.lower() for kw in _NONLEAGUE_KEYWORDS
            )
            if not is_nonleague:
                continue

            current_season = lg.get("currentseason") or lg.get("currentSeason")
            season_id = None
            if isinstance(current_season, dict):
                season_id = current_season.get("id")

            results[name] = {
                "league_id": lg_id,
                "current_season_id": season_id,
                "category": lg.get("category"),
                "sub_type": lg.get("sub_type"),
            }

        logger.info(
            "Sportmonks: discovered %d English non-league competitions",
            len(results),
        )
        return results


# ── CLI smoke-test ────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    if not SPORTMONKS_API_TOKEN:
        print("SPORTMONKS_API_TOKEN not set — cannot run smoke test.")
        sys.exit(1)

    client = SportmonksClient()
    print("\n=== Sportmonks — English Non-League Discovery ===\n")

    eng_id = client.england_country_id
    print(f"  England country_id: {eng_id}\n")

    comps = client.discover_english_nonleague()

    if not comps:
        print("  No non-league competitions found.")
        sys.exit(0)

    for name, info in sorted(comps.items()):
        sid = info.get("current_season_id", "?")
        lid = info.get("league_id", "?")
        print(f"  {name:<50s}  league={lid}  season={sid}")

    print(f"\n  Total: {len(comps)} competitions")
