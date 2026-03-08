"""Client for the Football Web Pages v2 JSON API.

``https://api.footballwebpages.co.uk/v2/`` provides JSON endpoints
covering English league football down to Step 4.  The **appearances**
endpoint is especially valuable — it gives per-player appearance
counts with club names that map directly to our ``player_seasons``
table.

**Authentication** — every request must include an ``FWP-API-Key``
header.  Request a free key by emailing info@footballwebpages.co.uk
and store it as ``FWP_API_KEY`` in your ``.env`` file.

**Rate limit** — 10 requests per minute.

Features:

* **Rate limiting** — enforces the 10 req/min ceiling and sleeps
  automatically when the budget is exhausted.
* **Retry with exponential back-off** — transient HTTP errors are
  retried up to 3 times.
* **Automatic pagination** — the ``appearances`` and ``goalscorers``
  endpoints paginate (25 per page); the client fetches every page
  and merges results.
* **Graceful error handling** — on failure the methods log the error
  and return an empty list / dict (never crash the pipeline).

Usage::

    from src.api_clients.football_web_pages import FootballWebPagesClient

    client = FootballWebPagesClient()
    comps  = client.get_competitions()
    table  = client.get_league_table(comp_id=1)

CLI smoke-test::

    python -m src.api_clients.football_web_pages
"""

import logging
import time
from collections import deque
from typing import Any

import requests

from src.config import FWP_API_KEY

logger = logging.getLogger(__name__)

# ── API constants ────────────────────────────────────────────────────────

BASE_URL = "https://api.footballwebpages.co.uk"

MAX_RETRIES = 3
INITIAL_BACKOFF_SECS = 2.0
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

RATE_LIMIT_CALLS = 10
RATE_LIMIT_WINDOW_SECS = 60.0

# Keywords used to identify non-league competitions (Steps 1-4).
_NONLEAGUE_KEYWORDS: list[str] = [
    "National League",
    "Northern Premier",
    "Southern League",
    "Southern Premier",
    "Isthmian",
    "NPL",
    "Vanarama",
]


class FootballWebPagesClient:
    """Thin wrapper around the Football Web Pages v2 JSON API."""

    def __init__(self, api_key: str = FWP_API_KEY) -> None:
        if not api_key:
            logger.warning(
                "FWP_API_KEY is not set — all requests will return 400. "
                "Email info@footballwebpages.co.uk to request a free key "
                "and add FWP_API_KEY to your .env file."
            )

        self._session = requests.Session()
        self._session.headers.update({
            "FWP-API-Key": api_key,
            "Accept": "application/json",
        })
        self._call_times: deque[float] = deque()

    # ─── low-level helpers ───────────────────────────────────────────────

    def _wait_for_rate_limit(self) -> None:
        """Block until we are allowed to make another request."""
        now = time.monotonic()

        while self._call_times and (now - self._call_times[0]) > RATE_LIMIT_WINDOW_SECS:
            self._call_times.popleft()

        if len(self._call_times) >= RATE_LIMIT_CALLS:
            oldest = self._call_times[0]
            sleep_for = RATE_LIMIT_WINDOW_SECS - (now - oldest) + 0.5
            if sleep_for > 0:
                logger.info("FWP rate limit reached — sleeping %.1fs", sleep_for)
                time.sleep(sleep_for)

        self._call_times.append(time.monotonic())

    def _get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> dict:
        """Send a GET request with rate limiting and retry logic.

        Returns the full parsed JSON body.  Raises
        ``requests.HTTPError`` only after all retries are exhausted.
        """
        url = f"{BASE_URL}/{path}"
        backoff = INITIAL_BACKOFF_SECS

        for attempt in range(1, MAX_RETRIES + 1):
            self._wait_for_rate_limit()

            logger.info(
                "FWP  GET /%s  params=%s  (attempt %d/%d)",
                path, params, attempt, MAX_RETRIES,
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

            logger.info("FWP  %d  /%s", resp.status_code, path)

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
                logger.warning(
                    "Retryable %d from /%s — backing off %.1fs",
                    resp.status_code, path, backoff,
                )
                time.sleep(backoff)
                backoff *= 2
                continue

            resp.raise_for_status()

        return {}

    def _safe_get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> dict:
        """GET and return parsed JSON, or ``{}`` on any error."""
        try:
            return self._get(path, params)
        except Exception:
            logger.exception("FWP request failed for /%s", path)
            return {}

    def _get_all_pages(
        self,
        path: str,
        *,
        comp_id: int,
        results_key: str,
    ) -> list[dict]:
        """Fetch every page of a paginated endpoint and merge results.

        Pages contain up to 25 records.  When the returned list is
        empty or the key is missing we stop.
        """
        all_items: list[dict] = []
        page = 1

        while True:
            body = self._safe_get(path, {"comp": comp_id, "page": page})
            if not body:
                break

            items = body.get(results_key, [])
            if not items:
                break

            all_items.extend(items)
            logger.info(
                "FWP  /%s comp=%d page=%d — %d items (total: %d)",
                path, comp_id, page, len(items), len(all_items),
            )

            page += 1

        return all_items

    # ─── public methods ──────────────────────────────────────────────────

    def get_competitions(self, include: str | None = None) -> list[dict]:
        """Return every competition the API covers.

        Args:
            include: One or both of ``"rounds"``, ``"teams"``
                     (comma-separated) to embed extra data.
        """
        params: dict[str, Any] = {}
        if include:
            params["include"] = include
        body = self._safe_get("v2/competitions.json", params or None)
        return body.get("competitions", [])

    def get_league_table(self, comp_id: int) -> list[dict]:
        """Return the league standings for a competition."""
        body = self._safe_get("v2/league-table.json", {"comp": comp_id})
        return body.get("league-table", [])

    def get_appearances(self, comp_id: int) -> list[dict]:
        """Return all player appearance records for a competition.

        Fetches every page automatically (25 records per page).
        """
        return self._get_all_pages(
            "v2/appearances.json",
            comp_id=comp_id,
            results_key="appearances",
        )

    def get_goalscorers(self, comp_id: int) -> list[dict]:
        """Return the goalscorers list for a competition (all pages)."""
        return self._get_all_pages(
            "v2/goalscorers.json",
            comp_id=comp_id,
            results_key="goalscorers",
        )

    def get_matches(self, comp_id: int) -> list[dict]:
        """Return every fixture / result for a competition."""
        body = self._safe_get("v2/fixtures-results.json", {"comp": comp_id})
        return body.get("fixtures-results", [])

    def get_match(self, match_id: int) -> dict:
        """Return detailed info for a single match."""
        body = self._safe_get("v2/match.json", {"match": match_id})
        return body.get("match", body)

    # ─── discovery helper ────────────────────────────────────────────────

    def discover_nonleague_competitions(self) -> dict[str, int]:
        """Find competitions covering Steps 1-4 of the English pyramid.

        Calls ``get_competitions()`` and keeps any whose name contains
        one of the known non-league keywords.

        Returns:
            ``{"National League": 42, "National League North": 87, …}``
        """
        all_comps = self.get_competitions()

        mapping: dict[str, int] = {}
        for comp in all_comps:
            name: str = comp.get("name", "")
            comp_id = comp.get("id")

            if not name or comp_id is None:
                continue

            name_lower = name.lower()
            for keyword in _NONLEAGUE_KEYWORDS:
                if keyword.lower() in name_lower:
                    mapping[name] = comp_id
                    break

        logger.info(
            "Discovered %d non-league competitions on FWP", len(mapping),
        )
        for name, cid in sorted(mapping.items()):
            logger.info("  %-50s  comp_id=%s", name, cid)

        return mapping


# ═══════════════════════════════════════════════════════════════════════════
# CLI smoke-test:  python -m src.api_clients.football_web_pages
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    client = FootballWebPagesClient()

    print("=" * 65)
    print("Football Web Pages — Discovering non-league competitions")
    print("=" * 65)

    mapping = client.discover_nonleague_competitions()

    if not mapping:
        print("\nNo competitions found (is FWP_API_KEY set in .env?).")
    else:
        print(f"\n{len(mapping)} competitions found:\n")
        for name, cid in sorted(mapping.items()):
            print(f"  {name:<50s}  comp_id={cid}")

    print("\n" + "=" * 65)
