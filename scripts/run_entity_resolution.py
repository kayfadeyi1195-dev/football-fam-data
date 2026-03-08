"""Run the player entity-resolution (deduplication) pipeline.

Finds duplicate player records, auto-merges high-confidence matches,
and queues borderline cases for human review.

Usage::

    python scripts/run_entity_resolution.py
"""

import logging

from src.etl.entity_resolution import run_entity_resolution

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)


def main() -> None:
    results = run_entity_resolution()

    print()
    print("=" * 55)
    print("  Entity Resolution Summary")
    print("=" * 55)
    print(f"  Candidate pairs found:   {results['candidates']}")
    print(f"  Auto-merged (>= 90):     {results['auto_merged']}")
    print(f"  Queued for review (70-89):{results['queued']}")
    print(f"  Skipped (< 70):          {results['skipped']}")
    print("=" * 55)
    print()


if __name__ == "__main__":
    main()
