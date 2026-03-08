#!/usr/bin/env python
"""Recalculate overall confidence scores for every player.

Prints:
  - Score distribution (how many 5s, 4s, 3s, 2s, 1s, 0s)
  - Average confidence by pyramid step
  - 20 lowest-confidence players for review

Usage::

    python scripts/run_confidence.py
"""

import logging
import sys

from src.etl.confidence import recalculate_confidence

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger(__name__)


def _print_distribution(dist: dict[int, int], total: int) -> None:
    """Pretty-print a score-distribution histogram."""
    print("\n╔══════════════════════════════════════════════╗")
    print("║         CONFIDENCE SCORE DISTRIBUTION        ║")
    print("╠══════════════════════════════════════════════╣")
    for bucket in range(5, -1, -1):
        count = dist.get(bucket, 0)
        pct = (count / total * 100) if total else 0
        bar = "█" * int(pct / 2)
        label = f"{bucket}.00–{bucket}.99"
        print(f"║  {label}  │ {count:>6,}  ({pct:5.1f}%)  {bar}")
    print(f"╠══════════════════════════════════════════════╣")
    print(f"║  TOTAL       │ {total:>6,}                      ║")
    print(f"╚══════════════════════════════════════════════╝")


def _print_step_averages(avg_by_step: dict[int, float]) -> None:
    """Print average confidence by pyramid step."""
    if not avg_by_step:
        print("\n  (no step data available)")
        return
    print("\n┌─────────────────────────────────────┐")
    print("│   AVERAGE CONFIDENCE BY STEP        │")
    print("├─────────────────────────────────────┤")
    for step in sorted(avg_by_step):
        avg = avg_by_step[step]
        bar = "█" * int(avg * 4)
        print(f"│  Step {step}  │  {avg:.2f}  {bar}")
    print("└─────────────────────────────────────┘")


def _print_lowest(lowest: list[dict]) -> None:
    """Print the 20 lowest-confidence players."""
    if not lowest:
        print("\n  (no players scored)")
        return
    print("\n┌─────────────────────────────────────────────────────────┐")
    print("│   20 LOWEST-CONFIDENCE PLAYERS (for review)            │")
    print("├───────┬───────┬─────────────────────────────────────────┤")
    print("│ Score │   ID  │ Name                                    │")
    print("├───────┼───────┼─────────────────────────────────────────┤")
    for entry in lowest:
        score = entry["score"]
        pid = entry["player_id"]
        name = entry["name"][:38]
        print(f"│ {score:5.2f} │ {pid:>5} │ {name:<39} │")
    print("└───────┴───────┴─────────────────────────────────────────┘")


def main() -> None:
    log.info("Starting confidence recalculation…")
    result = recalculate_confidence()

    _print_distribution(result["distribution"], result["total_scored"])
    _print_step_averages(result["avg_by_step"])
    _print_lowest(result["lowest_20"])

    print(f"\nDone — {result['total_scored']:,} players scored.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(1)
    except Exception:
        log.exception("Confidence scoring failed")
        sys.exit(2)
