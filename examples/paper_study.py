"""The paper's measured study: E1-E4.

Reproduces every table in "Stateless Protocol, Stateful Problem: Durability, Retry, and
Retention Policy for MCP Tasks". A thin reporting layer over `mcp_task_reliability.sim`,
so the numbers the paper cites and the numbers the test suite pins come from the same code.

    python examples/paper_study.py
"""

from __future__ import annotations

from mcp_task_reliability.sim import (
    classification_cost,
    crash_recovery,
    orphan_coupling,
    poll_frontier,
)
from mcp_task_reliability.types import FailureClass


def _rule(title: str) -> None:
    print(f"\n{title}\n{'=' * len(title)}")


def e1_durability() -> None:
    _rule("E1  handle survival across restart")
    print(f"{'store':<10}{'issued':>8}{'in flight':>11}{'resolve':>9}"
          f"{'lost':>7}{'resumable':>11}{'survival':>10}")
    for durable in (False, True):
        r = crash_recovery(durable=durable)
        print(f"{r.backend:<10}{r.issued:>8}{r.in_flight:>11}{r.resolved_after_restart:>9}"
              f"{r.lost:>7}{r.resumable:>11}{r.survival_rate:>9.0%}")
    print("\nA handle the server did not durably write is a handle that goes missing "
          "across a deploy.")


def e2_poll_frontier() -> None:
    _rule("E2  poll cost vs staleness (220 runs/cell: 20 seeds x 11 completion phases)")
    print(f"{'policy':<26}{'polls':>9}{'staleness p50':>15}{'p95':>9}{'p50/duration':>14}")
    for duration in (60.0, 300.0, 1800.0):
        print(f"\n  task duration ~{duration:g}s")
        for point in poll_frontier(duration):
            print(f"  {point.label:<24}{point.polls:>9.1f}"
                  f"{point.staleness_p50:>14.2f}s{point.staleness_p95:>8.2f}s"
                  f"{point.staleness_ratio:>13.1%}")


def e3_misclassification() -> None:
    _rule("E3  cost of the default class for unrecognised errors")
    print("100 failures: 20 recognised-transient, 20 recognised-permanent, 60 unrecognised.\n"
          "We sweep how many of the unrecognised would actually have succeeded on retry.")
    print(f"\n{'truly transient':>16}{'default':>12}{'attempts':>10}{'recovered':>11}"
          f"{'wasted':>9}{'lost work':>11}{'dead-letter':>13}")
    for fraction in (0.0, 0.25, 0.5, 0.75, 1.0):
        for default in (FailureClass.PERMANENT, FailureClass.TRANSIENT):
            c = classification_cost(fraction, default)
            print(f"{fraction:>15.0%}{c.default_class.value:>12}{c.attempts:>10}"
                  f"{c.recovered:>11}{c.wasted:>9}{c.lost:>11}{c.dead_lettered:>13}")


def e4_poll_orphan_coupling() -> None:
    _rule("E4  client backoff vs server orphan window")
    print(f"{'client max backoff':>20}{'orphan_after_s':>16}{'outcome':>14}{'cancelled at':>14}")
    for max_ms in (10_000, 60_000, 300_000, 900_000):
        for orphan_after_s in (900.0, 120.0):
            r = orphan_coupling(max_ms, orphan_after_s)
            at = f"{r.cancelled_at_s:.0f}s" if r.cancelled_at_s is not None else "-"
            print(f"{max_ms // 1000:>17}s{orphan_after_s:>16.0f}"
                  f"{'orphaned' if r.orphaned else 'survived':>14}{at:>14}")
    print("\nThe safe-backoff rule falls straight out: a client's maximum poll gap must stay\n"
          "below the server's orphan window, and neither side can check that alone.")


if __name__ == "__main__":
    print("mcp-task-reliability paper study")
    e1_durability()
    e2_poll_frontier()
    e3_misclassification()
    e4_poll_orphan_coupling()
