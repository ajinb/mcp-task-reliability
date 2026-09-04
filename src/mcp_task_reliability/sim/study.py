"""The paper's measured study.

`scenarios.py` answers "does this fail, and does the fix work?" — the demo's job. This
module answers "what does the fix cost?", which is the paper's job. Everything here is
deterministic: jitter is seeded, and completion times are swept rather than sampled, so a
cited table is reproducible to the digit.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass

from ..clock import FakeClock
from ..manager import TaskManager
from ..policy import PollPolicy, RetentionPolicy, RetryPolicy
from ..poll import PollScheduler
from ..retry import RetryClassifier
from ..store import InMemoryTaskStore
from ..types import FailureClass, TaskState

DEFAULT_SEEDS = tuple(range(20))
# Completion is swept across a +/-10% window so staleness measures the operational
# quantity — how stale your view is when a task you do not control finishes — rather than
# where one fixed instant happens to land in one poll schedule.
DEFAULT_PHASES = tuple(0.90 + 0.02 * i for i in range(11))


# ---------------------------------------------------------------------------------------
# E2 — the poll cost / staleness frontier
# ---------------------------------------------------------------------------------------

@dataclass(frozen=True)
class PollPoint:
    """One policy against one task-duration class."""

    label: str
    duration_s: float
    polls: float
    staleness_p50: float
    staleness_p95: float

    @property
    def staleness_ratio(self) -> float:
        """Median staleness as a fraction of the task's own duration.

        The absolute figure is not the operational one. Twenty seconds of staleness is a
        fifth of a one-minute task and a rounding error on a half-hour one, which is why
        the same backoff policy is wasteful in one regime and obviously correct in the
        other — and why the client, which does not know the duration class, cannot pick
        well without the server's `poll_after_ms` hint.
        """
        return self.staleness_p50 / self.duration_s if self.duration_s else 0.0


def single_poll_run(
    duration_s: float, *, fixed_ms: int | None = None,
    policy: PollPolicy | None = None, seed: int = 42,
) -> tuple[int, float]:
    """One client polling one task that completes at `duration_s`.

    Returns (polls issued up to and including the one that observes completion, staleness),
    where staleness is the gap between the task finishing and the client finding out.
    """
    if fixed_ms is not None and fixed_ms <= 0:
        raise ValueError("fixed_ms must be positive")

    elapsed = 0.0
    polls = 0
    scheduler = None if fixed_ms is not None else PollScheduler(
        policy=policy or PollPolicy(), seed=seed
    )
    while True:
        step = (
            fixed_ms / 1000.0 if fixed_ms is not None
            else scheduler.next_delay_ms() / 1000.0
        )
        elapsed += step
        polls += 1
        if elapsed >= duration_s:
            return polls, elapsed - duration_s


def _summarise(label: str, duration_s: float, polls, stale) -> PollPoint:
    ordered = sorted(stale)
    idx = min(len(ordered) - 1, int(0.95 * len(ordered)))
    return PollPoint(
        label=label,
        duration_s=duration_s,
        polls=statistics.mean(polls),
        staleness_p50=statistics.median(stale),
        staleness_p95=ordered[idx],
    )


def poll_frontier(
    duration_s: float,
    *,
    fixed_intervals_ms: tuple[int, ...] = (250, 1000, 5000),
    adaptive_max_ms: tuple[int, ...] = (10_000, 30_000, 60_000),
    seeds: tuple[int, ...] = DEFAULT_SEEDS,
    phases: tuple[float, ...] = DEFAULT_PHASES,
) -> list[PollPoint]:
    """Poll traffic against staleness, for one nominal task duration."""
    points: list[PollPoint] = []

    for interval_ms in fixed_intervals_ms:
        runs = [single_poll_run(duration_s * p, fixed_ms=interval_ms) for p in phases]
        points.append(_summarise(
            f"fixed {interval_ms}ms", duration_s,
            [r[0] for r in runs], [r[1] for r in runs],
        ))

    for max_ms in adaptive_max_ms:
        runs = [
            single_poll_run(duration_s * p, policy=PollPolicy(max_ms=max_ms), seed=s)
            for s in seeds for p in phases
        ]
        points.append(_summarise(
            f"adaptive max {max_ms // 1000}s", duration_s,
            [r[0] for r in runs], [r[1] for r in runs],
        ))

    return points


# ---------------------------------------------------------------------------------------
# E3 — what the default classification costs
# ---------------------------------------------------------------------------------------

@dataclass(frozen=True)
class ClassificationCost:
    """The price of one `default_class` choice against one error mix."""

    default_class: FailureClass
    unknown_transient_fraction: float
    attempts: int
    recovered: int
    wasted: int
    lost: int
    dead_lettered: int


def classification_cost(
    unknown_transient_fraction: float,
    default_class: FailureClass,
    *,
    known_transient: int = 20,
    known_permanent: int = 20,
    unknown: int = 60,
    max_attempts: int = 3,
) -> ClassificationCost:
    """Price one default against a workload of failures.

    Recovery is modelled the way `retry.py` defines the classes: a truly transient failure
    is one that *would succeed if tried again*, so it succeeds on attempt 2; a permanent one
    never succeeds. No randomness — the result is exact.
    """
    if not 0.0 <= unknown_transient_fraction <= 1.0:
        raise ValueError("unknown_transient_fraction must be in [0, 1]")

    errors = (
        ["upstream timeout after 30s"] * known_transient
        + ["invalid arguments: 'limit' must be an integer"] * known_permanent
        + ["widget subsystem returned E_BADSTATE"] * unknown
    )
    n_unknown_transient = int(unknown * unknown_transient_fraction)
    base = known_transient + known_permanent

    policy = RetryPolicy(max_attempts=max_attempts)
    classifier = RetryClassifier(policy=policy, default_class=default_class)

    attempts = recovered = wasted = lost = dead = 0
    for idx, error in enumerate(errors):
        truly_transient = (
            idx < known_transient
            or (idx >= base and idx - base < n_unknown_transient)
        )

        n = 1
        succeeded = False
        while True:
            failure_class = classifier.classify(error, attempts=n)
            if classifier.should_retry(failure_class, n) and n < max_attempts:
                n += 1
                if truly_transient:          # the retry that would have worked
                    succeeded = True
                    break
                continue
            break

        attempts += n
        if succeeded:
            recovered += 1
            continue
        if failure_class is FailureClass.POISON:
            dead += 1
        if not truly_transient and n > 1:
            wasted += n - 1                  # retries spent on the unsalvageable
        if truly_transient:
            lost += 1                        # would have worked, never tried again

    return ClassificationCost(
        default_class=default_class,
        unknown_transient_fraction=unknown_transient_fraction,
        attempts=attempts, recovered=recovered, wasted=wasted, lost=lost, dead_lettered=dead,
    )


# ---------------------------------------------------------------------------------------
# E4 — client backoff against server orphan reaping
# ---------------------------------------------------------------------------------------

@dataclass(frozen=True)
class CouplingResult:
    """Whether a live task survived its own client's backoff."""

    client_max_backoff_ms: int
    orphan_after_s: float
    orphaned: bool
    cancelled_at_s: float | None


def orphan_coupling(
    client_max_backoff_ms: int,
    orphan_after_s: float,
    *,
    task_duration_s: float = 3600.0,
    sweep_interval_s: float = 30.0,
    seed: int = 7,
) -> CouplingResult:
    """Run one long task, one backing-off client, and one reaper on a timer.

    The server presumes a quiet client has walked away. The client, following E2, goes
    deliberately quiet. Nothing in the spec makes the two agree.
    """
    start = 1_000_000.0
    clock = FakeClock(start)
    store = InMemoryTaskStore()
    manager = TaskManager(
        store,
        retention=RetentionPolicy(
            orphan_after_s=orphan_after_s,
            max_execution_s=task_duration_s * 2,
        ),
        clock=clock,
    )
    handle = manager.create("long_running_export")
    scheduler = PollScheduler(
        policy=PollPolicy(max_ms=client_max_backoff_ms), seed=seed
    )
    next_poll_at = scheduler.next_delay_ms() / 1000.0

    while clock.now - start < task_duration_s:
        clock.advance(sweep_interval_s)
        elapsed = clock.now - start

        if elapsed >= next_poll_at:
            record = store.get(handle.task_id)
            if record is not None and not record.is_terminal:
                manager.get(handle.task_id)
            next_poll_at = elapsed + scheduler.next_delay_ms() / 1000.0

        manager.reap()
        record = store.get(handle.task_id)
        if record is not None and record.state is TaskState.CANCELLED:
            return CouplingResult(
                client_max_backoff_ms=client_max_backoff_ms,
                orphan_after_s=orphan_after_s,
                orphaned=True,
                cancelled_at_s=elapsed,
            )

    return CouplingResult(
        client_max_backoff_ms=client_max_backoff_ms,
        orphan_after_s=orphan_after_s,
        orphaned=False,
        cancelled_at_s=None,
    )
