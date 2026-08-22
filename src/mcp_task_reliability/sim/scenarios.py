"""The three failure modes stateless MCP introduces, reproduced deterministically."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path

from ..clock import FakeClock
from ..manager import TaskManager
from ..policy import PollPolicy, RetentionPolicy, RetryPolicy
from ..poll import adaptive_polls, fixed_interval_polls
from ..store import InMemoryTaskStore, SqliteTaskStore
from ..types import FailureClass, TaskNotFound


@dataclass(frozen=True)
class CrashResult:
    """What survived a restart."""

    backend: str
    issued: int
    in_flight: int
    resolved_after_restart: int
    lost: int
    resumable: int

    @property
    def survival_rate(self) -> float:
        return self.resolved_after_restart / self.issued if self.issued else 0.0


def crash_recovery(
    *, durable: bool, tasks: int = 20, completed_before_crash: int = 12, seed: int = 42
) -> CrashResult:
    """Issue handles, complete some, crash mid-flight, then see what the client can still read.

    The client's position is the one that matters: it is holding `tasks` handles it was told
    were durable. After the server restarts, how many of them still answer?
    """
    clock = FakeClock()
    counter = iter(range(seed, seed + tasks + 1))
    ids = [f"task-{n:04d}" for n in counter]
    id_iter = iter(ids)

    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "tasks.db"

        def make_store():
            return SqliteTaskStore(db) if durable else InMemoryTaskStore()

        store = make_store()
        manager = TaskManager(store, clock=clock, id_factory=lambda: next(id_iter))
        handles = [manager.create("long_running_report") for _ in range(tasks)]

        clock.advance(5)
        for handle in handles[:completed_before_crash]:
            manager.complete(handle.task_id, {"rows": 128})

        # --- the crash. Process dies; whatever was only in memory dies with it. ---
        store.close()
        clock.advance(30)

        store = make_store()
        manager = TaskManager(store, clock=clock, id_factory=lambda: next(id_iter))
        report = manager.recover()

        resolved = 0
        for handle in handles:
            try:
                manager.get(handle.task_id)
                resolved += 1
            except TaskNotFound:
                pass

    return CrashResult(
        backend="sqlite" if durable else "memory",
        issued=tasks,
        in_flight=tasks - completed_before_crash,
        resolved_after_restart=resolved,
        lost=tasks - resolved,
        resumable=report.resumable,
    )


@dataclass(frozen=True)
class PollResult:
    """Poll traffic for one long task, naive versus adaptive."""

    duration_s: float
    fixed_polls: int
    adaptive_polls: int

    @property
    def reduction(self) -> float:
        return 1.0 - (self.adaptive_polls / self.fixed_polls) if self.fixed_polls else 0.0


def poll_storm(duration_s: float = 300.0, interval_ms: int = 250, seed: int = 42) -> PollResult:
    """One 5-minute task. Count what the client asks the server to do about it."""
    return PollResult(
        duration_s=duration_s,
        fixed_polls=fixed_interval_polls(duration_s, interval_ms),
        adaptive_polls=adaptive_polls(duration_s, PollPolicy(initial_ms=interval_ms), seed=seed),
    )


@dataclass(frozen=True)
class RetryResult:
    """How one failure string was classified, and what the policy did about it."""

    error: str
    failure_class: FailureClass
    attempts: int
    will_retry: bool
    dead_lettered: bool


def retry_classes(seed: int = 42) -> list[RetryResult]:
    """Three failures that look alike in a log line and must not be treated alike."""
    clock = FakeClock()
    counter = iter(range(1000))
    manager = TaskManager(
        clock=clock,
        retry=RetryPolicy(max_attempts=3),
        retention=RetentionPolicy(),
        id_factory=lambda: f"task-{next(counter):04d}",
    )

    cases = [
        ("upstream timeout after 30s", None),
        ("invalid arguments: 'limit' must be an integer", None),
        ("connection reset by peer", None),
    ]

    results: list[RetryResult] = []
    for error, code in cases:
        handle = manager.create("fetch_report")
        outcome = manager.fail(handle.task_id, error, code=code)

        # Drive the third case to exhaustion so the poison path is actually exercised.
        if error == "connection reset by peer":
            while outcome.will_retry:
                clock.advance(outcome.retry_after_s or 0.0)
                outcome = manager.fail(handle.task_id, error, code=code)

        results.append(
            RetryResult(
                error=error,
                failure_class=outcome.failure_class,
                attempts=outcome.attempts,
                will_retry=outcome.will_retry,
                dead_lettered=outcome.dead_lettered,
            )
        )
    return results
