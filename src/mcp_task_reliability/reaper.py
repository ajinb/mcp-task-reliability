"""The reaper.

Three things go wrong in a stateless task system, and all three are invisible without a
sweep: results nobody deleted, work nobody is waiting for, and tasks nobody finished. The
reaper is the periodic job that closes all three. Run it on a timer.
"""

from __future__ import annotations

from dataclasses import dataclass

from .policy import RetentionPolicy
from .store import TaskStore
from .types import TaskState


@dataclass(frozen=True)
class ReapReport:
    """What a single sweep did."""

    timed_out: int = 0
    orphaned: int = 0
    expired: int = 0

    @property
    def total(self) -> int:
        return self.timed_out + self.orphaned + self.expired

    def __str__(self) -> str:
        return (f"reaped {self.total} (timed_out={self.timed_out} "
                f"orphaned={self.orphaned} expired={self.expired})")


class Reaper:
    """Sweeps a store against a retention policy.

    Order matters. Timeouts run first so that a task which blew its deadline becomes terminal
    and earns a retention window; orphans run next over what is still live; expiry runs last
    and is the only step that actually deletes rows.
    """

    def __init__(self, store: TaskStore, policy: RetentionPolicy, clock) -> None:
        self._store = store
        self._policy = policy
        self._clock = clock

    def run_once(self) -> ReapReport:
        now = self._clock()
        return ReapReport(
            timed_out=self._sweep_timeouts(now),
            orphaned=self._sweep_orphans(now),
            expired=self._sweep_expired(now),
        )

    def _sweep_timeouts(self, now: float) -> int:
        count = 0
        for record in self._store.past_deadline(now):
            failed = record.with_state(TaskState.FAILED, now)
            failed.error = (
                f"task exceeded max_execution_s={self._policy.max_execution_s:g}"
            )
            failed.expires_at = now + self._policy.retention_s
            self._store.put(failed)
            count += 1
        return count

    def _sweep_orphans(self, now: float) -> int:
        """Cancel live work whose client stopped polling.

        A stateless server has no socket to notice a client leaving; silence is the only
        signal available. Orphans are *cancelled*, not deleted, so a client that comes back
        inside the retention window learns what happened instead of getting a bare 404.
        """
        cutoff = now - self._policy.orphan_after_s
        count = 0
        for record in self._store.unpolled_since(cutoff):
            if record.is_terminal:
                continue
            cancelled = record.with_state(TaskState.CANCELLED, now)
            cancelled.error = (
                f"orphaned: no poll for {self._policy.orphan_after_s:g}s"
            )
            cancelled.expires_at = now + self._policy.retention_s
            self._store.put(cancelled)
            count += 1
        return count

    def _sweep_expired(self, now: float) -> int:
        count = 0
        for record in self._store.expired(now):
            if self._store.delete(record.task_id):
                count += 1
        return count
