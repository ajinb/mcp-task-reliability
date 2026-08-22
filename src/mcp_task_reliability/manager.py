"""The task manager — the policy layer behind `tools/call`.

This is where the spec's lifecycle meets the operational decisions the spec leaves open:
whether a repeated call is the same task, whether a failure earns another attempt, and what
a restart does to work that was in flight.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .policy import RetentionPolicy, RetryPolicy
from .reaper import Reaper, ReapReport
from .retry import RetryClassifier
from .store import DeadTaskQueue, InMemoryTaskStore, TaskStore
from .types import (
    FailureClass,
    IllegalTransition,
    TaskHandle,
    TaskNotFound,
    TaskRecord,
    TaskState,
)


@dataclass(frozen=True)
class FailureOutcome:
    """What the policy layer decided about a failure."""

    task_id: str
    failure_class: FailureClass
    will_retry: bool
    attempts: int
    retry_after_s: float | None = None
    dead_lettered: bool = False


@dataclass(frozen=True)
class RecoveryReport:
    """What a restart found and did to work that was in flight."""

    resumable: int = 0
    timed_out: int = 0
    poisoned: int = 0

    @property
    def total(self) -> int:
        return self.resumable + self.timed_out + self.poisoned

    def __str__(self) -> str:
        return (f"recovered {self.total} in-flight tasks (resumable={self.resumable} "
                f"timed_out={self.timed_out} poisoned={self.poisoned})")


class TaskManager:
    """Durable task lifecycle with retry, retention and recovery policy attached."""

    def __init__(
        self,
        store: TaskStore | None = None,
        *,
        retention: RetentionPolicy | None = None,
        retry: RetryPolicy | None = None,
        classifier: RetryClassifier | None = None,
        dead_letter: DeadTaskQueue | None = None,
        clock: Callable[[], float] = time.time,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self.store: TaskStore = store or InMemoryTaskStore()
        self.retention = retention or RetentionPolicy()
        self.classifier = classifier or RetryClassifier(policy=retry or RetryPolicy())
        self.dead_letter = dead_letter or DeadTaskQueue()
        self._clock = clock
        self._id_factory = id_factory or (lambda: uuid.uuid4().hex)
        self.reaper = Reaper(self.store, self.retention, clock)

    # ---- lifecycle -------------------------------------------------------------------

    def create(
        self,
        tool: str,
        *,
        idempotency_key: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TaskHandle:
        """Start a task and hand back a durable handle.

        With an `idempotency_key`, a repeated call returns the *existing* handle rather than
        starting a second copy of the work. This is the difference between a client retry
        being free and a client retry doubling your bill.
        """
        now = self._clock()
        if idempotency_key:
            existing = self.store.by_idempotency_key(idempotency_key)
            if existing is not None:
                return TaskHandle(existing.task_id, poll_after_ms=self._hint_ms())

        record = TaskRecord(
            task_id=self._id_factory(),
            tool=tool,
            state=TaskState.WORKING,
            created_at=now,
            updated_at=now,
            idempotency_key=idempotency_key,
            deadline_at=now + self.retention.max_execution_s,
            metadata=dict(metadata or {}),
        )
        self.store.put(record)
        return TaskHandle(record.task_id, poll_after_ms=self._hint_ms())

    def get(self, task_id: str, *, record_poll: bool = True) -> TaskRecord:
        """`tasks/get`. Records the poll, which is what keeps the orphan reaper honest."""
        record = self.store.get(task_id)
        if record is None:
            raise TaskNotFound(task_id)
        if record_poll:
            record.last_polled_at = self._clock()
            record.poll_count += 1
            self.store.put(record)
        return record

    def complete(self, task_id: str, result: Any) -> TaskRecord:
        """`tasks/update` to a successful terminal state."""
        return self._terminal(task_id, TaskState.COMPLETED, result=result)

    def cancel(self, task_id: str) -> TaskRecord:
        """`tasks/cancel`. Cancelling an already-terminal task is a no-op, not an error."""
        record = self._require(task_id)
        if record.is_terminal:
            return record
        return self._terminal(task_id, TaskState.CANCELLED)

    def require_input(self, task_id: str) -> TaskRecord:
        record = self._require(task_id)
        now = self._clock()
        updated = record.with_state(TaskState.INPUT_REQUIRED, now)
        self.store.put(updated)
        return updated

    def fail(self, task_id: str, error: str, *, code: int | None = None) -> FailureOutcome:
        """Report a failure and let policy decide what happens next.

        Transient failures inside the attempt budget stay WORKING with a backoff hint —
        the task is not over, it is waiting. Everything else goes terminal, and poison goes
        to the dead-letter queue on the way out.
        """
        record = self._require(task_id)
        if record.is_terminal:
            raise IllegalTransition(f"task {task_id} already terminal ({record.state.value})")

        now = self._clock()
        failure = self.classifier.classify(error, attempts=record.attempts, code=code)

        if self.classifier.should_retry(failure, record.attempts):
            record.attempts += 1
            record.error = error
            record.failure_class = failure
            record.updated_at = now
            self.store.put(record)
            return FailureOutcome(
                task_id=task_id, failure_class=failure, will_retry=True,
                attempts=record.attempts,
                retry_after_s=self.classifier.delay_for(record.attempts),
            )

        failed = record.with_state(TaskState.FAILED, now)
        failed.error = error
        failed.failure_class = failure
        failed.expires_at = now + self.retention.retention_s
        self.store.put(failed)

        dead_lettered = failure is FailureClass.POISON
        if dead_lettered:
            self.dead_letter.push(failed, reason="attempt budget exhausted", now=now)

        return FailureOutcome(
            task_id=task_id, failure_class=failure, will_retry=False,
            attempts=failed.attempts, dead_lettered=dead_lettered,
        )

    # ---- restart ---------------------------------------------------------------------

    def recover(self) -> RecoveryReport:
        """Reconcile in-flight work after a restart.

        Called on startup against a durable store. Tasks the client is still holding handles
        for are triaged: past their deadline they are failed, out of attempts they are
        poisoned, and otherwise they are handed back for the caller to re-enqueue. Nothing
        here silently drops a handle the client believes in.
        """
        now = self._clock()
        resumable = timed_out = poisoned = 0

        for record in self.store.live():
            if record.deadline_at is not None and record.deadline_at <= now:
                failed = record.with_state(TaskState.FAILED, now)
                failed.error = "in-flight at restart, past deadline"
                failed.failure_class = FailureClass.PERMANENT
                failed.expires_at = now + self.retention.retention_s
                self.store.put(failed)
                timed_out += 1
                continue

            if record.attempts >= self.classifier.policy.max_attempts:
                failed = record.with_state(TaskState.FAILED, now)
                failed.error = "in-flight at restart, attempt budget exhausted"
                failed.failure_class = FailureClass.POISON
                failed.expires_at = now + self.retention.retention_s
                self.store.put(failed)
                self.dead_letter.push(failed, reason="restart, no attempts left", now=now)
                poisoned += 1
                continue

            record.attempts += 1
            record.updated_at = now
            record.metadata = {**record.metadata, "recovered": True}
            self.store.put(record)
            resumable += 1

        return RecoveryReport(resumable=resumable, timed_out=timed_out, poisoned=poisoned)

    def resumable(self) -> list[TaskRecord]:
        """Tasks a restart handed back, for the caller to re-enqueue onto its workers."""
        return [r for r in self.store.live() if r.metadata.get("recovered")]

    def reap(self) -> ReapReport:
        return self.reaper.run_once()

    # ---- internals -------------------------------------------------------------------

    def _hint_ms(self) -> int:
        return 250

    def _require(self, task_id: str) -> TaskRecord:
        record = self.store.get(task_id)
        if record is None:
            raise TaskNotFound(task_id)
        return record

    def _terminal(self, task_id: str, state: TaskState, result: Any = None) -> TaskRecord:
        record = self._require(task_id)
        now = self._clock()
        updated = record.with_state(state, now)
        updated.result = result
        updated.expires_at = now + self.retention.retention_s
        self.store.put(updated)
        return updated
