"""Core task types.

These mirror the wire shapes of the MCP Tasks extension (spec 2026-07-28): a `tools/call`
returns a durable handle, and the client drives the lifecycle with `tasks/get`,
`tasks/update` and `tasks/cancel`. Everything here is transport-agnostic on purpose —
this package is the policy layer you put *behind* an MCP server, not an MCP server.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any


class TaskState(str, Enum):
    """Lifecycle states. `WORKING` and `INPUT_REQUIRED` are live; the rest are terminal."""

    WORKING = "working"
    INPUT_REQUIRED = "input_required"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL


_TERMINAL = frozenset({TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED})

# Legal state transitions. A task may not leave a terminal state, and may not skip
# backwards from INPUT_REQUIRED into WORKING without the server saying so explicitly.
_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.WORKING: frozenset(
        {TaskState.WORKING, TaskState.INPUT_REQUIRED, TaskState.COMPLETED,
         TaskState.FAILED, TaskState.CANCELLED}
    ),
    TaskState.INPUT_REQUIRED: frozenset(
        {TaskState.WORKING, TaskState.INPUT_REQUIRED, TaskState.COMPLETED,
         TaskState.FAILED, TaskState.CANCELLED}
    ),
    TaskState.COMPLETED: frozenset(),
    TaskState.FAILED: frozenset(),
    TaskState.CANCELLED: frozenset(),
}


class FailureClass(str, Enum):
    """How a failure should be treated by the retry policy."""

    TRANSIENT = "transient"
    PERMANENT = "permanent"
    POISON = "poison"


class TaskNotFound(KeyError):
    """Raised when a handle refers to a task the store has never seen, or has reaped."""


class IllegalTransition(ValueError):
    """Raised when an update would move a task out of a terminal state."""


@dataclass(frozen=True)
class TaskHandle:
    """What `tools/call` hands back instead of blocking.

    `poll_after_ms` is the server's hint to the client about when to poll next. Clients that
    honour it are the difference between a calm system and a poll storm — see `poll.py`.
    """

    task_id: str
    poll_after_ms: int = 250


@dataclass
class TaskRecord:
    """The durable row behind a handle."""

    task_id: str
    tool: str
    state: TaskState
    created_at: float
    updated_at: float
    attempts: int = 1
    idempotency_key: str | None = None
    result: Any = None
    error: str | None = None
    failure_class: FailureClass | None = None
    last_polled_at: float | None = None
    poll_count: int = 0
    # Set when the task reaches a terminal state: when the *result* may be dropped.
    expires_at: float | None = None
    # Set at creation: when a still-working task is declared timed out.
    deadline_at: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def with_state(self, state: TaskState, now: float) -> TaskRecord:
        """Return a copy in a new state, enforcing the transition table."""
        if state not in _TRANSITIONS[self.state]:
            raise IllegalTransition(
                f"task {self.task_id}: {self.state.value} -> {state.value} is not a legal transition"
            )
        return replace(self, state=state, updated_at=now)

    @property
    def is_terminal(self) -> bool:
        return self.state.is_terminal
