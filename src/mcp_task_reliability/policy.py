"""Lifecycle policy.

The MCP Tasks extension defines the task *lifecycle*. It deliberately leaves the
*operational policy* open — the 2026 MCP roadmap still lists retry semantics and result
retention as unresolved. These are the knobs that fill that hole. Defaults are chosen to
be boring and safe rather than clever.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetentionPolicy:
    """How long results live, and when abandoned work is reclaimed.

    `retention_s`
        How long a *terminal* task's result is readable before the reaper drops it. This is
        the answer to "the client got its answer — how long do we keep paying to store it?"
    `orphan_after_s`
        How long a task may go unpolled before it is presumed abandoned. A stateless server
        has no connection to notice a client walking away; silence is the only signal there is.
    `max_execution_s`
        The wall-clock ceiling on a working task. Past this it is failed as a timeout rather
        than left working forever.
    """

    retention_s: float = 3600.0
    orphan_after_s: float = 900.0
    max_execution_s: float = 1800.0

    def __post_init__(self) -> None:
        for name in ("retention_s", "orphan_after_s", "max_execution_s"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")


@dataclass(frozen=True)
class RetryPolicy:
    """Capped exponential backoff with full jitter, and a hard attempt ceiling.

    `max_attempts` counts total attempts, not retries: 3 means one try plus two retries.
    A task that exhausts it is reclassified POISON and moved to the dead-task queue rather
    than retried forever.
    """

    max_attempts: int = 3
    base_delay_s: float = 0.5
    max_delay_s: float = 30.0
    multiplier: float = 2.0
    jitter: bool = True

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay_s <= 0 or self.max_delay_s <= 0:
            raise ValueError("delays must be positive")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")


@dataclass(frozen=True)
class PollPolicy:
    """Client-side adaptive backoff.

    Stateless MCP removed the open connection, which means every client now polls. Fixed-interval
    polling turns a long task into a load generator: a 5-minute task at 250ms is 1,200 requests
    that all say "not yet". Backing off multiplicatively answers the same question with a
    fraction of the traffic.
    """

    initial_ms: int = 250
    max_ms: int = 10_000
    multiplier: float = 1.6
    jitter: bool = True

    def __post_init__(self) -> None:
        if self.initial_ms <= 0 or self.max_ms < self.initial_ms:
            raise ValueError("require 0 < initial_ms <= max_ms")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")
