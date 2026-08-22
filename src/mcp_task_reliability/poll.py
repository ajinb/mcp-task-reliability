"""Client-side poll scheduling.

Stateless MCP took away the open connection. Every client now polls, and a client that polls
on a fixed interval turns one long task into sustained load that says "not yet" a thousand
times. This is the smallest thing that fixes it.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from .policy import PollPolicy


@dataclass
class PollScheduler:
    """Multiplicative backoff with a server-supplied floor.

    The server's `poll_after_ms` hint is treated as a *floor*, never a ceiling: the server
    knows how long its own work takes, so it may tell the client to slow down, but it may not
    talk a backed-off client into speeding up.
    """

    policy: PollPolicy = field(default_factory=PollPolicy)
    seed: int = 42
    _current_ms: float = field(init=False, repr=False)
    _rng: random.Random = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)
        self._current_ms = float(self.policy.initial_ms)

    def reset(self) -> None:
        self._current_ms = float(self.policy.initial_ms)

    @property
    def current_ms(self) -> float:
        return self._current_ms

    def next_delay_ms(self, *, server_hint_ms: int | None = None) -> float:
        """Return how long to wait before the next poll, then advance the backoff."""
        if server_hint_ms is not None:
            self._current_ms = max(self._current_ms, float(server_hint_ms))

        delay = min(self._current_ms, float(self.policy.max_ms))
        if self.policy.jitter:
            # Decorrelate a fleet of clients that all started polling at the same instant.
            delay = self._rng.uniform(delay * 0.5, delay)

        self._current_ms = min(self._current_ms * self.policy.multiplier, self.policy.max_ms)
        return delay


def fixed_interval_polls(duration_s: float, interval_ms: int) -> int:
    """How many polls a naive fixed-interval client makes over a task of this length."""
    if interval_ms <= 0:
        raise ValueError("interval_ms must be positive")
    return max(1, int(duration_s * 1000.0 / interval_ms))


def adaptive_polls(duration_s: float, policy: PollPolicy | None = None, seed: int = 42) -> int:
    """How many polls an adaptive client makes over a task of this length."""
    scheduler = PollScheduler(policy=policy or PollPolicy(), seed=seed)
    elapsed_ms = 0.0
    polls = 0
    horizon_ms = duration_s * 1000.0
    while elapsed_ms < horizon_ms:
        elapsed_ms += scheduler.next_delay_ms()
        polls += 1
    return polls
