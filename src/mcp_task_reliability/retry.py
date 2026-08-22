"""Failure classification and backoff.

The spec says a task can fail. It does not say which failures are worth trying again, and
that distinction is the whole game: retrying a permanent failure burns budget and amplifies
load, while not retrying a transient one throws away work that would have succeeded.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from .policy import RetryPolicy
from .types import FailureClass

# Substrings that reliably indicate the call may succeed if tried again.
DEFAULT_TRANSIENT_MARKERS: tuple[str, ...] = (
    "timeout", "timed out", "connection", "connection reset", "unavailable",
    "temporarily", "throttl", "rate limit", "too many requests", "overloaded",
    "deadline exceeded", "broken pipe", "econnreset",
)

# Substrings that indicate the call will fail identically forever.
DEFAULT_PERMANENT_MARKERS: tuple[str, ...] = (
    "invalid", "malformed", "schema", "validation", "unauthorized", "forbidden",
    "not found", "unknown tool", "unsupported", "parse error", "permission denied",
)

# HTTP-ish status codes, for servers that surface them.
TRANSIENT_CODES: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})
PERMANENT_CODES: frozenset[int] = frozenset({400, 401, 403, 404, 405, 409, 410, 422})


@dataclass
class RetryClassifier:
    """Sorts failures into transient / permanent / poison, and says how long to wait.

    The default for an *unrecognised* error is PERMANENT, which is the opposite of what most
    retry libraries do. The reasoning is operational: an error you cannot classify is an error
    you do not understand, and retrying what you do not understand is how one bad tool call
    becomes a thundering herd. Opt in to retrying a new error by naming it, not by default.
    Set `default_class=FailureClass.TRANSIENT` if you disagree — but measure the blast radius.
    """

    policy: RetryPolicy = field(default_factory=RetryPolicy)
    transient_markers: tuple[str, ...] = DEFAULT_TRANSIENT_MARKERS
    permanent_markers: tuple[str, ...] = DEFAULT_PERMANENT_MARKERS
    default_class: FailureClass = FailureClass.PERMANENT
    seed: int = 42
    _rng: random.Random = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)

    def classify(
        self, error: str, *, attempts: int = 1, code: int | None = None
    ) -> FailureClass:
        """Classify a failure. `attempts` is the count *including* the one that just failed."""
        # Attempt ceiling wins over everything: a transient error retried to exhaustion is
        # operationally a poison task, whatever its origin.
        if attempts >= self.policy.max_attempts:
            base = self._classify_ignoring_attempts(error, code)
            return FailureClass.POISON if base is FailureClass.TRANSIENT else base

        return self._classify_ignoring_attempts(error, code)

    def _classify_ignoring_attempts(self, error: str, code: int | None) -> FailureClass:
        if code is not None:
            if code in TRANSIENT_CODES:
                return FailureClass.TRANSIENT
            if code in PERMANENT_CODES:
                return FailureClass.PERMANENT

        haystack = (error or "").lower()
        # Permanent markers are checked first: "invalid connection string" is a config bug,
        # not a network blip, and the word "connection" should not rescue it.
        if any(m in haystack for m in self.permanent_markers):
            return FailureClass.PERMANENT
        if any(m in haystack for m in self.transient_markers):
            return FailureClass.TRANSIENT
        return self.default_class

    def should_retry(self, failure: FailureClass, attempts: int) -> bool:
        return failure is FailureClass.TRANSIENT and attempts < self.policy.max_attempts

    def delay_for(self, attempt: int) -> float:
        """Full-jitter capped exponential backoff for the given (1-based) attempt number."""
        if attempt < 1:
            raise ValueError("attempt is 1-based")
        raw = self.policy.base_delay_s * (self.policy.multiplier ** (attempt - 1))
        capped = min(raw, self.policy.max_delay_s)
        if not self.policy.jitter:
            return capped
        # Full jitter (AWS architecture blog's "Exponential Backoff and Jitter"): sampling the
        # whole interval decorrelates clients far better than capped or equal jitter.
        return self._rng.uniform(0.0, capped)
