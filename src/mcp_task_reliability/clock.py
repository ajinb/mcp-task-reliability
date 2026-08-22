"""Injectable clocks.

Nothing in this package calls `time.time()` or `time.sleep()` on its own. Every timing
decision goes through a clock you can hand it, which is why the whole test suite and both
demos run instantly and identically on every machine.
"""

from __future__ import annotations


class FakeClock:
    """A clock you advance by hand."""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> float:
        self.now += float(seconds)
        return self.now
