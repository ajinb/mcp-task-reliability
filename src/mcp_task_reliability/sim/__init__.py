"""Offline, deterministic scenarios. No network, no API key, no MCP server required."""

from .scenarios import (
    CrashResult,
    PollResult,
    RetryResult,
    crash_recovery,
    poll_storm,
    retry_classes,
)
from .study import (
    ClassificationCost,
    CouplingResult,
    PollPoint,
    classification_cost,
    orphan_coupling,
    poll_frontier,
    single_poll_run,
)

__all__ = [
    "ClassificationCost", "CouplingResult", "CrashResult", "PollPoint", "PollResult",
    "RetryResult", "classification_cost", "crash_recovery", "orphan_coupling",
    "poll_frontier", "poll_storm", "retry_classes", "single_poll_run",
]
