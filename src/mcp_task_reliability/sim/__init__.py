"""Offline, deterministic scenarios. No network, no API key, no MCP server required."""

from .scenarios import (
    CrashResult,
    PollResult,
    RetryResult,
    crash_recovery,
    poll_storm,
    retry_classes,
)

__all__ = [
    "CrashResult", "PollResult", "RetryResult",
    "crash_recovery", "poll_storm", "retry_classes",
]
