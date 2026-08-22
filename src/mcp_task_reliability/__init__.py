"""mcp-task-reliability — the operational layer under the MCP Tasks extension."""

from .clock import FakeClock
from .manager import FailureOutcome, RecoveryReport, TaskManager
from .policy import PollPolicy, RetentionPolicy, RetryPolicy
from .poll import PollScheduler, adaptive_polls, fixed_interval_polls
from .reaper import Reaper, ReapReport
from .retry import RetryClassifier
from .store import DeadTaskQueue, InMemoryTaskStore, SqliteTaskStore, TaskStore
from .types import (
    FailureClass,
    IllegalTransition,
    TaskHandle,
    TaskNotFound,
    TaskRecord,
    TaskState,
)

__version__ = "0.1.0"

__all__ = [
    "DeadTaskQueue", "FailureClass", "FailureOutcome", "FakeClock", "IllegalTransition",
    "InMemoryTaskStore", "PollPolicy", "PollScheduler", "ReapReport", "Reaper",
    "RecoveryReport", "RetentionPolicy", "RetryClassifier", "RetryPolicy", "SqliteTaskStore",
    "TaskHandle", "TaskManager", "TaskNotFound", "TaskRecord", "TaskState", "TaskStore",
    "adaptive_polls", "fixed_interval_polls",
]
