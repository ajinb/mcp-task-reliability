"""Task stores.

A stateless protocol moves the state somewhere else; it does not abolish it. The store *is*
the durability boundary — if a task handle outlives the process that issued it, it is because
something here wrote it down. `InMemoryTaskStore` is for tests and single-process demos;
`SqliteTaskStore` is what survives a restart.

Results must be JSON-serialisable, which MCP tool results already are.
"""

from __future__ import annotations

import copy
import json
import sqlite3
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Protocol

from .types import FailureClass, TaskRecord, TaskState

_COLUMNS = (
    "task_id", "tool", "state", "created_at", "updated_at", "attempts", "idempotency_key",
    "result", "error", "failure_class", "last_polled_at", "poll_count", "expires_at",
    "deadline_at", "metadata",
)


class TaskStore(Protocol):
    """The contract every backend satisfies. See `tests/test_store.py::TestStoreContract`."""

    def put(self, record: TaskRecord) -> None: ...
    def get(self, task_id: str) -> TaskRecord | None: ...
    def delete(self, task_id: str) -> bool: ...
    def by_idempotency_key(self, key: str) -> TaskRecord | None: ...
    def iter_all(self) -> Iterator[TaskRecord]: ...
    def live(self) -> list[TaskRecord]: ...
    def expired(self, now: float) -> list[TaskRecord]: ...
    def unpolled_since(self, cutoff: float) -> list[TaskRecord]: ...
    def past_deadline(self, now: float) -> list[TaskRecord]: ...
    def close(self) -> None: ...


class InMemoryTaskStore:
    """Dict-backed store. Fast, thread-safe, and gone the moment the process is.

    Reads hand back *copies*. A dict store that returns live references would let a caller
    mutate stored state without a `put`, which SQLite would never do — and a contract suite
    that both backends pass has to mean the same thing on both.
    """

    def __init__(self) -> None:
        self._rows: dict[str, TaskRecord] = {}
        self._by_key: dict[str, str] = {}
        self._lock = threading.RLock()

    def put(self, record: TaskRecord) -> None:
        with self._lock:
            self._rows[record.task_id] = copy.deepcopy(record)
            if record.idempotency_key:
                self._by_key[record.idempotency_key] = record.task_id

    @staticmethod
    def _copy(record: TaskRecord | None) -> TaskRecord | None:
        return copy.deepcopy(record) if record is not None else None

    def get(self, task_id: str) -> TaskRecord | None:
        with self._lock:
            return self._copy(self._rows.get(task_id))

    def delete(self, task_id: str) -> bool:
        with self._lock:
            record = self._rows.pop(task_id, None)
            if record is None:
                return False
            if record.idempotency_key:
                self._by_key.pop(record.idempotency_key, None)
            return True

    def by_idempotency_key(self, key: str) -> TaskRecord | None:
        with self._lock:
            task_id = self._by_key.get(key)
            return self._copy(self._rows.get(task_id)) if task_id else None

    def iter_all(self) -> Iterator[TaskRecord]:
        with self._lock:
            return iter([copy.deepcopy(r) for r in self._rows.values()])

    def live(self) -> list[TaskRecord]:
        return [r for r in self.iter_all() if not r.is_terminal]

    def expired(self, now: float) -> list[TaskRecord]:
        return [
            r for r in self.iter_all()
            if r.is_terminal and r.expires_at is not None and r.expires_at <= now
        ]

    def unpolled_since(self, cutoff: float) -> list[TaskRecord]:
        return [r for r in self.iter_all() if (r.last_polled_at or r.created_at) <= cutoff]

    def past_deadline(self, now: float) -> list[TaskRecord]:
        return [
            r for r in self.iter_all()
            if not r.is_terminal and r.deadline_at is not None and r.deadline_at <= now
        ]

    def close(self) -> None:
        return None


class SqliteTaskStore:
    """SQLite-backed store in WAL mode — the one that makes a handle survive a restart.

    Every write commits. That is deliberate: a task handle the client is holding but the
    server has not durably written is exactly the handle that goes missing across a deploy.
    """

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        if self.path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._create_schema()

    def _create_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id         TEXT PRIMARY KEY,
                    tool            TEXT NOT NULL,
                    state           TEXT NOT NULL,
                    created_at      REAL NOT NULL,
                    updated_at      REAL NOT NULL,
                    attempts        INTEGER NOT NULL DEFAULT 1,
                    idempotency_key TEXT,
                    result          TEXT,
                    error           TEXT,
                    failure_class   TEXT,
                    last_polled_at  REAL,
                    poll_count      INTEGER NOT NULL DEFAULT 0,
                    expires_at      REAL,
                    deadline_at     REAL,
                    metadata        TEXT NOT NULL DEFAULT '{}'
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idem
                    ON tasks(idempotency_key) WHERE idempotency_key IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_tasks_expires ON tasks(expires_at);
                CREATE INDEX IF NOT EXISTS idx_tasks_state   ON tasks(state);
                CREATE INDEX IF NOT EXISTS idx_tasks_polled  ON tasks(last_polled_at);
                """
            )
            self._conn.commit()

    @staticmethod
    def _to_row(r: TaskRecord) -> tuple[Any, ...]:
        return (
            r.task_id, r.tool, r.state.value, r.created_at, r.updated_at, r.attempts,
            r.idempotency_key, json.dumps(r.result) if r.result is not None else None,
            r.error, r.failure_class.value if r.failure_class else None,
            r.last_polled_at, r.poll_count, r.expires_at, r.deadline_at,
            json.dumps(r.metadata),
        )

    @staticmethod
    def _from_row(row: sqlite3.Row) -> TaskRecord:
        return TaskRecord(
            task_id=row["task_id"],
            tool=row["tool"],
            state=TaskState(row["state"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            attempts=row["attempts"],
            idempotency_key=row["idempotency_key"],
            result=json.loads(row["result"]) if row["result"] is not None else None,
            error=row["error"],
            failure_class=FailureClass(row["failure_class"]) if row["failure_class"] else None,
            last_polled_at=row["last_polled_at"],
            poll_count=row["poll_count"],
            expires_at=row["expires_at"],
            deadline_at=row["deadline_at"],
            metadata=json.loads(row["metadata"]),
        )

    def put(self, record: TaskRecord) -> None:
        placeholders = ", ".join("?" for _ in _COLUMNS)
        with self._lock:
            self._conn.execute(
                f"INSERT OR REPLACE INTO tasks ({', '.join(_COLUMNS)}) VALUES ({placeholders})",
                self._to_row(record),
            )
            self._conn.commit()

    def _query(self, sql: str, params: tuple[Any, ...] = ()) -> list[TaskRecord]:
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [self._from_row(r) for r in rows]

    def get(self, task_id: str) -> TaskRecord | None:
        rows = self._query("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
        return rows[0] if rows else None

    def delete(self, task_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
            self._conn.commit()
            return cur.rowcount > 0

    def by_idempotency_key(self, key: str) -> TaskRecord | None:
        rows = self._query("SELECT * FROM tasks WHERE idempotency_key = ?", (key,))
        return rows[0] if rows else None

    def iter_all(self) -> Iterator[TaskRecord]:
        return iter(self._query("SELECT * FROM tasks"))

    def live(self) -> list[TaskRecord]:
        return self._query(
            "SELECT * FROM tasks WHERE state IN (?, ?)",
            (TaskState.WORKING.value, TaskState.INPUT_REQUIRED.value),
        )

    def expired(self, now: float) -> list[TaskRecord]:
        return self._query(
            "SELECT * FROM tasks WHERE expires_at IS NOT NULL AND expires_at <= ? "
            "AND state NOT IN (?, ?)",
            (now, TaskState.WORKING.value, TaskState.INPUT_REQUIRED.value),
        )

    def unpolled_since(self, cutoff: float) -> list[TaskRecord]:
        return self._query(
            "SELECT * FROM tasks WHERE COALESCE(last_polled_at, created_at) <= ?", (cutoff,)
        )

    def past_deadline(self, now: float) -> list[TaskRecord]:
        return self._query(
            "SELECT * FROM tasks WHERE deadline_at IS NOT NULL AND deadline_at <= ? "
            "AND state IN (?, ?)",
            (now, TaskState.WORKING.value, TaskState.INPUT_REQUIRED.value),
        )

    def close(self) -> None:
        with self._lock:
            self._conn.close()


class DeadTaskQueue:
    """Where poison tasks go to be looked at by a human.

    A dead-letter queue is not a nicety. Without one, a task that can never succeed is either
    retried forever or silently dropped, and both are worse than a queue somebody reads.
    """

    def __init__(self, store: TaskStore | None = None) -> None:
        self._store: TaskStore = store or InMemoryTaskStore()

    def push(self, record: TaskRecord, reason: str, now: float) -> None:
        buried = TaskRecord(
            task_id=record.task_id, tool=record.tool, state=TaskState.FAILED,
            created_at=record.created_at, updated_at=now, attempts=record.attempts,
            idempotency_key=None, result=None, error=record.error,
            failure_class=FailureClass.POISON, metadata={**record.metadata, "reason": reason},
        )
        self._store.put(buried)

    def __len__(self) -> int:
        return len(list(self._store.iter_all()))

    def all(self) -> list[TaskRecord]:
        return sorted(self._store.iter_all(), key=lambda r: r.updated_at)

    def get(self, task_id: str) -> TaskRecord | None:
        return self._store.get(task_id)
