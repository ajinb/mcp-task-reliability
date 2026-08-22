"""Shared contract suite. Add a backend to the fixture and it is tested for free."""

import pytest

from mcp_task_reliability.store import (
    DeadTaskQueue,
    InMemoryTaskStore,
    SqliteTaskStore,
)
from mcp_task_reliability.types import FailureClass, TaskRecord, TaskState


@pytest.fixture(params=["memory", "sqlite"])
def store(request, tmp_path):
    s = InMemoryTaskStore() if request.param == "memory" else SqliteTaskStore(tmp_path / "t.db")
    yield s
    s.close()


def _record(task_id="t1", state=TaskState.WORKING, **kw):
    base = {"created_at": 100.0, "updated_at": 100.0}
    base.update(kw)
    return TaskRecord(task_id=task_id, tool="report", state=state, **base)


class TestStoreContract:
    def test_put_then_get_round_trips(self, store):
        store.put(_record(result={"rows": 1}, metadata={"k": "v"}))
        got = store.get("t1")
        assert got.task_id == "t1"
        assert got.result == {"rows": 1}
        assert got.metadata == {"k": "v"}
        assert got.state is TaskState.WORKING

    def test_get_missing_returns_none(self, store):
        assert store.get("nope") is None

    def test_put_is_upsert(self, store):
        store.put(_record())
        store.put(_record(state=TaskState.COMPLETED))
        assert store.get("t1").state is TaskState.COMPLETED
        assert len(list(store.iter_all())) == 1

    def test_delete(self, store):
        store.put(_record())
        assert store.delete("t1") is True
        assert store.delete("t1") is False
        assert store.get("t1") is None

    def test_idempotency_lookup(self, store):
        store.put(_record(idempotency_key="abc"))
        assert store.by_idempotency_key("abc").task_id == "t1"
        assert store.by_idempotency_key("other") is None

    def test_failure_class_round_trips(self, store):
        store.put(_record(state=TaskState.FAILED, failure_class=FailureClass.POISON))
        assert store.get("t1").failure_class is FailureClass.POISON

    def test_live_excludes_terminal(self, store):
        store.put(_record("a", TaskState.WORKING))
        store.put(_record("b", TaskState.INPUT_REQUIRED))
        store.put(_record("c", TaskState.COMPLETED))
        assert {r.task_id for r in store.live()} == {"a", "b"}

    def test_expired_only_matches_terminal_past_expiry(self, store):
        store.put(_record("done", TaskState.COMPLETED, expires_at=150.0))
        store.put(_record("fresh", TaskState.COMPLETED, expires_at=999.0))
        store.put(_record("live", TaskState.WORKING, expires_at=1.0))
        assert {r.task_id for r in store.expired(200.0)} == {"done"}

    def test_unpolled_since_falls_back_to_created_at(self, store):
        store.put(_record("never", TaskState.WORKING))
        store.put(_record("recent", TaskState.WORKING, last_polled_at=500.0))
        assert {r.task_id for r in store.unpolled_since(200.0)} == {"never"}

    def test_past_deadline_only_matches_live(self, store):
        store.put(_record("late", TaskState.WORKING, deadline_at=150.0))
        store.put(_record("ok", TaskState.WORKING, deadline_at=999.0))
        store.put(_record("done", TaskState.COMPLETED, deadline_at=1.0))
        assert {r.task_id for r in store.past_deadline(200.0)} == {"late"}


def test_sqlite_survives_reopening_the_file(tmp_path):
    path = tmp_path / "tasks.db"
    first = SqliteTaskStore(path)
    first.put(_record(result={"rows": 7}))
    first.close()

    second = SqliteTaskStore(path)
    assert second.get("t1").result == {"rows": 7}
    second.close()


def test_memory_store_does_not_survive_reinstantiation():
    InMemoryTaskStore().put(_record())
    assert InMemoryTaskStore().get("t1") is None


def test_dead_task_queue_buries_with_reason():
    q = DeadTaskQueue()
    q.push(_record(error="boom"), reason="attempt budget exhausted", now=500.0)
    assert len(q) == 1
    buried = q.get("t1")
    assert buried.failure_class is FailureClass.POISON
    assert buried.metadata["reason"] == "attempt budget exhausted"
    assert buried.idempotency_key is None


def test_mutating_a_read_record_does_not_touch_the_store(store):
    """Both backends must behave the same way here, or the contract suite means nothing."""
    store.put(_record())
    got = store.get("t1")
    got.state = TaskState.COMPLETED
    got.metadata["sneaky"] = True
    assert store.get("t1").state is TaskState.WORKING
    assert store.get("t1").metadata == {}
