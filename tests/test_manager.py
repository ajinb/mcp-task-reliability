import pytest

from mcp_task_reliability.clock import FakeClock
from mcp_task_reliability.manager import TaskManager
from mcp_task_reliability.policy import RetentionPolicy, RetryPolicy
from mcp_task_reliability.store import SqliteTaskStore
from mcp_task_reliability.types import (
    FailureClass,
    IllegalTransition,
    TaskNotFound,
    TaskState,
)


@pytest.fixture
def clock():
    return FakeClock(start=1000.0)


@pytest.fixture
def manager(clock):
    counter = iter(range(10_000))
    return TaskManager(
        clock=clock,
        retry=RetryPolicy(max_attempts=3, jitter=False),
        retention=RetentionPolicy(retention_s=100.0, orphan_after_s=200.0, max_execution_s=300.0),
        id_factory=lambda: f"task-{next(counter):04d}",
    )


def test_create_returns_a_handle_with_a_poll_hint(manager):
    handle = manager.create("build_report")
    assert handle.task_id == "task-0000"
    assert handle.poll_after_ms > 0


def test_create_sets_a_deadline(manager, clock):
    handle = manager.create("build_report")
    assert manager.get(handle.task_id).deadline_at == clock.now + 300.0


def test_get_unknown_task_raises(manager):
    with pytest.raises(TaskNotFound):
        manager.get("nope")


def test_get_records_the_poll(manager, clock):
    handle = manager.create("x")
    clock.advance(10)
    manager.get(handle.task_id)
    manager.get(handle.task_id)
    record = manager.get(handle.task_id, record_poll=False)
    assert record.poll_count == 2
    assert record.last_polled_at == clock.now


def test_idempotency_key_returns_the_same_task(manager):
    first = manager.create("charge_card", idempotency_key="order-99")
    second = manager.create("charge_card", idempotency_key="order-99")
    assert first.task_id == second.task_id
    assert len(list(manager.store.iter_all())) == 1


def test_no_idempotency_key_means_a_new_task_every_time(manager):
    assert manager.create("x").task_id != manager.create("x").task_id


def test_complete_stores_the_result_and_an_expiry(manager, clock):
    handle = manager.create("x")
    record = manager.complete(handle.task_id, {"rows": 42})
    assert record.state is TaskState.COMPLETED
    assert record.result == {"rows": 42}
    assert record.expires_at == clock.now + 100.0


def test_cancel_is_idempotent(manager):
    handle = manager.create("x")
    assert manager.cancel(handle.task_id).state is TaskState.CANCELLED
    assert manager.cancel(handle.task_id).state is TaskState.CANCELLED


def test_require_input_and_back_to_working(manager):
    handle = manager.create("x")
    assert manager.require_input(handle.task_id).state is TaskState.INPUT_REQUIRED
    assert manager.complete(handle.task_id, "ok").state is TaskState.COMPLETED


def test_transient_failure_keeps_the_task_alive_with_a_backoff(manager):
    handle = manager.create("x")
    outcome = manager.fail(handle.task_id, "upstream timeout after 30s")
    assert outcome.failure_class is FailureClass.TRANSIENT
    assert outcome.will_retry is True
    assert outcome.attempts == 2
    assert outcome.retry_after_s == 1.0
    assert manager.get(handle.task_id).state is TaskState.WORKING


def test_permanent_failure_goes_terminal_without_the_dead_letter_queue(manager):
    handle = manager.create("x")
    outcome = manager.fail(handle.task_id, "invalid arguments: 'limit' must be an integer")
    assert outcome.failure_class is FailureClass.PERMANENT
    assert outcome.will_retry is False
    assert outcome.dead_lettered is False
    assert manager.get(handle.task_id).state is TaskState.FAILED
    assert len(manager.dead_letter) == 0


def test_exhausted_transient_failure_is_poison_and_dead_lettered(manager, clock):
    handle = manager.create("x")
    outcome = manager.fail(handle.task_id, "connection reset by peer")
    while outcome.will_retry:
        clock.advance(outcome.retry_after_s or 0)
        outcome = manager.fail(handle.task_id, "connection reset by peer")

    assert outcome.failure_class is FailureClass.POISON
    assert outcome.attempts == 3
    assert outcome.dead_lettered is True
    assert len(manager.dead_letter) == 1
    assert manager.dead_letter.all()[0].metadata["reason"] == "attempt budget exhausted"


def test_failing_a_terminal_task_is_an_error(manager):
    handle = manager.create("x")
    manager.complete(handle.task_id, "done")
    with pytest.raises(IllegalTransition):
        manager.fail(handle.task_id, "too late")


def test_reap_is_reachable_from_the_manager(manager, clock):
    manager.create("x")
    clock.advance(1000)
    assert manager.reap().total >= 1


class TestRecovery:
    """What a restart does to work that was in flight."""

    def _restart(self, tmp_path, clock, seed_ids):
        store = SqliteTaskStore(tmp_path / "tasks.db")
        counter = iter(seed_ids)
        return TaskManager(
            store, clock=clock,
            retry=RetryPolicy(max_attempts=3, jitter=False),
            retention=RetentionPolicy(
                retention_s=100.0, orphan_after_s=200.0, max_execution_s=300.0
            ),
            id_factory=lambda: next(counter),
        )

    def test_in_flight_work_is_handed_back_for_re_enqueue(self, tmp_path, clock):
        before = self._restart(tmp_path, clock, ["a", "b", "c"])
        handles = [before.create("x") for _ in range(3)]
        before.complete(handles[0].task_id, "done")
        before.store.close()

        clock.advance(10)
        after = self._restart(tmp_path, clock, [])
        report = after.recover()

        assert report.resumable == 2
        assert report.total == 2
        assert {r.task_id for r in after.resumable()} == {"b", "c"}
        # The completed one is untouched and still readable.
        assert after.get("a").result == "done"

    def test_in_flight_work_past_its_deadline_is_failed(self, tmp_path, clock):
        before = self._restart(tmp_path, clock, ["a"])
        before.create("x")
        before.store.close()

        clock.advance(500)  # past max_execution_s = 300
        after = self._restart(tmp_path, clock, [])
        report = after.recover()

        assert report.timed_out == 1
        assert after.get("a").state is TaskState.FAILED

    def test_in_flight_work_out_of_attempts_is_poisoned(self, tmp_path, clock):
        before = self._restart(tmp_path, clock, ["a"])
        handle = before.create("x")
        outcome = before.fail(handle.task_id, "upstream timeout after 30s")
        assert outcome.will_retry
        before.fail(handle.task_id, "upstream timeout after 30s")  # attempts -> 3
        before.store.close()

        clock.advance(10)
        after = self._restart(tmp_path, clock, [])
        report = after.recover()

        assert report.poisoned == 1
        assert len(after.dead_letter) == 1
        assert after.get("a").failure_class is FailureClass.POISON

    def test_recovery_is_idempotent_across_repeated_restarts(self, tmp_path, clock):
        before = self._restart(tmp_path, clock, ["a"])
        before.create("x")
        before.store.close()

        after = self._restart(tmp_path, clock, [])
        assert after.recover().resumable == 1
        # attempts is now 2; a second restart still has budget, a third does not.
        assert after.recover().resumable == 1
        assert after.recover().poisoned == 1
