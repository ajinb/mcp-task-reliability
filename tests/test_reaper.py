import pytest

from mcp_task_reliability.clock import FakeClock
from mcp_task_reliability.policy import RetentionPolicy
from mcp_task_reliability.reaper import Reaper
from mcp_task_reliability.store import InMemoryTaskStore
from mcp_task_reliability.types import TaskRecord, TaskState


@pytest.fixture
def setup():
    clock = FakeClock(start=1000.0)
    store = InMemoryTaskStore()
    policy = RetentionPolicy(retention_s=100.0, orphan_after_s=200.0, max_execution_s=300.0)
    return clock, store, Reaper(store, policy, clock), policy


def _put(store, task_id, state=TaskState.WORKING, **kw):
    store.put(TaskRecord(
        task_id=task_id, tool="report", state=state, created_at=1000.0, updated_at=1000.0, **kw
    ))


def test_empty_sweep_reports_nothing(setup):
    _, _, reaper, _ = setup
    report = reaper.run_once()
    assert report.total == 0


def test_working_past_deadline_is_failed_with_retention(setup):
    clock, store, reaper, policy = setup
    _put(store, "late", deadline_at=1200.0, last_polled_at=1400.0)
    clock.advance(400)  # now = 1400

    assert reaper.run_once().timed_out == 1
    record = store.get("late")
    assert record.state is TaskState.FAILED
    assert "max_execution_s" in record.error
    assert record.expires_at == 1400.0 + policy.retention_s


def test_unpolled_live_work_is_cancelled_not_deleted(setup):
    clock, store, reaper, _ = setup
    _put(store, "abandoned", last_polled_at=1000.0)
    clock.advance(300)  # now = 1300, cutoff = 1100

    assert reaper.run_once().orphaned == 1
    record = store.get("abandoned")
    assert record is not None, "an orphan must stay readable inside its retention window"
    assert record.state is TaskState.CANCELLED
    assert "orphaned" in record.error


def test_a_polled_task_is_not_an_orphan(setup):
    clock, store, reaper, _ = setup
    _put(store, "attentive", last_polled_at=1250.0)
    clock.advance(300)  # cutoff = 1100, and 1250 > 1100

    assert reaper.run_once().orphaned == 0
    assert store.get("attentive").state is TaskState.WORKING


def test_terminal_tasks_are_never_orphaned(setup):
    clock, store, reaper, _ = setup
    _put(store, "done", TaskState.COMPLETED, expires_at=9999.0)
    clock.advance(300)
    assert reaper.run_once().orphaned == 0


def test_expired_results_are_deleted(setup):
    clock, store, reaper, _ = setup
    _put(store, "stale", TaskState.COMPLETED, expires_at=1100.0, last_polled_at=9999.0)
    _put(store, "fresh", TaskState.COMPLETED, expires_at=9000.0, last_polled_at=9999.0)
    clock.advance(200)  # now = 1200

    assert reaper.run_once().expired == 1
    assert store.get("stale") is None
    assert store.get("fresh") is not None


def test_timeouts_run_before_expiry_so_they_earn_a_retention_window(setup):
    """A task that times out during the sweep must not also be deleted by that same sweep."""
    clock, store, reaper, _ = setup
    _put(store, "late", deadline_at=1100.0, last_polled_at=9999.0)
    clock.advance(500)

    report = reaper.run_once()
    assert report.timed_out == 1
    assert report.expired == 0
    assert store.get("late") is not None


def test_report_str_is_readable(setup):
    _, _, reaper, _ = setup
    assert "reaped 0" in str(reaper.run_once())


def test_retention_policy_rejects_nonsense():
    with pytest.raises(ValueError):
        RetentionPolicy(retention_s=0)
    with pytest.raises(ValueError):
        RetentionPolicy(orphan_after_s=-1)
