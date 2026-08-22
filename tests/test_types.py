import pytest

from mcp_task_reliability.types import IllegalTransition, TaskRecord, TaskState


def _record(state=TaskState.WORKING):
    return TaskRecord(task_id="t1", tool="x", state=state, created_at=0.0, updated_at=0.0)


def test_terminal_states():
    assert TaskState.COMPLETED.is_terminal
    assert TaskState.FAILED.is_terminal
    assert TaskState.CANCELLED.is_terminal
    assert not TaskState.WORKING.is_terminal
    assert not TaskState.INPUT_REQUIRED.is_terminal


@pytest.mark.parametrize("target", [TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED])
def test_working_may_go_terminal(target):
    assert _record().with_state(target, now=1.0).state is target


@pytest.mark.parametrize("start", [TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED])
def test_terminal_is_a_one_way_door(start):
    with pytest.raises(IllegalTransition):
        _record(start).with_state(TaskState.WORKING, now=1.0)


def test_input_required_can_return_to_working():
    r = _record(TaskState.INPUT_REQUIRED).with_state(TaskState.WORKING, now=2.0)
    assert r.state is TaskState.WORKING
    assert r.updated_at == 2.0


def test_with_state_does_not_mutate_the_original():
    original = _record()
    original.with_state(TaskState.COMPLETED, now=5.0)
    assert original.state is TaskState.WORKING
