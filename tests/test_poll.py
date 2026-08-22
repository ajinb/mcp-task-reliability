import pytest

from mcp_task_reliability.policy import PollPolicy
from mcp_task_reliability.poll import PollScheduler, adaptive_polls, fixed_interval_polls


def test_fixed_interval_polls_is_arithmetic():
    assert fixed_interval_polls(300.0, 250) == 1200
    assert fixed_interval_polls(1.0, 5000) == 1  # never fewer than one poll


def test_fixed_interval_rejects_nonsense():
    with pytest.raises(ValueError):
        fixed_interval_polls(10.0, 0)


def test_scheduler_backs_off_multiplicatively():
    s = PollScheduler(PollPolicy(initial_ms=100, multiplier=2.0, jitter=False))
    assert [s.next_delay_ms() for _ in range(4)] == [100, 200, 400, 800]


def test_scheduler_respects_the_ceiling():
    s = PollScheduler(PollPolicy(initial_ms=100, max_ms=300, multiplier=2.0, jitter=False))
    assert [s.next_delay_ms() for _ in range(4)] == [100, 200, 300, 300]


def test_server_hint_is_a_floor_not_a_ceiling():
    s = PollScheduler(PollPolicy(initial_ms=100, multiplier=2.0, jitter=False))
    assert s.next_delay_ms(server_hint_ms=1000) == 1000
    # A later, smaller hint must not talk a backed-off client into speeding up.
    assert s.next_delay_ms(server_hint_ms=50) == 2000


def test_reset_returns_to_the_initial_interval():
    s = PollScheduler(PollPolicy(initial_ms=100, multiplier=2.0, jitter=False))
    s.next_delay_ms()
    s.next_delay_ms()
    s.reset()
    assert s.next_delay_ms() == 100


def test_jitter_is_bounded_and_seeded():
    a = PollScheduler(PollPolicy(initial_ms=1000, jitter=True), seed=3)
    b = PollScheduler(PollPolicy(initial_ms=1000, jitter=True), seed=3)
    first = a.next_delay_ms()
    assert 500.0 <= first <= 1000.0
    assert b.next_delay_ms() == first


def test_adaptive_polling_beats_fixed_and_the_gap_widens_with_length():
    gaps = []
    for duration in (30.0, 300.0, 1800.0):
        fixed = fixed_interval_polls(duration, 250)
        adaptive = adaptive_polls(duration, PollPolicy(initial_ms=250))
        assert adaptive < fixed
        gaps.append(1 - adaptive / fixed)
    assert gaps == sorted(gaps), "longer tasks should show a bigger saving, not a smaller one"


def test_adaptive_polls_is_deterministic():
    assert adaptive_polls(300.0, seed=11) == adaptive_polls(300.0, seed=11)


def test_invalid_poll_policies_are_rejected():
    with pytest.raises(ValueError):
        PollPolicy(initial_ms=0)
    with pytest.raises(ValueError):
        PollPolicy(initial_ms=500, max_ms=100)
