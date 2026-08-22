"""The scenarios must keep making the claims the README makes."""

from mcp_task_reliability.sim import crash_recovery, poll_storm, retry_classes
from mcp_task_reliability.types import FailureClass


class TestCrashRecovery:
    def test_memory_store_loses_every_handle(self):
        r = crash_recovery(durable=False)
        assert r.backend == "memory"
        assert r.resolved_after_restart == 0
        assert r.lost == r.issued
        assert r.survival_rate == 0.0

    def test_sqlite_store_loses_none(self):
        r = crash_recovery(durable=True)
        assert r.backend == "sqlite"
        assert r.resolved_after_restart == r.issued
        assert r.lost == 0
        assert r.survival_rate == 1.0

    def test_in_flight_work_is_handed_back_for_re_enqueue(self):
        r = crash_recovery(durable=True, tasks=20, completed_before_crash=12)
        assert r.in_flight == 8
        assert r.resumable == 8

    def test_is_deterministic(self):
        assert crash_recovery(durable=True) == crash_recovery(durable=True)


class TestPollStorm:
    def test_adaptive_polling_is_a_large_reduction(self):
        r = poll_storm()
        assert r.adaptive_polls < r.fixed_polls
        assert r.reduction > 0.9

    def test_fixed_polling_is_the_arithmetic_we_claim(self):
        r = poll_storm(duration_s=300.0, interval_ms=250)
        assert r.fixed_polls == 1200

    def test_is_deterministic(self):
        assert poll_storm() == poll_storm()


class TestRetryClasses:
    def test_three_errors_land_in_three_different_places(self):
        results = {r.error: r for r in retry_classes()}

        timeout = results["upstream timeout after 30s"]
        assert timeout.failure_class is FailureClass.TRANSIENT
        assert timeout.will_retry is True
        assert timeout.dead_lettered is False

        invalid = results["invalid arguments: 'limit' must be an integer"]
        assert invalid.failure_class is FailureClass.PERMANENT
        assert invalid.will_retry is False
        assert invalid.dead_lettered is False

        reset = results["connection reset by peer"]
        assert reset.failure_class is FailureClass.POISON
        assert reset.will_retry is False
        assert reset.dead_lettered is True
        assert reset.attempts == 3

    def test_is_deterministic(self):
        assert retry_classes() == retry_classes()
