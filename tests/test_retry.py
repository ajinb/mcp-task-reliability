import pytest

from mcp_task_reliability.policy import RetryPolicy
from mcp_task_reliability.retry import RetryClassifier
from mcp_task_reliability.types import FailureClass


@pytest.fixture
def classifier():
    return RetryClassifier(policy=RetryPolicy(max_attempts=3, jitter=False))


@pytest.mark.parametrize("error", [
    "upstream timeout after 30s", "connection reset by peer", "service unavailable",
    "rate limit exceeded", "deadline exceeded", "server is overloaded",
])
def test_transient_markers(classifier, error):
    assert classifier.classify(error, attempts=1) is FailureClass.TRANSIENT


@pytest.mark.parametrize("error", [
    "invalid arguments", "schema validation failed", "unknown tool: foo",
    "unauthorized", "permission denied", "parse error at line 3",
])
def test_permanent_markers(classifier, error):
    assert classifier.classify(error, attempts=1) is FailureClass.PERMANENT


@pytest.mark.parametrize("code,expected", [
    (429, FailureClass.TRANSIENT), (503, FailureClass.TRANSIENT),
    (400, FailureClass.PERMANENT), (404, FailureClass.PERMANENT),
])
def test_codes_win_over_text(classifier, code, expected):
    assert classifier.classify("something happened", attempts=1, code=code) is expected


def test_permanent_markers_beat_transient_ones(classifier):
    """'invalid connection string' is a config bug; the word 'connection' must not rescue it."""
    assert classifier.classify("invalid connection string", attempts=1) is FailureClass.PERMANENT


def test_unrecognised_errors_are_permanent_by_default(classifier):
    assert classifier.classify("kerfuffle in the doohickey", attempts=1) is FailureClass.PERMANENT


def test_default_class_is_configurable():
    optimistic = RetryClassifier(default_class=FailureClass.TRANSIENT)
    assert optimistic.classify("kerfuffle", attempts=1) is FailureClass.TRANSIENT


def test_transient_becomes_poison_at_the_attempt_ceiling(classifier):
    assert classifier.classify("timeout", attempts=2) is FailureClass.TRANSIENT
    assert classifier.classify("timeout", attempts=3) is FailureClass.POISON


def test_permanent_stays_permanent_at_the_ceiling(classifier):
    """A permanent failure is not poison — it never had a retry to exhaust."""
    assert classifier.classify("invalid arguments", attempts=9) is FailureClass.PERMANENT


def test_should_retry_only_for_transient_within_budget(classifier):
    assert classifier.should_retry(FailureClass.TRANSIENT, attempts=1) is True
    assert classifier.should_retry(FailureClass.TRANSIENT, attempts=3) is False
    assert classifier.should_retry(FailureClass.PERMANENT, attempts=1) is False
    assert classifier.should_retry(FailureClass.POISON, attempts=1) is False


def test_backoff_is_exponential_and_capped(classifier):
    delays = [classifier.delay_for(n) for n in (1, 2, 3, 4, 5, 6, 7, 8)]
    assert delays[:4] == [0.5, 1.0, 2.0, 4.0]
    assert max(delays) <= classifier.policy.max_delay_s


def test_full_jitter_stays_within_the_cap_and_is_seeded():
    a = RetryClassifier(policy=RetryPolicy(jitter=True), seed=7)
    b = RetryClassifier(policy=RetryPolicy(jitter=True), seed=7)
    samples = [a.delay_for(3) for _ in range(50)]
    assert all(0.0 <= s <= 2.0 for s in samples)
    assert len(set(samples)) > 1
    assert [b.delay_for(3) for _ in range(50)] == samples


def test_attempt_is_one_based(classifier):
    with pytest.raises(ValueError):
        classifier.delay_for(0)


def test_invalid_policies_are_rejected():
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError):
        RetryPolicy(base_delay_s=0)
    with pytest.raises(ValueError):
        RetryPolicy(multiplier=0.5)
