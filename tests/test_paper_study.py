"""Pins the paper's E1-E4 results.

The paper cites specific tables. These tests fail if those tables stop being true, which is
the only thing that keeps a published number and its artifact in the same universe.
"""

from __future__ import annotations

import pytest

from mcp_task_reliability.sim import (
    classification_cost,
    crash_recovery,
    orphan_coupling,
    poll_frontier,
    single_poll_run,
)
from mcp_task_reliability.types import FailureClass

# ---- E1 --------------------------------------------------------------------------------

def test_e1_memory_store_loses_every_handle():
    r = crash_recovery(durable=False)
    assert r.resolved_after_restart == 0
    assert r.lost == r.issued == 20
    assert r.survival_rate == 0.0


def test_e1_sqlite_store_honours_every_handle():
    r = crash_recovery(durable=True)
    assert r.resolved_after_restart == 20
    assert r.lost == 0
    assert r.survival_rate == 1.0


def test_e1_in_flight_work_is_triaged_not_dropped():
    """The eight tasks still working at the crash come back as resumable, not lost."""
    r = crash_recovery(durable=True)
    assert r.in_flight == 8
    assert r.resumable == 8


# ---- E2 --------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def frontier():
    return {d: {p.label: p for p in poll_frontier(d)} for d in (60.0, 300.0, 1800.0)}


def test_e2_backoff_cuts_poll_traffic_by_orders_of_magnitude(frontier):
    points = frontier[300.0]
    assert points["fixed 250ms"].polls == 1200
    assert points["adaptive max 10s"].polls == pytest.approx(47, abs=1)
    reduction = 1 - points["adaptive max 10s"].polls / points["fixed 250ms"].polls
    assert reduction > 0.95


def test_e2_staleness_is_monotone_in_the_backoff_ceiling(frontier):
    for duration in (60.0, 300.0, 1800.0):
        points = frontier[duration]
        assert (points["adaptive max 10s"].staleness_p50
                < points["adaptive max 30s"].staleness_p50
                < points["adaptive max 60s"].staleness_p50)


def test_e2_backoff_does_not_pay_on_short_tasks(frontier):
    """The finding that complicates the obvious advice.

    On a ~60s task, adaptive-max-30s saves under 5% of the polls that fixed-5s issues and
    pays more than 5x the staleness for it. Backoff buys a short task almost nothing.
    """
    fixed = frontier[60.0]["fixed 5000ms"]
    adaptive = frontier[60.0]["adaptive max 30s"]

    poll_saving = 1 - adaptive.polls / fixed.polls
    assert poll_saving < 0.05
    assert adaptive.staleness_p50 / fixed.staleness_p50 > 5
    assert adaptive.staleness_ratio > 0.20      # a fifth of the task spent not knowing


def test_e2_backoff_pays_heavily_on_long_tasks(frontier):
    """Same two policies, a 30-minute task, and the trade inverts."""
    fixed = frontier[1800.0]["fixed 5000ms"]
    adaptive = frontier[1800.0]["adaptive max 60s"]

    assert fixed.polls == pytest.approx(360, abs=1)
    assert adaptive.polls == pytest.approx(50, abs=1)
    assert fixed.polls / adaptive.polls > 7
    assert adaptive.staleness_ratio < 0.02      # and it costs ~1% of the task to do it


def test_e2_the_regime_is_set_by_duration_not_by_policy(frontier):
    """Why the server hint is load-bearing: one fixed policy is wasteful at one duration
    and correct at another, and the client cannot tell which it is in."""
    short = frontier[60.0]["adaptive max 60s"].staleness_ratio
    long = frontier[1800.0]["adaptive max 60s"].staleness_ratio
    assert short > 0.35
    assert long < 0.02
    assert short / long > 25


def test_e2_is_deterministic():
    assert single_poll_run(300.0, fixed_ms=250) == single_poll_run(300.0, fixed_ms=250)
    assert single_poll_run(300.0, seed=3) == single_poll_run(300.0, seed=3)


# ---- E3 --------------------------------------------------------------------------------

@pytest.mark.parametrize("fraction", [0.0, 0.25, 0.5, 0.75, 1.0])
def test_e3_permanent_default_has_flat_attempt_cost(fraction):
    """Defaulting to permanent never retries what it does not recognise, so its attempt
    budget does not move with the error mix."""
    c = classification_cost(fraction, FailureClass.PERMANENT)
    assert c.attempts == 120
    assert c.wasted == 0
    assert c.dead_lettered == 0


def test_e3_transient_default_is_pure_cost_when_nothing_is_retryable():
    """The worst case for the library convention: 2x the attempts and 60 dead-letters
    bought for exactly zero additional recovered work."""
    permanent = classification_cost(0.0, FailureClass.PERMANENT)
    transient = classification_cost(0.0, FailureClass.TRANSIENT)

    assert transient.attempts == 240 == 2 * permanent.attempts
    assert transient.wasted == 120
    assert transient.dead_lettered == 60
    assert transient.recovered == permanent.recovered == 20


def test_e3_transient_default_wins_only_when_unknowns_are_retryable():
    permanent = classification_cost(1.0, FailureClass.PERMANENT)
    transient = classification_cost(1.0, FailureClass.TRANSIENT)

    assert transient.recovered == 80
    assert permanent.recovered == 20
    assert permanent.lost == 60
    # 60 more tasks saved for 60 more attempts: one attempt per unit of recovered work.
    assert transient.attempts - permanent.attempts == 60


def test_e3_lost_work_and_wasted_effort_trade_off_linearly():
    for fraction in (0.0, 0.25, 0.5, 0.75, 1.0):
        permanent = classification_cost(fraction, FailureClass.PERMANENT)
        transient = classification_cost(fraction, FailureClass.TRANSIENT)
        assert permanent.lost == int(60 * fraction)
        assert transient.lost == 0
        assert transient.wasted == int(60 * (1 - fraction)) * 2


def test_e3_rejects_a_fraction_outside_the_unit_interval():
    with pytest.raises(ValueError):
        classification_cost(1.5, FailureClass.PERMANENT)


# ---- E4 --------------------------------------------------------------------------------

@pytest.mark.parametrize("max_backoff_ms", [10_000, 60_000, 300_000, 900_000])
def test_e4_default_orphan_window_is_safe_for_every_sane_backoff(max_backoff_ms):
    """With the shipped 900s orphan window, no reasonable client backoff is orphaned."""
    assert not orphan_coupling(max_backoff_ms, 900.0).orphaned


@pytest.mark.parametrize("max_backoff_ms", [10_000, 60_000])
def test_e4_short_backoff_survives_a_tightened_window(max_backoff_ms):
    assert not orphan_coupling(max_backoff_ms, 120.0).orphaned


@pytest.mark.parametrize("max_backoff_ms", [300_000, 900_000])
def test_e4_backoff_beyond_the_orphan_window_cancels_live_work(max_backoff_ms):
    """The hazard: the server cancels a task the client is still waiting on, because the
    client took the backoff advice and the server read silence as abandonment."""
    result = orphan_coupling(max_backoff_ms, 120.0)
    assert result.orphaned
    assert result.cancelled_at_s == pytest.approx(780.0, abs=60.0)


def test_e4_safe_backoff_rule_holds_across_the_sweep():
    """The rule the table yields: orphaned exactly when max backoff exceeds the window."""
    for max_ms in (10_000, 60_000, 300_000, 900_000):
        for window_s in (900.0, 120.0):
            result = orphan_coupling(max_ms, window_s)
            assert result.orphaned == (max_ms / 1000.0 > window_s)
