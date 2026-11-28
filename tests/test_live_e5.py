"""Pins the paper's §5.5 live study (E5) to its recorded results.

E5 runs the official ``@modelcontextprotocol/ext-tasks`` requester (0.2.3) over real HTTP
against ``live/server.py`` and kills the server process. It takes minutes of wall clock, so
CI does not re-run it; these tests hold the recorded file ``live/results/e5.json`` to the
claims the paper makes from it. Re-record with::

    cd live/client && npm ci && PYTHON=python3.11 node e5.mjs > ../results/e5.json

Assertions are on structure (who survives, who is cancelled, poll counts within the bounds a
fixed-interval requester implies), never on millisecond timings, which move run to run.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

RESULTS = Path(__file__).resolve().parents[1] / "live" / "results" / "e5.json"
pytestmark = pytest.mark.skipif(not RESULTS.exists(), reason="E5 live results not recorded")


@pytest.fixture(scope="module")
def e5() -> dict:
    return json.loads(RESULTS.read_text())


def test_requester_is_the_pinned_official_package(e5: dict) -> None:
    assert e5["package"] == "@modelcontextprotocol/ext-tasks@0.2.3"


# ---- E5a: restart ------------------------------------------------------------------------

def test_e5a_memory_store_loses_every_handle_including_completed(e5: dict) -> None:
    cells = [c for c in e5["e5a"] if c["store"] == "memory"]
    assert cells
    for c in cells:
        assert c["shortSettledBeforeCrash"] == 12
        assert c["resolvedAfterRestart"] == 0
        assert c["lostAfterRestart"] == 20
        assert any("Task not found" in err for err in c["resumeErrors"])


def test_e5a_durable_store_resolves_every_handle_and_reexecutes_in_flight(e5: dict) -> None:
    cells = [c for c in e5["e5a"] if c["store"] == "sqlite"]
    assert cells
    for c in cells:
        assert c["recovery"]["resumable"] == 8
        assert c["resolvedAfterRestart"] == 20
        assert c["lostAfterRestart"] == 0
        assert c["resumedCompletedInFlight"] == 8
        assert c["reexecuted"] == 8


def test_e5a_requester_alone_never_survives_the_restart(e5: dict) -> None:
    # The package retries a retryable transport error once, immediately; an execution
    # that was polling when the process died does not complete on its own, whatever the
    # store. Recovery is the application's resumeTask(reference), measured above.
    for c in e5["e5a"]:
        assert c["inFlightSurvivedWithoutResume"] == 0


def test_e5a_transport_loss_is_reported_as_task_failure(e5: dict) -> None:
    # Every in-flight execution settles as status "failed" — a TaskFailedError wrapping the
    # transport error, with no server terminal snapshot attached — even under SQLite, where
    # the same eight tasks then complete on the server once resumed.
    for c in e5["e5a"]:
        assert c["inFlightSettledAs"] == {"failed": 8}
        assert c["inFlightWithTerminalSnapshot"] == 0
        assert c["inFlightCauses"] == ["DispatchError"]
    sqlite = [c for c in e5["e5a"] if c["store"] == "sqlite"]
    assert all(c["resumedCompletedInFlight"] == 8 for c in sqlite)


# ---- E5b: the hint is the whole policy -----------------------------------------------------

def _cell(e5: dict, hint: str, duration_ms: int) -> dict:
    run = next(r for r in e5["e5b"] if r["hint"] == hint)
    return next(c for c in run["cells"] if c["durationMs"] == duration_ms)


@pytest.mark.parametrize("hint", ["250", "1000", "5000"])
def test_e5b_fixed_hint_sets_poll_count(e5: dict, hint: str) -> None:
    h = int(hint)
    run = next(r for r in e5["e5b"] if r["hint"] == hint)
    for c in run["cells"]:
        assert c["completed"] == c["reps"]
        upper = math.ceil(c["durationMs"] / h) + 1
        # Each poll waits the hint *plus* a round trip, so the count sits at or just below d/h.
        lower = max(1, math.floor(0.8 * c["durationMs"] / h))
        assert lower <= c["pollsMin"] <= c["pollsMax"] <= upper, (hint, c)


def test_e5b_no_hint_falls_back_to_one_second(e5: dict) -> None:
    run = next(r for r in e5["e5b"] if r["hint"] == "none")
    for c in run["cells"]:
        assert c["pollsMax"] <= math.ceil(c["durationMs"] / 1000) + 1


def test_e5b_zero_hint_hits_the_ten_millisecond_floor(e5: dict) -> None:
    # A hint of 0 is floored to 10 ms by the package: tens of polls per second per task.
    c = _cell(e5, "0", 5000)
    assert c["pollsMean"] > 5000 / 100  # more than one poll per 100 ms


def test_e5b_duration_aware_hint_bounds_long_task_polls(e5: dict) -> None:
    longest = max(c["durationMs"] for c in next(r for r in e5["e5b"] if r["hint"] == "auto")["cells"])
    auto = _cell(e5, "auto", longest)
    fixed = _cell(e5, "250", longest)
    assert auto["pollsMax"] <= 22
    assert fixed["pollsMean"] >= 4 * auto["pollsMean"]


# ---- E5c: the orphan window is a lease the client is never told about ----------------------

def test_e5c_handoff_gap_beyond_orphan_window_cancels_live_work(e5: dict) -> None:
    for c in e5["e5c"]["handoff"]:
        expect_cancel = c["gapMs"] > c["orphanAfterS"] * 1000
        assert (c["settled"] == "cancelled") is expect_cancel, c
        if expect_cancel:
            assert "orphaned" in c["message"]


def test_e5c_hint_beyond_orphan_window_cancels_an_obedient_client(e5: dict) -> None:
    for c in e5["e5c"]["hint"]:
        expect_cancel = c["hintMs"] > c["orphanAfterS"] * 1000
        assert (c["settled"] == "cancelled") is expect_cancel, c


# ---- E5d: ttlMs is measured from creation --------------------------------------------------

def test_e5d_ttl_from_creation_fails_work_that_outlives_it(e5: dict) -> None:
    for c in e5["e5d"]:
        expect_fail = c["durationMs"] > c["ttlMs"]
        assert (c["settled"] == "failed") is expect_fail, c
