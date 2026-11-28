"""Render the paper's E5 tables (Tables 8-11) as markdown from a recorded results file.

    python live/tables.py live/results/e5.json

The paper's §4.5 tables are pasted from this output, so the numbers in the manuscript are
the numbers in the recorded file.
"""

from __future__ import annotations

import json
import sys


def e5a(d: dict) -> list[str]:
    out = ["*Table 8 — restart through the official requester (20 handles: 12 completed before the crash, 8 in flight; `SIGKILL`, restart on the same port and store).*", "",
           "| store | downtime | in-flight executions settle as | with server snapshot | handles resolved on `resumeTask` | lost (`Task not found`) | in-flight re-executed and completed |",
           "|---|---|---|---|---|---|---|"]
    for c in d["e5a"]:
        settled = ", ".join(f"{k} {v}" for k, v in c["inFlightSettledAs"].items())
        out.append(f"| {'SQLite' if c['store'] == 'sqlite' else 'in-memory'} | {c['downtimeMs']} ms | {settled} | "
                   f"{c['inFlightWithTerminalSnapshot']} of 8 | {c['resolvedAfterRestart']} of 20 | {c['lostAfterRestart']} | "
                   f"{c['resumedCompletedInFlight']} of 8 |")
    return out


def e5b(d: dict) -> list[str]:
    durations = [c["durationMs"] for c in d["e5b"][0]["cells"]]
    reps = d["e5b"][0]["cells"][0]["reps"]
    label = {"none": "no hint (package default)", "0": "0 ms (package floor)", "auto": "duration-aware"}
    head = "| server `pollIntervalMs` | " + " | ".join(f"{ms // 1000} s task: polls | {ms // 1000} s: stale p50" for ms in durations) + " |"
    out = [f"*Table 9 — polls per task issued by the official requester, and median detection lag, by server hint ({reps} concurrent tasks per cell; durations time-scaled).*", "",
           head, "|---|" + "---|---|" * len(durations)]
    for r in d["e5b"]:
        name = label.get(r["hint"], f"{r['hint']} ms")
        cells = []
        for c in r["cells"]:
            polls = f"{c['pollsMean']:.1f}" if c["pollsMin"] != c["pollsMax"] else f"{c['pollsMin']}"
            cells += [polls, f"{c['staleMsP50'] / 1000:.2f} s"]
        out.append(f"| {name} | " + " | ".join(cells) + " |")
    return out


def e5c(d: dict) -> list[str]:
    out = ["*Table 10 — the orphan window against the official requester (8 s task, reaper every 0.1 s).*", "",
           "| mechanism | client poll gap | orphan window 1 s | orphan window 3 s |", "|---|---|---|---|"]
    for kind, key, unit in (("hint", "hintMs", "server hint"), ("handoff", "gapMs", "handoff → resume")):
        rows = d["e5c"][kind]
        for gap in sorted({c[key] for c in rows}):
            cell = {c["orphanAfterS"]: c for c in rows if c[key] == gap}

            def fmt(c: dict) -> str:
                s = "**cancelled**" if c["settled"] == "cancelled" else c["settled"]
                return s + (f" ({c['polls']} poll{'s' if c['polls'] != 1 else ''})" if "polls" in c else "")
            out.append(f"| {unit} | {gap / 1000:g} s | {fmt(cell[1])} | {fmt(cell[3])} |")
    return out


def e5d(d: dict) -> list[str]:
    out = ["*Table 11 — `ttlMs` measured from creation, enforced as the extension permits.*", "",
           "| `ttlMs` | task duration | outcome |", "|---|---|---|"]
    for c in d["e5d"]:
        s = f"**failed** ({c['message']})" if c["settled"] == "failed" else c["settled"]
        out.append(f"| {c['ttlMs'] / 1000:g} s | {c['durationMs'] / 1000:g} s | {s} |")
    return out


if __name__ == "__main__":
    data = json.load(open(sys.argv[1]))
    for block in (e5a(data), e5b(data), e5c(data), e5d(data)):
        print("\n".join(block))
        print()
    print(f"<!-- recorded {data['startedAt']} → {data['finishedAt']}, {data['package']}, node {data['node']} -->")
