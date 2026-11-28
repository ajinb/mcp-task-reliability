"""Print a compact summary of an E5 results file."""
import json
import sys

with open(sys.argv[1]) as f:
    d = json.load(f)
for r in d["e5a"]:
    print("E5a", {k: r[k] for k in ["store", "downtimeMs", "recovery", "shortSettledBeforeCrash",
          "inFlightSurvivedWithoutResume", "inFlightRejectedErrors", "resolvedAfterRestart",
          "lostAfterRestart", "inFlightSettledAs", "inFlightMessages", "inFlightWithTerminalSnapshot", "inFlightCauses", "resumedCompletedInFlight", "reexecuted", "resumeErrors"]})
for r in d["e5b"]:
    print(f"E5b hint={r['hint']}", [(c["durationMs"], c["pollsMean"], c["pollsMin"], c["pollsMax"], c["staleMsP50"]) for c in r["cells"]])
for r in d["e5c"]["handoff"]:
    print("E5c-handoff", r)
for r in d["e5c"]["hint"]:
    print("E5c-hint", r)
for r in d["e5d"]:
    print("E5d", r)
