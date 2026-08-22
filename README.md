# mcp-task-reliability

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![MCP spec 2026-07-28](https://img.shields.io/badge/MCP%20spec-2026--07--28-6f42c1.svg)](https://blog.modelcontextprotocol.io/posts/2026-07-28/)

> The reliability layer the MCP **Tasks** extension deliberately doesn't specify — a durable
> task store, failure classification, retention and orphan reaping, and adaptive poll backoff.
> Dependency-free, deterministic, and runnable offline.

The [`2026-07-28` specification](https://blog.modelcontextprotocol.io/posts/2026-07-28/) made
the MCP core **stateless**: the handshake and `Mcp-Session-Id` are gone, `tools/call` returns a
durable handle, and the client drives the lifecycle through `tasks/get`, `tasks/update` and
`tasks/cancel`. That is a good trade — servers can now run on serverless and edge infrastructure.

It also moved a pile of state out of the connection and into your problem. The
[2026 MCP roadmap](https://blog.modelcontextprotocol.io/posts/2026-mcp-roadmap/) still lists
**retry semantics on transient task failure** and **result retention policy** as open gaps.
This repo is one opinionated set of answers, small enough to read in a sitting.

## What this is, and is not

**Is not:** an MCP server, an SDK, or a reimplementation of the extension. The protocol
reference is [`modelcontextprotocol/ext-tasks`](https://github.com/modelcontextprotocol/ext-tasks)
and you should use it for the wire format.

**Is:** the policy layer you put *behind* one. It answers the four operational questions the
spec leaves to you — is this call the same task as last time, does this failure deserve
another attempt, when may this result be deleted, and what happens to work that was in flight
when the process died.

Companion to [`mcp-chaos`](https://github.com/ajinb/mcp-chaos) (which breaks the tool-call
plane on purpose) and [`mcp-gateway`](https://github.com/ajinb/mcp-gateway) (which polices it).

## Install

```bash
pip install -e ".[dev]"
```

Requires Python 3.11+. **Zero runtime dependencies** — the durable store is stdlib `sqlite3`.

## The 30-second demo (offline, no API key, no server)

```bash
mcp-task-reliability demo
```

```
crash recovery — 20 handles issued, 8 in flight when the process dies

store       resolved    lost   resumable   survival
memory             0      20           0        0%
sqlite            20       0           8      100%

poll traffic — one 300s task, per client

client                 polls
fixed 250ms             1200
adaptive backoff          48

96% fewer requests for the same answer.

failure classification — three errors that look alike in a log line

error                                         class        attempts  dead-letter
upstream timeout after 30s                    transient           2           no
invalid arguments: 'limit' must be an int...  permanent           1           no
connection reset by peer                      poison              3          yes
```

Three scenarios, one seed, same numbers on every machine. `--json` for machine-readable output.

---

## The three things that break

### 1. A handle is only as durable as your store

The client is holding a handle the server told it to come back to. After a deploy, a crash, or
an autoscaler decision, does that handle still answer?

```bash
mcp-task-reliability crash
python examples/crash_recovery_demo.py
```

The in-memory store loses **every handle it ever issued** — including completed tasks whose
results the client had not picked up yet. Same protocol, same client, different promise.
`SqliteTaskStore` runs in WAL mode and commits on every write, because a handle the client
believes in but the server has not durably written is exactly the handle that goes missing
across a deploy.

`TaskManager.recover()` then triages what was in flight: past its deadline it is failed, out of
attempts it is poisoned, otherwise it is handed back through `resumable()` for you to
re-enqueue. Nothing silently drops a handle a client is still holding.

### 2. Stateless made every client a poller

There is no open connection any more, so everyone polls. Fixed-interval polling is the obvious
implementation and it turns one long task into sustained load that says "not yet" a thousand times:

```
 task length   fixed 250ms  adaptive  reduction
        30s           120        11       91%
       300s          1200        48       96%
      1800s          7200       248       97%
```

Note the direction: **the longer the task, the worse fixed polling gets** — exactly backwards,
since long tasks are the reason the extension exists. `PollScheduler` backs off multiplicatively
with jitter, and treats the server's `poll_after_ms` as a *floor*: the server may tell a client
to slow down, but may not talk a backed-off client into speeding up.

### 3. "Failed" is three different words

Retrying a permanent failure burns budget and amplifies load. Not retrying a transient one
throws away work that would have succeeded. `RetryClassifier` splits them, and moves anything
that exhausts its attempt budget to a dead-letter queue a human can read.

The default for an **unrecognised** error is `PERMANENT`, which is the opposite of what most
retry libraries do. The reasoning is operational: an error you cannot classify is an error you
do not understand, and retrying what you do not understand is how one bad tool call becomes a
thundering herd. Opt in to retrying a new error by naming it. Set
`default_class=FailureClass.TRANSIENT` if you disagree — but measure the blast radius with
[`mcp-chaos`](https://github.com/ajinb/mcp-chaos) first.

---

## Using it

```python
from mcp_task_reliability import RetentionPolicy, RetryPolicy, SqliteTaskStore, TaskManager

manager = TaskManager(
    SqliteTaskStore("tasks.db"),
    retention=RetentionPolicy(retention_s=3600, orphan_after_s=900, max_execution_s=1800),
    retry=RetryPolicy(max_attempts=3),
)

# tools/call -> a durable handle. The idempotency key makes a client retry free
# instead of doubling your bill.
handle = manager.create("generate_report", idempotency_key="order-99")

# tasks/get -> also records the poll, which is what keeps the orphan reaper honest.
record = manager.get(handle.task_id)

# Your worker reports back.
outcome = manager.fail(handle.task_id, "upstream timeout after 30s")
if outcome.will_retry:
    schedule_retry(after=outcome.retry_after_s)
else:
    manager.complete(handle.task_id, {"rows": 128})

# On a timer.
manager.reap()

# On startup, after a restart.
report = manager.recover()
for task in manager.resumable():
    reenqueue(task)
```

## Policy reference

| Knob | Default | What it decides |
|---|---|---|
| `RetentionPolicy.retention_s` | 3600s | How long a terminal task's result stays readable |
| `RetentionPolicy.orphan_after_s` | 900s | How long work may go unpolled before it is presumed abandoned |
| `RetentionPolicy.max_execution_s` | 1800s | Wall-clock ceiling on a working task |
| `RetryPolicy.max_attempts` | 3 | Total attempts, not retries. Exhausting it means poison |
| `RetryPolicy.base_delay_s` / `max_delay_s` | 0.5s / 30s | Capped exponential backoff, full jitter |
| `PollPolicy.initial_ms` / `max_ms` | 250ms / 10s | Client backoff floor and ceiling |
| `RetryClassifier.default_class` | `PERMANENT` | What an unrecognised error is treated as |

The reaper runs three sweeps in a fixed order — timeouts, then orphans, then expiry — so that a
task which times out during a sweep earns its retention window instead of being deleted by the
same pass that failed it.

## Design notes

- **Nothing calls `time.time()` or `time.sleep()`.** Every timing decision takes an injectable
  clock, which is why 117 tests and both demos run in well under a second and produce identical
  output everywhere. A test that sleeps will be rejected.
- **Stores are interchangeable.** `InMemoryTaskStore` and `SqliteTaskStore` pass the same
  contract suite (`tests/test_store.py::TestStoreContract`), down to returning copies rather
  than live references — a backend that behaves differently under mutation would make the
  contract meaningless.
- **Terminal is a one-way door**, enforced by the transition table in `types.py` rather than by
  convention.

## Status

**v0.1.** The policy layer is complete and tested; the offline scenarios reproduce every number
in this README. Not yet wired to a live MCP server — a `mcp` SDK adapter for the real
`tasks/*` wire calls is the v0.2 milestone. The spec is three weeks old at the time of writing;
re-check it before depending on this in production.

```bash
pytest -q                              # 117 tests
ruff check src tests examples
mcp-task-reliability demo              # the table above
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The core stays dependency-free and clock-injected.

## License

Apache 2.0. More at [cloudandsre.com](https://cloudandsre.com).
