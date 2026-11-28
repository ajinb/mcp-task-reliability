"""E5 live server: the policy layer behind a real MCP Tasks wire endpoint.

A deliberately small JSON-RPC-over-HTTP server that speaks the subset of the 2026-07-28
Tasks extension a requester needs: ``tools/call`` returning a flat ``CreateTaskResult``
(``resultType: "task"``), ``tasks/get`` returning a ``DetailedTask`` (``resultType:
"complete"``), and ``tasks/cancel``. It is not an MCP SDK server and does not claim to be
one; it exists so the official ``@modelcontextprotocol/ext-tasks`` requester can be pointed
at a real process, over a real socket, that the harness can kill.

Work is modelled as wall-clock duration: a ``long_job`` with ``durationMs`` completes when a
worker thread observes that its due time has passed. On a restart against a durable store,
``TaskManager.recover()`` triages in-flight work and resumable tasks are re-executed from
the start (the work, not the handle, was lost with the process).

Stdlib only, plus the library under test. Python 3.11+.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mcp_task_reliability.manager import TaskManager  # noqa: E402
from mcp_task_reliability.policy import RetentionPolicy  # noqa: E402
from mcp_task_reliability.store import InMemoryTaskStore, SqliteTaskStore  # noqa: E402
from mcp_task_reliability.types import TaskNotFound, TaskRecord, TaskState  # noqa: E402

TASKS_EXT = "io.modelcontextprotocol/tasks"
CLIENT_CAPS = "io.modelcontextprotocol/clientCapabilities"
INVALID_PARAMS = -32602
METHOD_NOT_FOUND = -32601
MISSING_CLIENT_CAPABILITY = -32021


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="milliseconds")


class LiveServer:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        store = SqliteTaskStore(args.db) if args.store == "sqlite" else InMemoryTaskStore()
        orphan = args.orphan_after_s if args.orphan_after_s > 0 else 10**9
        self.manager = TaskManager(
            store,
            retention=RetentionPolicy(
                retention_s=args.retention_s,
                orphan_after_s=orphan,
                max_execution_s=args.max_execution_s,
            ),
        )
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.events: list[dict[str, Any]] = []
        self.started_at = time.time()
        with self.lock:
            report = self.manager.recover()
            for record in self.manager.resumable():
                # Re-execute from the start: the process that was doing the work is gone.
                record.metadata = {**record.metadata, "due_at": time.time() + record.metadata["duration_s"],
                                   "reexecuted": record.metadata.get("reexecuted", 0) + 1}
                self.manager.store.put(record)
            self.recovery = {"resumable": report.resumable, "timed_out": report.timed_out,
                             "poisoned": report.poisoned}

    # ---- wire shapes ---------------------------------------------------------------------

    def hint_ms(self, duration_s: float) -> int | None:
        h = self.args.hint
        if h == "none":
            return None
        if h == "auto":
            # Duration-aware: about twenty polls over the expected run, clamped.
            return int(min(30_000, max(250, duration_s * 1000 / 20)))
        return int(h)

    def ttl_ms(self, record: TaskRecord) -> int | None:
        return None if self.args.ttl_ms <= 0 else int(self.args.ttl_ms)

    def task_obj(self, record: TaskRecord) -> dict[str, Any]:
        obj: dict[str, Any] = {
            "taskId": record.task_id,
            "status": record.state.value,
            "createdAt": iso(record.created_at),
            "lastUpdatedAt": iso(record.updated_at),
            "ttlMs": self.ttl_ms(record),
        }
        hint = self.hint_ms(record.metadata.get("duration_s", 0.0))
        if hint is not None:
            obj["pollIntervalMs"] = hint
        if record.error and record.state in (TaskState.CANCELLED, TaskState.FAILED):
            obj["statusMessage"] = record.error
        return obj

    def detailed(self, record: TaskRecord) -> dict[str, Any]:
        obj = self.task_obj(record)
        if record.state is TaskState.COMPLETED:
            obj["result"] = {"resultType": "complete", "content": [{"type": "text", "text": json.dumps(record.result)}]}
        elif record.state is TaskState.FAILED:
            obj["error"] = {"code": -32603, "message": record.error or "failed"}
        obj["resultType"] = "complete"
        return obj

    # ---- methods ---------------------------------------------------------------------------

    def handle(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if method == "server/discover":
            return {"resultType": "complete", "capabilities": {"extensions": {TASKS_EXT: {}}},
                    "serverInfo": {"name": "mcp-task-reliability-live", "version": "0.2.0"}}
        if method == "tools/list":
            return {"resultType": "complete", "tools": [{
                "name": "long_job",
                "description": "Wall-clock job of durationMs; returns a task handle.",
                "inputSchema": {"type": "object", "properties": {
                    "durationMs": {"type": "number"}, "label": {"type": "string"},
                    "idempotencyKey": {"type": "string"}}},
                "execution": {"taskSupport": "optional"},
            }]}
        if method == "tools/call":
            return self.tools_call(params)
        if method == "tasks/get":
            return self.tasks_get(params["taskId"])
        if method == "tasks/cancel":
            with self.lock:
                self.manager.cancel(params["taskId"])
            self.log("cancel", params["taskId"])
            return {"resultType": "complete"}
        raise RpcError(METHOD_NOT_FOUND, f"Method not found: {method}")

    def tools_call(self, params: dict[str, Any]) -> dict[str, Any]:
        caps = (params.get("_meta") or {}).get(CLIENT_CAPS) or {}
        if TASKS_EXT not in (caps.get("extensions") or {}):
            raise RpcError(MISSING_CLIENT_CAPABILITY, "Missing required client capability: tasks")
        if params.get("name") != "long_job":
            raise RpcError(INVALID_PARAMS, f"Unknown tool: {params.get('name')}")
        args = params.get("arguments") or {}
        duration_s = float(args.get("durationMs", 1000)) / 1000.0
        with self.lock:
            handle = self.manager.create(
                "long_job", idempotency_key=args.get("idempotencyKey"),
                metadata={"duration_s": duration_s, "due_at": time.time() + duration_s,
                          "label": args.get("label")},
            )
            record = self.manager.store.get(handle.task_id)
        assert record is not None
        self.log("create", record.task_id, label=args.get("label"))
        out = self.task_obj(record)
        out["resultType"] = "task"
        return out

    def tasks_get(self, task_id: str) -> dict[str, Any]:
        with self.lock:
            try:
                record = self.manager.get(task_id)
            except TaskNotFound:
                self.log("get_not_found", task_id)
                raise RpcError(INVALID_PARAMS, "Failed to retrieve task: Task not found") from None
        self.log("get", task_id, status=record.state.value)
        return self.detailed(record)

    # ---- background loops ------------------------------------------------------------------

    def worker(self) -> None:
        while not self.stop.wait(0.02):
            now = time.time()
            with self.lock:
                for record in self.manager.store.live():
                    if record.state is TaskState.WORKING and record.metadata.get("due_at", now + 1) <= now:
                        self.manager.complete(record.task_id, {"label": record.metadata.get("label"),
                                                               "attempts": record.attempts})
                        self.log("complete", record.task_id)
                    elif (self.args.ttl_enforce and self.args.ttl_ms > 0 and record.state is TaskState.WORKING
                          and now >= record.created_at + self.args.ttl_ms / 1000.0):
                        # The spec permits this: servers MAY mark a task failed after createdAt + ttlMs.
                        failed = record.with_state(TaskState.FAILED, now)
                        failed.error = "ttl elapsed while working"
                        failed.expires_at = now + self.args.retention_s
                        self.manager.store.put(failed)
                        self.log("ttl_failed", record.task_id)

    def reaper(self) -> None:
        while not self.stop.wait(self.args.reap_every_s):
            with self.lock:
                before = {r.task_id: r.state for r in self.manager.store.live()}
                report = self.manager.reap()
                if report.orphaned:
                    for task_id, state in before.items():
                        rec = self.manager.store.get(task_id)
                        if rec is not None and rec.state is TaskState.CANCELLED and state is not TaskState.CANCELLED:
                            self.log("orphaned", task_id)

    def log(self, kind: str, task_id: str, **extra: Any) -> None:
        self.events.append({"t": time.time(), "kind": kind, "task": task_id, **extra})

    def stats(self) -> dict[str, Any]:
        with self.lock:
            tasks = [{"taskId": r.task_id, "state": r.state.value, "polls": r.poll_count,
                      "label": r.metadata.get("label"), "attempts": r.attempts,
                      "reexecuted": r.metadata.get("reexecuted", 0)}
                     for r in self.manager.store.iter_all()]
        return {"started_at": self.started_at, "recovery": self.recovery, "tasks": tasks,
                "events": self.events}


class RpcError(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def make_handler(server: LiveServer) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_: Any) -> None:  # quiet
            return

        def _send(self, code: int, body: dict[str, Any]) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/stats":
                self._send(200, server.stats())
            elif self.path == "/healthz":
                self._send(200, {"ok": True})
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            req = json.loads(self.rfile.read(length) or b"{}")
            rid = req.get("id")
            try:
                result = server.handle(req.get("method", ""), req.get("params") or {})
                self._send(200, {"jsonrpc": "2.0", "id": rid, "result": result})
            except RpcError as e:
                self._send(200, {"jsonrpc": "2.0", "id": rid, "error": {"code": e.code, "message": e.message}})
            except Exception as e:  # surfaced, not swallowed
                self._send(200, {"jsonrpc": "2.0", "id": rid, "error": {"code": -32603, "message": repr(e)}})

    return Handler


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--store", choices=["memory", "sqlite"], default="sqlite")
    p.add_argument("--db", default="live-tasks.db")
    p.add_argument("--hint", default="1000", help="pollIntervalMs: an integer, 'auto', or 'none'")
    p.add_argument("--ttl-ms", type=float, default=3_600_000)
    p.add_argument("--ttl-enforce", action="store_true", help="fail working tasks after createdAt+ttlMs")
    p.add_argument("--orphan-after-s", type=float, default=900.0, help="0 disables orphan reaping")
    p.add_argument("--reap-every-s", type=float, default=0.25)
    p.add_argument("--retention-s", type=float, default=3600.0)
    p.add_argument("--max-execution-s", type=float, default=1800.0)
    args = p.parse_args(argv)

    live = LiveServer(args)

    class Server(ThreadingHTTPServer):
        # The stdlib default backlog of 5 resets concurrent connects; E5b opens 30 at once.
        request_queue_size = 256
        allow_reuse_address = True

    httpd = Server(("127.0.0.1", args.port), make_handler(live))
    httpd.daemon_threads = True
    for target in (live.worker, live.reaper):
        threading.Thread(target=target, daemon=True).start()

    def shutdown(*_: Any) -> None:
        live.stop.set()
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    print(json.dumps({"ready": True, "port": args.port, "recovery": live.recovery}), flush=True)
    httpd.serve_forever(poll_interval=0.05)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
