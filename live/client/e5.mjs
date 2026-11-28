// E5 live study: the official @modelcontextprotocol/ext-tasks requester (pinned 0.2.3),
// over real HTTP, against live/server.py — a real process the harness starts and kills.
//
// The requester is driven through its public port interface (`withTasks(port)`), which is
// the package's own seam for hosts that are not the TypeScript SDK `Client`. The port does
// nothing but frame JSON-RPC and POST it; every polling, retry, settlement and resume
// decision below is the package's, not ours.
//
//   node e5.mjs [--quick] > ../results/e5.json

import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { DispatchError, withTasks } from "@modelcontextprotocol/ext-tasks/client";

const HERE = dirname(fileURLToPath(import.meta.url));
const SERVER = join(HERE, "..", "server.py");
const PYTHON = process.env.PYTHON ?? "python3";
const QUICK = process.argv.includes("--quick");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let nextPort = 18700 + Math.floor(Math.random() * 500);

// ---- the port: JSON-RPC framing over fetch, nothing else ---------------------------------

class HttpPort {
  constructor(url, endpointId = "live-e5") {
    this.url = url;
    this.endpointId = endpointId;
    this.taskCapabilities = { generation: "v2", capabilities: {} };
    this.invalidated = false;
    this.seq = 0;
    this.dispatched = 0;
    this.transportErrors = 0;
  }
  async dispatch(request, options) {
    this.dispatched += 1;
    const body = JSON.stringify({ jsonrpc: "2.0", id: ++this.seq, ...request });
    let res;
    try {
      res = await fetch(this.url, {
        method: "POST",
        headers: { "content-type": "application/json", ...(options?.context?.headers ?? {}) },
        body,
        signal: options?.signal,
      });
    } catch (err) {
      this.transportErrors += 1;
      // Connection refused / reset: a transport failure the package may retry.
      throw new DispatchError(`transport: ${err?.cause?.code ?? err?.message}`, true, { cause: err });
    }
    const msg = await res.json();
    return "error" in msg ? { kind: "error", error: msg.error } : { kind: "result", result: msg.result };
  }
  onServerRequest() { return () => {}; }
  onNotification() { return () => {}; }
  onInvalidated() { return () => {}; }
}

// ---- server process control ------------------------------------------------------------

// Every spawned server dies with the harness, including when a cell throws.
const children = new Set();
process.on("exit", () => { for (const c of children) if (c.exitCode === null) c.kill("SIGKILL"); });

async function startServer(args, port = nextPort++) {
  const child = spawn(PYTHON, [SERVER, "--port", String(port), ...args], { stdio: ["ignore", "pipe", "pipe"] });
  children.add(child);
  child.once("exit", () => children.delete(child));
  let stderr = "";
  child.stderr.on("data", (d) => (stderr += d));
  const ready = await new Promise((resolve, reject) => {
    let buf = "";
    child.stdout.on("data", (d) => {
      buf += d;
      const line = buf.split("\n").find((l) => l.startsWith("{"));
      if (line) resolve(JSON.parse(line));
    });
    child.on("exit", (code) => reject(new Error(`server exited ${code}: ${stderr}`)));
  });
  return { child, port, url: `http://127.0.0.1:${port}/mcp`, ready };
}

async function stats(srv) {
  return (await fetch(`http://127.0.0.1:${srv.port}/stats`)).json();
}

function kill(srv, sig = "SIGKILL") {
  return new Promise((resolve) => {
    if (srv.child.exitCode !== null) return resolve();
    srv.child.once("exit", () => resolve());
    srv.child.kill(sig);
  });
}

async function settleSafely(execution) {
  try {
    const { outcome } = await execution.settle();
    return {
      settled: outcome.status,
      message: outcome.task?.statusMessage ?? outcome.error?.message,
      // Whether the outcome carries the server's terminal snapshot, and what the error wraps:
      // the only two signals that separate "the task failed" from "we lost the connection".
      terminalSnapshot: outcome.task !== undefined,
      cause: outcome.error?.cause?.name,
    };
  } catch (err) {
    return { settled: "rejected", error: `${err?.name}: ${err?.message}` };
  }
}

// ---- E5a: restart durability through the official requester ------------------------------

async function e5a(store, downtimeMs) {
  const dir = mkdtempSync(join(tmpdir(), "e5a-"));
  const args = ["--store", store, "--db", join(dir, "t.db"), "--hint", "200", "--orphan-after-s", "0"];
  let srv = await startServer(args);
  const port = new HttpPort(srv.url);
  const session = withTasks(port);

  // 12 short tasks that finish before the crash, 8 long ones in flight across it.
  const execs = [];
  for (let i = 0; i < 20; i++) {
    const durationMs = i < 12 ? 300 : 3000;
    const ex = await session.callTool("long_job", { durationMs, label: `t${i}` });
    execs.push({ i, ex, ref: ex.serializeReference(), inFlight: i >= 12 });
  }
  // Short tasks: let the requester observe completion, as an application would.
  const shortOutcomes = await Promise.all(execs.filter((e) => !e.inFlight).map((e) => settleSafely(e.ex)));
  // Long tasks: the requester is mid-poll when the process dies.
  const longSettles = execs.filter((e) => e.inFlight).map((e) => settleSafely(e.ex));
  await sleep(500);
  await kill(srv);
  await sleep(downtimeMs);
  srv = await startServer(args, srv.port);
  const recovery = srv.ready.recovery;
  const longOutcomes = await Promise.all(longSettles);

  // The application-level path: resume every stored reference on a fresh session.
  const session2 = withTasks(new HttpPort(srv.url));
  const resumed = [];
  for (const e of execs) {
    let r;
    try {
      const ex2 = await session2.resumeTask(e.ref);
      r = await settleSafely(ex2);
    } catch (err) {
      r = { settled: "rejected", error: `${err?.name}: ${err?.message}` };
    }
    resumed.push({ i: e.i, inFlight: e.inFlight, ...r });
  }
  const st = await stats(srv);
  await kill(srv, "SIGTERM");
  rmSync(dir, { recursive: true, force: true });

  const count = (xs, pred) => xs.filter(pred).length;
  return {
    store, downtimeMs, recovery,
    shortSettledBeforeCrash: count(shortOutcomes, (o) => o.settled === "completed"),
    inFlightSurvivedWithoutResume: count(longOutcomes, (o) => o.settled === "completed"),
    inFlightSettledAs: longOutcomes.reduce((acc, o) => ({ ...acc, [o.settled]: (acc[o.settled] ?? 0) + 1 }), {}),
    inFlightMessages: [...new Set(longOutcomes.map((o) => o.error ?? o.message).filter(Boolean))],
    inFlightWithTerminalSnapshot: count(longOutcomes, (o) => o.terminalSnapshot),
    inFlightCauses: [...new Set(longOutcomes.map((o) => o.cause).filter(Boolean))],
    inFlightRejectedErrors: [...new Set(longOutcomes.filter((o) => o.settled === "rejected").map((o) => o.error))],
    resolvedAfterRestart: count(resumed, (o) => o.settled === "completed"),
    lostAfterRestart: count(resumed, (o) => o.settled === "rejected"),
    resumedCompletedInFlight: count(resumed, (o) => o.inFlight && o.settled === "completed"),
    reexecuted: st.tasks.filter((t) => t.reexecuted > 0).length,
    resumeErrors: [...new Set(resumed.filter((o) => o.settled === "rejected").map((o) => o.error))],
    transportErrors: port.transportErrors,
  };
}

// ---- E5b: who decides poll cost? ---------------------------------------------------------

async function e5b(hint, durationsMs, reps) {
  const srv = await startServer(["--store", "memory", "--hint", hint, "--orphan-after-s", "0"]);
  const session = withTasks(new HttpPort(srv.url));
  const runs = [];
  for (const d of durationsMs) {
    for (let r = 0; r < reps; r++) runs.push({ d, label: `d${d}-r${r}` });
  }
  const t0 = Date.now();
  const results = await Promise.all(runs.map(async (run) => {
    const started = Date.now();
    const ex = await session.callTool("long_job", { durationMs: run.d, label: run.label });
    const o = await settleSafely(ex);
    return { ...run, ...o, observedMs: Date.now() - started };
  }));
  const st = await stats(srv);
  await kill(srv, "SIGTERM");
  const byLabel = Object.fromEntries(st.tasks.map((t) => [t.label, t]));
  const cells = durationsMs.map((d) => {
    const rs = results.filter((x) => x.d === d);
    const polls = rs.map((x) => byLabel[x.label].polls);
    const lag = rs.map((x) => x.observedMs - d);
    const mean = (xs) => xs.reduce((a, b) => a + b, 0) / xs.length;
    const sorted = [...lag].sort((a, b) => a - b);
    return {
      durationMs: d, reps: rs.length, completed: rs.filter((x) => x.settled === "completed").length,
      pollsMean: +mean(polls).toFixed(2), pollsMin: Math.min(...polls), pollsMax: Math.max(...polls),
      staleMsP50: sorted[Math.floor(sorted.length / 2)], staleMsMax: sorted[sorted.length - 1],
    };
  });
  return { hint, cells, wallMs: Date.now() - t0 };
}

// ---- E5c: the orphan coupling, through handoff/resume and through the hint ----------------

async function e5cHandoff(orphanAfterS, gapMs, durationMs) {
  const srv = await startServer(["--store", "memory", "--hint", "200", "--orphan-after-s", String(orphanAfterS),
    "--reap-every-s", "0.1"]);
  const session = withTasks(new HttpPort(srv.url));
  const ex = await session.callTool("long_job", { durationMs, label: "handoff" });
  let stored;
  await sleep(300);
  await ex.handoff((ref) => { stored = ref; });
  await sleep(gapMs);
  const ex2 = await session.resumeTask(stored);
  const o = await settleSafely(ex2);
  await kill(srv, "SIGTERM");
  return { orphanAfterS, gapMs, durationMs, ...o };
}

async function e5cHint(orphanAfterS, hintMs, durationMs) {
  const srv = await startServer(["--store", "memory", "--hint", String(hintMs), "--orphan-after-s",
    String(orphanAfterS), "--reap-every-s", "0.1"]);
  const session = withTasks(new HttpPort(srv.url));
  const ex = await session.callTool("long_job", { durationMs, label: "hint" });
  const o = await settleSafely(ex);
  const st = await stats(srv);
  await kill(srv, "SIGTERM");
  return { orphanAfterS, hintMs, durationMs, polls: st.tasks[0].polls, ...o };
}

// ---- E5d: ttlMs is measured from creation --------------------------------------------------

async function e5d(ttlMs, durationMs) {
  const srv = await startServer(["--store", "memory", "--hint", "200", "--orphan-after-s", "0",
    "--ttl-ms", String(ttlMs), "--ttl-enforce"]);
  const session = withTasks(new HttpPort(srv.url));
  const ex = await session.callTool("long_job", { durationMs, label: "ttl" });
  const o = await settleSafely(ex);
  await kill(srv, "SIGTERM");
  return { ttlMs, durationMs, ...o };
}

// ---- main --------------------------------------------------------------------------------

async function main() {
  const out = { package: "@modelcontextprotocol/ext-tasks@0.2.3", node: process.version, startedAt: new Date().toISOString() };
  const reps = QUICK ? 2 : 10;

  out.e5a = [];
  for (const store of ["memory", "sqlite"]) {
    for (const downtimeMs of QUICK ? [500] : [100, 500, 2000]) out.e5a.push(await e5a(store, downtimeMs));
  }

  // Time-scaled durations: 1 s, 5 s, 30 s stand in for the paper's short/medium/long regimes.
  const durations = QUICK ? [1000, 5000] : [1000, 5000, 30000];
  out.e5b = [];
  for (const hint of ["none", "0", "250", "1000", "5000", "auto"]) out.e5b.push(await e5b(hint, durations, reps));

  out.e5c = { handoff: [], hint: [] };
  for (const orphan of [1, 3]) {
    for (const gap of [500, 2000, 4000]) out.e5c.handoff.push(await e5cHandoff(orphan, gap, 8000));
    for (const hint of [500, 2000, 5000]) out.e5c.hint.push(await e5cHint(orphan, hint, 8000));
  }

  out.e5d = [];
  for (const [ttl, d] of [[60000, 2000], [3000, 2000], [3000, 6000]]) out.e5d.push(await e5d(ttl, d));

  out.finishedAt = new Date().toISOString();
  process.stdout.write(JSON.stringify(out, null, 2) + "\n");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
