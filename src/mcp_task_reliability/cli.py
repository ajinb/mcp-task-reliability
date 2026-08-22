"""mcp-task-reliability command-line interface."""

from __future__ import annotations

import argparse
import json
import sys

from .sim import crash_recovery, poll_storm, retry_classes


def _crash_rows() -> list:
    return [crash_recovery(durable=False), crash_recovery(durable=True)]


def cmd_crash(as_json: bool = False) -> int:
    rows = _crash_rows()
    if as_json:
        print(json.dumps([r.__dict__ for r in rows], indent=2))
        return 0
    print("crash recovery — 20 handles issued, 8 in flight when the process dies\n")
    print(f"{'store':<10}{'resolved':>10}{'lost':>8}{'resumable':>12}{'survival':>11}")
    for r in rows:
        print(f"{r.backend:<10}{r.resolved_after_restart:>10}{r.lost:>8}"
              f"{r.resumable:>12}{r.survival_rate:>10.0%}")
    return 0


def cmd_poll(as_json: bool = False) -> int:
    r = poll_storm()
    if as_json:
        print(json.dumps({**r.__dict__, "reduction": r.reduction}, indent=2))
        return 0
    print(f"\npoll traffic — one {r.duration_s:g}s task, per client\n")
    print(f"{'client':<20}{'polls':>8}")
    print(f"{'fixed 250ms':<20}{r.fixed_polls:>8}")
    print(f"{'adaptive backoff':<20}{r.adaptive_polls:>8}")
    print(f"\n{r.reduction:.0%} fewer requests for the same answer.")
    return 0


def cmd_retry(as_json: bool = False) -> int:
    rows = retry_classes()
    if as_json:
        print(json.dumps(
            [{**r.__dict__, "failure_class": r.failure_class.value} for r in rows], indent=2
        ))
        return 0
    print("\nfailure classification — three errors that look alike in a log line\n")
    print(f"{'error':<46}{'class':<12}{'attempts':>9}{'dead-letter':>13}")
    for r in rows:
        error = r.error if len(r.error) <= 44 else r.error[:41] + "..."
        print(f"{error:<46}{r.failure_class.value:<12}{r.attempts:>9}"
              f"{'yes' if r.dead_lettered else 'no':>13}")
    return 0


def cmd_demo(as_json: bool = False) -> int:
    if as_json:
        rows = _crash_rows()
        poll = poll_storm()
        print(json.dumps({
            "crash_recovery": [r.__dict__ for r in rows],
            "poll_storm": {**poll.__dict__, "reduction": poll.reduction},
            "retry_classes": [
                {**r.__dict__, "failure_class": r.failure_class.value} for r in retry_classes()
            ],
        }, indent=2))
        return 0
    cmd_crash()
    cmd_poll()
    cmd_retry()
    return 0


def build_parser() -> argparse.ArgumentParser:
    # `--json` lives on both sides so that `demo --json` and `--json demo` both work. The
    # subcommand copy uses SUPPRESS so an unset flag there cannot clobber the top-level one.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--json", action="store_true", default=argparse.SUPPRESS,
        help="machine-readable output",
    )

    parser = argparse.ArgumentParser(
        prog="mcp-task-reliability",
        description="Reliability policy for the MCP Tasks extension. All demos run offline.",
        parents=[common],
    )
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("demo", parents=[common], help="run all three scenarios (default)")
    sub.add_parser("crash", parents=[common], help="durability across a restart")
    sub.add_parser("poll", parents=[common], help="poll traffic, fixed versus adaptive")
    sub.add_parser(
        "retry", parents=[common], help="failure classification and the dead-letter queue"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handlers = {
        "crash": cmd_crash, "poll": cmd_poll, "retry": cmd_retry, "demo": cmd_demo, None: cmd_demo,
    }
    return handlers[args.command](getattr(args, "json", False))


if __name__ == "__main__":
    sys.exit(main())
