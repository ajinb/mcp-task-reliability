"""Stateless MCP made every client a poller. This is what that costs.

Fixed-interval polling is the obvious implementation and the one that turns a long task into
sustained load. Multiplicative backoff answers the same question with a fraction of the traffic.
"""

from mcp_task_reliability.policy import PollPolicy
from mcp_task_reliability.poll import adaptive_polls, fixed_interval_polls

print(f"{'task length':>12}{'fixed 250ms':>14}{'adaptive':>10}{'reduction':>11}")
for duration_s in (30, 60, 300, 900, 1800):
    fixed = fixed_interval_polls(duration_s, 250)
    adaptive = adaptive_polls(duration_s, PollPolicy(initial_ms=250))
    print(f"{duration_s:>10}s{fixed:>14}{adaptive:>10}{1 - adaptive / fixed:>10.0%}")

print(
    "\nThe longer the task, the worse fixed polling gets — which is exactly backwards, "
    "since long tasks are the reason the Tasks extension exists."
)
