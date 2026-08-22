"""What a client's handles are worth after the server restarts.

The MCP Tasks extension hands the client a durable handle and tells it to come back later.
"Durable" is a promise about your storage, not about the protocol — this is what the two
answers look like side by side.
"""

from mcp_task_reliability.sim import crash_recovery

for durable in (False, True):
    r = crash_recovery(durable=durable)
    print(
        f"{r.backend:<8} issued={r.issued:<4} in_flight={r.in_flight:<4} "
        f"resolved_after_restart={r.resolved_after_restart:<4} lost={r.lost:<4} "
        f"resumable={r.resumable:<4} survival={r.survival_rate:.0%}"
    )

print(
    "\nThe in-memory store loses every handle it ever issued, including the completed ones "
    "whose results the client never picked up. Same protocol, same client, different promise."
)
