"""Choice-resource variant of the deadlock simulator.

Each job may use ``waiting_any`` (a non-empty array of candidate
resource ids) instead of ``waiting_for``; being granted *any one*
candidate lets the job continue.  Jobs without ``waiting_any`` keep the
classic single-request semantics.

This is a thin validated wrapper over :func:`deadlock_simulator.solve`:
candidate lists are handled natively by the simulator, so the original
payload is inspected as given and never rewritten.
"""

from deadlock_simulator import solve


def solve_choices(payload):
    """Validate ``payload`` (including ``waiting_any``) and run the
    full grant/complete/abort replay with choice resources."""
    return solve(payload, allow_choices=True)
