"""Alternative-resource waiting (``waiting_any``) entry point.

A job may wait for *any one* of several candidate resources instead of a
single one.  The semantics are implemented by the core engine in
:mod:`deadlock_simulator`; this module simply exposes them under a
dedicated entry point used by ``POST /simulate/choices``.  The input
payload is validated, never mutated.
"""

from deadlock_simulator import solve


def solve_choices(payload):
    """Solve a payload whose jobs may use ``waiting_any``.

    A job with ``waiting_any`` (a non-empty list of resource ids)
    continues as soon as any one candidate is granted to it; jobs with
    ``waiting_for`` keep the original single-resource semantics, and one
    job may not combine the two.  Grants are adjudicated in ascending
    resource id order (smallest waiting job id first), each waiting job
    receives at most one candidate, and abort sets are re-simulated from
    the real stalled state under the usual minimum-cost / smallest-id-list
    rule.  Protected jobs are never aborted; duplicate, unknown,
    already-held or otherwise illegal candidates reject the whole request.
    """
    return solve(payload, allow_choices=True)
