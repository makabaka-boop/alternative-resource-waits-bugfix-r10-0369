"""Deadlock detection and resolution simulator.

Model
-----
The backend receives the current state of at most 12 jobs and at most 15
single-instance resources:

* each resource is held by at most one job;
* each job may hold several resources, and either waits for exactly one
  resource (``waiting_for``) or for any one of several candidate
  resources (``waiting_any``, only accepted with ``allow_choices``) --
  never both at once;
* some jobs are protected (cannot be aborted); every other job carries a
  positive integer abort cost.

Simulation semantics
--------------------
The simulator repeats rounds until no further progress is possible:

1. Completion phase: every active job that is not waiting has everything
   it needs, so it completes and releases all held resources
   (jobs are processed in ascending id order).
2. Grant phase: resources are scanned in ascending id order and every
   idle resource is granted on the spot to its smallest-id current
   waiter.  A granted job stops waiting immediately, so a job with
   several candidates receives exactly one of them and is out of the
   running for the rest of the round.  After a full grant round the
   simulator returns to the completion phase.

A job stops waiting exactly when one of its requested resources is
granted to it, which makes it complete in the next completion phase.

Deadlock resolution
-------------------
If jobs remain active after the simulation stalls, each of them waits
only for resources held by other stuck jobs: they are deadlocked.  The
resolver then picks a set of abortable jobs with minimum total abort
cost such that aborting them (releasing their resources) lets every
remaining job complete.  Every candidate set is re-simulated
independently from the real stalled state.  Ties are broken by taking
the lexicographically smallest sorted list of aborted job ids.  If no
such set exists -- which can only happen when protected jobs are
deadlocked among themselves -- the situation is reported explicitly;
protected resources are never force-released.
"""

from dataclasses import dataclass
from itertools import combinations

MAX_JOBS = 12
MAX_RESOURCES = 15

STATUS_COMPLETED = "completed"
STATUS_RESOLVED = "resolved_with_aborts"
STATUS_UNRESOLVABLE = "unresolvable"


@dataclass
class Job:
    id: int
    holding: set  # set of resource ids currently held
    waiting_for: int  # resource id or None
    abortable: bool
    abort_cost: int  # positive int when abortable, else None
    waiting_any: frozenset = None  # candidate resource ids, or None

    def is_waiting(self):
        return self.waiting_for is not None or self.waiting_any is not None

    def waits_for(self, rid):
        """True while ``rid`` is one of the resources this job waits for."""
        return self.waiting_for == rid or (
            self.waiting_any is not None and rid in self.waiting_any
        )


@dataclass
class Resource:
    id: int
    holder: int  # job id or None


class State:
    """Mutable simulation state.  ``jobs`` holds only *active* jobs:
    completed or aborted jobs are removed."""

    def __init__(self, jobs, resources):
        self.jobs = jobs  # job id -> Job
        self.resources = resources  # resource id -> Resource

    def clone(self):
        return State(
            {
                jid: Job(
                    j.id,
                    set(j.holding),
                    j.waiting_for,
                    j.abortable,
                    j.abort_cost,
                    j.waiting_any,
                )
                for jid, j in self.jobs.items()
            },
            {rid: Resource(r.id, r.holder) for rid, r in self.resources.items()},
        )


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def build_state(payload, allow_choices=False):
    """Validate the request payload and build a fresh :class:`State`.

    With ``allow_choices`` a job may replace ``waiting_for`` by
    ``waiting_any``: a non-empty list of candidate resource ids, any one
    of which lets the job continue.  Raises :class:`ValueError` with a
    descriptive message on any inconsistency.  Never mutates ``payload``.
    """
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object with 'jobs' and 'resources'")
    jobs_in = payload.get("jobs")
    resources_in = payload.get("resources")
    if not isinstance(jobs_in, list) or not isinstance(resources_in, list):
        raise ValueError("'jobs' and 'resources' must be lists")
    if len(jobs_in) > MAX_JOBS:
        raise ValueError(f"at most {MAX_JOBS} jobs are allowed, got {len(jobs_in)}")
    if len(resources_in) > MAX_RESOURCES:
        raise ValueError(
            f"at most {MAX_RESOURCES} resources are allowed, got {len(resources_in)}"
        )

    jobs = {}
    for entry in jobs_in:
        if not isinstance(entry, dict):
            raise ValueError("each job must be an object")
        jid = entry.get("id")
        if not _is_int(jid):
            raise ValueError("job id must be an integer")
        if jid in jobs:
            raise ValueError(f"duplicate job id {jid}")

        holding = entry.get("holding", [])
        if not isinstance(holding, list) or any(not _is_int(r) for r in holding):
            raise ValueError(f"job {jid}: 'holding' must be a list of resource ids")
        if len(set(holding)) != len(holding):
            raise ValueError(f"job {jid}: 'holding' lists a resource more than once")

        waiting_for = entry.get("waiting_for")
        if waiting_for is not None and not _is_int(waiting_for):
            raise ValueError(f"job {jid}: 'waiting_for' must be a resource id or null")

        waiting_any = entry.get("waiting_any")
        if waiting_any is not None:
            if not allow_choices:
                raise ValueError(
                    f"job {jid}: 'waiting_any' is only accepted by the "
                    "alternative-resource (choices) endpoint"
                )
            if not isinstance(waiting_any, list) or any(
                not _is_int(r) for r in waiting_any
            ):
                raise ValueError(
                    f"job {jid}: 'waiting_any' must be a list of resource ids"
                )
            if not waiting_any:
                raise ValueError(f"job {jid}: 'waiting_any' must not be empty")
            if len(set(waiting_any)) != len(waiting_any):
                raise ValueError(
                    f"job {jid}: 'waiting_any' lists a resource more than once"
                )
            if waiting_for is not None:
                raise ValueError(
                    f"job {jid}: 'waiting_for' and 'waiting_any' are mutually"
                    " exclusive"
                )

        abortable = entry.get("abortable", False)
        if not isinstance(abortable, bool):
            raise ValueError(f"job {jid}: 'abortable' must be a boolean")
        cost = entry.get("abort_cost")
        if abortable:
            if not _is_int(cost) or cost <= 0:
                raise ValueError(
                    f"abortable job {jid} needs a positive integer 'abort_cost'"
                )
        elif cost is not None:
            raise ValueError(f"protected job {jid} must not carry an 'abort_cost'")

        jobs[jid] = Job(
            jid,
            set(holding),
            waiting_for,
            abortable,
            cost if abortable else None,
            frozenset(waiting_any) if waiting_any is not None else None,
        )

    resources = {}
    for entry in resources_in:
        if not isinstance(entry, dict):
            raise ValueError("each resource must be an object")
        rid = entry.get("id")
        if not _is_int(rid):
            raise ValueError("resource id must be an integer")
        if rid in resources:
            raise ValueError(f"duplicate resource id {rid}")
        holder = entry.get("holder")
        if holder is not None:
            if not _is_int(holder):
                raise ValueError(f"resource {rid}: 'holder' must be a job id or null")
            if holder not in jobs:
                raise ValueError(f"resource {rid} is held by unknown job {holder}")
        resources[rid] = Resource(rid, holder)

    # Cross-check the two views of ownership and the wait references.
    for job in jobs.values():
        for rid in job.holding:
            if rid not in resources:
                raise ValueError(f"job {job.id} holds unknown resource {rid}")
            if resources[rid].holder != job.id:
                raise ValueError(
                    f"job {job.id} claims resource {rid} but the resource record disagrees"
                )
        if job.waiting_for is not None:
            wanted = [job.waiting_for]
        elif job.waiting_any is not None:
            wanted = sorted(job.waiting_any)
        else:
            wanted = []
        for rid in wanted:
            if rid not in resources:
                raise ValueError(f"job {job.id} waits for unknown resource {rid}")
            if rid in job.holding:
                raise ValueError(
                    f"job {job.id} waits for resource {rid} it already holds"
                )
    for res in resources.values():
        if res.holder is not None and res.id not in jobs[res.holder].holding:
            raise ValueError(
                f"resource {res.id} is recorded as held by job {res.holder} "
                f"but the job does not list it"
            )

    return State(jobs, resources)


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------


def simulate(state):
    """Run grant/complete rounds on ``state`` until no progress is possible.

    Mutates ``state``.  Returns ``(events, stuck)`` where ``events`` is the
    ordered replay of grant/complete events and ``stuck`` is the sorted list
    of job ids still active (deadlocked) when the simulation stalls.
    """
    events = []
    while True:
        # Phase 1: every job that is not waiting completes and releases.
        progressed = False
        for jid in sorted(state.jobs):
            job = state.jobs[jid]
            if not job.is_waiting():
                released = sorted(job.holding)
                for rid in released:
                    state.resources[rid].holder = None
                job.holding.clear()
                del state.jobs[jid]
                events.append({"type": "complete", "job": jid, "released": released})
                progressed = True
        if progressed:
            continue

        # Phase 2: resources are scanned in ascending id order and every
        # idle resource is granted on the spot to its smallest-id current
        # waiter.  The granted job stops waiting at once, so a job with
        # several candidates receives exactly one resource and drops out
        # of the running for the rest of the round.
        granted = False
        for rid in sorted(state.resources):
            res = state.resources[rid]
            if res.holder is not None:
                continue
            waiters = [j.id for j in state.jobs.values() if j.waits_for(rid)]
            if not waiters:
                continue
            jid = min(waiters)
            job = state.jobs[jid]
            res.holder = jid
            job.holding.add(rid)
            job.waiting_for = None
            job.waiting_any = None
            events.append({"type": "grant", "job": jid, "resource": rid})
            granted = True
        if not granted:
            break

    return events, sorted(state.jobs)


def abort_job(state, jid):
    """Abort an active job, releasing its resources.  Returns the event."""
    job = state.jobs[jid]
    released = sorted(job.holding)
    for rid in released:
        state.resources[rid].holder = None
    job.holding.clear()
    del state.jobs[jid]
    return {"type": "abort", "job": jid, "released": released}


# ---------------------------------------------------------------------------
# Deadlock resolution
# ---------------------------------------------------------------------------


def find_min_abort_set(state, stuck):
    """Minimum-cost abort set resolving the deadlock, or ``None``.

    ``state`` must be the stalled state (``simulate`` already ran to a
    stop) and ``stuck`` its remaining active job ids.  Every abortable
    stuck subset is enumerated and independently re-simulated; among the
    subsets that let all remaining jobs complete, the one with minimum
    total abort cost wins, ties broken by the lexicographically smallest
    sorted id list.
    """
    abortable = [j for j in stuck if state.jobs[j].abortable]
    best_key = None
    best_set = None
    for size in range(len(abortable) + 1):
        for combo in combinations(abortable, size):
            candidate = state.clone()
            for jid in combo:
                abort_job(candidate, jid)
            _, remaining = simulate(candidate)
            if not remaining:
                ids = sorted(combo)
                key = (sum(state.jobs[j].abort_cost for j in ids), ids)
                if best_key is None or key < best_key:
                    best_key, best_set = key, ids
    return best_set


def solve(payload, allow_choices=False):
    """Validate the payload, simulate, resolve deadlocks, return the replay.

    The result is a JSON-serialisable dict with ``status`` one of
    ``completed``, ``resolved_with_aborts`` or ``unresolvable``.
    ``allow_choices`` enables the ``waiting_any`` candidate-resource
    semantics used by the choices endpoint.  ``payload`` is never mutated.
    """
    state = build_state(payload, allow_choices=allow_choices)
    events, stuck = simulate(state)
    if not stuck:
        return {
            "status": STATUS_COMPLETED,
            "events": events,
            "aborted": [],
            "abort_cost": 0,
        }

    abort_set = find_min_abort_set(state, stuck)
    if abort_set is None:
        protected = [j for j in stuck if not state.jobs[j].abortable]
        return {
            "status": STATUS_UNRESOLVABLE,
            "events": events,
            "stuck": stuck,
            "protected_stuck": protected,
            "message": (
                "Deadlock cannot be resolved: protected job(s) "
                f"{protected} remain blocked no matter which abortable jobs are "
                "aborted. Protected resources were left untouched."
            ),
        }

    total_cost = sum(state.jobs[j].abort_cost for j in abort_set)
    abort_events = [abort_job(state, j) for j in abort_set]
    continuation, remaining = simulate(state)
    assert not remaining, "chosen abort set must resolve the deadlock"
    return {
        "status": STATUS_RESOLVED,
        "events": events + abort_events + continuation,
        "aborted": abort_set,
        "abort_cost": total_cost,
    }
