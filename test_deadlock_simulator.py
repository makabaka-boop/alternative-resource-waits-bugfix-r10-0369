"""Tests for the deadlock simulator backend.

The cross-check test follows the required methodology: it enumerates
small abort subsets, re-simulates each one independently, and verifies
both that the returned abort set resolves the deadlock and that no
cheaper (or lexicographically smaller) set would.  A separate replay
verifier re-applies the returned event stream from scratch without
using any simulator internals.
"""

import copy
import http.client
import itertools
import json
import random
import threading
import unittest

from deadlock_simulator import (
    MAX_JOBS,
    MAX_RESOURCES,
    abort_job,
    build_state,
    find_min_abort_set,
    simulate,
    solve,
)
from backend_server import create_server


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def mk_payload(job_specs, extra_resources=()):
    """Build a consistent payload; resource holders are derived from the
    jobs' ``holding`` lists, resources only mentioned in ``waiting_for``
    / ``waiting_any`` (or ``extra_resources``) start out free."""
    resources = {}
    for spec in job_specs:
        for r in spec.get("holding", []):
            resources[r] = spec["id"]
        w = spec.get("waiting_for")
        if w is not None:
            resources.setdefault(w, None)
        for r in spec.get("waiting_any") or []:
            resources.setdefault(r, None)
    for r in extra_resources:
        resources.setdefault(r, None)
    return {
        "jobs": job_specs,
        "resources": [{"id": r, "holder": h} for r, h in sorted(resources.items())],
    }


def waited_resources(job):
    """The set of resources a payload job is currently waiting for.

    Independent of the simulator internals: ``waiting_any`` yields the
    whole candidate set, ``waiting_for`` a singleton."""
    if job.get("waiting_any") is not None:
        return set(job["waiting_any"])
    if job.get("waiting_for") is not None:
        return {job["waiting_for"]}
    return set()


def replay_events(payload, events):
    """Independently re-apply an event replay to the input state.

    Shares no code with the simulator: every event is checked for
    legality against a locally maintained state.  Returns the final
    ``(active, waiting, holder)`` triple.
    """
    holding = {j["id"]: set(j.get("holding", [])) for j in payload["jobs"]}
    waiting = {j["id"]: waited_resources(j) for j in payload["jobs"]}
    abortable = {j["id"]: j.get("abortable", False) for j in payload["jobs"]}
    holder = {r["id"]: r.get("holder") for r in payload["resources"]}
    active = set(holding)
    checked_stall = False
    for ev in events:
        kind = ev["type"]
        if kind == "grant":
            j, r = ev["job"], ev["resource"]
            assert j in active, f"grant to inactive job {j}"
            assert r in waiting[j], f"job {j} is not waiting for resource {r}"
            assert holder[r] is None, f"resource {r} is not idle"
            holder[r] = j
            holding[j].add(r)
            # The job takes exactly one candidate and drops all others.
            waiting[j] = set()
        elif kind == "complete":
            j = ev["job"]
            assert j in active, f"complete event for inactive job {j}"
            assert waiting[j] == set(), f"job {j} completes while still waiting"
            assert (
                sorted(holding[j]) == ev["released"]
            ), f"job {j} released set mismatch"
            for r in holding[j]:
                assert holder[r] == j, f"resource {r} not held by job {j}"
                holder[r] = None
            holding[j] = set()
            active.discard(j)
        elif kind == "abort":
            j = ev["job"]
            if not checked_stall:
                # Aborts may only start once the system is truly stuck:
                # every active job must be waiting, and every waited
                # resource (of a choice job: every candidate) must be
                # held, so no grant phase could make progress.
                for a in active:
                    assert waiting[a], f"job {a} could still complete"
                    for wr in waiting[a]:
                        assert (
                            holder[wr] is not None
                        ), f"job {a} could still be granted resource {wr}"
                checked_stall = True
            assert j in active, f"abort event for inactive job {j}"
            assert abortable[j], f"protected job {j} was aborted"
            assert (
                sorted(holding[j]) == ev["released"]
            ), f"job {j} released set mismatch"
            for r in holding[j]:
                assert holder[r] == j, f"resource {r} not held by job {j}"
                holder[r] = None
            holding[j] = set()
            active.discard(j)
        else:
            raise AssertionError(f"unknown event type {kind!r}")
    return active, waiting, holder


def brute_force_min_abort_set(payload, allow_choices=False):
    """Enumerate every abortable stuck subset and independently re-simulates
    each candidate; return the minimum (cost, ids) feasible set or None."""
    state = build_state(payload, allow_choices=allow_choices)
    _, stuck = simulate(state)
    abortable = [j for j in stuck if state.jobs[j].abortable]
    best_key = None
    best = None
    for size in range(len(abortable) + 1):
        for combo in itertools.combinations(abortable, size):
            candidate = state.clone()
            for jid in combo:
                abort_job(candidate, jid)
            _, remaining = simulate(candidate)
            if not remaining:
                ids = sorted(combo)
                key = (sum(state.jobs[j].abort_cost for j in ids), ids)
                if best_key is None or key < best_key:
                    best_key, best = key, ids
    return best


def random_payload(rng):
    n_jobs = rng.randint(1, 8)
    n_res = rng.randint(1, 15)
    job_ids = rng.sample(range(1, 40), n_jobs)
    res_ids = rng.sample(range(100, 200), n_res)
    holder = {r: (rng.choice(job_ids) if rng.random() < 0.7 else None) for r in res_ids}
    jobs = []
    for jid in job_ids:
        holding = [r for r in res_ids if holder[r] == jid]
        choices = [r for r in res_ids if r not in holding]
        waiting_for = rng.choice(choices) if choices and rng.random() < 0.6 else None
        abortable = rng.random() < 0.6
        job = {
            "id": jid,
            "holding": holding,
            "waiting_for": waiting_for,
            "abortable": abortable,
        }
        if abortable:
            job["abort_cost"] = rng.randint(1, 9)
        jobs.append(job)
    resources = [{"id": r, "holder": holder[r]} for r in res_ids]
    return {"jobs": jobs, "resources": resources}


def random_choice_payload(rng):
    """Random instance that may mix ``waiting_for`` and ``waiting_any``."""
    n_jobs = rng.randint(1, 8)
    n_res = rng.randint(1, 15)
    job_ids = rng.sample(range(1, 40), n_jobs)
    res_ids = rng.sample(range(100, 200), n_res)
    holder = {r: (rng.choice(job_ids) if rng.random() < 0.7 else None) for r in res_ids}
    jobs = []
    for jid in job_ids:
        holding = [r for r in res_ids if holder[r] == jid]
        free_to_wait = [r for r in res_ids if r not in holding]
        job = {"id": jid, "holding": holding}
        if free_to_wait and rng.random() < 0.65:
            if rng.random() < 0.6:
                k = rng.randint(1, min(len(free_to_wait), 4))
                job["waiting_any"] = sorted(rng.sample(free_to_wait, k))
            else:
                job["waiting_for"] = rng.choice(free_to_wait)
        abortable = rng.random() < 0.6
        job["abortable"] = abortable
        if abortable:
            job["abort_cost"] = rng.randint(1, 9)
        jobs.append(job)
    resources = [{"id": r, "holder": holder[r]} for r in res_ids]
    return {"jobs": jobs, "resources": resources}


def reference_solve(payload):
    """Fully independent plain-dict re-implementation of the full solve.

    Shares no code with the simulator module: it parses the raw payload,
    runs the documented completion/grant rounds (dynamic per-resource
    adjudication, one grant per waiting job per phase), snapshots the
    real stalled state, enumerates every abortable subset with
    independent replays, and rebuilds the complete event trace of the
    chosen recovery.  Used to cross-check every field of the API result.
    """
    def parse(p):
        holding = {j["id"]: set(j.get("holding", [])) for j in p["jobs"]}
        waiting = {j["id"]: waited_resources(j) for j in p["jobs"]}
        costs = {
            j["id"]: j.get("abort_cost") for j in p["jobs"] if j.get("abortable")
        }
        holder = {r["id"]: r.get("holder") for r in p["resources"]}
        return holding, waiting, costs, holder

    def run(holding, waiting, holder):
        events = []
        active = set(holding)
        while True:
            progressed = False
            for j in sorted(active):
                if not waiting[j]:
                    released = sorted(holding[j])
                    for r in released:
                        holder[r] = None
                    events.append({"type": "complete", "job": j, "released": released})
                    del holding[j]
                    del waiting[j]
                    progressed = True
            active = set(holding)
            if progressed:
                continue
            granted = False
            for r in sorted(holder):
                if holder[r] is not None:
                    continue
                waiters = [j for j in sorted(active) if r in waiting[j]]
                if not waiters:
                    continue
                j = waiters[0]
                holder[r] = j
                holding[j].add(r)
                waiting[j] = set()
                events.append({"type": "grant", "job": j, "resource": r})
                granted = True
            if not granted:
                return events, sorted(active)

    def clone(h, w, ho):
        return {j: set(s) for j, s in h.items()}, {j: set(s) for j, s in w.items()}, dict(ho)

    holding0, waiting0, costs, holder0 = parse(payload)
    holding, waiting, holder = clone(holding0, waiting0, holder0)
    events, stuck = run(holding, waiting, holder)

    if not stuck:
        return {"status": "completed", "events": events,
                "aborted": [], "abort_cost": 0}

    # Enumerate subsets from the *real* stalled state.
    abortable = [j for j in stuck if j in costs]
    best_key, best_set = None, None
    for size in range(len(abortable) + 1):
        for combo in itertools.combinations(abortable, size):
            h, w, ho = clone(holding, waiting, holder)
            for j in combo:
                for r in h[j]:
                    ho[r] = None
                del h[j]
                del w[j]
            _, remaining = run(h, w, ho)
            if not remaining:
                ids = sorted(combo)
                key = (sum(costs[j] for j in ids), ids)
                if best_key is None or key < best_key:
                    best_key, best_set = key, ids

    if best_set is None:
        return {
            "status": "unresolvable",
            "events": events,
            "stuck": stuck,
            "protected_stuck": [j for j in stuck if j not in costs],
        }

    h, w, ho = clone(holding, waiting, holder)
    abort_events = []
    for j in best_set:
        released = sorted(h[j])
        for r in released:
            ho[r] = None
        del h[j]
        del w[j]
        abort_events.append({"type": "abort", "job": j, "released": released})
    continuation, remaining = run(h, w, ho)
    assert not remaining
    return {
        "status": "resolved_with_aborts",
        "events": events + abort_events + continuation,
        "aborted": best_set,
        "abort_cost": sum(costs[j] for j in best_set),
    }


# ---------------------------------------------------------------------------
# Simulation semantics
# ---------------------------------------------------------------------------


class SimulationTests(unittest.TestCase):
    def test_idle_jobs_complete_first_then_grants_by_smallest_id(self):
        payload = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1],
                    "waiting_for": 2,
                    "abortable": True,
                    "abort_cost": 4,
                },
                {
                    "id": 2,
                    "holding": [],
                    "waiting_for": None,
                    "abortable": True,
                    "abort_cost": 1,
                },
                {
                    "id": 3,
                    "holding": [3],
                    "waiting_for": 2,
                    "abortable": True,
                    "abort_cost": 1,
                },
            ]
        )
        result = solve(payload)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(
            result["events"],
            [
                {"type": "complete", "job": 2, "released": []},
                {"type": "grant", "job": 1, "resource": 2},  # smallest-id waiter of r2
                {"type": "complete", "job": 1, "released": [1, 2]},
                {"type": "grant", "job": 3, "resource": 2},
                {"type": "complete", "job": 3, "released": [2, 3]},
            ],
        )
        self.assertEqual(result["aborted"], [])

    def test_completion_frees_resource_for_waiter(self):
        payload = mk_payload(
            [
                {"id": 1, "holding": [], "waiting_for": 1, "abortable": False},
                {"id": 2, "holding": [1], "waiting_for": None, "abortable": False},
            ]
        )
        result = solve(payload)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(
            result["events"],
            [
                {"type": "complete", "job": 2, "released": [1]},
                {"type": "grant", "job": 1, "resource": 1},
                {"type": "complete", "job": 1, "released": [1]},
            ],
        )

    def test_empty_system_completes_immediately(self):
        result = solve({"jobs": [], "resources": []})
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["events"], [])

    def test_free_resource_nobody_waits_for_stays_idle(self):
        payload = mk_payload(
            [{"id": 5, "holding": [], "waiting_for": None, "abortable": False}],
            extra_resources=(9,),
        )
        result = solve(payload)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(
            result["events"], [{"type": "complete", "job": 5, "released": []}]
        )


# ---------------------------------------------------------------------------
# Deadlock resolution
# ---------------------------------------------------------------------------


class ResolutionTests(unittest.TestCase):
    def test_two_cycle_aborts_cheapest_job(self):
        payload = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1],
                    "waiting_for": 2,
                    "abortable": True,
                    "abort_cost": 5,
                },
                {
                    "id": 2,
                    "holding": [2],
                    "waiting_for": 1,
                    "abortable": True,
                    "abort_cost": 3,
                },
            ]
        )
        result = solve(payload)
        self.assertEqual(result["status"], "resolved_with_aborts")
        self.assertEqual(result["aborted"], [2])
        self.assertEqual(result["abort_cost"], 3)
        self.assertEqual(
            result["events"],
            [
                {"type": "abort", "job": 2, "released": [2]},
                {"type": "grant", "job": 1, "resource": 2},
                {"type": "complete", "job": 1, "released": [1, 2]},
            ],
        )

    def test_protected_holder_forces_abort_of_the_other_job(self):
        payload = mk_payload(
            [
                {"id": 1, "holding": [1], "waiting_for": 2, "abortable": False},
                {
                    "id": 2,
                    "holding": [2],
                    "waiting_for": 1,
                    "abortable": True,
                    "abort_cost": 7,
                },
            ]
        )
        result = solve(payload)
        self.assertEqual(result["status"], "resolved_with_aborts")
        self.assertEqual(result["aborted"], [2])
        self.assertEqual(result["abort_cost"], 7)

    def test_three_cycle_tie_breaks_to_smallest_id(self):
        payload = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1],
                    "waiting_for": 2,
                    "abortable": True,
                    "abort_cost": 3,
                },
                {
                    "id": 2,
                    "holding": [2],
                    "waiting_for": 3,
                    "abortable": True,
                    "abort_cost": 3,
                },
                {
                    "id": 3,
                    "holding": [3],
                    "waiting_for": 1,
                    "abortable": True,
                    "abort_cost": 3,
                },
            ]
        )
        result = solve(payload)
        self.assertEqual(
            result["aborted"], [1]
        )  # equal costs -> lexicographically smallest
        self.assertEqual(result["abort_cost"], 3)

    def test_lexicographic_tie_break_across_two_cycles(self):
        payload = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1],
                    "waiting_for": 2,
                    "abortable": True,
                    "abort_cost": 4,
                },
                {
                    "id": 2,
                    "holding": [2],
                    "waiting_for": 1,
                    "abortable": True,
                    "abort_cost": 4,
                },
                {
                    "id": 3,
                    "holding": [3],
                    "waiting_for": 4,
                    "abortable": True,
                    "abort_cost": 1,
                },
                {
                    "id": 4,
                    "holding": [4],
                    "waiting_for": 3,
                    "abortable": True,
                    "abort_cost": 1,
                },
            ]
        )
        result = solve(payload)
        # candidates {1,3} {1,4} {2,3} {2,4} all cost 5 -> [1, 3] wins
        self.assertEqual(result["aborted"], [1, 3])
        self.assertEqual(result["abort_cost"], 5)

    def test_cheap_job_outside_the_cycle_is_not_aborted(self):
        # Job 3 is cheap but aborting it cannot break the 1<->2 cycle.
        payload = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1],
                    "waiting_for": 2,
                    "abortable": True,
                    "abort_cost": 2,
                },
                {
                    "id": 2,
                    "holding": [2],
                    "waiting_for": 1,
                    "abortable": True,
                    "abort_cost": 9,
                },
                {
                    "id": 3,
                    "holding": [3],
                    "waiting_for": 1,
                    "abortable": True,
                    "abort_cost": 1,
                },
            ]
        )
        result = solve(payload)
        self.assertEqual(result["aborted"], [1])
        self.assertEqual(result["abort_cost"], 2)
        # Job 3 must survive and complete once r1 is released.
        self.assertEqual(
            result["events"][-1], {"type": "complete", "job": 3, "released": [1, 3]}
        )

    def test_abort_releases_all_held_resources(self):
        payload = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1, 2],
                    "waiting_for": 3,
                    "abortable": True,
                    "abort_cost": 1,
                },
                {"id": 2, "holding": [3], "waiting_for": 1, "abortable": False},
            ]
        )
        result = solve(payload)
        self.assertEqual(result["aborted"], [1])
        self.assertEqual(
            result["events"],
            [
                {"type": "abort", "job": 1, "released": [1, 2]},
                {"type": "grant", "job": 2, "resource": 1},
                {"type": "complete", "job": 2, "released": [1, 3]},
            ],
        )

    def test_twelve_job_cycle_at_scale_limit(self):
        jobs = []
        n = MAX_JOBS
        for i in range(1, n + 1):
            jobs.append(
                {
                    "id": i,
                    "holding": [i],
                    "waiting_for": i % n + 1,
                    "abortable": True,
                    "abort_cost": (i % 3) + 1,
                }
            )
        result = solve(mk_payload(jobs))
        self.assertEqual(result["status"], "resolved_with_aborts")
        # cheapest cost is 1, achieved by ids 3, 6, 9, 12 -> smallest id wins
        self.assertEqual(result["aborted"], [3])
        self.assertEqual(result["abort_cost"], 1)


# ---------------------------------------------------------------------------
# Unresolvable deadlocks (protected jobs)
# ---------------------------------------------------------------------------


class UnresolvableTests(unittest.TestCase):
    def test_protected_cycle_is_reported_not_force_released(self):
        payload = mk_payload(
            [
                {"id": 1, "holding": [1], "waiting_for": 2, "abortable": False},
                {"id": 2, "holding": [2], "waiting_for": 1, "abortable": False},
            ]
        )
        snapshot = copy.deepcopy(payload)
        result = solve(payload)
        self.assertEqual(result["status"], "unresolvable")
        self.assertEqual(result["stuck"], [1, 2])
        self.assertEqual(result["protected_stuck"], [1, 2])
        self.assertIn("protected", result["message"].lower())
        self.assertEqual(result["events"], [])  # no progress, no aborts
        self.assertEqual(payload, snapshot)  # input untouched
        # Both protected jobs still hold their resources: nothing was released.
        active, _, holder = replay_events(payload, result["events"])
        self.assertEqual(active, {1, 2})
        self.assertEqual(holder[1], 1)
        self.assertEqual(holder[2], 2)

    def test_abortable_cycle_elsewhere_does_not_make_it_resolvable(self):
        payload = mk_payload(
            [
                {"id": 1, "holding": [1], "waiting_for": 2, "abortable": False},
                {"id": 2, "holding": [2], "waiting_for": 1, "abortable": False},
                {
                    "id": 3,
                    "holding": [3],
                    "waiting_for": 4,
                    "abortable": True,
                    "abort_cost": 1,
                },
                {
                    "id": 4,
                    "holding": [4],
                    "waiting_for": 3,
                    "abortable": True,
                    "abort_cost": 1,
                },
            ]
        )
        result = solve(payload)
        self.assertEqual(result["status"], "unresolvable")
        self.assertEqual(result["stuck"], [1, 2, 3, 4])
        self.assertEqual(result["protected_stuck"], [1, 2])
        # No abort events at all: partial resolution is not performed.
        self.assertFalse(any(ev["type"] == "abort" for ev in result["events"]))

    def test_find_min_abort_set_returns_none_for_protected_cycle(self):
        payload = mk_payload(
            [
                {"id": 1, "holding": [1], "waiting_for": 2, "abortable": False},
                {"id": 2, "holding": [2], "waiting_for": 1, "abortable": False},
            ]
        )
        state = build_state(payload)
        _, stuck = simulate(state)
        self.assertEqual(stuck, [1, 2])
        self.assertIsNone(find_min_abort_set(state, stuck))


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


class ValidationTests(unittest.TestCase):
    def assert_invalid(self, payload, fragment):
        with self.subTest(fragment=fragment):
            with self.assertRaises(ValueError) as ctx:
                solve(payload)
            self.assertIn(fragment, str(ctx.exception))

    def test_validation_errors(self):
        base_job = {"id": 1, "holding": [], "waiting_for": None, "abortable": False}
        cases = [
            (
                {
                    "jobs": [dict(base_job)] * 0
                    + [
                        {
                            "id": i,
                            "holding": [],
                            "waiting_for": None,
                            "abortable": False,
                        }
                        for i in range(MAX_JOBS + 1)
                    ],
                    "resources": [],
                },
                "at most 12 jobs",
            ),
            (
                {
                    "jobs": [],
                    "resources": [
                        {"id": i, "holder": None} for i in range(MAX_RESOURCES + 1)
                    ],
                },
                "at most 15 resources",
            ),
            (mk_payload([dict(base_job), dict(base_job)]), "duplicate job id"),
            (
                {
                    "jobs": [],
                    "resources": [{"id": 1, "holder": None}, {"id": 1, "holder": None}],
                },
                "duplicate resource id",
            ),
            (
                {"jobs": [dict(base_job)], "resources": [{"id": 1, "holder": 99}]},
                "held by unknown job",
            ),
            (
                {"jobs": [dict(base_job, holding=[7])], "resources": []},
                "holds unknown resource",
            ),
            (
                {"jobs": [dict(base_job, waiting_for=7)], "resources": []},
                "waits for unknown resource",
            ),
            (
                {"jobs": [dict(base_job)], "resources": [{"id": 1, "holder": 1}]},
                "does not list it",
            ),
            (
                {
                    "jobs": [dict(base_job, holding=[1])],
                    "resources": [{"id": 1, "holder": None}],
                },
                "record disagrees",
            ),
            (
                mk_payload(
                    [{"id": 1, "holding": [1], "waiting_for": 1, "abortable": False}]
                ),
                "already holds",
            ),
            (
                mk_payload([dict(base_job, abortable=True, abort_cost=0)]),
                "positive integer",
            ),
            (
                mk_payload([dict(base_job, abortable=True, abort_cost=-3)]),
                "positive integer",
            ),
            (mk_payload([dict(base_job, abortable=True)]), "positive integer"),
            (
                mk_payload([dict(base_job, abortable=True, abort_cost=True)]),
                "positive integer",
            ),
            (mk_payload([dict(base_job, abort_cost=5)]), "must not carry"),
            ({"jobs": "nope", "resources": []}, "must be lists"),
            ("nope", "payload must be an object"),
            (mk_payload([dict(base_job, id="a")]), "job id must be an integer"),
        ]
        for payload, fragment in cases:
            self.assert_invalid(payload, fragment)

    def test_scale_limits_are_accepted(self):
        jobs = [
            {"id": i, "holding": [i], "waiting_for": None, "abortable": False}
            for i in range(MAX_JOBS)
        ]
        payload = mk_payload(jobs, extra_resources=range(MAX_JOBS, MAX_RESOURCES))
        self.assertEqual(len(payload["resources"]), MAX_RESOURCES)
        self.assertEqual(solve(payload)["status"], "completed")


# ---------------------------------------------------------------------------
# Randomised cross-check: independent replay + subset enumeration
# ---------------------------------------------------------------------------


class RandomCrossCheckTests(unittest.TestCase):
    def test_random_instances(self):
        for seed in range(150):
            with self.subTest(seed=seed):
                rng = random.Random(seed)
                payload = random_payload(rng)
                snapshot = copy.deepcopy(payload)
                result = solve(payload)

                self.assertEqual(payload, snapshot, "solve must not mutate its input")
                json.dumps(result)  # the reply must be JSON-serialisable

                # Independent replay of the returned event stream.
                active, _, holder = replay_events(payload, result["events"])

                # Independent minimality check by subset enumeration.
                expected = brute_force_min_abort_set(payload)

                if result["status"] == "unresolvable":
                    self.assertIsNone(expected)
                    self.assertEqual(sorted(active), result["stuck"])
                    for jid in result["protected_stuck"]:
                        self.assertIn(jid, active)
                    # Nothing was force-released: stuck jobs keep resources.
                    for r in payload["resources"]:
                        if r["holder"] in result["stuck"]:
                            self.assertEqual(holder[r["id"]], r["holder"])
                else:
                    self.assertEqual(active, set(), "all jobs must complete")
                    self.assertEqual(result["aborted"], expected or [])
                    costs = {j["id"]: j.get("abort_cost") for j in payload["jobs"]}
                    if result["status"] == "resolved_with_aborts":
                        self.assertEqual(
                            result["abort_cost"],
                            sum(costs[j] for j in result["aborted"]),
                        )
                    else:
                        self.assertEqual(result["status"], "completed")
                        self.assertEqual(expected, [])


# ---------------------------------------------------------------------------
# Choice resources (waiting_any) — simulation semantics
# ---------------------------------------------------------------------------


class ChoiceSimulationTests(unittest.TestCase):
    def test_free_alternative_avoids_false_stall(self):
        # Job 1 waits on [2, 3]; r2 is held and r3 is idle.  Only
        # taking the first candidate (the old translation bug) saw
        # r1->r2 blocked forever and reported a stall with an abort
        # suggestion.  Here the holder of r2 is itself blocked on r1,
        # so without using r3 nothing could ever move; the correct run
        # grants r3 immediately.
        payload = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1],
                    "waiting_any": [2, 3],
                    "abortable": True,
                    "abort_cost": 9,
                },
                {
                    "id": 2,
                    "holding": [2],
                    "waiting_for": 1,
                    "abortable": True,
                    "abort_cost": 9,
                },
            ],
            extra_resources=(3,),
        )
        from resource_choices import solve_choices

        result = solve_choices(payload)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["aborted"], [])
        self.assertEqual(
            result["events"],
            [
                {"type": "grant", "job": 1, "resource": 3},
                {"type": "complete", "job": 1, "released": [1, 3]},
                {"type": "grant", "job": 2, "resource": 1},
                {"type": "complete", "job": 2, "released": [1, 2]},
            ],
        )

    def test_waiter_picks_candidate_released_later_by_completion(self):
        # Both candidates start held; the holders complete (id ascending)
        # and the waiting job must then be adjudicated against the
        # resources actually freed, not the first candidate only.
        payload = mk_payload(
            [
                {"id": 1, "holding": [1], "waiting_for": None, "abortable": False},
                {
                    "id": 2,
                    "holding": [],
                    "waiting_any": [1, 2],
                    "abortable": False,
                },
                {"id": 3, "holding": [2], "waiting_for": None, "abortable": False},
            ]
        )
        from resource_choices import solve_choices

        result = solve_choices(payload)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(
            result["events"],
            [
                {"type": "complete", "job": 1, "released": [1]},
                {"type": "complete", "job": 3, "released": [2]},
                {"type": "grant", "job": 2, "resource": 1},
                {"type": "complete", "job": 2, "released": [1]},
            ],
        )

    def test_grant_phase_dynamically_drops_granted_jobs(self):
        # r3 -> smallest waiter (job 2); job 2 must immediately drop out
        # of r4's waiter list, so job 3 still receives r4 in the same
        # phase (and no waiting job gets two candidate resources).
        payload = mk_payload(
            [
                {"id": 2, "holding": [], "waiting_any": [3, 4], "abortable": False},
                {"id": 3, "holding": [], "waiting_any": [3, 4], "abortable": False},
            ]
        )
        from resource_choices import solve_choices

        result = solve_choices(payload)
        self.assertEqual(
            [e for e in result["events"] if e["type"] == "grant"],
            [
                {"type": "grant", "job": 2, "resource": 3},
                {"type": "grant", "job": 3, "resource": 4},
            ],
        )
        self.assertEqual(result["status"], "completed")

    def test_waiting_job_gets_at_most_one_resource_per_phase(self):
        # Both candidates of job 1 are idle; only r3 (smallest idle
        # resource with a waiter) is granted, never both.
        payload = mk_payload(
            [{"id": 1, "holding": [], "waiting_any": [4, 3], "abortable": False}]
        )
        from resource_choices import solve_choices

        result = solve_choices(payload)
        grants = [e for e in result["events"] if e["type"] == "grant"]
        self.assertEqual(grants, [{"type": "grant", "job": 1, "resource": 3}])

    def test_grants_adjudicated_by_resource_id_ascending(self):
        # Job 1 waits on r3, job 2 on [3, 4]; both resources idle.
        # r3 (smaller) adjudicates first and goes to its smallest waiter
        # (job 1); job 2 then takes r4.
        payload = mk_payload(
            [
                {"id": 1, "holding": [], "waiting_for": 3, "abortable": False},
                {"id": 2, "holding": [], "waiting_any": [3, 4], "abortable": False},
            ]
        )
        from resource_choices import solve_choices

        result = solve_choices(payload)
        self.assertEqual(
            [e for e in result["events"] if e["type"] == "grant"],
            [
                {"type": "grant", "job": 1, "resource": 3},
                {"type": "grant", "job": 2, "resource": 4},
            ],
        )

    def test_mixed_waiting_for_and_waiting_any(self):
        # Classic single-wait job and choice job coexist.
        payload = mk_payload(
            [
                {"id": 1, "holding": [1], "waiting_for": 2, "abortable": False},
                {
                    "id": 2,
                    "holding": [2],
                    "waiting_any": [3],
                    "abortable": True,
                    "abort_cost": 5,
                },
            ],
            extra_resources=(3,),
        )
        from resource_choices import solve_choices

        result = solve_choices(payload)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(
            result["events"],
            [
                {"type": "grant", "job": 2, "resource": 3},
                {"type": "complete", "job": 2, "released": [2, 3]},
                {"type": "grant", "job": 1, "resource": 2},
                {"type": "complete", "job": 1, "released": [1, 2]},
            ],
        )

    def test_singleton_waiting_any_matches_waiting_for(self):
        classic = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1],
                    "waiting_for": 2,
                    "abortable": True,
                    "abort_cost": 4,
                },
                {"id": 2, "holding": [2], "waiting_for": None, "abortable": False},
            ]
        )
        choice = copy.deepcopy(classic)
        choice["jobs"][0].pop("waiting_for")
        choice["jobs"][0]["waiting_any"] = [2]
        from resource_choices import solve_choices

        self.assertEqual(solve_choices(choice)["events"], solve(classic)["events"])


# ---------------------------------------------------------------------------
# Choice resources (waiting_any) — deadlock resolution
# ---------------------------------------------------------------------------


class ChoiceResolutionTests(unittest.TestCase):
    def test_free_alternative_means_no_abort_at_all(self):
        # 1<->2 hold each other's first candidate, but job 1 has a free
        # alternative r3: recovery must complete everything, not abort.
        payload = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1],
                    "waiting_any": [2, 3],
                    "abortable": True,
                    "abort_cost": 9,
                },
                {
                    "id": 2,
                    "holding": [2],
                    "waiting_any": [1],
                    "abortable": True,
                    "abort_cost": 9,
                },
            ],
            extra_resources=(3,),
        )
        from resource_choices import solve_choices

        result = solve_choices(payload)
        self.assertEqual(result["status"], "completed")
        self.assertFalse(any(e["type"] == "abort" for e in result["events"]))
        self.assertEqual(
            result["events"],
            [
                {"type": "grant", "job": 1, "resource": 3},
                {"type": "complete", "job": 1, "released": [1, 3]},
                {"type": "grant", "job": 2, "resource": 1},
                {"type": "complete", "job": 2, "released": [1, 2]},
            ],
        )

    def test_choice_cycle_aborts_cheapest_job(self):
        payload = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1],
                    "waiting_any": [2],
                    "abortable": True,
                    "abort_cost": 5,
                },
                {
                    "id": 2,
                    "holding": [2],
                    "waiting_any": [1],
                    "abortable": True,
                    "abort_cost": 3,
                },
            ]
        )
        from resource_choices import solve_choices

        result = solve_choices(payload)
        self.assertEqual(result["status"], "resolved_with_aborts")
        self.assertEqual(result["aborted"], [2])
        self.assertEqual(result["abort_cost"], 3)
        self.assertEqual(
            result["events"],
            [
                {"type": "abort", "job": 2, "released": [2]},
                {"type": "grant", "job": 1, "resource": 2},
                {"type": "complete", "job": 1, "released": [1, 2]},
            ],
        )

    def test_choice_unlocks_after_cheapest_abort(self):
        # j1 waits on candidates [2,3], both held; j2 (holds r2) waits
        # on r1, j3 (holds r3) waits on r1 too.  Everything is stuck.
        # Aborting j2 (cost 1) frees r2 and resolves the stall; aborting
        # j1 costs 8, so the cheapest plan is {2} -- recovery is
        # computed from the real stalled state via independent replays.
        payload = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1],
                    "waiting_any": [2, 3],
                    "abortable": True,
                    "abort_cost": 8,
                },
                {
                    "id": 2,
                    "holding": [2],
                    "waiting_for": 1,
                    "abortable": True,
                    "abort_cost": 1,
                },
                {
                    "id": 3,
                    "holding": [3],
                    "waiting_for": 1,
                    "abortable": True,
                    "abort_cost": 7,
                },
            ]
        )
        from resource_choices import solve_choices

        result = solve_choices(payload)
        self.assertEqual(result["aborted"], [2])
        self.assertEqual(result["abort_cost"], 1)
        active, _, _ = replay_events(payload, result["events"])
        self.assertEqual(active, set())

    def test_equal_cost_choice_tie_breaks_by_sorted_ids(self):
        # Full 3-cycle: each job waits on a candidate held by another.
        # Aborting any single job costs 2 and resolves it, so the
        # lexicographically smallest sorted id list ([1]) wins.
        payload = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1],
                    "waiting_any": [2],
                    "abortable": True,
                    "abort_cost": 2,
                },
                {
                    "id": 2,
                    "holding": [2],
                    "waiting_any": [3],
                    "abortable": True,
                    "abort_cost": 2,
                },
                {
                    "id": 3,
                    "holding": [3],
                    "waiting_any": [1],
                    "abortable": True,
                    "abort_cost": 2,
                },
            ]
        )
        from resource_choices import solve_choices

        result = solve_choices(payload)
        self.assertEqual(result["aborted"], [1])
        self.assertEqual(result["abort_cost"], 2)


# ---------------------------------------------------------------------------
# Choice resources (waiting_any) — unresolvable / validation / non-mutation
# ---------------------------------------------------------------------------


class ChoiceUnresolvableTests(unittest.TestCase):
    def test_protected_choice_cycle_is_unresolvable(self):
        payload = mk_payload(
            [
                {"id": 1, "holding": [1], "waiting_any": [2], "abortable": False},
                {"id": 2, "holding": [2], "waiting_any": [1], "abortable": False},
            ]
        )
        snapshot = copy.deepcopy(payload)
        from resource_choices import solve_choices

        result = solve_choices(payload)
        self.assertEqual(result["status"], "unresolvable")
        self.assertEqual(result["stuck"], [1, 2])
        self.assertEqual(result["protected_stuck"], [1, 2])
        self.assertIn("protected", result["message"].lower())
        self.assertEqual(result["events"], [])
        self.assertEqual(payload, snapshot)
        active, _, holder = replay_events(payload, result["events"])
        self.assertEqual(active, {1, 2})
        self.assertEqual(holder[1], 1)
        self.assertEqual(holder[2], 2)


class ChoiceValidationTests(unittest.TestCase):
    def assert_invalid(self, payload, fragment):
        from resource_choices import solve_choices

        with self.subTest(fragment=fragment):
            with self.assertRaises(ValueError) as ctx:
                solve_choices(payload)
            self.assertIn(fragment, str(ctx.exception))

    def test_waiting_any_validation(self):
        resources = [
            {"id": 1, "holder": 1},
            {"id": 2, "holder": None},
            {"id": 3, "holder": None},
        ]

        def job(**kw):
            return {"id": 1, "holding": [1], "abortable": False, **kw}

        cases = [
            ({"jobs": [job(waiting_any=[2, 2])], "resources": resources}, "more than once"),
            ({"jobs": [job(waiting_any=[2, 9])], "resources": resources}, "unknown resource 9"),
            ({"jobs": [job(waiting_any=[1, 2])], "resources": resources}, "already holds"),
            ({"jobs": [job(waiting_any=[])], "resources": resources}, "non-empty"),
            ({"jobs": [job(waiting_any="x")], "resources": resources}, "non-empty"),
            ({"jobs": [job(waiting_any=[2, "x"])], "resources": resources}, "list of resource ids"),
            (
                {
                    "jobs": [job(waiting_for=2, waiting_any=[2, 3])],
                    "resources": resources,
                },
                "not both",
            ),
        ]
        for payload, fragment in cases:
            self.assert_invalid(payload, fragment)

    def test_classic_endpoint_rejects_waiting_any(self):
        payload = mk_payload(
            [{"id": 1, "holding": [], "waiting_any": [2], "abortable": False}],
            extra_resources=(2,),
        )
        with self.assertRaises(ValueError) as ctx:
            solve(payload)
        self.assertIn("choices endpoint", str(ctx.exception))

    def test_null_waiting_any_means_not_waiting(self):
        payload = mk_payload(
            [{"id": 1, "holding": [], "waiting_any": None, "abortable": False}]
        )
        from resource_choices import solve_choices

        result = solve_choices(payload)
        self.assertEqual(
            result["events"], [{"type": "complete", "job": 1, "released": []}]
        )


class ChoiceInputUntouchedTests(unittest.TestCase):
    def test_payload_never_rewritten(self):
        payload = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1],
                    "waiting_any": [2, 3],
                    "abortable": True,
                    "abort_cost": 2,
                },
                {"id": 2, "holding": [2], "waiting_any": [1], "abortable": False},
            ],
            extra_resources=(3,),
        )
        snapshot = copy.deepcopy(payload)
        from resource_choices import solve_choices

        result = solve_choices(payload)
        self.assertEqual(payload, snapshot)
        json.dumps(result)
        # The candidate list is visible verbatim in the replay: job 1
        # took r3, its declared candidate, and never shows a rewritten
        # singleton wait on r2.
        self.assertEqual(result["events"][0], {"type": "grant", "job": 1, "resource": 3})


# ---------------------------------------------------------------------------
# Randomised cross-check for choice resources
# ---------------------------------------------------------------------------


class ChoiceRandomCrossCheckTests(unittest.TestCase):
    def test_random_choice_instances(self):
        from resource_choices import solve_choices

        for seed in range(300):
            with self.subTest(seed=seed):
                rng = random.Random(seed)
                payload = random_choice_payload(rng)
                snapshot = copy.deepcopy(payload)
                result = solve_choices(payload)
                expected = reference_solve(payload)

                self.assertEqual(payload, snapshot, "input must not be mutated")
                json.dumps(result)

                # Complete ordered trace, status and recovery decision
                # must match the independent reference solver exactly.
                self.assertEqual(result["status"], expected["status"])
                self.assertEqual(result["events"], expected["events"])
                if result["status"] == "unresolvable":
                    self.assertEqual(result["stuck"], expected["stuck"])
                    self.assertEqual(
                        result["protected_stuck"], expected["protected_stuck"]
                    )
                else:
                    self.assertEqual(result["aborted"], expected["aborted"])
                    self.assertEqual(result["abort_cost"], expected["abort_cost"])

                # Independent legality replay of the returned events.
                active, _, holder = replay_events(payload, result["events"])
                if result["status"] == "unresolvable":
                    self.assertEqual(sorted(active), result["stuck"])
                    for jid in result["protected_stuck"]:
                        self.assertIn(jid, active)
                    for r in payload["resources"]:
                        if r["holder"] in result["stuck"]:
                            self.assertEqual(holder[r["id"]], r["holder"])
                else:
                    self.assertEqual(active, set())
                    self.assertEqual(
                        result["aborted"],
                        brute_force_min_abort_set(payload, allow_choices=True) or [],
                    )


# ---------------------------------------------------------------------------
# HTTP backend
# ---------------------------------------------------------------------------


class ServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = create_server("127.0.0.1", 0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.thread.join()

    def _post(self, path, body):
        conn = http.client.HTTPConnection("127.0.0.1", self.port)
        conn.request(
            "POST", path, json.dumps(body), {"Content-Type": "application/json"}
        )
        response = conn.getresponse()
        data = json.loads(response.read())
        conn.close()
        return response.status, data

    def test_simulate_endpoint(self):
        payload = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1],
                    "waiting_for": 2,
                    "abortable": True,
                    "abort_cost": 5,
                },
                {
                    "id": 2,
                    "holding": [2],
                    "waiting_for": 1,
                    "abortable": True,
                    "abort_cost": 3,
                },
            ]
        )
        status, data = self._post("/simulate", payload)
        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "resolved_with_aborts")
        self.assertEqual(data["aborted"], [2])

    def test_invalid_payload_returns_400(self):
        status, data = self._post("/simulate", {"jobs": [{"id": "x"}], "resources": []})
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_choices_endpoint_uses_free_alternative(self):
        payload = mk_payload(
            [
                {
                    "id": 1,
                    "holding": [1],
                    "waiting_any": [2, 3],
                    "abortable": True,
                    "abort_cost": 9,
                },
                {
                    "id": 2,
                    "holding": [2],
                    "waiting_for": 1,
                    "abortable": True,
                    "abort_cost": 9,
                },
            ],
            extra_resources=(3,),
        )
        status, data = self._post("/simulate/choices", payload)
        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "completed")
        self.assertEqual(data["events"][0], {"type": "grant", "job": 1, "resource": 3})

    def test_choices_endpoint_rejects_bad_candidates_with_400(self):
        payload = {
            "jobs": [{"id": 1, "holding": [], "waiting_any": [2, 2]}],
            "resources": [{"id": 2, "holder": None}],
        }
        status, data = self._post("/simulate/choices", payload)
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_choices_endpoint_rejects_mixed_wait_specs_with_400(self):
        payload = {
            "jobs": [{"id": 1, "holding": [], "waiting_for": 2, "waiting_any": [2]}],
            "resources": [{"id": 2, "holder": None}],
        }
        status, data = self._post("/simulate/choices", payload)
        self.assertEqual(status, 400)
        self.assertIn("not both", data["error"])

    def test_classic_endpoint_rejects_choices_field_with_400(self):
        payload = {
            "jobs": [{"id": 1, "holding": [], "waiting_any": [2], "abortable": False}],
            "resources": [{"id": 2, "holder": None}],
        }
        status, _ = self._post("/simulate", payload)
        self.assertEqual(status, 400)

    def test_health_endpoint(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port)
        conn.request("GET", "/health")
        response = conn.getresponse()
        self.assertEqual(response.status, 200)
        self.assertEqual(json.loads(response.read()), {"status": "ok"})
        conn.close()


if __name__ == "__main__":
    unittest.main()
