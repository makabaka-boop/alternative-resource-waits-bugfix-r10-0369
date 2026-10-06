import copy
from deadlock_simulator import solve


def solve_choices(payload):
    translated = copy.deepcopy(payload)
    for job in translated["jobs"]:
        choices = job.pop("waiting_any", None)
        if choices is not None:
            job["waiting_for"] = choices[0] if choices else None
    return solve(translated)
