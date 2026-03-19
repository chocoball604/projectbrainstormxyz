# QA Rules — Project Brainstorm V1 (Ben QA)

## Overview

Ben QA is the internal quality assurance step applied to every completed study run.
It must be performed before a study is considered final.

## QA Verdicts

Only three verdicts are allowed:

| Verdict    | Meaning                                                      | Next Step                         |
|------------|--------------------------------------------------------------|-----------------------------------|
| PASS       | Study output meets quality bar                               | Mark study as COMPLETED           |
| DOWNGRADE  | Study output is usable but below ideal quality               | Mark study as COMPLETED_DOWNGRADE |
| FAIL       | Study output does not meet minimum bar                       | Mark study as FAILED              |

## QA Checklist (PLACEHOLDER — non-automated)

The following checks are PLACEHOLDER descriptions. Real automated QA is not implemented.

1. **Grounding Check** — Did the run produce a grounding trace? (Y/N)
2. **Response Completeness** — Did all simulated responses arrive? (Y/N)
3. **Cost Within Budget** — Did cost stay under budget_limits.yaml thresholds? (Y/N)
4. **Study Type Match** — Did the run match the declared study type? (Y/N)
5. **No Hallucination Flags** — Were there any hallucination flags logged? (Y/N)

## QA Logging Requirement

- QA result MUST be written to the grounding_trace table before the study status is updated.
- QA result MUST reference the study_run_id.

## Escalation

- Two or more FAIL results on the same study → flag for admin review.
- This rule is logged but not automatically enforced in V1 (TODO).
