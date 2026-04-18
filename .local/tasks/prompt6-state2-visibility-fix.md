# Prompt 6 — Make State 2 Mark tile actually appear

## What & Why
Right now, when a user creates a study with the type set, the brief auto-precheck almost always passes immediately (because Market/Geography, Product/Concept, and Target Audience are pre-filled from defaults). The `derive_ui_phase` helper currently treats `qa_status == 'precheck_passed'` as "execution ready" and **hides the Mark tile entirely** — so the State 2 minimised "Brief Consistency Helper" the spec calls for is effectively never seen.

Per the Prompt 6 spec, State 2 means "after a study type is set but before execution readiness". Execution readiness should mean the study has actually been kicked off (status moved out of `draft`), not merely that the brief currently passes precheck. While the brief is still a draft, the user should see the minimised Mark tile so they can sanity-check alignment with the saved Business Problem and Decision before starting the run.

This was also flagged as the non-blocking comment in the latest code review for Task #45.

## Done looks like
- After setting a study type (e.g. `synthetic_idi`), the user always sees the minimised Mark tile labelled **"Mark — Brief Consistency Helper (optional)"**, with a one-line summary and an **Expand** button — regardless of whether the precheck currently passes or fails.
- Clicking **Expand** reveals exactly the three buttons: *Check alignment with Business Problem*, *Check alignment with Decision*, *Remind me what question this study is answering*.
- The Mark tile only disappears once the study has actually moved past `draft` status (i.e. has been started / is executing / completed). Before that, even a brief that passes precheck still shows the State 2 tile.
- Existing State 1 (Problem Framing Helper, before any study type) and the alignment-check route continue to work; the route's server-side phase guard is updated in lock-step so it accepts requests from any draft study with a study type set.
- Test suite still passes (12/12), with derivation cases and the alignment-check route test updated to match the new mapping.

## Out of scope
- Any change to Mark replies, system prompts, or telemetry behaviour.
- Re-introducing Mark in State 3 (study already started). That stays hidden — see follow-up that was previously cancelled.
- Changes to the precheck logic itself or to how MG/PC/TA defaults are pre-filled at study creation.
- Visual redesign of the State 2 tile beyond what already exists.

## Steps
1. **Narrow the State 3 condition in `derive_ui_phase`** so STEP_3_EXECUTION_READY is reached only when the study's `status` is no longer `draft`. Drop the `qa_status == 'precheck_passed'` branch — a passing precheck while still in draft now resolves to STEP_2_ANCHORS, keeping the minimised Mark tile visible.
2. **Update the alignment-check route's server-side phase guard** to use the same revised helper, so a draft study with a passing precheck is no longer rejected with 409.
3. **Update the Prompt 6 derivation tests** so the precheck-passed-while-draft case asserts STEP_2_ANCHORS, and remove the `ta=""` workaround in the alignment-check route test (it was only needed to keep precheck failing under the old logic).
4. **Restart the brainstorm workflow** and re-run `python -m unittest test_prompt6_mark_presence test_prompt5_continuity -v`; expect 12/12 passing.
5. **Manually verify in the Preview** by opening any draft study with a study type set: the minimised Mark tile must appear, expand correctly, and run the three alignment buttons end-to-end against the live route.

## Relevant files
- `artifacts/brainstorm/app.py:354-380`
- `artifacts/brainstorm/app.py:3436-3438`
- `artifacts/brainstorm/app.py:7995-8007`
- `artifacts/brainstorm/templates/index.html:626-717`
- `artifacts/brainstorm/test_prompt6_mark_presence.py`
- `replit.md`
