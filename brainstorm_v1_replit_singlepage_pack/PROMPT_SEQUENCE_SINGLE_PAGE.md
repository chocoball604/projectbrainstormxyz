# PROMPT SEQUENCE — SINGLE PAGE APP (copy/paste into Replit coding agent)

IMPORTANT RULES FOR YOU (the coding agent):
- Follow `00_FROZEN_RULES_FROM_PRD.md` exactly.
- Keep UI as ONE main page that switches sections.
- Use Python + Flask + SQLite.
- After each step, the app must run.

---

## PROMPT 1 — Make the simplest runnable app
Create a Python Flask app that serves ONE HTML page (index.html).
That one page must show different sections depending on state:
- Logged out (login/signup form)
- Pending approval message
- Active dashboard

Use SQLite to store users with fields:
- id, email, username, password_hash, state (pending/active/disabled), created_at

Add a very simple Admin login (hardcoded admin password in .env for now) so I can approve users.

Deliverables:
- Flask app runs
- One HTML page
- Can sign up
- After signup, user becomes Pending
- Admin can approve user (changes to Active)

---

## PROMPT 2 — Add the Study list (still one page)
On the Active dashboard section, add a Study List showing studies for the logged-in user.
Each study row shows:
- title
- study_type
- status (draft/in_progress/qa_blocked/terminated_system/terminated_user/completed)
- created_at

Add a button: “New Research”.

Deliverables:
- Study list works
- New Research button opens the Research Brief section (still on the same page)

---

## PROMPT 3 — Add the Research Brief (6 anchors)
Add a “New Research” section on the same page.
The user must answer these 6 required anchors (one screen with 6 fields is fine):
1) Business Problem
2) Decision to Support
3) Known vs Unknown
4) Target Audience
5) Study Fit (why this study + what it cannot answer)
6) Definition of Useful Insight

Do not allow saving unless all 6 fields are filled.
When saved, create a study with status = draft.

Deliverables:
- Draft study saved
- Shows up in Study list

---

## PROMPT 4 — Choose study type and limits
Add a study type selector for each draft study:
- synthetic_survey
- synthetic_idi
- synthetic_focus_group

Enforce simple limits:
- survey: max 12 questions, max 400 respondents
- idi: 1–3 personas
- focus group: 4–6 personas

Store study_type on the study.

Deliverables:
- Draft study can be configured with a study type

---

## PROMPT 5 — Personas (create, save, version, view)
Add a Persona section (still same page) with:
- Create persona (with dossier fields)
- Save persona (auto assigns persona_id + version v1, v2...)
- View persona (read-only)

Persona dossier must include:
- Persona Summary
- Demographic Frame
- Psychographic Profile
- Contextual Constraints
- Behavioural Tendencies
- AI Model Provenance (provider + model id + selection method)
- Grounding Sources (list)
- Confidence and Limits

Deliverables:
- Personas can be created and viewed
- Saved persona versions cannot be edited

---

## PROMPT 6 — Grounding Trace logging
Implement Grounding Trace logging using `schemas/grounding_trace.schema.json`.
Every time a persona is created OR a study is executed, write a Grounding Trace record.
Also add an Admin‑Directed Web Sources table (admin only) with fields:
- url, name, city(optional), country(optional), language, status(active/disabled)

When generating a grounding trace:
- If admin sources exist and are active, set admin_sources_configured=true and admin_sources_queried=true.
- For now you can set matched/used to false and require a reason code.

Deliverables:
- Grounding traces stored
- Reason code enforced when not used
- Admin can manage admin-directed sources

---

## PROMPT 7 — Execute studies (placeholder outputs)
Add a button “Run Study” for draft studies.
When clicked:
- set status=in_progress
- generate placeholder outputs based on study type:
  - survey: fake aggregated results (clearly labeled PLACEHOLDER)
  - idi: fake transcript (clearly labeled PLACEHOLDER)
  - focus group: fake transcript (clearly labeled PLACEHOLDER)

Then send the output to Ben QA step.

Deliverables:
- Draft -> In progress -> QA step flow works

---

## PROMPT 8 — Ben QA gate + confidence labels
Implement Ben QA using `qa_rules.md`.
Ben must output PASS / DOWNGRADE / FAIL.
If FAIL: status=qa_blocked.
If PASS: status=completed.
If DOWNGRADE: status=completed AND any insights labeled “Indicative” or “Exploratory”.

Deliverables:
- QA decisions affect study status
- Confidence labels exist on the report

---

## PROMPT 9 — Cost telemetry + enforce token ceilings
Implement cost telemetry logging using `schemas/cost_telemetry.schema.json`.
Every time a study runs, write a cost record.
If your model provider does not give token counts, use placeholders, but keep the structure.

Enforce token ceilings from `budget_limits.yaml`:
- If the total would exceed the ceiling, stop and set status=terminated_system.

Deliverables:
- Cost telemetry records exist
- Budget enforcement works

---

## PROMPT 10 — Make it easy to export measurement data
Add an Admin-only button: “Export Telemetry”.
It should download a CSV of:
- studies
- cost telemetry
- grounding traces

Deliverables:
- CSV export works (pricing measurement support)
