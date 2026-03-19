# FROZEN RULES FROM PRD — Project Brainstorm V1

These rules are ABSOLUTE. If your implementation conflicts with any rule below,
you must STOP and add a TODO comment, then implement the safer/stricter option.
Do NOT invent new behavior.

## User Lifecycle Rules

1. New users sign up with email + password.
2. New users start in status=PENDING. They cannot access the dashboard until approved.
3. Only an ADMIN can promote a user from PENDING → ACTIVE.
4. An ADMIN is identified by role=admin in the database.
5. The first registered user is automatically made ACTIVE (not admin). Admins must be set manually in the DB.

## Research Brief Rules

6. A Research Brief requires exactly 6 answers before it can be submitted.
7. The 6 required questions are:
   - Q1: What is the research objective?
   - Q2: Who is the target audience?
   - Q3: What key decisions will this research inform?
   - Q4: What is the timeline for results?
   - Q5: What is the budget range?
   - Q6: Any constraints or special requirements?
8. A brief cannot be saved if any of the 6 answers is blank.

## Study Rules

9. A study must be linked to an approved Research Brief.
10. Study types allowed: Survey, IDI (In-Depth Interview), Focus Group.
11. A study starts in status=DRAFT.
12. Running a study transitions it to status=RUNNING (placeholder execution).
13. A study can only be QA'd after it is RUNNING or COMPLETED.

## QA Rules (Ben QA)

14. QA results are: PASS, DOWNGRADE, or FAIL — no other values allowed.
15. QA is performed per study run.
16. QA result must be logged to grounding_trace before being saved.

## Telemetry Rules

17. Every study run must log a Grounding Trace (see schemas/grounding_trace.schema.json).
18. Every study run must log Cost Telemetry (see schemas/cost_telemetry.schema.json).
19. Telemetry must be exportable as JSON.
20. Cost values are PLACEHOLDER unless clearly labeled as real.

## Security Rules

21. Passwords must be hashed before storage (use werkzeug.security).
22. Session-based auth only (Flask sessions). No JWT.
23. Admin-only endpoints must check role=admin and return 403 if not.
24. Users can only see their own briefs, studies, and runs (not other users').
