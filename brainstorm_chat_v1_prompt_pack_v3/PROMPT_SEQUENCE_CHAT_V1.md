# PROMPT SEQUENCE — CHAT-FIRST V1 (Prompts 11–25)

Paste ONE prompt at a time into the Replit coding agent. After each prompt, run the app and do the tests in `TEST_CHECKLIST_CHAT_V1.md`.

---

## PROMPT 11 — Study Selection Funnel (chat starts after selecting/opening a study)
Implement ONLY this prompt.

Goal:
- Make it explicit that chat starts only after a user opens a study.

Requirements:
1) Update Dashboard UX to show a clear funnel:
   - Step 1: Create Draft Study (title + study type)
   - Step 2: Open the Draft Study to continue setup
2) “New Research” creates a Draft study with:
   - title
   - study_type (synthetic_survey / synthetic_idi / synthetic_focus_group)
   - status=draft
3) No chat on dashboard.

Deliverables:
- Files changed
- How to test

---

## PROMPT 12 — Add Study-Scoped Chat + Message Logging (no smart Mark yet)
Implement ONLY this prompt.

Requirements:
1) Add DB table `chat_messages`:
   - id, study_id, sender ('user'|'mark'), message_text, timestamp_utc
2) In Study Detail view, add:
   - Chat thread
   - Input box + Send
3) When user sends a message:
   - save user message
   - create a simple canned Mark reply and save it
4) Chat appears only inside Study Detail.

Deliverables:
- Files changed
- How to test

---

## PROMPT 13 — Add Side Panel “Brief so far” + Missing Items checklist
Implement ONLY this prompt.

Requirements:
1) Add a right-side panel in Study Detail showing:
   - study title, type, status
   - checklist of required items by study type

Survey required:
- respondent_count set
- question_count set
- survey questions count EXACTLY equals question_count

IDI/FG required:
- all 6 anchors complete (Business Problem, Decision, Known/Unknown, Target, Fit, Useful Insight)
- persona count within bounds (IDI 1–3, FG 4–6)

2) Add “Ready for QA Review” button disabled until checklist complete.

Deliverables:
- Files changed
- How to test

---

## PROMPT 14 — Mark Coaching Nudges (chat suggests next missing item)
Implement ONLY this prompt.

Requirements:
1) When user sends any message, Mark replies with ONE coaching nudge for the NEXT missing item.
2) Study-type aware nudges:
   - Survey: coach toward config + question list
   - IDI/FG: coach toward missing anchors, then personas
3) No automatic writing to fields yet.

Deliverables:
- Files changed
- How to test

---

## PROMPT 15 — Inline “Save this as …” controls (user-confirmed field filling)
Implement ONLY this prompt.

Requirements:
1) After a user message, Mark offers 1–3 inline buttons such as:
   - Save as Business Problem
   - Save as Target Audience
   - Save as Known vs Unknown
   - Save as Survey Question (append)
2) Clicking a button saves the user’s LAST message into the selected field.
3) Side panel checklist updates immediately.
4) Never auto-write without a click.

Deliverables:
- Files changed
- How to test

---

## PROMPT 16 — Branching setup inside Study Detail (Survey vs IDI/FG)
Implement ONLY this prompt.

Goal:
Make Study Detail the main setup surface:
- Survey: respondent_count + question_count + questions builder
- IDI/FG: 6 anchors + persona attach/detach

Requirements:
1) Survey studies must NOT show persona attach UI.
2) IDI/FG must show persona attach UI with dropdown filtering out already attached personas.
3) Keep side panel checklist + Ready button.

Deliverables:
- Files changed
- How to test

---

## PROMPT 17 — Ben Pre-Execution QA Gate (before Run Study)
Implement ONLY this prompt.

Requirements:
1) Clicking “Ready for QA Review” runs a Ben precheck.
2) If FAIL:
   - show missing items
   - keep study in draft (or mark qa_blocked_precheck if you already have a field)
3) If PASS:
   - enable “Run Study”
4) Do NOT change Ben post-run QA.

Deliverables:
- Files changed
- How to test

---

## PROMPT 18 — Report Viewer + PDF Download (minimum PRD output)
Implement ONLY this prompt.

Requirements:
1) Add Report Viewer sections:
   - Executive Summary
   - What was studied
   - Key findings (with confidence labels)
   - Risks/unknowns
   - Sources/citations (placeholder ok)
2) Add “Download PDF” button for the report.

Deliverables:
- Files changed
- How to test

---

## PROMPT 19 — Follow-ups for IDI/FG (max 2 rounds)
Implement ONLY this prompt.

Requirements:
1) Allow follow-up questions ONLY for IDI/FG and only after completion.
2) Max 2 follow-up rounds.
3) Append follow-up outputs to the same study record.
4) Block follow-ups for surveys.

Deliverables:
- Files changed
- How to test

---

## PROMPT 20 — Usage meters + limit enforcement (minimum)
Implement ONLY this prompt.

Requirements:
1) Track “studies run this month” per user.
2) Show meter on dashboard: “X of Y used”.
3) If limit reached, disable “New Research”.
4) Admin can set monthly limit (minimum: Free tier only is OK).

Deliverables:
- Files changed
- How to test

---

## PROMPT 21 — User + Admin Document Uploads (Grounding) + file type limits
Implement ONLY this prompt.

Goal:
Add document uploads to improve grounding:
- User uploads are private to the user and study-scoped
- Admin uploads are global

Requirements:
A) Enforce limits (V1):
- Max files per study: 10
- Max size per file: 1MB
- Max total upload per study: 10MB
- Allowed file types: PDF, DOCX, TXT, CSV, PNG, JPG/JPEG

B) Data model:
- Create table user_uploads: id, user_id, study_id, filename, file_type, file_size_bytes, storage_path, uploaded_at
- Create table admin_uploads: id, filename, file_type, file_size_bytes, storage_path, uploaded_at, status(active/disabled)

C) UI:
- In Study Detail, add “Uploads” section to upload files for that study (user only)
- In Admin panel, add “Global Documents” section to upload admin documents and enable/disable them

D) Minimal integration:
- Store and display uploaded files list.
- Show uploaded files names in “Grounding Sources” display.

Deliverables:
- Files changed
- How to test

---

## PROMPT 22 — Admin Model Configuration (Mark/Lisa/Ben) + Persona Model Pool (specific model IDs)
Implement ONLY this prompt.

Goal:
Allow Admin to configure:
1) Specific model ID Mark uses
2) Specific model ID Lisa uses
3) Specific model ID Ben uses
4) Pool of specific model IDs for persona generation (random selection)

Requirements:
A) Data model:
- model_config: key (mark_model, lisa_model, ben_model), value (model_id)
- persona_model_pool: model_id, status(active/disabled)

B) Admin UI:
- “Model Configuration” section (set Mark/Lisa/Ben model IDs)
- “Persona Model Pool” editor (add/disable/remove)

C) Runtime behavior:
- When creating a new persona (auto-generation path), record provenance:
  selection_method="random from pool" and model_id=random active pool entry
- If pool empty, block auto-generation with message.

Deliverables:
- Files changed
- How to test

---

## PROMPT 23 — Marketing Landing Page (12 sections from Appendix 2)
Implement ONLY this prompt.

Goal:
Add a public, unauthenticated landing page with the 12 ordered sections.

Requirements:
1) Create a public route (e.g., / or /landing) that shows the marketing landing page.
2) Implement the 12 sections in the exact order defined in PRD:
   1 Hero
   2 The Problem
   3 The Insight Gap
   4 Our Solution
   5 Who It’s For
   6 Why Use Project Brainstorm
   7 What Makes Us Different
   8 What You Get
   9 One-Line Differentiator
   10 Final CTA
   11 See How It Works
   12 Blog/News (preview)
3) Use the exact copy from Appendix 2 for sections 1–11.
4) Section backgrounds alternate: White then Ocean Blue #064273 repeating.
5) Header must include Sign Up / Login / Blog/News / Language selector for unauthenticated users.

Deliverables:
- Files changed
- How to test

---

## PROMPT 24 — Blog/News (public list + post view + previews)
Implement ONLY this prompt.

Goal:
Create a simple public Blog/News section used in three places:
- Header link
- Landing page preview section
- User dashboard preview (1–2 latest posts)

Requirements:
1) Public /blog page listing posts (title + date).
2) Public /blog/<id> page showing full post.
3) Store blog posts in DB table blog_posts (id, title, slug, body, created_at, status).
4) Admin-only: add a minimal way to create a post (can be a simple form).
5) Landing page section #12 shows 1–2 latest posts.
6) User dashboard shows 1–2 latest posts.

Deliverables:
- Files changed
- How to test

---

## PROMPT 25 — Manage Account basics + email verification cadence (minimal)
Implement ONLY this prompt.

Goal:
Implement a minimal Manage Account page and basic email verification cadence as described in PRD.

Requirements (minimal V1):
1) Manage Account page for logged-in users:
   - view/edit profile fields (name, company, role, location, LinkedIn optional)
   - change password (requires current password)
2) Email verification cadence (minimal implementation):
   - store last_email_verification_timestamp
   - require verification on first login and at least once every 7 days
   - implement verification by a simple one-time code shown in-app for prototype (no real email sending required)
3) If verification required and not completed, block access to dashboard and show verification screen.

Deliverables:
- Files changed
- How to test
