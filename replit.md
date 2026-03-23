# Workspace

## Overview

pnpm workspace monorepo using TypeScript. Each package manages its own dependencies.

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)

## Structure

```text
artifacts-monorepo/
├── artifacts/              # Deployable applications
│   ├── api-server/         # Express API server
│   └── brainstorm/         # Project Brainstorm V1 — Flask + SQLite single-page app
│       ├── app.py          # Flask backend (token-based auth, studies, admin)
│       ├── templates/
│       │   └── index.html  # Single-page template with section switching
│       └── brainstorm.db   # SQLite database (auto-created)
├── lib/                    # Shared libraries
│   ├── api-spec/           # OpenAPI spec + Orval codegen config
│   ├── api-client-react/   # Generated React Query hooks
│   ├── api-zod/            # Generated Zod schemas from OpenAPI
│   └── db/                 # Drizzle ORM schema + DB connection
├── scripts/                # Utility scripts (single workspace package)
│   └── src/                # Individual .ts scripts
├── brainstorm_v1_replit_singlepage_pack/  # Frozen rules, schemas, task files
│   ├── 00_FROZEN_RULES_FROM_PRD.md
│   ├── PROMPT_SEQUENCE_SINGLE_PAGE.md
│   ├── budget_limits.yaml
│   └── schemas/
├── pnpm-workspace.yaml
├── tsconfig.base.json
├── tsconfig.json
└── package.json
```

## Brainstorm App (artifacts/brainstorm)

Python Flask + SQLite single-page app for AI-Native Market Research.

- **Auth**: Token-based (URL query param `?token=xxx`) — needed because Replit preview iframe blocks cookies. Sessions stored in `sessions` table.
- **Admin password**: `admin123` (env var `ADMIN_PASSWORD`)
- **DB tables**: `users`, `sessions`, `studies`, `personas`, `admin_web_sources`, `grounding_traces`
- **Prompt progress**: Prompts 1–8 complete
  - P1: Auth/signup/admin approval
  - P2: Study list + "New Research" button
  - P3: Research Brief with 6 required anchors
  - P4: Study type selector + limits
  - P5: Personas — immutable, clone-as-new, no versioning, `persona_instance_id` model
  - P6: Grounding Trace logging + Admin-Directed Web Sources
  - P7: Execute studies with placeholder outputs, branching new research flow, UX fixes
  - P8: Ben QA Gate — PASS/FAIL/DOWNGRADE decisions, confidence labels (Strong/Indicative/Exploratory), qa_blocked status, final_report
  - P9: Cost telemetry + budget ceilings (100K survey, 150K IDI, 300K FG). `cost_telemetry` table. Admin telemetry view.
  - P10: Admin-only CSV export — 3 routes for studies, cost_telemetry, grounding_traces.
  - Flow Change: New Research now offers "Let Mark recommend" (creates TBD study → discovery → recommendation) or "I already know" (existing flow).
  - Bug Fix: Survey branch now requires entering exactly `question_count` questions via `/save-survey-questions`. IDI/FG branch now requires completing all 6 Research Brief anchors via `/save-remaining-anchors`. Run Study gated on completeness in both UI and server-side.
  - P11: Study Selection Funnel — dashboard shows 2-step guide, "Open Study" replaces "Configure", Study Detail view, no chat on dashboard.
  - P12: Study-scoped chat — `chat_messages` table (id, study_id, sender, message_text, timestamp_utc). Chat thread + input in Study Detail only. Canned Mark reply on each message. `/send-chat/<id>` route.
  - P13: Side panel "Brief So Far" — right-side panel in Study Detail with title/type/status + checklist. Survey: respondent_count, question_count, questions match. IDI/FG: 6 anchors + persona bounds. "Ready for QA Review" button disabled until complete. `/ready-for-qa/<id>` route sets `qa_status=pending_review`.
  - P14: Mark coaching nudges — `get_coaching_nudge()` returns one nudge per missing item. Survey: respondent_count → question_count → questions match. IDI/FG: anchors one-by-one → personas. TBD: BP → DS. No auto-writing.
  - P15: Inline "Save this as…" buttons — context-aware buttons after Mark's reply. `/save-chat-field/<id>` saves user's last message into the chosen field. Survey: "Save as Survey Question" (append). IDI/FG: up to 3 missing anchor buttons. TBD: BP/DS only. Side panel updates after save.
  - P16: Branching setup — Survey shows respondent_count + question_count + questions builder, NO persona UI. IDI/FG shows anchors + persona attach/detach with filtered dropdown. Side panel + Ready button + chat + save buttons all preserved.
  - P17: Ben pre-execution QA gate — `ben_precheck(study, persona_count)` validates completeness before Run Study. Survey: respondent_count + question_count + questions match. IDI/FG: 6 anchors + persona bounds. FAIL → `qa_status=precheck_failed` + `qa_notes` JSON list of issues. PASS → `qa_status=precheck_passed`. Side panel shows PASS/FAIL with details. Run Study button only shows when precheck_passed. Re-run button on FAIL.
- **Personas**: Each persona has a unique immutable `persona_instance_id` (e.g. `P-5EB8581A`). Clone creates a new persona. Delete auto-detaches from non-completed studies. Delete blocked if used in completed study.
- **Grounding Traces**: Recorded on persona creation (and study execution when implemented). Schema follows `grounding_trace.schema.json`. Reason code required when `admin_sources_used_in_output` is false.
- **Admin Web Sources**: Admin can add/toggle/delete web sources. Active sources set `admin_sources_configured=true` and `admin_sources_queried=true` in grounding traces.
- **Study statuses**: draft, in_progress, qa_blocked, terminated_system, terminated_user, completed
- **Study types**: synthetic_survey (max 12Q/400R), synthetic_idi (1-3 personas), synthetic_focus_group (4-6 personas)
- **Run**: `python artifacts/brainstorm/app.py` (port from `PORT` env var, default 5000)

## TypeScript & Composite Projects

Every package extends `tsconfig.base.json` which sets `composite: true`. The root `tsconfig.json` lists all packages as project references. This means:

- **Always typecheck from the root** — run `pnpm run typecheck` (which runs `tsc --build --emitDeclarationOnly`). This builds the full dependency graph so that cross-package imports resolve correctly. Running `tsc` inside a single package will fail if its dependencies haven't been built yet.
- **`emitDeclarationOnly`** — we only emit `.d.ts` files during typecheck; actual JS bundling is handled by esbuild/tsx/vite...etc, not `tsc`.
- **Project references** — when package A depends on package B, A's `tsconfig.json` must list B in its `references` array. `tsc --build` uses this to determine build order and skip up-to-date packages.

## Root Scripts

- `pnpm run build` — runs `typecheck` first, then recursively runs `build` in all packages that define it
- `pnpm run typecheck` — runs `tsc --build --emitDeclarationOnly` using project references

## Packages

### `artifacts/api-server` (`@workspace/api-server`)

Express 5 API server. Routes live in `src/routes/` and use `@workspace/api-zod` for request and response validation and `@workspace/db` for persistence.

- Entry: `src/index.ts` — reads `PORT`, starts Express
- App setup: `src/app.ts` — mounts CORS, JSON/urlencoded parsing, routes at `/api`
- Routes: `src/routes/index.ts` mounts sub-routers; `src/routes/health.ts` exposes `GET /health` (full path: `/api/health`)
- Depends on: `@workspace/db`, `@workspace/api-zod`
- `pnpm --filter @workspace/api-server run dev` — run the dev server
- `pnpm --filter @workspace/api-server run build` — production esbuild bundle (`dist/index.cjs`)
- Build bundles an allowlist of deps (express, cors, pg, drizzle-orm, zod, etc.) and externalizes the rest

### `lib/db` (`@workspace/db`)

Database layer using Drizzle ORM with PostgreSQL. Exports a Drizzle client instance and schema models.

- `src/index.ts` — creates a `Pool` + Drizzle instance, exports schema
- `src/schema/index.ts` — barrel re-export of all models
- `src/schema/<modelname>.ts` — table definitions with `drizzle-zod` insert schemas (no models definitions exist right now)
- `drizzle.config.ts` — Drizzle Kit config (requires `DATABASE_URL`, automatically provided by Replit)
- Exports: `.` (pool, db, schema), `./schema` (schema only)

Production migrations are handled by Replit when publishing. In development, we just use `pnpm --filter @workspace/db run push`, and we fallback to `pnpm --filter @workspace/db run push-force`.

### `lib/api-spec` (`@workspace/api-spec`)

Owns the OpenAPI 3.1 spec (`openapi.yaml`) and the Orval config (`orval.config.ts`). Running codegen produces output into two sibling packages:

1. `lib/api-client-react/src/generated/` — React Query hooks + fetch client
2. `lib/api-zod/src/generated/` — Zod schemas

Run codegen: `pnpm --filter @workspace/api-spec run codegen`

### `lib/api-zod` (`@workspace/api-zod`)

Generated Zod schemas from the OpenAPI spec (e.g. `HealthCheckResponse`). Used by `api-server` for response validation.

### `lib/api-client-react` (`@workspace/api-client-react`)

Generated React Query hooks and fetch client from the OpenAPI spec (e.g. `useHealthCheck`, `healthCheck`).

### `scripts` (`@workspace/scripts`)

Utility scripts package. Each script is a `.ts` file in `src/` with a corresponding npm script in `package.json`. Run scripts via `pnpm --filter @workspace/scripts run <script>`. Scripts can import any workspace package (e.g., `@workspace/db`) by adding it as a dependency in `scripts/package.json`.
