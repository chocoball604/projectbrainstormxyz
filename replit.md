# Overview

This is a pnpm workspace monorepo using TypeScript, designed for building AI-Native Market Research applications. The project aims to provide a robust and scalable platform for conducting various types of market research, including synthetic surveys, in-depth interviews (IDIs), and focus groups, leveraging AI for efficiency and advanced insights.

The core of the project is the "Brainstorm App," a Python Flask application that facilitates the entire research lifecycle from study creation and persona management to report generation and follow-up analysis. This platform focuses on streamlining the market research process, enabling rapid iteration, and delivering actionable intelligence.

Key capabilities include:
- Token-based authentication for secure access.
- Comprehensive study management with various statuses and types.
- AI-powered persona generation and management.
- Detailed grounding trace logging for transparency.
- Cost telemetry and budget enforcement for resource management.
- Admin functionalities for model configuration, web sources, and usage monitoring.
- Document grounding library for attaching and managing research materials.
- Structured report generation with PDF export.
- Follow-up mechanisms for iterative research.
- Usage metering and limits to manage resource consumption.
- Health checks for integrated AI models.

The project emphasizes a modular architecture, utilizing pnpm workspaces to manage different applications and shared libraries, ensuring maintainability and scalability.

# User Preferences

I want iterative development.
Ask before making major changes.
I prefer detailed explanations.
Do not make changes to the folder `brainstorm_v1_replit_singlepage_pack/`.
Do not make changes to the file `artifacts/brainstorm/brainstorm.db`.

# System Architecture

The project is structured as a pnpm workspace monorepo. It leverages Node.js 24 and TypeScript 5.9 for backend services and shared libraries.

**Core Applications:**
- **`api-server`**: An Express 5 API server handling primary API requests, utilizing `@workspace/api-zod` for validation and `@workspace/db` for data persistence.
- **`brainstorm`**: A Python Flask + SQLite single-page application focused on AI-Native Market Research. It manages studies, personas, reports, and user interactions.

**Shared Libraries (`lib/`):**
- **`api-spec`**: Manages the OpenAPI 3.1 specification (`openapi.yaml`) and Orval configuration for API client and schema generation.
- **`api-client-react`**: Generates React Query hooks and a fetch client from the OpenAPI spec for frontend consumption.
- **`api-zod`**: Generates Zod schemas from the OpenAPI spec for request and response validation, ensuring data integrity across services.
- **`db`**: Encapsulates the database layer using Drizzle ORM with PostgreSQL, providing a centralized way to interact with the database schema.

**Build and Type-checking:**
- **`esbuild`**: Used for bundling CJS modules.
- **TypeScript Composite Projects**: All packages extend a base `tsconfig.base.json` with `composite: true`, enabling efficient type-checking and dependency resolution across the monorepo. Type-checking is performed from the root using `tsc --build --emitDeclarationOnly`.

**Feature Specifications (Brainstorm App):**
- **Authentication**: Token-based authentication (URL query parameter `?token=xxx`).
- **Database**: Uses SQLite with a predefined set of tables for users, sessions, studies, personas, chat messages, cost telemetry, and more.
- **Study Types**: Supports `synthetic_survey`, `synthetic_idi`, and `synthetic_focus_group`, each with specific configurations and limits.
- **Persona Management**: Immutable personas with unique `persona_instance_id`s; cloning creates new instances.
- **Grounding Traces**: Records detailed traces of AI model interactions, including sources used. MLG Step 1 populates traces with real retrieval data (tier-level reason codes) for persona creation.
- **MLG (Minimal Live Grounding)**: Step 1 implemented — live web retrieval during persona creation (IDI/FG only). Uses 5-tier priority: user uploads → admin uploads → admin web sources → local web search → general web search. Produces synthesized grounding summaries (≤800 tokens) for Lisa's persona prompt. Uses `ddgs` (DuckDuckGo) for web search and `requests` for admin URL fetching. SSRF guards block private/localhost URLs. Step 2 adds execution-level calibration (injected into study prompts as "Population Calibration Context (NOT evidence)"). Execution MLG data persisted in `studies.exec_grounding_data` column for report surfacing. Wikipedia downgraded to tertiary calibration source: -25 score penalty, capped at 1 per grounding bundle, labeled "[Tertiary Calibration Only]" in reports. Secondary authoritative sources (gov, edu, intergovernmental orgs like UN/OECD/World Bank) get +25 boost. Ben QA fails studies with Wikipedia over-representation.
- **Admin Features**: Includes capabilities for managing admin web sources, configuring AI models (`mark_model`, `lisa_model`, `ben_model`), managing allowed models, and editing persona model pools.
- **Reporting**: Generates structured reports with confidence labels and supports PDF download.
- **Follow-ups**: Allows up to two follow-up rounds for completed IDI/FG studies.
- **Usage Monitoring**: Implements monthly usage meters and enforces free-tier limits for study creation.
- **Document Management**: Provides a document grounding library for user and admin uploads, with file size and storage limits.
- **Model Health Checks**: Includes a system for daily model health checks and weekly QA reports for AI models. Admin LLM smoke test endpoint at `/admin/llm-smoke` (POST) for quick single-model verification.
- **Marketing Landing Page**: A dedicated landing page (`landing.html`) for unauthenticated users, providing an overview of the platform.
- **Blog/News**: A blogging platform with public list and detail views, admin capabilities for post creation and image uploads, post pinning (up to 3 pinned posts with rank 1-3), and paginated blog listing (10 per page). Pinned posts appear first in rank order on page 1, followed by unpinned posts in reverse chronological order. Language selector on landing page shows 4 options: English, CN-Simplified简, CN-Traditional繁, Japanese日.
- **Manage Account**: Logged-in users can access `/account` to edit profile (name, company, role, location, LinkedIn) and change password (6–10 chars, current password required). Link in dashboard header.
- **Email Verification**: Enforced via `before_request` hook on all authenticated routes (except login/signup/logout/landing/blog/verify-email). Users without a verified email (or whose verification expired after 7 days) are redirected to `/verify-email`. Prototype shows 6-digit code inline (production would email it). Verification timestamp stored in `users.last_email_verification_timestamp`.
- **LLM Integration**: `call_llm()` uses OpenAI SDK pointed at Replit AI Integrations (OpenRouter). Env vars: `AI_INTEGRATIONS_OPENROUTER_BASE_URL`, `AI_INTEGRATIONS_OPENROUTER_API_KEY`. Falls back to `NotImplementedError` if env vars missing.
- **Branding & Theme**: Brand lockup (Enso logo `static/brand/enso_logo_256.png` + "Project Brainstorm" wordmark) in all page headers. CJK wordmark variant ("Project Brainstorm 集思廣益") shown for zh-Hans/zh-Hant/ja languages via client-side JS. CSS variables define palette: `--pb-accent: #5C7E8F`, `--pb-grey: #A2A2A2`, `--pb-lightgrey: #D4DDE2`. Typography uses Calibri base with CJK-specific font stacks via `html[data-lang]` selectors. Language persisted in `pb_lang` cookie, read by Flask context processor `inject_lang()` and applied as `data-lang` attribute on `<html>`. Password validation enforces 6–10 characters (backend + frontend `maxlength=10`).

# External Dependencies

- **Node.js**: Version 24
- **pnpm**: Package manager
- **TypeScript**: Version 5.9
- **Express**: Version 5 (API framework)
- **PostgreSQL**: Database
- **Drizzle ORM**: Object-relational mapper for PostgreSQL
- **Zod**: Validation library (`zod/v4`)
- **`drizzle-zod`**: Integration for Zod with Drizzle ORM
- **Orval**: API codegen tool (from OpenAPI spec)
- **esbuild**: JavaScript bundler
- **Python Flask**: Web framework for the Brainstorm app
- **SQLite**: Database for the Brainstorm app
- **fpdf2**: Python library for PDF generation (used in Brainstorm app)
- **React Query**: For client-side data fetching and caching (in `api-client-react`)
- **OpenRouter**: Integrated for AI model selection and management (`replit_openrouter`)