# Overview

This project is an AI-Native Market Research platform built as a pnpm workspace monorepo using TypeScript and Python Flask. It aims to revolutionize market research by leveraging AI for synthetic surveys, in-depth interviews (IDIs), and focus groups. The "Brainstorm App" (a Python Flask application) orchestrates the entire research lifecycle, from study creation and AI-powered persona generation to structured report generation and follow-up analysis. The platform prioritizes efficiency, rapid iteration, and delivering actionable insights while maintaining secure access, detailed grounding traces, and cost management. Its modular architecture ensures scalability and maintainability.

# User Preferences

I want iterative development.
Ask before making major changes.
I prefer detailed explanations.
Do not make changes to the folder `brainstorm_v1_replit_singlepage_pack/`.
Do not make changes to the file `artifacts/brainstorm/brainstorm.db`.

# System Architecture

The project employs a pnpm workspace monorepo structure, utilizing Node.js 24 and TypeScript 5.9 for backend services and shared libraries, and Python Flask for the core application.

**Core Applications & Services:**
- **`api-server`**: An Express 5 API server managing primary API requests, leveraging Zod for validation and Drizzle ORM for PostgreSQL data persistence.
- **`brainstorm`**: A Python Flask + SQLite single-page application forming the central AI-Native Market Research platform. It handles studies, personas, reports, authentication (token-based), and user interactions.

**Shared Libraries:**
- **`api-spec`**: Defines the OpenAPI 3.1 specification for API consistency.
- **`api-client-react`**: Generates React Query hooks and a fetch client from the OpenAPI spec for frontend consumption.
- **`api-zod`**: Generates Zod schemas from the OpenAPI spec for robust data validation.
- **`db`**: Encapsulates PostgreSQL database interactions using Drizzle ORM.

**Technical Implementations & Features:**
- **Authentication**: Token-based for secure access.
- **Database**: SQLite for the Flask app, PostgreSQL with Drizzle ORM for other services.
- **Study Management**: Supports `synthetic_survey`, `synthetic_idi`, and `synthetic_focus_group` with specific configurations and limits.
- **AI Integration**: Features AI-powered persona generation, detailed grounding traces for AI model interactions, and "Minimal Live Grounding" (MLG) for web retrieval. Live web retrieval is mediated via OpenAI Responses API with `web_search_preview`.
- **Admin Functionality**: Tools for managing AI models, web sources, usage, and system configuration.
- **Reporting**: Generates structured reports with confidence labels and PDF export.
- **Security Hardening**: Implements `ProxyFix`, secure session cookies, CSRF tokens on state-changing POST requests, and comprehensive security response headers (e.g., CSP, X-Frame-Options). Includes fail-fast secret validation and brute-force throttling for login/signup endpoints.
- **Survey Setup**: UI-driven configuration for surveys, including respondent count caps, question wizard with inline guardrails, and contextual guidance.
- **Follow-ups**: Backend support for multiple follow-up rounds (UI for input is temporarily gated).
- **Usage Monitoring**: Monthly usage meters and free-tier limits enforced.
- **Document Management**: Library for user and admin uploads with size and storage limits.
- **Model Health Checks**: Daily checks and weekly QA reports for AI models, with an admin LLM smoke test endpoint.
- **Marketing & UI**: Includes a dedicated marketing landing page, a blogging platform with admin capabilities, and account management for logged-in users.
- **Email Verification**: Enforced on authenticated routes.
- **LLM Integration**: Utilizes `call_llm()` pointing to Replit AI Integrations (OpenRouter) with environment variable configuration.
- **Branding & Theme**: Consistent branding with specific color palette, typography (Calibri base with CJK variants), and dynamic language selection.
- **Mark Presence Model**: A 3-state UI model for the "Mark" AI helper, adapting its visibility and functionality based on study progress.
- **Framing Continuity**: "Core Framing" card for continuous visibility and editing of Business Problem and Decision to Support, with anchoring upon study type selection.
- **Step 1 Telemetry**: Observational logging of user interactions during the initial framing stage, append-only to an SQLite table, with a kill switch for enabling/disabling.

# External Dependencies

- **Node.js**: Version 24
- **pnpm**: Package manager
- **TypeScript**: Version 5.9
- **Express**: Version 5
- **PostgreSQL**
- **Drizzle ORM**
- **Zod**: Validation library
- **Orval**: API codegen tool
- **esbuild**: JavaScript bundler
- **Python Flask**
- **SQLite**
- **fpdf2**: Python PDF generation library
- **React Query**
- **OpenRouter**: AI model integration
- **bleach**: HTML sanitization for Python