"""
app.py — Project Brainstorm V1 (PROMPT 1–7 + PRD fixes + branching flow + P11-P18)

Single-page Flask app with SQLite.
Sections: Login/Signup | Pending Approval | Active Dashboard | Admin Panel

PROMPT 2: studies table, study list on dashboard, "New Research" button.
PROMPT 3: Research Brief form with 6 required anchors; saving creates a draft study.
PROMPT 4: Study type selector + limits enforcement.
PROMPT 5: Personas — immutable once saved, clone-as-new, no versioning.
PROMPT 6: Grounding Trace logging + Admin-Directed Web Sources.
PROMPT 7: Execute studies with placeholder outputs.
PRD FIX: Survey no personas (respondent/question config), IDI 1-3 personas, FG 4-6 personas.
BRANCHING: Study type chosen first in New Research flow. Survey gets survey brief + questions.
           IDI/FG get existing 6-anchor brief. Type set at creation, not after.
P18: Report Viewer + PDF download for completed studies.

Rules: See brainstorm_v1_replit_singlepage_pack/00_FROZEN_RULES_FROM_PRD.md
"""

import hashlib
import io
import json
import os
import signal
import sys
import secrets
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from flask import (
    Flask,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    send_file,
    send_from_directory,
    url_for,
)
from fpdf import FPDF
from werkzeug.security import check_password_hash, generate_password_hash

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-fallback-key")


@app.route("/__health")
def __health():
    return jsonify({"status": "ok", "service": "brainstorm_flask"})


VALID_LANGS = {"en", "zh-Hans", "zh-Hant", "ja"}


@app.context_processor
def inject_lang():
    lang = request.cookies.get("pb_lang", "en")
    if lang not in VALID_LANGS:
        lang = "en"
    return dict(lang=lang)


VERIFY_EXEMPT_ENDPOINTS = {
    "index",
    "verify_email",
    "login",
    "signup",
    "admin_login",
    "logout",
    "landing_page",
    "blog_list",
    "blog_single",
    "set_language",
    "static",
    "serve_blog_image",
}


@app.before_request
def enforce_email_verification():
    endpoint = request.endpoint
    if endpoint in VERIFY_EXEMPT_ENDPOINTS or endpoint is None:
        return None
    if request.path.startswith("/static/"):
        return None
    token = request.args.get("token") or request.form.get("token") or ""
    if not token:
        return None
    user, is_admin = get_session_data(token)
    redir = require_verified_user(token, user, is_admin)
    if redir:
        return redir
    return None


DB_PATH = os.path.join(os.path.dirname(__file__), "brainstorm.db")

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin123")

VALID_STUDY_STATUSES = [
    "draft",
    "in_progress",
    "qa_blocked",
    "terminated_system",
    "terminated_user",
    "completed",
]

VALID_STUDY_TYPES = ["synthetic_survey", "synthetic_idi", "synthetic_focus_group"]

BILLABLE_STATUSES = ("completed", "qa_blocked", "terminated_system", "terminated_user")
FREE_TIER_MONTHLY_LIMIT = 6

UPLOAD_MAX_FILES_PER_STUDY = 5
UPLOAD_MAX_FILE_SIZE = 1 * 1024 * 1024
UPLOAD_MAX_TOTAL_PER_STUDY = 5 * 1024 * 1024
UPLOAD_USER_STORAGE_CAP = 15 * 1024 * 1024
UPLOAD_ALLOWED_EXTENSIONS = {"pdf", "docx", "txt", "csv", "png", "jpg", "jpeg"}
DOCS_PAGE_SIZE = 10
USER_UPLOADS_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "uploads", "user"
)
ADMIN_UPLOADS_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "uploads", "admin"
)
os.makedirs(USER_UPLOADS_DIR, exist_ok=True)
os.makedirs(ADMIN_UPLOADS_DIR, exist_ok=True)

BLOG_IMAGE_MAX_SIZE = 300 * 1024
BLOG_IMAGE_ALLOWED = {"png", "jpg", "jpeg"}
BLOG_STATIC_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "static", "blog"
)
os.makedirs(BLOG_STATIC_DIR, exist_ok=True)


LLM_HARD_TIMEOUT_SECONDS = 75


class LLMHardTimeout(Exception):
    pass


@contextmanager
def hard_timeout(seconds):
    def _raise(signum, frame):
        raise LLMHardTimeout(f"LLM hard timeout after {seconds}s")
    old_handler = signal.signal(signal.SIGALRM, _raise)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def _record_llm_incident(model_id, purpose, error_type, error_msg):
    try:
        conn = get_db()
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        run_id = secrets.token_hex(8)
        run_type = f"incident_{purpose}" if purpose else "incident_llm_call"
        details = {
            "model_id": model_id,
            "error_type": error_type,
            "error_message": str(error_msg)[:500],
        }
        conn.execute(
            "INSERT INTO model_health_checks "
            "(run_id, run_type, started_at, finished_at, integration_mode, summary_status, details_json) "
            "VALUES (?, ?, ?, ?, 'live_calls_enabled', 'fail', ?)",
            (run_id, run_type, now_str, now_str, json.dumps(details)),
        )
        conn.commit()
        conn.close()
        print(f"LLM_INCIDENT model={model_id} type={error_type} run_id={run_id}", flush=True)
    except Exception as e:
        print(f"LLM_INCIDENT_RECORD_ERROR: {e}", flush=True)


def call_llm(model_id, messages, purpose=""):
    """Single wrapper for all LLM calls via Replit AI Integrations (OpenRouter)."""
    import openai as _openai

    base_url = os.environ.get("AI_INTEGRATIONS_OPENROUTER_BASE_URL")
    api_key = os.environ.get("AI_INTEGRATIONS_OPENROUTER_API_KEY")
    if not base_url or not api_key:
        raise NotImplementedError("LLM integration not connected yet")

    client = _openai.OpenAI(base_url=base_url, api_key=api_key, timeout=60)
    try:
        with hard_timeout(LLM_HARD_TIMEOUT_SECONDS):
            resp = client.chat.completions.create(
                model=model_id,
                messages=messages,
                max_tokens=8192,
            )
            return resp.choices[0].message.content or ""
    except LLMHardTimeout as e:
        _record_llm_incident(model_id, purpose, "hard_timeout", e)
        raise
    except _openai.APITimeoutError as e:
        _record_llm_incident(model_id, purpose, "api_timeout", e)
        raise RuntimeError(f"LLM timeout for {model_id}: {str(e)[:200]}")
    except _openai.APIError as e:
        _record_llm_incident(model_id, purpose, "api_error", e)
        raise RuntimeError(f"LLM error for {model_id}: {str(e)[:200]}")


# from datetime import datetime  ->  from datetime import datetime, timezone


def run_model_health_check(run_type="manual"):
    try:
        run_id = secrets.token_hex(8)
        started_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        integration_mode = "placeholder_not_connected"

        try:
            call_llm(
                "_probe_",
                [{"role": "user", "content": "ping"}],
                purpose="integration_probe",
            )
            integration_mode = "live_calls_enabled"
        except NotImplementedError:
            integration_mode = "placeholder_not_connected"
        except Exception:
            integration_mode = "error_probe_failed"

        conn = get_db()
        mc = {
            r["key"]: r["value"]
            for r in conn.execute("SELECT key, value FROM model_config").fetchall()
        }
        active_allowed = {
            r["model_id"]
            for r in conn.execute(
                "SELECT model_id FROM allowed_models WHERE status = 'active'"
            ).fetchall()
        }
        pool_models = conn.execute(
            "SELECT model_id, status FROM persona_model_pool"
        ).fetchall()
        active_pool = [r["model_id"] for r in pool_models if r["status"] == "active"]
        conn.close()

        config_errors = []
        for role, key in [
            ("Mark", "mark_model"),
            ("Lisa", "lisa_model"),
            ("Ben", "ben_model"),
        ]:
            mid = mc.get(key)
            if not mid:
                config_errors.append(f"{role} model not configured")
            elif mid not in active_allowed:
                config_errors.append(
                    f"{role} model '{mid}' not in active allowed models"
                )

        if not active_pool:
            config_errors.append("Persona model pool has no active entries")
        for pm in active_pool:
            if pm not in active_allowed:
                config_errors.append(f"Pool model '{pm}' not in active allowed models")

        config_valid = len(config_errors) == 0

        all_model_ids = set()
        for key in ("mark_model", "lisa_model", "ben_model"):
            if mc.get(key):
                all_model_ids.add(mc[key])
        all_model_ids.update(active_pool)

        per_model = {}
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        if integration_mode == "placeholder_not_connected":
            for mid in all_model_ids:
                if mid in active_allowed:
                    per_model[mid] = {
                        "status": "not_connected",
                        "error": "LLM integration not connected (placeholder mode)",
                    }
                else:
                    per_model[mid] = {
                        "status": "fail",
                        "error": "Model not in active allowed list",
                    }

            summary_status = "fail" if not config_valid else "unknown"

        else:
            for mid in all_model_ids:
                if mid not in active_allowed:
                    per_model[mid] = {
                        "status": "fail",
                        "error": "Model not in active allowed list",
                        "latency_s": None,
                    }
                    continue
                try:
                    t0 = time.time()
                    result = call_llm(
                        mid,
                        [{"role": "user", "content": "Reply with the single word OK"}],
                        purpose="health_check",
                    )
                    latency = round(time.time() - t0, 2)
                    if latency > 10:
                        per_model[mid] = {
                            "status": "fail",
                            "error": f"High latency: {latency}s",
                            "latency_s": latency,
                        }
                    elif "ok" in (result or "").lower():
                        per_model[mid] = {"status": "pass", "error": None, "latency_s": latency}
                    else:
                        per_model[mid] = {
                            "status": "fail",
                            "error": f"Unexpected response: {(result or '')[:300]}",
                            "latency_s": latency,
                        }
                except Exception as e:
                    per_model[mid] = {"status": "fail", "error": str(e)[:300], "latency_s": None}

            if not config_valid:
                summary_status = "fail"
            elif any(v["status"] == "fail" for v in per_model.values()):
                summary_status = "fail"
            else:
                summary_status = "pass"

        conn = get_db()

        pending = conn.execute(
            "SELECT COUNT(*) FROM chat_messages "
            "WHERE sender='mark' AND message_text='\u23f3 Mark is thinking...'"
        ).fetchone()[0]
        if pending > 0:
            summary_status = "fail"

        for mid, info in per_model.items():
            conn.execute(
                "INSERT INTO model_health_status (model_id, status, last_tested_at, last_error) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(model_id) DO UPDATE SET status=?, last_tested_at=?, last_error=?",
                (
                    mid,
                    info["status"],
                    now_str,
                    info["error"],
                    info["status"],
                    now_str,
                    info["error"],
                ),
            )

        for model_id, info in per_model.items():
            if info["status"] == "fail":
                conn.execute(
                    "UPDATE allowed_models SET status='disabled' WHERE model_id = ?",
                    (model_id,),
                )

        finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        details = {
            "config_errors": config_errors,
            "per_model": per_model,
            "models_checked": list(all_model_ids),
        }
        if pending > 0:
            details["stuck_mark_messages"] = pending

        conn.execute(
            "INSERT INTO model_health_checks "
            "(run_id, run_type, started_at, finished_at, integration_mode, summary_status, details_json) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                run_id,
                run_type,
                started_at,
                finished_at,
                integration_mode,
                summary_status,
                json.dumps(details),
            ),
        )
        conn.commit()
        conn.close()
        print(f"HEALTH_CHECK_RUN={run_id} status={summary_status} started={started_at} finished={finished_at}", flush=True)
        return {
            "run_id": run_id,
            "run_type": run_type,
            "integration_mode": integration_mode,
            "summary_status": summary_status,
            "details": details,
        }

    except Exception as e:
        try:
            conn = get_db()
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            conn.execute(
                """INSERT INTO model_health_checks
                (run_id, run_type, started_at, finished_at,
                 integration_mode, summary_status, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    secrets.token_hex(8),
                    run_type,
                    now,
                    now,
                    "error",
                    "fail",
                    json.dumps({"fatal_error": str(e)[:500]}),
                ),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

        print("HEALTH_CHECK_FATAL_ERROR", str(e))
        return {
            "summary_status": "fail",
            "error": "Health check encountered an unexpected error",
        }


def generate_weekly_qa_report():
    conn = get_db()
    try:
        from datetime import timedelta

        today = datetime.utcnow().date()
        week_start = today - timedelta(days=today.weekday())
        week_start_str = week_start.strftime("%Y-%m-%d")

        existing = conn.execute(
            "SELECT id FROM weekly_qa_reports WHERE week_start_date = ?", (week_start_str,)
        ).fetchone()
        if existing:
            return None

        last_check = conn.execute(
            "SELECT * FROM model_health_checks ORDER BY id DESC LIMIT 1"
        ).fetchone()
        health_rows = conn.execute(
            "SELECT * FROM model_health_status ORDER BY model_id"
        ).fetchall()
        mc = {
            r["key"]: r["value"]
            for r in conn.execute("SELECT key, value FROM model_config").fetchall()
        }
        pool_total = conn.execute("SELECT COUNT(*) FROM persona_model_pool").fetchone()[0]
        pool_active = conn.execute(
            "SELECT COUNT(*) FROM persona_model_pool WHERE status='active'"
        ).fetchone()[0]

        lines = [f"Weekly QA Report — Week of {week_start_str}", "=" * 50, ""]
        lines.append("## System Model Health")
        if last_check:
            lines.append(f"Integration Mode: {last_check['integration_mode']}")
            lines.append(f"Last Daily Check: {last_check['finished_at']}")
            lines.append(f"Summary Status: {last_check['summary_status'].upper()}")
        else:
            lines.append("No health checks have been run yet.")
        lines.append("")
        lines.append(f"Mark Model: {mc.get('mark_model', 'not set')}")
        lines.append(f"Lisa Model: {mc.get('lisa_model', 'not set')}")
        lines.append(f"Ben Model: {mc.get('ben_model', 'not set')}")
        lines.append(f"Persona Pool: {pool_active} active / {pool_total} total")
        lines.append("")

        failing = [dict(r) for r in health_rows if r["status"] == "fail"]
        not_connected = [dict(r) for r in health_rows if r["status"] == "not_connected"]
        if failing:
            lines.append("### Failing Models:")
            for f in failing:
                lines.append(f"  - {f['model_id']}: {f['last_error'] or 'unknown error'}")
        if not_connected:
            lines.append("### Not Connected Models:")
            for nc in not_connected:
                lines.append(
                    f"  - {nc['model_id']}: {nc['last_error'] or 'placeholder mode'}"
                )
        if not failing and not not_connected:
            lines.append("All models OK or no checks run yet.")

        lines.append("")
        lines.append("## Model Reliability Issues")
        incident_rows = conn.execute(
            "SELECT details_json, finished_at FROM model_health_checks "
            "WHERE run_type = 'incident_mark_chat_timeout' AND started_at >= ?",
            (week_start_str,),
        ).fetchall()
        if incident_rows:
            incident_counts = {}
            incident_latest = {}
            for row in incident_rows:
                try:
                    d = json.loads(row["details_json"])
                    mid = d.get("model_id", "unknown")
                except (json.JSONDecodeError, TypeError):
                    mid = "unknown"
                incident_counts[mid] = incident_counts.get(mid, 0) + 1
                ts = row["finished_at"] or ""
                if ts > incident_latest.get(mid, ""):
                    incident_latest[mid] = ts
            for mid, count in sorted(incident_counts.items()):
                latest = incident_latest.get(mid, "unknown")
                lines.append(f"  Model {mid}: {count} Mark chat timeouts this week (latest: {latest})")
        else:
            lines.append("No reliability incidents recorded this week.")

        report_text = "\n".join(lines)
        conn.execute(
            "INSERT INTO weekly_qa_reports (week_start_date, report_text) VALUES (?, ?)",
            (week_start_str, report_text),
        )
        conn.commit()
        return report_text
    finally:
        conn.close()


EMAIL_VERIFY_INTERVAL_DAYS = 7

SEED_ALLOWED_MODELS = [
    "openai/gpt-5.4-mini",
    "openai/gpt-5.4-nano",
    "mistral/mistral-small-4",
    "anthropic/claude-opus-4.6",
    "google/gemini-3.1-pro-preview",
    "minimax/minimax-m2.7",
    "openrouter/free",
]


def is_gpt_family(model_id: str) -> bool:
    mid = model_id.lower().strip()
    return (
        mid.startswith("openai/gpt-") or mid.startswith("openai/gpt_") or "/gpt-" in mid
    )


def _extract_optional_context(study_dict):
    oc = {}
    sb_raw = study_dict.get("survey_brief") or ""
    if sb_raw:
        try:
            sb = json.loads(sb_raw) if isinstance(sb_raw, str) else sb_raw
            if isinstance(sb, dict):
                oc = sb.get("optional_context", {})
                if not isinstance(oc, dict):
                    oc = {}
        except (json.JSONDecodeError, TypeError):
            pass
    fields = [
        ("competitive_context", "Competitive context"),
        ("cultural_sensitivities", "Cultural sensitivities"),
        ("adoption_barriers", "Adoption barriers"),
        ("risk_tolerance", "Risk tolerance"),
    ]
    present = {k: (oc.get(k) or "").strip() for k, _ in fields if (oc.get(k) or "").strip()}
    if not present:
        return "", []
    lines = ["Optional context (optional):"]
    for k, label in fields:
        v = present.get(k)
        if v:
            lines.append(f"  {label}: {v}")
    return "\n".join(lines), sorted(present.keys())


def lisa_generate_personas(study_dict, n, lisa_model_id):
    brief_fields = [
        ("business_problem", "Business Problem"),
        ("decision_to_support", "Decision to Support"),
        ("known_vs_unknown", "Known vs Unknown"),
        ("target_audience", "Target Audience"),
        ("study_fit", "Study Fit"),
        ("definition_useful_insight", "Definition of Useful Insight"),
    ]
    brief_text = ""
    for field, label in brief_fields:
        val = (study_dict.get(field) or "").strip()
        brief_text += f"{label}: {val or 'Not specified'}\n"

    study_type_label = (
        "In-Depth Interview (IDI)"
        if study_dict.get("study_type") == "synthetic_idi"
        else "Focus Group"
    )

    system_prompt = (
        "You are Lisa, a senior qualitative research analyst at Project Brainstorm. "
        "Generate realistic, diverse synthetic research personas for a qualitative study.\n\n"
        "Return STRICT JSON only — no markdown, no commentary, no code fences.\n"
        "Schema:\n"
        '{"personas": [{"name": "...", "persona_summary": "...", "demographic_frame": "...", '
        '"psychographic_profile": "...", "contextual_constraints": "...", '
        '"behavioural_tendencies": "...", "grounding_sources": "...", '
        '"confidence_and_limits": "..."}]}\n\n'
        "RULES:\n"
        f"1. Generate exactly {n} persona(s).\n"
        "2. Each persona must be distinct in demographics, psychographics, and behaviour.\n"
        "3. Personas should be grounded in the target audience and research context.\n"
        "4. Use realistic names appropriate for the target market.\n"
        "5. Be culturally grounded for Asia-Pacific markets where relevant.\n"
        "6. Each field must be substantive (50-200 words), not placeholder text."
    )

    oc_block, oc_keys = _extract_optional_context(study_dict)
    if oc_block:
        brief_text += f"\n{oc_block}\n"
    study_title = study_dict.get("title", "Untitled Study")
    print(f'OPTIONAL_CONTEXT_INJECT_LISA_PERSONA study_title="{study_title}" included={"true" if oc_keys else "false"} keys={",".join(oc_keys)}', flush=True)

    user_prompt = (
        f"Study: {study_title}\n"
        f"Type: {study_type_label}\n\n"
        f"Research Brief:\n{brief_text}\n"
        f"Generate {n} persona(s) for this study."
    )

    raw = call_llm(
        lisa_model_id,
        [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        purpose="lisa_auto_persona_generation",
    )

    if not raw or not raw.strip():
        raise ValueError("LLM returned empty response for persona generation")

    cleaned = raw.strip()
    if cleaned.startswith("```"):
        first_nl = cleaned.find("\n")
        if first_nl != -1:
            cleaned = cleaned[first_nl + 1 :]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

    parsed = json.loads(cleaned)
    personas = parsed.get("personas", [])
    if not isinstance(personas, list) or len(personas) < 1:
        raise ValueError(f"Expected {n} personas, got invalid structure")

    return personas[:n]


def get_monthly_usage(conn, user_id):
    import calendar

    now = datetime.utcnow()
    window_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_day = calendar.monthrange(now.year, now.month)[1]
    window_end = now.replace(day=last_day, hour=23, minute=59, second=59, microsecond=0)
    month_start_str = window_start.strftime("%Y-%m-%d %H:%M:%S")
    placeholders = ",".join("?" for _ in BILLABLE_STATUSES)
    count = conn.execute(
        f"SELECT COUNT(*) FROM studies WHERE user_id = ? AND status IN ({placeholders}) AND created_at >= ?",
        (user_id, *BILLABLE_STATUSES, month_start_str),
    ).fetchone()[0]
    return (
        count,
        FREE_TIER_MONTHLY_LIMIT,
        window_start.strftime("%Y-%m-%d"),
        window_end.strftime("%Y-%m-%d"),
    )


STUDY_TYPE_LIMITS = {
    "synthetic_survey": {"max_questions": 12, "max_respondents": 400},
    "synthetic_idi": {"min_personas": 1, "max_personas": 3},
    "synthetic_focus_group": {"min_personas": 4, "max_personas": 6},
}

V1A_FIELD_MAP = {
    "market_geography": "study_fit",
    "product_concept": "known_vs_unknown",
}

def get_v1a_value(study_dict, key):
    db_key = V1A_FIELD_MAP.get(key, key)
    return (study_dict.get(db_key) or "").strip()

def set_v1a_value(conn, study_id, key, value):
    db_key = V1A_FIELD_MAP.get(key, key)
    conn.execute(f"UPDATE studies SET {db_key} = ? WHERE id = ?", (value, study_id))

V1A_LABELS = {
    "business_problem": "Business Problem",
    "decision_to_support": "Decision to Support",
    "market_geography": "Market / Geography",
    "product_concept": "Product / Concept",
    "target_audience": "Target Audience",
    "definition_useful_insight": "Definition of Useful Insight",
}

_VALID_V1A_FIELDS = set(V1A_LABELS.keys())

# ============================================================
# MARK SYSTEM PROMPT — single source of truth (hardened)
# ============================================================

MARK_SYSTEM_PROMPT = (
    "You are Mark, the Market Intelligence Copilot for Project Brainstorm.\n\n"
    "Project Brainstorm is an AI native market research platform that helps users "
    "define business problems and make disciplined, evidence based decisions "
    "using structured research (Synthetic Survey, Synthetic IDI, Synthetic Focus Group).\n\n"
    "AUTHORITY BOUNDARY (CRITICAL)\n"
    "- You do NOT know whether data was saved.\n"
    "- You MUST NEVER claim that anything was \"saved\", \"applied\", or \"persisted\".\n"
    "- Only the system can confirm persistence.\n"
    "- If the user confirms an action, you acknowledge and WAIT for the UI to reflect the change.\n\n"
    "ROLE\n"
    "You guide the user through research setup.\n"
    "You propose updates. The system executes them.\n\n"
    "CORE BEHAVIOR RULES\n"
    "1) Be professional, direct, and concise.\n"
    "2) Ask EXACTLY ONE targeted question per turn.\n"
    "3) Always move the study toward execution.\n"
    "4) Never invent state changes.\n"
    "5) Never repeat or stack proposals if the UI state has not changed.\n"
    "6) Never propose more than ONE field at a time.\n"
    "7) Never proceed to later fields if earlier phase gate fields are missing.\n\n"
    "PHASE GATE (STRICT — NO EXCEPTIONS)\n\n"
    "If Study Type is NOT selected:\n\n"
    "A) If Business Problem is not present:\n"
    "   - You may ONLY ask for or propose Business Problem.\n"
    "   - You MUST NOT propose Decision to Support or anything else.\n\n"
    "B) Else if Decision to Support is not present:\n"
    "   - You may ONLY ask for or propose Decision to Support.\n\n"
    "C) Else (Business Problem AND Decision to Support are present):\n"
    "   - Recommend ONE study type.\n"
    "   - Ask the user to confirm by selecting a study type using UI buttons.\n"
    "   - DO NOT propose any additional fields.\n\n"
    "You MUST NOT proceed past a phase gate unless the UI confirms the field exists.\n\n"
    "CONFIRMATION HANDLING (HARD — NO EXCEPTIONS)\n\n"
    "When the user says \"yes\", \"ok\", \"confirm\", \"save\", or any confirmation:\n"
    "- Acknowledge briefly.\n"
    "- DO NOT say \"saved\", \"applied\", or \"done\".\n"
    "- DO NOT propose a new field in the same turn.\n"
    "- DO NOT ask the next question in the same turn.\n"
    "- WAIT for the system to reflect the update before continuing.\n"
    "- Only after the UI shows the updated field may you proceed.\n\n"
    "Required response pattern:\n"
    "\"Thanks -- once the update is reflected in the UI, we'll move to the next step.\"\n\n"
    "CONFIRMATION TEXT IS NEVER CONTENT\n\n"
    "Confirmation language (\"yes\", \"ok\", \"save\", \"confirmed\", \"sure\", etc.) "
    "is NEVER valid study content.\n"
    "If the user sends only confirmation text without substantive content:\n"
    "- Do NOT propose it as a value.\n"
    "- Do NOT rephrase it into a value.\n"
    "- Do NOT infer meaning from it.\n"
    "- Instead, ask one clarification question for the actual content.\n\n"
    "Example (correct):\n"
    "User: \"yes save\"\n"
    "Mark: \"I need the actual statement here, not a confirmation. "
    "Please describe the business problem in one clear sentence.\"\n\n"
    "FIELD REPLACEMENT (OVERWRITE) RULES\n\n"
    "A saved field may be replaced ONLY through an explicit proposal + confirmation flow.\n"
    "Silent overwrites are forbidden.\n\n"
    "Mark MAY propose replacing an existing field ONLY when:\n"
    "- The field already has a saved value, AND\n"
    "- The user explicitly indicates correction, deletion, or replacement intent "
    "(e.g. \"that's wrong\", \"replace it with...\", \"delete that\", \"that's not right\")\n\n"
    "When replacing, use the standard proposal format but change the confirmation question to:\n"
    "\"This will replace the existing <Field Label>. Should I save this update?\"\n\n"
    "PROPOSAL VALIDATION (HARD — NEVER PROPOSE INVALID VALUES)\n\n"
    "Before emitting a proposal, you MUST internally validate that the value is:\n"
    "- Substantive (not confirmation text, not empty, not trivial)\n"
    "- Within reasonable length (under 2000 characters)\n"
    "- Relevant to the specified field\n\n"
    "If the value would be invalid, do NOT propose. Instead:\n"
    "- Ask one clarification question\n"
    "- Explain what is missing or invalid\n\n"
    "PROPOSAL FORMAT (MANDATORY)\n\n"
    "ONLY when the user provides a clear, usable answer for the currently allowed field, "
    "end your reply with EXACTLY this structure:\n\n"
    "Proposed updates:\n"
    "- field: <one valid field key>\n"
    "  value: <concise extracted text, 1-2 lines, no analysis>\n"
    "  confidence: <high | medium | low>\n\n"
    "Confirmation question:\n"
    "Should I save these updates?\n\n"
    "Rules:\n"
    "- Exactly ONE field.\n"
    "- No explanations after the proposal block.\n"
    "- In Proposed updates, 'field:', 'value:', and 'confidence:' must each be on their own line. "
    "Never put field/value/confidence on the same line.\n"
    "- The confirmation question must be the FINAL line.\n\n"
    "PROPOSAL INVARIANTS (MANDATORY — NEVER VIOLATE)\n\n"
    "1) If you emit a 'Proposed updates:' block, the message MUST contain ONLY "
    "the proposal block and the confirmation question. No other text before or after. "
    "No questions, explanations, or acknowledgements.\n\n"
    "2) You must NEVER ask a question and emit a 'Proposed updates:' block in the same message. "
    "Asking a question and proposing are mutually exclusive actions. "
    "If you asked a question, wait for the user's answer. "
    "If you propose an update, do not ask any question except the confirmation question.\n\n"
    "3) Every proposal MUST follow the exact format shown above. Deviations are forbidden.\n\n"
    "4) If you previously attempted a proposal and it was rejected or failed to parse, "
    "you MUST restate the proposal using the exact format above and nothing else.\n\n"
    "VALID FIELD KEYS\n"
    "business_problem\n"
    "decision_to_support\n"
    "market_geography\n"
    "product_concept\n"
    "target_audience\n"
    "definition_useful_insight\n\n"
    "STYLE CONSTRAINTS\n"
    "- Max 100 words per response.\n"
    "- No markdown.\n"
    "- No emojis.\n"
    "- No system claims.\n"
    "- No speculative language.\n\n"
    "COMPLETION RULE\n"
    "If the study is complete and valid, clearly state that it is ready "
    "and instruct the user to proceed via the UI.\n\n"
    "REMEMBER\n"
    "You propose.\n"
    "The system saves.\n"
    "If the UI does not change, you do not advance."
)


def build_mark_system_message(study_dict, persona_count, study_id=None):
    anchor_keys = [
        ("business_problem", "Business Problem"),
        ("decision_to_support", "Decision to Support"),
        ("market_geography", "Market / Geography"),
        ("product_concept", "Product / Concept"),
        ("target_audience", "Target Audience"),
        ("definition_useful_insight", "Definition of Useful Insight"),
    ]
    snapshot_lines = [f"Study Title: {study_dict.get('title', 'Untitled')}"]
    snapshot_lines.append(f"Study Type: {study_dict.get('study_type') or 'Not yet selected'}")
    for key, label in anchor_keys:
        val = get_v1a_value(study_dict, key)
        snapshot_lines.append(f"{label}: {val or '[not yet provided]'}")
    snapshot_lines.append(f"Personas attached: {persona_count}")
    oc_block, oc_keys = _extract_optional_context(study_dict)
    if oc_block:
        snapshot_lines.append("")
        snapshot_lines.append(oc_block)
    study_snapshot = "\n".join(snapshot_lines)
    sid = study_id or study_dict.get("id", "?")
    print(
        f"OPTIONAL_CONTEXT_INJECT_MARK study={sid} "
        f"included={'true' if oc_keys else 'false'} keys={','.join(oc_keys)}",
        flush=True,
    )
    return MARK_SYSTEM_PROMPT + "\n\nCurrent study state:\n" + study_snapshot


# ============================================================
# PROPOSAL POLICY (V1A Prompt 3) — single source of truth
# ============================================================

_FIELD_QUESTION_KEYWORDS = {
    "business_problem": ["business problem", "problem are you trying", "challenge"],
    "decision_to_support": ["decision", "inform", "support"],
    "market_geography": ["market / geography", "market", "geography", "region", "country", "city"],
    "product_concept": ["product / concept", "product", "service", "concept", "offering"],
    "target_audience": ["target audience", "who is the audience", "customer segment"],
    "definition_useful_insight": ["useful insight", "definition", "what would a useful insight"],
}

_UNCERTAINTY_WORDS = ["not sure", "i don't know", "maybe", "tbd", "depends", "unsure"]
_MULTI_TOPIC_MARKERS = [" and ", " also ", " plus ", " as well as ", ";", " / ", " then "]

_MODEL_CONFIDENCE_MAP = {"high": 0.90, "medium": 0.70, "low": 0.40}
_CONFIDENCE_THRESHOLD = 0.70


def policy_parse_last_mark_proposal(chat_messages):
    if not chat_messages:
        return None
    last = chat_messages[-1]
    if last.get("sender") != "mark":
        return None
    text = last.get("message_text", "")
    if "Proposed updates:" not in text:
        return None
    lines = text.split("\n")
    field = None
    value_lines = []
    model_confidence = None
    capturing_value = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("- field:") or stripped.startswith("field:"):
            remainder = stripped.split("field:", 1)[1].strip()
            if "value:" in remainder:
                field_part, rest = remainder.split("value:", 1)
                field = field_part.strip()
                rest = rest.strip()
                if "confidence:" in rest:
                    val_part, conf_part = rest.rsplit("confidence:", 1)
                    value_lines = [val_part.strip()] if val_part.strip() else []
                    conf_val = conf_part.strip().lower()
                    if conf_val in _MODEL_CONFIDENCE_MAP:
                        model_confidence = conf_val
                else:
                    value_lines = [rest] if rest else []
                capturing_value = True
            else:
                field = remainder.strip()
                capturing_value = False
                value_lines = []
        elif field and (stripped.startswith("value:") or stripped.startswith("- value:")):
            val_part = stripped.split("value:", 1)[1].strip()
            if "confidence:" in val_part:
                v, c = val_part.rsplit("confidence:", 1)
                if v.strip():
                    value_lines.append(v.strip())
                conf_val = c.strip().lower()
                if conf_val in _MODEL_CONFIDENCE_MAP:
                    model_confidence = conf_val
            else:
                if val_part:
                    value_lines.append(val_part)
            capturing_value = True
        elif stripped.startswith("confidence:") or stripped.startswith("- confidence:"):
            conf_val = stripped.split("confidence:", 1)[1].strip().lower()
            if conf_val in _MODEL_CONFIDENCE_MAP:
                model_confidence = conf_val
            capturing_value = False
        elif capturing_value:
            if stripped.lower().startswith("confirmation question") or "Should I save" in stripped or stripped == "":
                capturing_value = False
            else:
                value_lines.append(stripped)
    if not field or field not in _VALID_V1A_FIELDS:
        return None
    value = " ".join(value_lines).strip()
    if not value:
        return None
    return {
        "field": field,
        "value": value,
        "model_confidence": model_confidence,
        "server_confidence": None,
        "final_confidence": None,
        "allow_confirm": None,
        "block_reason": None,
    }


def policy_score_proposal(chat_messages, proposal, study_dict, study_id=None):
    field = proposal["field"]
    value = proposal["value"]
    score = 0.40
    mark_msgs_before = []
    found_last_user = False
    for m in reversed(chat_messages[:-1]):
        if m.get("sender") == "user" and not found_last_user:
            found_last_user = True
            continue
        if m.get("sender") == "mark" and found_last_user:
            mark_msgs_before.append(m.get("message_text", ""))
            break
    if mark_msgs_before:
        prev_mark = mark_msgs_before[0].lower()
        keywords = _FIELD_QUESTION_KEYWORDS.get(field, [])
        if any(kw in prev_mark for kw in keywords):
            score += 0.35
    vlen = len(value)
    if 3 <= vlen <= 180:
        score += 0.15
    if (value.count("\n") + 1) <= 2:
        score += 0.05
    val_lower = value.lower()
    if field != "business_problem":
        if any(uw in val_lower for uw in _UNCERTAINTY_WORDS):
            score -= 0.40
    if any(mt in val_lower for mt in _MULTI_TOPIC_MARKERS):
        score -= 0.20
    server_score = max(0.0, min(1.0, score))
    mc_num = _MODEL_CONFIDENCE_MAP.get(proposal.get("model_confidence"))
    if mc_num is not None:
        final_conf = min(server_score, mc_num)
    else:
        final_conf = server_score
    proposal["server_confidence"] = round(server_score, 2)
    proposal["final_confidence"] = round(final_conf, 2)
    proposal["allow_confirm"] = final_conf >= _CONFIDENCE_THRESHOLD
    if not proposal["allow_confirm"]:
        if field != "business_problem" and any(uw in val_lower for uw in _UNCERTAINTY_WORDS):
            proposal["block_reason"] = "ambiguous_value"
        elif mc_num is not None and mc_num < _CONFIDENCE_THRESHOLD:
            proposal["block_reason"] = "low_confidence"
        else:
            proposal["block_reason"] = "low_confidence"
    sid = study_id or (study_dict.get("id") if isinstance(study_dict, dict) else None) or "?"
    print(
        f"PROPOSAL_CONFIDENCE study={sid} field={field} "
        f"server={server_score:.2f} model={proposal.get('model_confidence') or 'none'} "
        f"final={final_conf:.2f} allow={'true' if proposal['allow_confirm'] else 'false'}",
        flush=True,
    )
    return proposal


def policy_should_show_confirm(proposal):
    if proposal is None:
        return False
    return bool(proposal.get("allow_confirm"))


_CONFIRMATION_PREFIXES = ("yes", "ok", "save", "confirmed", "confirm", "sure", "go ahead", "do it", "yep", "yeah")

def policy_validate_for_save(field, value):
    if not field or not value:
        return False, "Missing required fields."
    if len(value) > 2000:
        return False, "Proposed value is too long; please clarify in chat."
    if field not in _VALID_V1A_FIELDS:
        return False, "Invalid field."
    if value.lower().strip().startswith(_CONFIRMATION_PREFIXES) and len(value) < 40:
        return False, "Confirmation text cannot be saved as study content."
    return True, None


def policy_apply_save(conn, study_id, field, value):
    set_v1a_value(conn, study_id, field, value)
    save_label = V1A_LABELS.get(field, field.replace("_", " ").title())
    conn.execute(
        "INSERT INTO chat_messages (study_id, sender, message_text) VALUES (?, 'mark', ?)",
        (study_id, f"Saved: {save_label}."),
    )
    return save_label


def parse_mark_proposal_or_none(chat_messages):
    if not chat_messages:
        return None
    for msg in reversed(chat_messages):
        if msg.get("sender") != "mark":
            continue
        text = msg.get("message_text", "")
        if "Proposed updates:" not in text:
            continue
        before_proposal = text.split("Proposed updates:", 1)[0]
        if "?" in before_proposal:
            continue
        parsed = policy_parse_last_mark_proposal([msg])
        if parsed:
            return parsed
    return None


# Legacy aliases (used by worker fallback)
_parse_proposed_update = policy_parse_last_mark_proposal
compute_server_confidence = lambda msgs, pu, sd: policy_score_proposal(msgs, dict(pu), sd)["server_confidence"]
_model_confidence_to_num = lambda mc: _MODEL_CONFIDENCE_MAP.get(mc)

# ============================================================
# END PROPOSAL POLICY
# ============================================================


BUDGET_CEILINGS = {
    "synthetic_survey": 100_000,
    "synthetic_idi": 150_000,
    "synthetic_focus_group": 300_000,
}

GROUNDING_REASON_CODES = [
    "ADMIN_SOURCE_NO_MATCH",
    "ADMIN_SOURCE_OUT_OF_SCOPE",
    "ADMIN_SOURCE_TEMP_UNAVAILABLE",
    "ADMIN_SOURCE_NOT_RELEVANT",
]

PERSONA_DOSSIER_FIELDS = [
    ("persona_summary", "Persona Summary"),
    ("demographic_frame", "Demographic Frame"),
    ("psychographic_profile", "Psychographic Profile"),
    ("contextual_constraints", "Contextual Constraints"),
    ("behavioural_tendencies", "Behavioural Tendencies"),
    (
        "ai_model_provenance",
        "AI Model Provenance (provider family + model id + selection method)",
    ),
    ("grounding_sources", "Grounding Sources (list)"),
    ("confidence_and_limits", "Confidence and Limits"),
]


def get_db():
    conn = sqlite3.connect(
        DB_PATH,
        timeout=30,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row

    # FIX 3A: SQLite concurrency hardening
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 30000")

    return conn


def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            email         TEXT    UNIQUE NOT NULL,
            username      TEXT    NOT NULL,
            password_hash TEXT    NOT NULL,
            state         TEXT    NOT NULL DEFAULT 'pending',
            created_at    TEXT    NOT NULL DEFAULT (datetime('now')),
            name          TEXT    NOT NULL DEFAULT '',
            company       TEXT    NOT NULL DEFAULT '',
            role          TEXT    NOT NULL DEFAULT '',
            location      TEXT    NOT NULL DEFAULT '',
            linkedin      TEXT    NOT NULL DEFAULT '',
            last_email_verification_timestamp TEXT
        )
    """)
    for col_def in [
        ("name", "TEXT NOT NULL DEFAULT ''"),
        ("company", "TEXT NOT NULL DEFAULT ''"),
        ("role", "TEXT NOT NULL DEFAULT ''"),
        ("location", "TEXT NOT NULL DEFAULT ''"),
        ("linkedin", "TEXT NOT NULL DEFAULT ''"),
        ("last_email_verification_timestamp", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE users ADD COLUMN {col_def[0]} {col_def[1]}")
        except Exception:
            pass
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            token      TEXT PRIMARY KEY,
            user_id    INTEGER,
            is_admin   INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS studies (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id                 INTEGER NOT NULL,
            title                   TEXT    NOT NULL,
            study_type              TEXT,
            status                  TEXT    NOT NULL DEFAULT 'draft',
            business_problem        TEXT,
            decision_to_support     TEXT,
            known_vs_unknown        TEXT,
            target_audience         TEXT,
            study_fit               TEXT,
            definition_useful_insight TEXT,
            persona_versions_used   TEXT    NOT NULL DEFAULT '[]',
            personas_used           TEXT    NOT NULL DEFAULT '[]',
            created_at              TEXT    NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS personas (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id                 INTEGER NOT NULL,
            persona_id              TEXT    NOT NULL,
            version                 INTEGER NOT NULL DEFAULT 1,
            persona_instance_id     TEXT,
            name                    TEXT    NOT NULL,
            persona_summary         TEXT    NOT NULL,
            demographic_frame       TEXT    NOT NULL,
            psychographic_profile   TEXT    NOT NULL,
            contextual_constraints  TEXT    NOT NULL,
            behavioural_tendencies  TEXT    NOT NULL,
            ai_model_provenance     TEXT    NOT NULL,
            grounding_sources       TEXT    NOT NULL,
            confidence_and_limits   TEXT    NOT NULL,
            created_at              TEXT    NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id),
            UNIQUE(persona_id, version)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS admin_web_sources (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            url        TEXT    NOT NULL,
            name       TEXT    NOT NULL,
            city       TEXT,
            country    TEXT,
            language   TEXT    NOT NULL DEFAULT 'en',
            status     TEXT    NOT NULL DEFAULT 'active',
            created_at TEXT    NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS grounding_traces (
            id                           INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id                     TEXT,
            persona_id                   TEXT,
            trigger_event                TEXT    NOT NULL,
            admin_sources_configured     INTEGER NOT NULL DEFAULT 0,
            admin_sources_queried        INTEGER NOT NULL DEFAULT 0,
            admin_sources_matched        INTEGER NOT NULL DEFAULT 0,
            admin_sources_used_in_output INTEGER NOT NULL DEFAULT 0,
            admin_source_reason_code     TEXT,
            timestamp_utc                TEXT    NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cost_telemetry (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id             INTEGER,
            study_type           TEXT,
            tokens_mark          INTEGER DEFAULT 0,
            tokens_lisa          INTEGER DEFAULT 0,
            tokens_ben           INTEGER DEFAULT 0,
            tokens_total         INTEGER DEFAULT 0,
            model_call_count     INTEGER DEFAULT 0,
            qa_retry_count       INTEGER DEFAULT 0,
            followup_round_count INTEGER DEFAULT 0,
            status               TEXT,
            termination_reason   TEXT,
            timestamp_utc        TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS chat_messages (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id      INTEGER NOT NULL,
            sender        TEXT NOT NULL,
            message_text  TEXT NOT NULL,
            timestamp_utc TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS followups (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id            INTEGER NOT NULL,
            followup_round      INTEGER NOT NULL CHECK (followup_round BETWEEN 1 AND 2),
            user_question       TEXT NOT NULL,
            generated_output    TEXT,
            qa_status           TEXT,
            qa_notes            TEXT,
            timestamp_utc       TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(study_id, followup_round)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_uploads (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id               INTEGER NOT NULL,
            filename              TEXT    NOT NULL,
            file_type             TEXT    NOT NULL,
            file_size_bytes       INTEGER NOT NULL,
            storage_path          TEXT,
            uploaded_at           TEXT    NOT NULL DEFAULT (datetime('now')),
            status                TEXT    NOT NULL DEFAULT 'active',
            deleted_at            TEXT,
            content_sha256        TEXT,
            retained_excerpt_text TEXT,
            retained_excerpt_bytes INTEGER,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS study_documents (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id    INTEGER NOT NULL,
            user_doc_id INTEGER NOT NULL,
            attached_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (study_id) REFERENCES studies(id),
            FOREIGN KEY (user_doc_id) REFERENCES user_uploads(id),
            UNIQUE(study_id, user_doc_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS admin_uploads (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            filename        TEXT    NOT NULL,
            file_type       TEXT    NOT NULL,
            file_size_bytes INTEGER NOT NULL,
            storage_path    TEXT    NOT NULL,
            uploaded_at     TEXT    NOT NULL DEFAULT (datetime('now')),
            status          TEXT    NOT NULL DEFAULT 'active'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS model_config (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS allowed_models (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            model_id   TEXT NOT NULL UNIQUE,
            source     TEXT NOT NULL DEFAULT 'replit_openrouter',
            status     TEXT NOT NULL DEFAULT 'active',
            added_at   TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS persona_model_pool (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            model_id TEXT NOT NULL UNIQUE,
            status   TEXT NOT NULL DEFAULT 'active',
            added_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS model_health_checks (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id           TEXT NOT NULL,
            run_type         TEXT NOT NULL,
            started_at       TEXT NOT NULL,
            finished_at      TEXT,
            integration_mode TEXT,
            summary_status   TEXT,
            details_json     TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS model_health_status (
            model_id       TEXT PRIMARY KEY,
            status         TEXT NOT NULL DEFAULT 'unknown',
            last_tested_at TEXT,
            last_error     TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weekly_qa_reports (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            week_start_date TEXT NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            report_text     TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS blog_posts (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            title            TEXT NOT NULL,
            slug             TEXT,
            body             TEXT NOT NULL,
            status           TEXT NOT NULL DEFAULT 'published',
            image_path       TEXT,
            image_type       TEXT,
            image_size_bytes INTEGER,
            is_pinned        INTEGER NOT NULL DEFAULT 0,
            pinned_rank      INTEGER,
            created_at       TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    seed_count = conn.execute("SELECT COUNT(*) FROM allowed_models").fetchone()[0]
    if seed_count == 0:
        for m in SEED_ALLOWED_MODELS:
            conn.execute(
                "INSERT OR IGNORE INTO allowed_models (model_id, source, status) VALUES (?, 'replit_openrouter', 'active')",
                (m,),
            )
        for m in SEED_ALLOWED_MODELS[:3]:
            conn.execute(
                "INSERT OR IGNORE INTO persona_model_pool (model_id, status) VALUES (?, 'active')",
                (m,),
            )
        conn.execute(
            "INSERT OR IGNORE INTO model_config (key, value) VALUES ('mark_model', ?)",
            (SEED_ALLOWED_MODELS[0],),
        )
        conn.execute(
            "INSERT OR IGNORE INTO model_config (key, value) VALUES ('lisa_model', ?)",
            (SEED_ALLOWED_MODELS[1],),
        )
        conn.execute(
            "INSERT OR IGNORE INTO model_config (key, value) VALUES ('ben_model', ?)",
            (SEED_ALLOWED_MODELS[2],),
        )
    migrate_db(conn)
    conn.commit()
    conn.close()


def migrate_db(conn):
    cols = [row[1] for row in conn.execute("PRAGMA table_info(personas)").fetchall()]
    if "persona_instance_id" not in cols:
        conn.execute("ALTER TABLE personas ADD COLUMN persona_instance_id TEXT")

    conn.execute("""
        UPDATE personas SET persona_instance_id = persona_id || '-v' || version
        WHERE persona_instance_id IS NULL
    """)

    study_cols = [
        row[1] for row in conn.execute("PRAGMA table_info(studies)").fetchall()
    ]
    if "personas_used" not in study_cols:
        conn.execute(
            "ALTER TABLE studies ADD COLUMN personas_used TEXT NOT NULL DEFAULT '[]'"
        )
    if "study_output" not in study_cols:
        conn.execute("ALTER TABLE studies ADD COLUMN study_output TEXT")
    if "respondent_count" not in study_cols:
        conn.execute(
            "ALTER TABLE studies ADD COLUMN respondent_count INTEGER DEFAULT 100"
        )
    if "question_count" not in study_cols:
        conn.execute("ALTER TABLE studies ADD COLUMN question_count INTEGER DEFAULT 8")
    if "survey_brief" not in study_cols:
        conn.execute("ALTER TABLE studies ADD COLUMN survey_brief TEXT")
    if "survey_questions" not in study_cols:
        conn.execute("ALTER TABLE studies ADD COLUMN survey_questions TEXT")
    if "qa_status" not in study_cols:
        conn.execute("ALTER TABLE studies ADD COLUMN qa_status TEXT")
    if "qa_notes" not in study_cols:
        conn.execute("ALTER TABLE studies ADD COLUMN qa_notes TEXT")
    if "confidence_summary" not in study_cols:
        conn.execute("ALTER TABLE studies ADD COLUMN confidence_summary TEXT")
    if "final_report" not in study_cols:
        conn.execute("ALTER TABLE studies ADD COLUMN final_report TEXT")

    fu_info = conn.execute("PRAGMA table_info(followups)").fetchall()
    fu_cols = [r[1] for r in fu_info]
    if fu_cols and "qa_status" not in fu_cols:
        conn.execute("DROP TABLE followups")
        conn.execute("""
            CREATE TABLE followups (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                study_id            INTEGER NOT NULL,
                followup_round      INTEGER NOT NULL CHECK (followup_round BETWEEN 1 AND 2),
                user_question       TEXT NOT NULL,
                generated_output    TEXT,
                qa_status           TEXT,
                qa_notes            TEXT,
                timestamp_utc       TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(study_id, followup_round)
            )
        """)

    rows = conn.execute(
        "SELECT id, persona_versions_used, personas_used FROM studies"
    ).fetchall()
    for row in rows:
        old_data = json.loads(row["persona_versions_used"] or "[]")
        new_data = json.loads(row["personas_used"] or "[]")
        if old_data and not new_data:
            migrated = []
            for entry in old_data:
                if isinstance(entry, dict):
                    pid = entry.get("persona_id", "")
                    ver = entry.get("version", 1)
                    migrated.append(f"{pid}-v{ver}")
                elif isinstance(entry, str):
                    migrated.append(entry)
            conn.execute(
                "UPDATE studies SET personas_used = ? WHERE id = ?",
                (json.dumps(migrated), row["id"]),
            )

    uu_cols = [
        row[1] for row in conn.execute("PRAGMA table_info(user_uploads)").fetchall()
    ]

    if "study_id" in uu_cols:
        existing_links = conn.execute(
            "SELECT id, study_id FROM user_uploads WHERE study_id IS NOT NULL AND study_id != 0"
        ).fetchall()
        sd_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='study_documents'"
        ).fetchone()
        if sd_exists:
            conn.execute("DROP TABLE study_documents")
        conn.execute("""
            CREATE TABLE study_documents (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                study_id    INTEGER NOT NULL,
                user_doc_id INTEGER NOT NULL,
                attached_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(study_id, user_doc_id)
            )
        """)
        for link in existing_links:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO study_documents (study_id, user_doc_id) VALUES (?, ?)",
                    (link["study_id"], link["id"]),
                )
            except Exception:
                pass
        conn.execute("""
            CREATE TABLE user_uploads_new (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id               INTEGER NOT NULL,
                filename              TEXT    NOT NULL,
                file_type             TEXT    NOT NULL,
                file_size_bytes       INTEGER NOT NULL,
                storage_path          TEXT,
                uploaded_at           TEXT    NOT NULL DEFAULT (datetime('now')),
                status                TEXT    NOT NULL DEFAULT 'active',
                deleted_at            TEXT,
                content_sha256        TEXT,
                retained_excerpt_text TEXT,
                retained_excerpt_bytes INTEGER
            )
        """)
        conn.execute("""
            INSERT INTO user_uploads_new (id, user_id, filename, file_type, file_size_bytes, storage_path, uploaded_at)
            SELECT id, user_id, filename, file_type, file_size_bytes, storage_path, uploaded_at
            FROM user_uploads
        """)
        conn.execute("DROP TABLE user_uploads")
        conn.execute("ALTER TABLE user_uploads_new RENAME TO user_uploads")
    else:
        if "status" not in uu_cols:
            conn.execute(
                "ALTER TABLE user_uploads ADD COLUMN status TEXT NOT NULL DEFAULT 'active'"
            )
        if "deleted_at" not in uu_cols:
            conn.execute("ALTER TABLE user_uploads ADD COLUMN deleted_at TEXT")
        if "content_sha256" not in uu_cols:
            conn.execute("ALTER TABLE user_uploads ADD COLUMN content_sha256 TEXT")
        if "retained_excerpt_text" not in uu_cols:
            conn.execute(
                "ALTER TABLE user_uploads ADD COLUMN retained_excerpt_text TEXT"
            )
        if "retained_excerpt_bytes" not in uu_cols:
            conn.execute(
                "ALTER TABLE user_uploads ADD COLUMN retained_excerpt_bytes INTEGER"
            )

    conn.execute("""
        CREATE TABLE IF NOT EXISTS study_documents (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id    INTEGER NOT NULL,
            user_doc_id INTEGER NOT NULL,
            attached_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (study_id) REFERENCES studies(id),
            FOREIGN KEY (user_doc_id) REFERENCES user_uploads(id),
            UNIQUE(study_id, user_doc_id)
        )
    """)

    bp_cols = [
        row[1] for row in conn.execute("PRAGMA table_info(blog_posts)").fetchall()
    ]
    if "is_pinned" not in bp_cols:
        conn.execute(
            "ALTER TABLE blog_posts ADD COLUMN is_pinned INTEGER NOT NULL DEFAULT 0"
        )
    if "pinned_rank" not in bp_cols:
        conn.execute("ALTER TABLE blog_posts ADD COLUMN pinned_rank INTEGER")


def create_session(user_id=None, is_admin=False):
    token = secrets.token_urlsafe(32)
    conn = get_db()
    conn.execute(
        "INSERT INTO sessions (token, user_id, is_admin) VALUES (?, ?, ?)",
        (token, user_id, 1 if is_admin else 0),
    )
    conn.commit()
    conn.close()
    return token


def get_session_data(token):
    if not token:
        return None, False
    conn = get_db()
    row = conn.execute("SELECT * FROM sessions WHERE token = ?", (token,)).fetchone()
    if not row:
        conn.close()
        return None, False
    user = None
    if row["user_id"]:
        user = conn.execute(
            "SELECT * FROM users WHERE id = ?", (row["user_id"],)
        ).fetchone()
        if user:
            user = dict(user)
    conn.close()
    return user, bool(row["is_admin"])


def user_needs_verification(user):
    if not user or user.get("state") != "active":
        return False
    last_ts = user.get("last_email_verification_timestamp")
    if not last_ts:
        return True
    try:
        last_dt = datetime.strptime(last_ts, "%Y-%m-%d %H:%M:%S")
        from datetime import timedelta

        return (datetime.utcnow() - last_dt).days >= EMAIL_VERIFY_INTERVAL_DAYS
    except (ValueError, TypeError):
        return True


def require_verified_user(token, user, is_admin):
    if is_admin:
        return None
    if user and user_needs_verification(user):
        return redirect(url_for("verify_email", token=token))
    return None


def delete_session(token):
    if token:
        conn = get_db()
        conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
        conn.commit()
        conn.close()


def get_token():
    return request.args.get("token") or request.form.get("token") or ""


def get_user_personas_list(conn, user_id):
    rows = conn.execute(
        "SELECT persona_instance_id, name, created_at FROM personas WHERE user_id = ? ORDER BY id DESC",
        (user_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_admin_web_sources(conn, status_filter=None):
    if status_filter:
        rows = conn.execute(
            "SELECT * FROM admin_web_sources WHERE status = ? ORDER BY id DESC",
            (status_filter,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM admin_web_sources ORDER BY id DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def create_grounding_trace(conn, trigger_event, study_id=None, persona_id=None):
    active_sources = get_admin_web_sources(conn, status_filter="active")
    has_active = len(active_sources) > 0

    admin_sources_configured = has_active
    admin_sources_queried = has_active
    admin_sources_matched = False
    admin_sources_used_in_output = False

    reason_code = None
    if not admin_sources_used_in_output:
        if has_active:
            reason_code = "ADMIN_SOURCE_NOT_RELEVANT"
        else:
            reason_code = "ADMIN_SOURCE_NO_MATCH"

    now = datetime.utcnow()
    ts = now.strftime("%Y-%m-%d %H:%M:%S.") + f"{now.microsecond:06d}"
    conn.execute(
        """INSERT INTO grounding_traces
           (study_id, persona_id, trigger_event,
            admin_sources_configured, admin_sources_queried,
            admin_sources_matched, admin_sources_used_in_output,
            admin_source_reason_code, timestamp_utc)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            study_id,
            persona_id,
            trigger_event,
            1 if admin_sources_configured else 0,
            1 if admin_sources_queried else 0,
            1 if admin_sources_matched else 0,
            1 if admin_sources_used_in_output else 0,
            reason_code,
            ts,
        ),
    )


def normalize_personas_used(raw_json):
    try:
        data = json.loads(raw_json) if raw_json else []
    except (json.JSONDecodeError, TypeError):
        data = []
    if not isinstance(data, list):
        data = []
    result = []
    for item in data:
        if isinstance(item, str) and item.strip():
            result.append(item.strip())
        elif isinstance(item, dict):
            if "persona_instance_id" in item:
                val = str(item["persona_instance_id"]).strip()
                if val:
                    result.append(val)
            elif "persona_id" in item:
                pid = str(item["persona_id"]).strip()
                ver = item.get("version", 1)
                if pid:
                    result.append(f"{pid}-v{ver}")
    seen = set()
    deduped = []
    for p in result:
        if p not in seen:
            seen.add(p)
            deduped.append(p)
    return deduped


def persona_used_in_completed_study(conn, persona_instance_id, user_id):
    rows = conn.execute(
        "SELECT id, personas_used FROM studies WHERE user_id = ? AND status = 'completed'",
        (user_id,),
    ).fetchall()
    for row in rows:
        used = normalize_personas_used(row["personas_used"])
        if persona_instance_id in used:
            return True
    return False


def remove_persona_from_non_completed_studies(conn, user_id, persona_instance_id):
    rows = conn.execute(
        "SELECT id, personas_used FROM studies WHERE user_id = ? AND status != 'completed'",
        (user_id,),
    ).fetchall()
    for row in rows:
        used = normalize_personas_used(row["personas_used"])
        if persona_instance_id in used:
            used = [p for p in used if p != persona_instance_id]
            conn.execute(
                "UPDATE studies SET personas_used = ? WHERE id = ?",
                (json.dumps(used), row["id"]),
            )
            print(f"Cleaned dangling persona references from study {row['id']}")


@app.route("/")
def index():
    token = get_token()
    user, is_admin = get_session_data(token)

    if not user and not is_admin:
        error = request.args.get("error")
        show_auth_tab = request.args.get("show_auth_tab", "signup")
        return render_template(
            "landing.html",
            error=error,
            show_auth_tab=show_auth_tab,
            latest_blog_posts=get_latest_blog_posts(2),
        )

    verify_redirect = require_verified_user(token, user, is_admin)
    if verify_redirect:
        return verify_redirect

    pending_users = []
    all_users = []
    studies = []
    personas_list = []
    view_persona = None
    configure_study = None
    configure_study_personas = []
    available_personas = []
    clone_source = None
    study_uploads = []
    study_uploads_total_size = 0
    study_attachable_docs = []
    view_study_output = None
    chat_messages = []
    chat_save_buttons = []
    proposed_update = None
    last_mark_has_proposal_text = False
    has_pending_mark = False
    mark_recommendation = ""
    mark_recommendation_label = ""
    mark_recommendation_reason = ""
    admin_web_sources = []
    grounding_traces = []
    cost_telemetry_rows = []
    admin_uploads_list = []
    all_blog_posts = []
    model_config = {}
    allowed_models_list = []
    persona_pool_list = []
    health_status_list = []
    latest_weekly_report = None
    latest_health_check = None
    docs_list = []
    docs_page = 1
    docs_q = ""
    docs_total_pages = 1
    docs_total = 0
    user_storage_used = 0
    user_storage_cap_mb = UPLOAD_USER_STORAGE_CAP // (1024 * 1024)

    if is_admin:
        conn = get_db()
        pending_users = [
            dict(r)
            for r in conn.execute(
                "SELECT id, email, username, state, created_at FROM users WHERE state = 'pending' ORDER BY created_at DESC"
            ).fetchall()
        ]
        all_users = [
            dict(r)
            for r in conn.execute(
                "SELECT id, email, username, state, created_at FROM users ORDER BY created_at DESC"
            ).fetchall()
        ]
        admin_web_sources = get_admin_web_sources(conn)
        grounding_traces = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM grounding_traces ORDER BY id DESC LIMIT 50"
            ).fetchall()
        ]
        cost_telemetry_rows = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM cost_telemetry ORDER BY id DESC LIMIT 50"
            ).fetchall()
        ]
        admin_uploads_list = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM admin_uploads ORDER BY uploaded_at DESC"
            ).fetchall()
        ]
        all_blog_posts = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM blog_posts ORDER BY is_pinned DESC, pinned_rank ASC, created_at DESC, id DESC"
            ).fetchall()
        ]
        for row in conn.execute("SELECT key, value FROM model_config").fetchall():
            model_config[row["key"]] = row["value"]
        allowed_models_list = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM allowed_models ORDER BY model_id"
            ).fetchall()
        ]
        persona_pool_list = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM persona_model_pool ORDER BY model_id"
            ).fetchall()
        ]
        conn.close()

        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        conn2 = get_db()
        today_check = conn2.execute(
            "SELECT id FROM model_health_checks WHERE started_at >= ? LIMIT 1",
            (today_str,),
        ).fetchone()
        conn2.close()
        if not today_check:
            try:
                run_model_health_check("auto_daily")
            except Exception as e:
                print(f"AUTO_HEALTH_CHECK_ERROR: {e}", flush=True)

        try:
            generate_weekly_qa_report()
        except Exception as e:
            print(f"WEEKLY_REPORT_ERROR: {e}", flush=True)

        conn3 = get_db()
        health_status_list = [
            dict(r)
            for r in conn3.execute(
                "SELECT * FROM model_health_status ORDER BY model_id"
            ).fetchall()
        ]
        hc_row = conn3.execute(
            "SELECT * FROM model_health_checks ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        if hc_row:
            latest_health_check = dict(hc_row)
        wr = conn3.execute(
            "SELECT * FROM weekly_qa_reports ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if wr:
            latest_weekly_report = dict(wr)
        conn3.close()

    studies_page = 1
    studies_q = ""
    studies_total_pages = 1
    studies_total = 0
    personas_page = 1
    personas_q = ""
    personas_total_pages = 1
    personas_total = 0
    PAGE_SIZE = 10
    usage_count = 0
    usage_limit = FREE_TIER_MONTHLY_LIMIT
    usage_limit_reached = False
    usage_window_start = ""
    usage_window_end = ""

    if user and user["state"] == "active":
        conn = get_db()

        usage_count, usage_limit, usage_window_start, usage_window_end = (
            get_monthly_usage(conn, user["id"])
        )
        usage_limit_reached = usage_count >= usage_limit

        studies_page = max(1, int(request.args.get("studies_page", "1") or "1"))
        studies_q = (request.args.get("studies_q") or "").strip()
        if studies_q:
            studies_total = conn.execute(
                "SELECT COUNT(*) FROM studies WHERE user_id = ? AND title LIKE ?",
                (user["id"], f"%{studies_q}%"),
            ).fetchone()[0]
        else:
            studies_total = conn.execute(
                "SELECT COUNT(*) FROM studies WHERE user_id = ?",
                (user["id"],),
            ).fetchone()[0]
        studies_total_pages = max(1, (studies_total + PAGE_SIZE - 1) // PAGE_SIZE)
        studies_page = min(studies_page, studies_total_pages)
        s_offset = (studies_page - 1) * PAGE_SIZE
        if studies_q:
            studies = [
                dict(r)
                for r in conn.execute(
                    "SELECT id, title, study_type, status, created_at, study_output, qa_status, confidence_summary, final_report FROM studies WHERE user_id = ? AND title LIKE ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
                    (user["id"], f"%{studies_q}%", PAGE_SIZE, s_offset),
                ).fetchall()
            ]
        else:
            studies = [
                dict(r)
                for r in conn.execute(
                    "SELECT id, title, study_type, status, created_at, study_output, qa_status, confidence_summary, final_report FROM studies WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
                    (user["id"], PAGE_SIZE, s_offset),
                ).fetchall()
            ]

        configure_id = request.args.get("configure")
        if configure_id:
            row = conn.execute(
                "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
                (configure_id, user["id"]),
            ).fetchone()
            if row:
                configure_study = dict(row)
                if configure_study.get("survey_questions"):
                    try:
                        configure_study["survey_questions_list"] = json.loads(
                            configure_study["survey_questions"]
                        )
                    except (json.JSONDecodeError, TypeError):
                        configure_study["survey_questions_list"] = []
                else:
                    configure_study["survey_questions_list"] = []
                if configure_study.get("survey_brief"):
                    try:
                        configure_study["survey_brief_data"] = json.loads(
                            configure_study["survey_brief"]
                        )
                    except (json.JSONDecodeError, TypeError):
                        configure_study["survey_brief_data"] = {}
                else:
                    configure_study["survey_brief_data"] = {}
                if (
                    configure_study.get("qa_notes")
                    and configure_study.get("qa_status") == "precheck_failed"
                ):
                    try:
                        configure_study["precheck_failures"] = json.loads(
                            configure_study["qa_notes"]
                        )
                    except (json.JSONDecodeError, TypeError):
                        configure_study["precheck_failures"] = []
                else:
                    configure_study["precheck_failures"] = []
                raw_ids = normalize_personas_used(configure_study.get("personas_used"))
                cleaned_ids = []
                for pid in raw_ids:
                    p_row = conn.execute(
                        "SELECT name FROM personas WHERE persona_instance_id = ? AND user_id = ?",
                        (pid, user["id"]),
                    ).fetchone()
                    if p_row:
                        configure_study_personas.append(
                            {"id": pid, "name": p_row["name"], "exists": True}
                        )
                        cleaned_ids.append(pid)
                    else:
                        pass
                if len(cleaned_ids) != len(raw_ids):
                    conn.execute(
                        "UPDATE studies SET personas_used = ? WHERE id = ?",
                        (json.dumps(cleaned_ids), configure_study["id"]),
                    )
                    conn.commit()
                    print(
                        f"Cleaned dangling persona references from study {configure_study['id']}"
                    )
                all_personas = get_user_personas_list(conn, user["id"])
                attached_ids = set(cleaned_ids)
                available_personas = [
                    p
                    for p in all_personas
                    if p["persona_instance_id"] not in attached_ids
                ]

                conn.execute(
                    "DELETE FROM chat_messages "
                    "WHERE sender='mark' AND message_text='\u23f3 Mark is thinking...' "
                    "AND timestamp_utc < datetime('now','-5 minutes')"
                )

                chat_messages = [
                    dict(r)
                    for r in conn.execute(
                        "SELECT * FROM chat_messages WHERE study_id = ? ORDER BY id ASC",
                        (configure_study["id"],),
                    ).fetchall()
                ]

                chat_save_buttons = []
                proposed_update = parse_mark_proposal_or_none(chat_messages)
                if proposed_update and configure_study:
                    proposed_update = policy_score_proposal(
                        chat_messages, proposed_update, dict(configure_study)
                    )
                last_mark_has_proposal_text = False
                if not proposed_update and chat_messages:
                    last_msg = chat_messages[-1]
                    if last_msg.get("sender") == "mark" and "Proposed updates:" in last_msg.get("message_text", ""):
                        last_mark_has_proposal_text = True
                has_pending_mark = any(
                    m.get("sender") == "mark" and m.get("message_text", "").startswith("\u23f3 Mark is thinking")
                    for m in chat_messages
                )

                if configure_study.get("status") == "draft" and configure_study.get("study_type"):
                    try:
                        auto_ben_precheck(conn, configure_study, user["id"])
                    except Exception as e:
                        print(f"AUTO_BEN_PRECHECK_ERROR study={configure_study.get('id')}: {e}", flush=True)
                    if configure_study.get("qa_status") == "precheck_failed" and configure_study.get("qa_notes"):
                        try:
                            configure_study["precheck_failures"] = json.loads(configure_study["qa_notes"])
                        except (json.JSONDecodeError, TypeError):
                            configure_study["precheck_failures"] = []
                    else:
                        configure_study["precheck_failures"] = []

                study_uploads = [
                    dict(r)
                    for r in conn.execute(
                        """SELECT u.id, u.filename, u.file_type, u.file_size_bytes, u.uploaded_at, u.status,
                              sd.id as attachment_id
                       FROM study_documents sd
                       JOIN user_uploads u ON u.id = sd.user_doc_id
                       WHERE sd.study_id = ? AND u.user_id = ?
                       ORDER BY sd.attached_at ASC""",
                        (configure_study["id"], user["id"]),
                    ).fetchall()
                ]
                study_uploads_total_size = sum(
                    u["file_size_bytes"]
                    for u in study_uploads
                    if u["status"] == "active"
                )
                attached_doc_ids = {u["id"] for u in study_uploads}
                all_active_docs = [
                    dict(r)
                    for r in conn.execute(
                        "SELECT id, filename, file_type, file_size_bytes FROM user_uploads WHERE user_id = ? AND status = 'active' ORDER BY uploaded_at DESC",
                        (user["id"],),
                    ).fetchall()
                ]
                study_attachable_docs = [
                    d for d in all_active_docs if d["id"] not in attached_doc_ids
                ]

                if not configure_study.get("study_type"):
                    bp = configure_study.get("business_problem") or ""
                    ds = configure_study.get("decision_to_support") or ""
                    if bp.strip() and ds.strip():
                        (
                            mark_recommendation,
                            mark_recommendation_label,
                            mark_recommendation_reason,
                        ) = get_mark_recommendation(bp, ds)

        personas_page = max(1, int(request.args.get("personas_page", "1") or "1"))
        personas_q = (request.args.get("personas_q") or "").strip()
        if personas_q:
            personas_total = conn.execute(
                "SELECT COUNT(*) FROM personas WHERE user_id = ? AND (persona_instance_id LIKE ? OR name LIKE ?)",
                (user["id"], f"%{personas_q}%", f"%{personas_q}%"),
            ).fetchone()[0]
        else:
            personas_total = conn.execute(
                "SELECT COUNT(*) FROM personas WHERE user_id = ?",
                (user["id"],),
            ).fetchone()[0]
        personas_total_pages = max(1, (personas_total + PAGE_SIZE - 1) // PAGE_SIZE)
        personas_page = min(personas_page, personas_total_pages)
        p_offset = (personas_page - 1) * PAGE_SIZE
        if personas_q:
            personas_list = [
                dict(r)
                for r in conn.execute(
                    "SELECT persona_instance_id, name, created_at FROM personas WHERE user_id = ? AND (persona_instance_id LIKE ? OR name LIKE ?) ORDER BY id DESC LIMIT ? OFFSET ?",
                    (
                        user["id"],
                        f"%{personas_q}%",
                        f"%{personas_q}%",
                        PAGE_SIZE,
                        p_offset,
                    ),
                ).fetchall()
            ]
        else:
            personas_list = [
                dict(r)
                for r in conn.execute(
                    "SELECT persona_instance_id, name, created_at FROM personas WHERE user_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
                    (user["id"], PAGE_SIZE, p_offset),
                ).fetchall()
            ]

        user_storage_used = conn.execute(
            "SELECT COALESCE(SUM(file_size_bytes), 0) FROM user_uploads WHERE user_id = ? AND status = 'active'",
            (user["id"],),
        ).fetchone()[0]
        docs_page = max(1, int(request.args.get("docs_page", "1") or "1"))
        docs_q = (request.args.get("docs_q") or "").strip()
        doc_id_num = (
            docs_q.upper().replace("DOC-", "").replace("D-", "") if docs_q else ""
        )
        doc_id_like = f"%{doc_id_num}%" if doc_id_num else ""
        if docs_q:
            docs_total = conn.execute(
                "SELECT COUNT(*) FROM user_uploads WHERE user_id = ? AND (filename LIKE ? OR CAST(id AS TEXT) LIKE ?)",
                (user["id"], f"%{docs_q}%", doc_id_like),
            ).fetchone()[0]
        else:
            docs_total = conn.execute(
                "SELECT COUNT(*) FROM user_uploads WHERE user_id = ?",
                (user["id"],),
            ).fetchone()[0]
        docs_total_pages = max(1, (docs_total + DOCS_PAGE_SIZE - 1) // DOCS_PAGE_SIZE)
        docs_page = min(docs_page, docs_total_pages)
        d_offset = (docs_page - 1) * DOCS_PAGE_SIZE
        if docs_q:
            docs_list = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM user_uploads WHERE user_id = ? AND (filename LIKE ? OR CAST(id AS TEXT) LIKE ?) ORDER BY id DESC LIMIT ? OFFSET ?",
                    (user["id"], f"%{docs_q}%", doc_id_like, DOCS_PAGE_SIZE, d_offset),
                ).fetchall()
            ]
        else:
            docs_list = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM user_uploads WHERE user_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
                    (user["id"], DOCS_PAGE_SIZE, d_offset),
                ).fetchall()
            ]

        view_pid = request.args.get("view_persona")
        if view_pid:
            row = conn.execute(
                "SELECT * FROM personas WHERE persona_instance_id = ? AND user_id = ?",
                (view_pid, user["id"]),
            ).fetchone()
            if row:
                view_persona = dict(row)

        clone_from = request.args.get("clone_from")
        if clone_from:
            row = conn.execute(
                "SELECT * FROM personas WHERE persona_instance_id = ? AND user_id = ?",
                (clone_from, user["id"]),
            ).fetchone()
            if row:
                clone_source = dict(row)

        view_output_id = request.args.get("view_output")
        if view_output_id:
            row = conn.execute(
                "SELECT * FROM studies WHERE id = ? AND user_id = ?",
                (view_output_id, user["id"]),
            ).fetchone()
            if row and (
                row["study_output"]
                or row["status"] in ("qa_blocked", "completed", "terminated_system")
            ):
                view_study_output = dict(row)
                if view_study_output.get("confidence_summary"):
                    try:
                        view_study_output["confidence_parsed"] = json.loads(
                            view_study_output["confidence_summary"]
                        )
                    except (json.JSONDecodeError, TypeError):
                        view_study_output["confidence_parsed"] = None
                else:
                    view_study_output["confidence_parsed"] = None
                followup_rows = conn.execute(
                    "SELECT * FROM followups WHERE study_id = ? ORDER BY followup_round ASC",
                    (view_study_output["id"],),
                ).fetchall()
                view_study_output["followups"] = [dict(f) for f in followup_rows]
                view_study_output["followup_count"] = len(followup_rows)
                report_version = request.args.get("report_version")
                report_version = (
                    int(report_version)
                    if report_version and report_version.isdigit()
                    else None
                )
                upload_names = [
                    r["filename"]
                    for r in conn.execute(
                        """SELECT u.filename FROM study_documents sd
                       JOIN user_uploads u ON u.id = sd.user_doc_id
                       WHERE sd.study_id = ? AND u.user_id = ?""",
                        (view_study_output["id"], user["id"]),
                    ).fetchall()
                ]
                active_admin_names = [
                    r["filename"]
                    for r in conn.execute(
                        "SELECT filename FROM admin_uploads WHERE status = 'active'"
                    ).fetchall()
                ]
                all_upload_names = upload_names + active_admin_names
                view_study_output["report_sections"] = build_structured_report(
                    view_study_output,
                    followups=view_study_output["followups"],
                    version=report_version,
                    uploaded_filenames=all_upload_names,
                )

        conn.close()

    new_research_step = request.args.get("new_research")
    new_research_type = request.args.get("nr_type", "")

    return render_template(
        "index.html",
        user=user,
        is_admin=is_admin,
        pending_users=pending_users,
        all_users=all_users,
        studies=studies,
        token=token,
        new_research_step=new_research_step,
        new_research_type=new_research_type,
        configure_study=configure_study,
        configure_study_personas=configure_study_personas,
        available_personas=available_personas,
        study_type_limits=STUDY_TYPE_LIMITS,
        personas_list=personas_list,
        show_new_persona=request.args.get("new_persona") == "1",
        clone_source=clone_source,
        view_persona=view_persona,
        view_study_output=view_study_output,
        admin_web_sources=admin_web_sources,
        grounding_traces=grounding_traces,
        cost_telemetry_rows=cost_telemetry_rows,
        model_config=model_config,
        allowed_models_list=allowed_models_list,
        persona_pool_list=persona_pool_list,
        health_status_list=health_status_list,
        latest_weekly_report=latest_weekly_report,
        latest_health_check=latest_health_check,
        mark_recommendation=mark_recommendation,
        mark_recommendation_label=mark_recommendation_label,
        mark_recommendation_reason=mark_recommendation_reason,
        chat_messages=chat_messages,
        chat_save_buttons=chat_save_buttons,
        proposed_update=proposed_update,
        last_mark_has_proposal_text=last_mark_has_proposal_text,
        has_pending_mark=has_pending_mark,
        studies_page=studies_page,
        studies_q=studies_q,
        studies_total_pages=studies_total_pages,
        studies_total=studies_total,
        personas_page=personas_page,
        personas_q=personas_q,
        personas_total_pages=personas_total_pages,
        personas_total=personas_total,
        usage_count=usage_count,
        usage_limit=usage_limit,
        usage_limit_reached=usage_limit_reached,
        usage_window_start=usage_window_start,
        usage_window_end=usage_window_end,
        study_uploads=study_uploads,
        study_uploads_total_size=study_uploads_total_size,
        study_attachable_docs=study_attachable_docs,
        upload_max_files=UPLOAD_MAX_FILES_PER_STUDY,
        upload_max_file_size_mb=UPLOAD_MAX_FILE_SIZE // (1024 * 1024),
        upload_max_total_mb=UPLOAD_MAX_TOTAL_PER_STUDY // (1024 * 1024),
        upload_allowed_extensions=", ".join(sorted(UPLOAD_ALLOWED_EXTENSIONS)),
        admin_uploads_list=admin_uploads_list,
        all_blog_posts=all_blog_posts,
        docs_list=docs_list,
        docs_page=docs_page,
        docs_q=docs_q,
        docs_total_pages=docs_total_pages,
        docs_total=docs_total,
        user_storage_used=user_storage_used,
        user_storage_cap_mb=user_storage_cap_mb,
        user_storage_cap=UPLOAD_USER_STORAGE_CAP,
        latest_blog_posts=get_latest_blog_posts(2),
    )


@app.route("/signup", methods=["POST"])
def signup():
    email = (request.form.get("email") or "").strip().lower()
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""

    if not email or not username or not password:
        return render_error("All fields are required.")
    if len(password) < 6 or len(password) > 10:
        return render_error("Password must be 6–10 characters.")

    conn = get_db()
    existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if existing:
        conn.close()
        return render_error("That email is already registered.")

    password_hash = generate_password_hash(password)
    conn.execute(
        "INSERT INTO users (email, username, password_hash, state) VALUES (?, ?, ?, 'pending')",
        (email, username, password_hash),
    )
    conn.commit()

    user_row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    conn.close()

    token = create_session(user_id=user_row["id"])
    return redirect(url_for("index", token=token))


@app.route("/login", methods=["POST"])
def login():
    email = (request.form.get("email") or "").strip().lower()
    password = request.form.get("password") or ""

    if not email or not password:
        return render_error("Email and password are required.")

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    conn.close()

    if not user or not check_password_hash(user["password_hash"], password):
        return render_error("Invalid email or password.")

    token = create_session(user_id=user["id"])
    return redirect(url_for("index", token=token))


@app.route("/admin-login", methods=["POST"])
def admin_login():
    password = request.form.get("admin_password") or ""
    if password != ADMIN_PASSWORD:
        return render_error("Invalid admin password.")

    token = create_session(is_admin=True)
    return redirect(url_for("index", token=token))


@app.route("/logout", methods=["POST"])
def logout():
    delete_session(get_token())
    return redirect(url_for("index"))


@app.route("/landing")
def landing_page():
    error = request.args.get("error")
    show_auth_tab = request.args.get("show_auth_tab", "signup")
    return render_template(
        "landing.html",
        error=error,
        show_auth_tab=show_auth_tab,
        latest_blog_posts=get_latest_blog_posts(2),
    )


@app.route("/static/blog/<path:filename>")
def blog_static(filename):
    return send_from_directory(BLOG_STATIC_DIR, filename)


BLOG_PAGE_SIZE = 10


def get_latest_blog_posts(limit=2):
    conn = get_db()
    posts = [
        dict(r)
        for r in conn.execute(
            """SELECT id, title, slug, body, image_path, is_pinned, pinned_rank, created_at FROM blog_posts
           WHERE status = 'published'
           ORDER BY is_pinned DESC, pinned_rank ASC, created_at DESC, id DESC LIMIT ?""",
            (limit,),
        ).fetchall()
    ]
    conn.close()
    return posts


@app.route("/terms")
def terms_page():
    token = get_token()
    return render_template("terms.html", token=token)


@app.route("/privacy")
def privacy_page():
    token = get_token()
    return render_template("privacy.html", token=token)


@app.route("/disclaimer")
def disclaimer_page():
    token = get_token()
    return render_template("disclaimer.html", token=token)


@app.route("/blog")
def blog_page():
    token = get_token()  # ADD THIS

    page = max(1, int(request.args.get("page", "1") or "1"))
    conn = get_db()

    pinned = []
    if page == 1:
        pinned = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM blog_posts WHERE status = 'published' AND is_pinned = 1 ORDER BY pinned_rank ASC"
            ).fetchall()
        ]

    pinned_count = conn.execute(
        "SELECT COUNT(*) FROM blog_posts WHERE status = 'published' AND is_pinned = 1"
    ).fetchone()[0]

    unpinned_total = conn.execute(
        "SELECT COUNT(*) FROM blog_posts WHERE status = 'published' AND is_pinned = 0"
    ).fetchone()[0]

    first_page_unpinned = max(0, BLOG_PAGE_SIZE - pinned_count)

    if unpinned_total <= first_page_unpinned:
        total_pages = 1
    else:
        remaining = unpinned_total - first_page_unpinned
        total_pages = 1 + max(1, (remaining + BLOG_PAGE_SIZE - 1) // BLOG_PAGE_SIZE)

    page = min(page, total_pages)

    if page == 1:
        unpinned_limit = first_page_unpinned
        unpinned_offset = 0
    else:
        unpinned_limit = BLOG_PAGE_SIZE
        unpinned_offset = first_page_unpinned + (page - 2) * BLOG_PAGE_SIZE

    unpinned = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM blog_posts WHERE status = 'published' AND is_pinned = 0 "
            "ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
            (unpinned_limit, unpinned_offset),
        ).fetchall()
    ]

    conn.close()
    posts = pinned + unpinned

    return render_template(
        "blog_list.html",
        posts=posts,
        page=page,
        total_pages=total_pages,
        token=token,  # ADD THIS
    )


@app.route("/blog/<int:post_id>")
def blog_post(post_id):
    token = get_token()  # ADD THIS

    conn = get_db()
    post = conn.execute(
        "SELECT * FROM blog_posts WHERE id = ? AND status = 'published'",
        (post_id,),
    ).fetchone()
    conn.close()

    if not post:
        return (
            render_template(
                "blog_list.html",
                posts=[],
                error="Post not found.",
                token=token,  # ADD THIS
            ),
            404,
        )

    return render_template(
        "blog_post.html",
        post=dict(post),
        token=token,  # ADD THIS
    )


@app.route("/admin/create-blog-post", methods=["POST"])
def admin_create_blog_post():
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")

    title = (request.form.get("blog_title") or "").strip()
    body = (request.form.get("blog_body") or "").strip()
    status = request.form.get("blog_status", "published").strip()
    if status not in ("published", "draft"):
        status = "published"

    if not title or not body:
        return render_error("Blog post title and body are required.")

    slug = title.lower().replace(" ", "-")
    slug = "".join(c for c in slug if c.isalnum() or c == "-")[:80]

    image_path = None
    image_type = None
    image_size_bytes = None

    img_file = request.files.get("blog_image")
    if img_file and img_file.filename:
        fname = img_file.filename
        ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
        if ext not in BLOG_IMAGE_ALLOWED:
            return render_error(f"Blog image must be PNG or JPG. Got: .{ext}")
        img_data = img_file.read()
        if len(img_data) > BLOG_IMAGE_MAX_SIZE:
            return render_error(
                f"Blog image must be under 300KB. Got: {len(img_data) // 1024}KB"
            )
        safe_name = f"{int(datetime.utcnow().timestamp())}_{secrets.token_hex(4)}.{ext}"
        dest = os.path.join(BLOG_STATIC_DIR, safe_name)
        with open(dest, "wb") as f:
            f.write(img_data)
        image_path = safe_name
        image_type = ext
        image_size_bytes = len(img_data)

    is_pinned = 1 if request.form.get("blog_pin") else 0
    pinned_rank = None
    if is_pinned:
        rank_val = request.form.get("blog_pin_rank", "").strip()
        if rank_val not in ("1", "2", "3"):
            return render_error("Pin position must be 1, 2, or 3.")
        pinned_rank = int(rank_val)
        conn = get_db()
        pin_count = conn.execute(
            "SELECT COUNT(*) FROM blog_posts WHERE is_pinned = 1"
        ).fetchone()[0]
        if pin_count >= 3:
            conn.close()
            return render_error("You can pin up to 3 posts. Unpin another post first.")
        existing_rank = conn.execute(
            "SELECT id FROM blog_posts WHERE is_pinned = 1 AND pinned_rank = ?",
            (pinned_rank,),
        ).fetchone()
        if existing_rank:
            conn.close()
            return render_error(
                "Pin position already in use. Choose another position or unpin the existing one."
            )
        conn.close()

    conn = get_db()
    conn.execute(
        "INSERT INTO blog_posts (title, slug, body, status, image_path, image_type, image_size_bytes, is_pinned, pinned_rank) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            title,
            slug,
            body,
            status,
            image_path,
            image_type,
            image_size_bytes,
            is_pinned,
            pinned_rank,
        ),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/toggle-pin/<int:post_id>", methods=["POST"])
def admin_toggle_pin(post_id):
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")
    action = request.form.get("pin_action", "").strip()
    conn = get_db()
    post = conn.execute("SELECT * FROM blog_posts WHERE id = ?", (post_id,)).fetchone()
    if not post:
        conn.close()
        return render_error("Blog post not found.")
    if action == "unpin":
        conn.execute(
            "UPDATE blog_posts SET is_pinned = 0, pinned_rank = NULL WHERE id = ?",
            (post_id,),
        )
        conn.commit()
        conn.close()
        return redirect(url_for("index", token=token))
    elif action == "pin":
        rank_val = request.form.get("pin_rank", "").strip()
        if rank_val not in ("1", "2", "3"):
            conn.close()
            return render_error("Pin position must be 1, 2, or 3.")
        pinned_rank = int(rank_val)
        pin_count = conn.execute(
            "SELECT COUNT(*) FROM blog_posts WHERE is_pinned = 1 AND id != ?",
            (post_id,),
        ).fetchone()[0]
        if pin_count >= 3:
            conn.close()
            return render_error("You can pin up to 3 posts. Unpin another post first.")
        existing_rank = conn.execute(
            "SELECT id FROM blog_posts WHERE is_pinned = 1 AND pinned_rank = ? AND id != ?",
            (pinned_rank, post_id),
        ).fetchone()
        if existing_rank:
            conn.close()
            return render_error(
                "Pin position already in use. Choose another position or unpin the existing one."
            )
        conn.execute(
            "UPDATE blog_posts SET is_pinned = 1, pinned_rank = ? WHERE id = ?",
            (pinned_rank, post_id),
        )
        conn.commit()
        conn.close()
        return redirect(url_for("index", token=token))
    conn.close()
    return render_error("Invalid pin action.")


@app.route("/admin/delete-blog-post/<int:post_id>", methods=["POST"])
def admin_delete_blog_post(post_id):
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")
    conn = get_db()
    post = conn.execute("SELECT * FROM blog_posts WHERE id = ?", (post_id,)).fetchone()
    if not post:
        conn.close()
        return render_error("Blog post not found.")
    image_path = post["image_path"] if "image_path" in post.keys() else None
    if image_path:
        full_path = os.path.join(BLOG_STATIC_DIR, image_path)
        try:
            if os.path.exists(full_path):
                os.remove(full_path)
        except Exception:
            pass
    conn.execute("DELETE FROM blog_posts WHERE id = ?", (post_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/approve/<int:user_id>", methods=["POST"])
def admin_approve(user_id):
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    if not user:
        conn.close()
        return render_error("User not found.")
    if user["state"] != "pending":
        conn.close()
        return render_error("User is not in pending state.")

    conn.execute("UPDATE users SET state = 'active' WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/disable/<int:user_id>", methods=["POST"])
def admin_disable(user_id):
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")

    conn = get_db()
    conn.execute("UPDATE users SET state = 'disabled' WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


RESEARCH_BRIEF_FIELDS = [
    ("business_problem", "Business Problem"),
    ("decision_to_support", "Decision to Support"),
    ("known_vs_unknown", "Known vs Unknown"),
    ("target_audience", "Target Audience"),
    ("study_fit", "Study Fit"),
    ("definition_useful_insight", "Definition of Useful Insight"),
]


@app.route("/create-study", methods=["POST"])
def create_study():
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user to create a study.")

    conn_check = get_db()
    used, limit, _, _ = get_monthly_usage(conn_check, user["id"])
    conn_check.close()
    if used >= limit:
        return render_error(
            f"Monthly study limit reached ({used}/{limit}). You cannot create new studies until next month."
        )

    title = (request.form.get("title") or "").strip()
    study_type = (request.form.get("study_type") or "").strip()

    if not title:
        return render_error("Title is required.")
    if study_type not in VALID_STUDY_TYPES:
        return render_error("Please select a valid study type.")

    if study_type == "synthetic_survey":
        decision_to_support = (request.form.get("decision_to_support") or "").strip()
        target_audience = (request.form.get("target_audience") or "").strip()
        definition_useful_insight = (
            request.form.get("definition_useful_insight") or ""
        ).strip()
        top_hypotheses = (request.form.get("top_hypotheses") or "").strip()
        if (
            not decision_to_support
            or not target_audience
            or not definition_useful_insight
            or not top_hypotheses
        ):
            return render_error(
                "All survey brief fields are required (Decision to Support, Target Audience, Definition of Useful Insight, Top Hypotheses)."
            )

        try:
            respondent_count = int(request.form.get("respondent_count", 100))
        except (ValueError, TypeError):
            respondent_count = 100
        try:
            question_count = int(request.form.get("question_count", 8))
        except (ValueError, TypeError):
            question_count = 8
        respondent_count = max(1, min(400, respondent_count))
        question_count = max(1, min(12, question_count))

        survey_questions = []
        for i in range(1, question_count + 1):
            q = (request.form.get(f"survey_q_{i}") or "").strip()
            if q:
                survey_questions.append(q)
        if len(survey_questions) == 0:
            return render_error("At least 1 survey question is required.")
        if len(survey_questions) > 12:
            survey_questions = survey_questions[:12]

        survey_brief = json.dumps(
            {
                "decision_to_support": decision_to_support,
                "target_audience": target_audience,
                "definition_useful_insight": definition_useful_insight,
                "top_hypotheses": top_hypotheses,
            }
        )

        conn = get_db()
        conn.execute(
            """INSERT INTO studies
               (user_id, title, status, study_type, respondent_count, question_count,
                survey_brief, survey_questions)
               VALUES (?, ?, 'draft', ?, ?, ?, ?, ?)""",
            (
                user["id"],
                title,
                study_type,
                respondent_count,
                question_count,
                survey_brief,
                json.dumps(survey_questions),
            ),
        )
        conn.commit()
        conn.close()
        return redirect(url_for("index", token=token))

    else:
        brief = {}
        for field_key, field_label in RESEARCH_BRIEF_FIELDS:
            val = (request.form.get(field_key) or "").strip()
            if not val:
                return render_error(
                    f'"{field_label}" is required. All 6 anchors must be filled.',
                )
            brief[field_key] = val

        conn = get_db()
        conn.execute(
            """INSERT INTO studies
               (user_id, title, status, study_type, business_problem, decision_to_support,
                known_vs_unknown, target_audience, study_fit, definition_useful_insight)
               VALUES (?, ?, 'draft', ?, ?, ?, ?, ?, ?, ?)""",
            (
                user["id"],
                title,
                study_type,
                brief["business_problem"],
                brief["decision_to_support"],
                brief["known_vs_unknown"],
                brief["target_audience"],
                brief["study_fit"],
                brief["definition_useful_insight"],
            ),
        )
        conn.commit()
        conn.close()
        return redirect(url_for("index", token=token))


@app.route("/create-study-tbd", methods=["POST"])
def create_study_tbd():
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user to create a study.")

    conn_check = get_db()
    used, limit, _, _ = get_monthly_usage(conn_check, user["id"])
    conn_check.close()
    if used >= limit:
        return render_error(
            f"Monthly study limit reached ({used}/{limit}). You cannot create new studies until next month."
        )

    title = (request.form.get("title") or "").strip()
    if not title:
        return render_error("Title is required.")

    conn = get_db()
    conn.execute(
        "INSERT INTO studies (user_id, title, status, study_type) VALUES (?, ?, 'draft', '')",
        (user["id"], title),
    )
    conn.commit()
    study_id = conn.execute(
        "SELECT id FROM studies ORDER BY id DESC LIMIT 1"
    ).fetchone()["id"]
    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


def _allowed_upload(filename):
    return (
        "." in filename
        and filename.rsplit(".", 1)[1].lower() in UPLOAD_ALLOWED_EXTENSIONS
    )


def _compute_sha256(data):
    return hashlib.sha256(data).hexdigest()


def _extract_excerpt(file_data, file_type, max_chars=4096):
    if file_type in ("txt", "csv"):
        try:
            return file_data[:max_chars].decode("utf-8", errors="replace")
        except Exception:
            return "[Text extraction failed]"
    return "[Binary document retained by hash only]"


def _get_user_storage_used(conn, user_id):
    return conn.execute(
        "SELECT COALESCE(SUM(file_size_bytes), 0) FROM user_uploads WHERE user_id = ? AND status = 'active'",
        (user_id,),
    ).fetchone()[0]


def _get_study_attachment_stats(conn, study_id):
    row = conn.execute(
        """SELECT COUNT(*) as cnt, COALESCE(SUM(u.file_size_bytes), 0) as total
           FROM study_documents sd JOIN user_uploads u ON u.id = sd.user_doc_id
           WHERE sd.study_id = ? AND u.status = 'active'""",
        (study_id,),
    ).fetchone()
    return row["cnt"], row["total"]


@app.route("/upload-study-file/<int:study_id>", methods=["POST"])
def upload_study_file(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT id FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Study not found or not in draft status.")

    f = request.files.get("file")
    if not f or not f.filename:
        conn.close()
        return render_error("No file selected.")

    if not _allowed_upload(f.filename):
        conn.close()
        ext = f.filename.rsplit(".", 1)[-1] if "." in f.filename else "(none)"
        return render_error(
            f"File type '.{ext}' is not allowed. Allowed types: {', '.join(sorted(UPLOAD_ALLOWED_EXTENSIONS))}."
        )

    file_data = f.read()
    file_size = len(file_data)

    if file_size > UPLOAD_MAX_FILE_SIZE:
        conn.close()
        return render_error(
            f"File exceeds the {UPLOAD_MAX_FILE_SIZE // (1024 * 1024)}MB size limit."
        )

    current_storage = _get_user_storage_used(conn, user["id"])
    if current_storage + file_size > UPLOAD_USER_STORAGE_CAP:
        conn.close()
        return render_error(
            f"User storage cap ({UPLOAD_USER_STORAGE_CAP // (1024 * 1024)}MB) would be exceeded. Delete some documents to free space."
        )

    att_cnt, att_total = _get_study_attachment_stats(conn, study_id)
    if att_cnt >= UPLOAD_MAX_FILES_PER_STUDY:
        conn.close()
        return render_error(
            f"Maximum {UPLOAD_MAX_FILES_PER_STUDY} files attached to this study."
        )

    if att_total + file_size > UPLOAD_MAX_TOTAL_PER_STUDY:
        conn.close()
        return render_error(
            f"Total attached size would exceed {UPLOAD_MAX_TOTAL_PER_STUDY // (1024 * 1024)}MB limit for this study."
        )

    import uuid

    safe_name = f"{uuid.uuid4().hex}_{f.filename}"
    storage_path = os.path.join(USER_UPLOADS_DIR, safe_name)
    with open(storage_path, "wb") as out:
        out.write(file_data)

    ext = f.filename.rsplit(".", 1)[1].lower()
    content_sha = _compute_sha256(file_data)
    excerpt = _extract_excerpt(file_data, ext)
    conn.execute(
        "INSERT INTO user_uploads (user_id, filename, file_type, file_size_bytes, storage_path, content_sha256, retained_excerpt_text, retained_excerpt_bytes, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active')",
        (
            user["id"],
            f.filename,
            ext,
            file_size,
            storage_path,
            content_sha,
            excerpt,
            len(excerpt),
        ),
    )
    doc_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO study_documents (study_id, user_doc_id) VALUES (?, ?)",
        (study_id, doc_id),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


@app.route("/upload-user-doc", methods=["POST"])
def upload_user_doc():
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    f = request.files.get("file")
    if not f or not f.filename:
        return render_error("No file selected.")

    if not _allowed_upload(f.filename):
        ext = f.filename.rsplit(".", 1)[-1] if "." in f.filename else "(none)"
        return render_error(
            f"File type '.{ext}' is not allowed. Allowed types: {', '.join(sorted(UPLOAD_ALLOWED_EXTENSIONS))}."
        )

    file_data = f.read()
    file_size = len(file_data)

    if file_size > UPLOAD_MAX_FILE_SIZE:
        return render_error(
            f"File exceeds the {UPLOAD_MAX_FILE_SIZE // (1024 * 1024)}MB size limit."
        )

    conn = get_db()
    current_storage = _get_user_storage_used(conn, user["id"])
    if current_storage + file_size > UPLOAD_USER_STORAGE_CAP:
        conn.close()
        return render_error(
            f"User storage cap ({UPLOAD_USER_STORAGE_CAP // (1024 * 1024)}MB) would be exceeded. Delete some documents to free space."
        )

    import uuid

    safe_name = f"{uuid.uuid4().hex}_{f.filename}"
    storage_path = os.path.join(USER_UPLOADS_DIR, safe_name)
    with open(storage_path, "wb") as out:
        out.write(file_data)

    ext = f.filename.rsplit(".", 1)[1].lower()
    content_sha = _compute_sha256(file_data)
    excerpt = _extract_excerpt(file_data, ext)
    conn.execute(
        "INSERT INTO user_uploads (user_id, filename, file_type, file_size_bytes, storage_path, content_sha256, retained_excerpt_text, retained_excerpt_bytes, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active')",
        (
            user["id"],
            f.filename,
            ext,
            file_size,
            storage_path,
            content_sha,
            excerpt,
            len(excerpt),
        ),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/attach-doc-to-study/<int:study_id>", methods=["POST"])
def attach_doc_to_study(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT id FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Study not found or not in draft status.")

    doc_id = request.form.get("doc_id")
    if not doc_id:
        conn.close()
        return render_error("No document selected.")

    doc = conn.execute(
        "SELECT * FROM user_uploads WHERE id = ? AND user_id = ? AND status = 'active'",
        (doc_id, user["id"]),
    ).fetchone()
    if not doc:
        conn.close()
        return render_error("Document not found or not active.")

    already = conn.execute(
        "SELECT id FROM study_documents WHERE study_id = ? AND user_doc_id = ?",
        (study_id, doc_id),
    ).fetchone()
    if already:
        conn.close()
        return render_error("Document already attached to this study.")

    att_cnt, att_total = _get_study_attachment_stats(conn, study_id)
    if att_cnt >= UPLOAD_MAX_FILES_PER_STUDY:
        conn.close()
        return render_error(
            f"Maximum {UPLOAD_MAX_FILES_PER_STUDY} files attached to this study."
        )

    if att_total + doc["file_size_bytes"] > UPLOAD_MAX_TOTAL_PER_STUDY:
        conn.close()
        return render_error(
            f"Total attached size would exceed {UPLOAD_MAX_TOTAL_PER_STUDY // (1024 * 1024)}MB limit for this study."
        )

    conn.execute(
        "INSERT INTO study_documents (study_id, user_doc_id) VALUES (?, ?)",
        (study_id, int(doc_id)),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


@app.route("/detach-doc-from-study/<int:attachment_id>", methods=["POST"])
def detach_doc_from_study(attachment_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    conn = get_db()
    att = conn.execute(
        """SELECT sd.id, sd.study_id FROM study_documents sd
           JOIN user_uploads u ON u.id = sd.user_doc_id
           JOIN studies s ON s.id = sd.study_id
           WHERE sd.id = ? AND u.user_id = ? AND s.status = 'draft'""",
        (attachment_id, user["id"]),
    ).fetchone()
    if not att:
        conn.close()
        return render_error("Attachment not found or study not in draft.")

    study_id = att["study_id"]
    conn.execute("DELETE FROM study_documents WHERE id = ?", (attachment_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


@app.route("/delete-user-doc/<int:doc_id>", methods=["POST"])
def delete_user_doc(doc_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    conn = get_db()
    doc = conn.execute(
        "SELECT * FROM user_uploads WHERE id = ? AND user_id = ? AND status = 'active'",
        (doc_id, user["id"]),
    ).fetchone()
    if not doc:
        conn.close()
        return render_error("Document not found or already deleted.")

    if doc["storage_path"] and os.path.exists(doc["storage_path"]):
        os.remove(doc["storage_path"])

    conn.execute(
        """UPDATE user_uploads SET status = 'deleted', deleted_at = datetime('now'), storage_path = NULL
           WHERE id = ?""",
        (doc_id,),
    )

    draft_study_ids = [
        r["study_id"]
        for r in conn.execute(
            """SELECT sd.study_id FROM study_documents sd
           JOIN studies s ON s.id = sd.study_id
           WHERE sd.user_doc_id = ? AND s.status = 'draft'""",
            (doc_id,),
        ).fetchall()
    ]
    for sid in draft_study_ids:
        conn.execute(
            "DELETE FROM study_documents WHERE study_id = ? AND user_doc_id = ?",
            (sid, doc_id),
        )

    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/upload-document", methods=["POST"])
def admin_upload_document():
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")

    f = request.files.get("file")
    if not f or not f.filename:
        return render_error("No file selected.")

    if not _allowed_upload(f.filename):
        ext = f.filename.rsplit(".", 1)[-1] if "." in f.filename else "(none)"
        return render_error(
            f"File type '.{ext}' is not allowed. Allowed types: {', '.join(sorted(UPLOAD_ALLOWED_EXTENSIONS))}."
        )

    file_data = f.read()
    file_size = len(file_data)

    if file_size > UPLOAD_MAX_FILE_SIZE:
        return render_error(
            f"File exceeds the {UPLOAD_MAX_FILE_SIZE // (1024 * 1024)}MB size limit."
        )

    import uuid

    safe_name = f"{uuid.uuid4().hex}_{f.filename}"
    storage_path = os.path.join(ADMIN_UPLOADS_DIR, safe_name)
    with open(storage_path, "wb") as out:
        out.write(file_data)

    ext = f.filename.rsplit(".", 1)[1].lower()
    conn = get_db()
    conn.execute(
        "INSERT INTO admin_uploads (filename, file_type, file_size_bytes, storage_path) VALUES (?, ?, ?, ?)",
        (f.filename, ext, file_size, storage_path),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/toggle-upload/<int:upload_id>", methods=["POST"])
def admin_toggle_upload(upload_id):
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")

    conn = get_db()
    upload = conn.execute(
        "SELECT * FROM admin_uploads WHERE id = ?", (upload_id,)
    ).fetchone()
    if not upload:
        conn.close()
        return render_error("Upload not found.")

    new_status = "disabled" if upload["status"] == "active" else "active"
    conn.execute(
        "UPDATE admin_uploads SET status = ? WHERE id = ?", (new_status, upload_id)
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/delete-upload/<int:upload_id>", methods=["POST"])
def admin_delete_upload(upload_id):
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")

    conn = get_db()
    upload = conn.execute(
        "SELECT * FROM admin_uploads WHERE id = ?", (upload_id,)
    ).fetchone()
    if not upload:
        conn.close()
        return render_error("Upload not found.")

    if os.path.exists(upload["storage_path"]):
        os.remove(upload["storage_path"])
    conn.execute("DELETE FROM admin_uploads WHERE id = ?", (upload_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/save-discovery/<int:study_id>", methods=["POST"])
def save_discovery(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    field = request.form.get("field", "").strip()
    value = (request.form.get("value") or "").strip()

    if field not in ("business_problem", "decision_to_support"):
        return render_error("Invalid discovery field.")
    if not value:
        return render_error("Value cannot be empty.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Study not found or not in draft status.")

    conn.execute(f"UPDATE studies SET {field} = ? WHERE id = ?", (value, study_id))
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


def get_mark_recommendation(business_problem, decision_to_support):
    bp = (business_problem or "").lower()
    ds = (decision_to_support or "").lower()
    combined = bp + " " + ds

    survey_signals = [
        "how many",
        "percentage",
        "quantif",
        "measure",
        "scale",
        "survey",
        "poll",
        "prevalence",
        "frequency",
        "rate",
        "volume",
        "count",
        "trend",
        "satisfaction score",
        "nps",
        "benchmark",
        "metric",
        "statistical",
    ]
    fg_signals = [
        "group dynamic",
        "focus group",
        "collective",
        "social",
        "community",
        "consensus",
        "debate",
        "discussion",
        "react to concept",
        "co-creation",
        "brainstorm together",
        "group reaction",
        "shared experience",
    ]
    idi_signals = [
        "why",
        "understand",
        "motivation",
        "explore",
        "deep dive",
        "journey",
        "experience",
        "interview",
        "personal",
        "individual",
        "feeling",
        "perception",
        "narrative",
        "story",
        "insight",
        "qualitative",
    ]

    survey_score = sum(1 for s in survey_signals if s in combined)
    fg_score = sum(1 for s in fg_signals if s in combined)
    idi_score = sum(1 for s in idi_signals if s in combined)

    if survey_score > idi_score and survey_score > fg_score:
        return (
            "synthetic_survey",
            "Synthetic Survey",
            "Your inputs suggest quantitative measurement would best address this problem.",
        )
    elif fg_score > idi_score:
        return (
            "synthetic_focus_group",
            "Synthetic Focus Group",
            "Your inputs suggest group dynamics and collective reactions would yield the richest insights.",
        )
    else:
        return (
            "synthetic_idi",
            "Synthetic IDI",
            "Your inputs suggest in-depth individual exploration would best uncover the insights you need.",
        )


@app.route("/set-mark-recommendation/<int:study_id>", methods=["POST"])
def set_mark_recommendation(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    study_type = request.form.get("study_type", "").strip()
    if study_type not in VALID_STUDY_TYPES:
        return render_error("Invalid study type.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Study not found or not in draft status.")

    if study_type == "synthetic_survey":
        conn.execute(
            "UPDATE studies SET study_type = ?, personas_used = '[]' WHERE id = ?",
            (study_type, study_id),
        )
    else:
        conn.execute(
            "UPDATE studies SET study_type = ? WHERE id = ?",
            (study_type, study_id),
        )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


@app.route("/set-study-type/<int:study_id>", methods=["POST"])
def set_study_type(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ?",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Study not found.")
    if study["status"] != "draft":
        conn.close()
        return render_error("Study type can only be set for draft studies.")

    study_type = request.form.get("study_type", "").strip()
    if study_type not in VALID_STUDY_TYPES:
        conn.close()
        return render_error("Invalid study type. Choose survey, IDI, or focus group.")

    if study_type == "synthetic_survey":
        conn.execute(
            "UPDATE studies SET study_type = ?, personas_used = '[]' WHERE id = ?",
            (study_type, study_id),
        )
    else:
        conn.execute(
            "UPDATE studies SET study_type = ? WHERE id = ?",
            (study_type, study_id),
        )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


STUDY_TYPE_LABELS = {
    "synthetic_survey": "Synthetic Survey",
    "synthetic_idi": "Synthetic IDI (In-Depth Interview)",
    "synthetic_focus_group": "Synthetic Focus Group",
}

ANCHOR_FIELDS = [
    ("business_problem", "Business Problem"),
    ("decision_to_support", "Decision to Support"),
    ("known_vs_unknown", "Known vs Unknown"),
    ("target_audience", "Target Audience"),
    ("study_fit", "Study Fit"),
    ("definition_useful_insight", "Definition of Useful Insight"),
]


def build_structured_report(
    study, followups=None, version=None, uploaded_filenames=None
):
    if followups is None:
        followups = []
    if uploaded_filenames is None:
        uploaded_filenames = []
    max_version = 1 + len(followups)
    if version is None:
        version = max_version
    version = max(1, min(version, max_version))

    title = study.get("title") or "Untitled Study"
    st = study.get("study_type") or "Unknown"
    st_label = STUDY_TYPE_LABELS.get(st, st)
    output_raw = study.get("final_report") or study.get("study_output") or ""
    is_placeholder = "SIMULATED PLACEHOLDER" in output_raw

    sections = {}

    exec_lines = [f"Study: {title}", f"Type: {st_label}"]
    for field, label in ANCHOR_FIELDS:
        val = (study.get(field) or "").strip()
        if val:
            exec_lines.append(f"{label}: {val}")
    if is_placeholder:
        exec_lines.append("")
        exec_lines.append(
            "Note: This output is simulated placeholder data for development/testing purposes. It does not represent real research findings."
        )
    else:
        exec_lines.append("")
        exec_lines.append(
            "This report summarizes the findings from the study execution."
        )
    sections["executive_summary"] = "\n".join(exec_lines)

    studied_lines = [f"Study Title: {title}", f"Study Type: {st_label}"]
    if st == "synthetic_survey":
        rc = study.get("respondent_count") or 0
        qc = study.get("question_count") or 0
        studied_lines.append(f"Respondent Count: {rc}")
        studied_lines.append(f"Question Count: {qc}")
        sq = []
        if study.get("survey_questions"):
            try:
                sq = json.loads(study["survey_questions"])
            except (json.JSONDecodeError, TypeError):
                sq = []
        if sq:
            studied_lines.append("Questions:")
            for i, q in enumerate(sq, 1):
                studied_lines.append(f"  {i}. {q}")
    else:
        for field, label in ANCHOR_FIELDS:
            val = (study.get(field) or "").strip()
            studied_lines.append(f"{label}: {val or '(not provided)'}")
    sections["what_was_studied"] = "\n".join(studied_lines)

    confidence = None
    if study.get("confidence_summary"):
        try:
            confidence = json.loads(study["confidence_summary"])
        except (json.JSONDecodeError, TypeError):
            confidence = None

    findings_lines = []
    if is_placeholder:
        findings_lines.append("[Placeholder findings — not based on real data]")
    if st == "synthetic_survey" and output_raw:
        try:
            survey_data = json.loads(
                output_raw.split("\n", 1)[-1] if "===" in output_raw else output_raw
            )
            if isinstance(survey_data, dict) and "questions" in survey_data:
                for qi, qobj in enumerate(survey_data["questions"], 1):
                    q_text = qobj.get("q", f"Question {qi}")
                    conf_label = "Indicative" if confidence else "Exploratory"
                    if confidence and confidence.get("Strong", 0) > 0 and qi == 1:
                        conf_label = "Strong"
                    findings_lines.append(f"Finding {qi} [{conf_label}]: {q_text}")
                    results = qobj.get("results", {})
                    for k, v in results.items():
                        findings_lines.append(f"  - {k}: {v}")
                    findings_lines.append("")
        except (json.JSONDecodeError, TypeError, IndexError):
            findings_lines.append(
                "Raw output available but could not be parsed into structured findings."
            )
    else:
        clean_output = output_raw
        for prefix in ["=== QA REVIEW: PASS ===", "=== QA REVIEW: DOWNGRADE ==="]:
            if clean_output.startswith(prefix):
                parts = clean_output.split("=" * 40, 1)
                if len(parts) > 1:
                    clean_output = parts[1].strip()
                break
        if clean_output.startswith("*** SIMULATED"):
            lines_raw = clean_output.split("\n")
            clean_output = "\n".join(l for l in lines_raw if not l.startswith("***"))
        interview_blocks = (
            clean_output.split("--- Interview with ")
            if "--- Interview with " in clean_output
            else []
        )
        if interview_blocks and len(interview_blocks) > 1:
            for bi, block in enumerate(interview_blocks[1:], 1):
                conf_label = "Indicative"
                if confidence and confidence.get("Strong", 0) >= bi:
                    conf_label = "Strong"
                speaker = block.split("---")[0].strip()
                findings_lines.append(
                    f"Finding {bi} [{conf_label}]: Key themes from {speaker}"
                )
                snippet_lines = [
                    l.strip()
                    for l in block.split("\n")
                    if l.strip() and not l.startswith("---")
                ][:4]
                for sl in snippet_lines:
                    findings_lines.append(f"  {sl}")
                findings_lines.append("")
        elif clean_output.strip():
            findings_lines.append(f"Finding 1 [Exploratory]: Study output summary")
            for line in clean_output.strip().split("\n")[:10]:
                if line.strip():
                    findings_lines.append(f"  {line.strip()}")
            findings_lines.append("")

    if not findings_lines:
        findings_lines.append("No findings available.")
    sections["key_findings"] = "\n".join(findings_lines)

    risk_lines = []
    qa_status = study.get("qa_status") or ""
    qa_notes = study.get("qa_notes") or ""
    if qa_status == "fail" or study.get("status") == "qa_blocked":
        risk_lines.append(f"QA Status: BLOCKED — {qa_notes}")
    elif qa_status == "downgrade":
        risk_lines.append(f"QA Status: DOWNGRADE — {qa_notes}")
    if is_placeholder:
        risk_lines.append(
            "All data in this report is simulated. Do not use for real business decisions."
        )
    risk_lines.append(
        "Synthetic research outputs are experimental and should be validated with traditional research methods before acting on findings."
    )
    if st == "synthetic_survey":
        risk_lines.append(
            "Survey responses are AI-generated and may not reflect real consumer behavior."
        )
    elif st in ("synthetic_idi", "synthetic_focus_group"):
        risk_lines.append(
            "Interview/discussion outputs are AI-generated based on persona profiles and may contain biases inherent in the training data."
        )
    sections["risks_limits"] = "\n".join(risk_lines)

    source_lines = [
        "AI Model: Simulated output (placeholder)",
        "Grounding Sources: As configured by admin web sources (if any)",
        "Persona Profiles: Generated from user-defined demographic and psychographic parameters",
    ]
    if study.get("personas_used"):
        try:
            pids = (
                json.loads(study["personas_used"])
                if isinstance(study["personas_used"], str)
                else study["personas_used"]
            )
            if pids:
                source_lines.append(f"Personas Used: {', '.join(pids)}")
        except (json.JSONDecodeError, TypeError):
            pass
    if uploaded_filenames:
        source_lines.append("")
        source_lines.append("Uploaded Documents:")
        for uf in uploaded_filenames:
            source_lines.append(f"  - {uf}")
    source_lines.append("")
    source_lines.append(
        "Note: Real citations will be populated when live AI model integration is enabled."
    )
    sections["sources_citations"] = "\n".join(source_lines)

    sections["followup_sections"] = []
    for fu in followups:
        if fu.get("followup_round", 0) > version - 1:
            break
        fu_lines = []
        fu_lines.append(f"Question: {fu.get('user_question', '')}")
        fu_lines.append("")
        fu_qa = fu.get("qa_status", "")
        if fu_qa == "fail":
            fu_lines.append(f"[QA BLOCKED] {fu.get('qa_notes', '')}")
        else:
            if fu_qa == "downgrade":
                fu_lines.append(f"[QA DOWNGRADE] {fu.get('qa_notes', '')}")
            fu_lines.append(fu.get("generated_output", ""))
        sections["followup_sections"].append(
            {
                "round": fu["followup_round"],
                "content": "\n".join(fu_lines),
            }
        )

    sections["version"] = version
    sections["max_version"] = max_version

    return sections


CJK_FONT_PATH = os.path.join(
    os.path.dirname(__file__), "fonts", "NotoSansCJK-Regular.ttc"
)
TRAD_MARKERS = set("漢歡測臺體繁廣")
SIMP_MARKERS = set("汉欢测台体简")


def _has_non_ascii(text):
    for ch in text:
        if ord(ch) > 127:
            return True
    return False


def _has_cjk(text):
    for ch in text:
        cp = ord(ch)
        if (
            0x3000 <= cp <= 0x9FFF
            or 0xF900 <= cp <= 0xFAFF
            or 0x20000 <= cp <= 0x2FA1F
            or 0xFF00 <= cp <= 0xFFEF
        ):
            return True
    return False


def _has_kana(text):
    for ch in text:
        cp = ord(ch)
        if 0x3040 <= cp <= 0x30FF or 0x31F0 <= cp <= 0x31FF:
            return True
    return False


def _pick_cjk_font(text):
    if _has_kana(text):
        return "CJK_JP"
    chars = set(text)
    if chars & TRAD_MARKERS:
        return "CJK_TC"
    if chars & SIMP_MARKERS:
        return "CJK_SC"
    return "CJK_TC"


def _register_cjk_fonts(pdf):
    if not os.path.exists(CJK_FONT_PATH):
        return False
    try:
        pdf.add_font("CJK_JP", fname=CJK_FONT_PATH, collection_font_number=0)
        pdf.add_font("CJK_SC", fname=CJK_FONT_PATH, collection_font_number=2)
        pdf.add_font("CJK_TC", fname=CJK_FONT_PATH, collection_font_number=3)
        return True
    except Exception:
        return False


def _pdf_set_font(pdf, text, size, style="", cjk_available=True):
    if cjk_available and _has_cjk(text):
        font_name = _pick_cjk_font(text)
        pdf.set_font(font_name, "", size)
    else:
        pdf.set_font("Helvetica", style, size)


def _pdf_write_text(
    pdf, text, size, style="", cjk_available=True, method="cell", **kwargs
):
    needs_unicode = _has_non_ascii(text)
    if not cjk_available or not needs_unicode:
        pdf.set_font("Helvetica", style, size)
        if method == "multi_cell":
            safe = text.encode("latin-1", "replace").decode("latin-1")
            pdf.multi_cell(kwargs.get("w", 0), kwargs.get("h", 5), safe)
        else:
            safe = text.encode("latin-1", "replace").decode("latin-1")
            pdf.cell(
                kwargs.get("w", 0),
                kwargs.get("h", 10),
                safe,
                new_x=kwargs.get("new_x", "LMARGIN"),
                new_y=kwargs.get("new_y", "NEXT"),
                align=kwargs.get("align", ""),
                fill=kwargs.get("fill", False),
            )
        return

    font_name = _pick_cjk_font(text)
    fallback_order = ["CJK_JP", "CJK_SC", "CJK_TC"]
    fallback_order.remove(font_name)
    fallback_order.insert(0, font_name)

    for fn in fallback_order:
        try:
            pdf.set_font(fn, "", size)
            if method == "multi_cell":
                pdf.multi_cell(kwargs.get("w", 0), kwargs.get("h", 5), text)
            else:
                pdf.cell(
                    kwargs.get("w", 0),
                    kwargs.get("h", 10),
                    text,
                    new_x=kwargs.get("new_x", "LMARGIN"),
                    new_y=kwargs.get("new_y", "NEXT"),
                    align=kwargs.get("align", ""),
                    fill=kwargs.get("fill", False),
                )
            return
        except Exception:
            continue

    pdf.set_font("Helvetica", style, size)
    safe = text.encode("latin-1", "replace").decode("latin-1")
    if method == "multi_cell":
        pdf.multi_cell(kwargs.get("w", 0), kwargs.get("h", 5), safe)
    else:
        pdf.cell(
            kwargs.get("w", 0),
            kwargs.get("h", 10),
            safe,
            new_x=kwargs.get("new_x", "LMARGIN"),
            new_y=kwargs.get("new_y", "NEXT"),
            align=kwargs.get("align", ""),
            fill=kwargs.get("fill", False),
        )


DISCLAIMER_TEXT = (
    "Disclaimer\n"
    "This output was generated by an experimental AI-powered simulation system. "
    "All personas, behaviours, and insights are synthetic and do not represent real "
    "individuals or guaranteed real-world outcomes. Results are provided for exploratory "
    "purposes only and should not be relied upon as professional, legal, financial, or "
    "investment advice."
)


class BrainstormPDF(FPDF):
    def footer(self):
        self.set_y(-35)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(130, 130, 130)
        self.multi_cell(0, 3, DISCLAIMER_TEXT, align="C")
        self.set_text_color(0, 0, 0)


def generate_report_pdf(study, sections):
    pdf = BrainstormPDF()
    pdf.set_auto_page_break(auto=True, margin=38)
    pdf.add_page()

    cjk_ok = _register_cjk_fonts(pdf)

    pdf.set_font("Helvetica", "B", 18)
    pdf.cell(
        0,
        12,
        "Project Brainstorm - Research Report",
        new_x="LMARGIN",
        new_y="NEXT",
        align="C",
    )
    pdf.ln(4)

    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(
        0,
        6,
        "Experimental / simulated output - not for production use",
        new_x="LMARGIN",
        new_y="NEXT",
        align="C",
    )
    pdf.set_text_color(0, 0, 0)
    pdf.ln(4)

    title_text = study.get("title", "Untitled Study")
    _pdf_write_text(pdf, title_text, 14, style="B", cjk_available=cjk_ok, h=10)
    st_label = STUDY_TYPE_LABELS.get(
        study.get("study_type", ""), study.get("study_type", "Unknown")
    )
    status_line = f"Type: {st_label}  |  Status: {study.get('status', 'unknown')}"
    _pdf_write_text(pdf, status_line, 10, cjk_available=cjk_ok, h=6)
    pdf.ln(6)

    heading_sections = [
        ("1. Executive Summary", sections.get("executive_summary", "")),
        ("2. What Was Studied", sections.get("what_was_studied", "")),
        ("3. Key Findings", sections.get("key_findings", "")),
        ("4. Risks, Limits, and Unknowns", sections.get("risks_limits", "")),
        ("5. Sources and Citations", sections.get("sources_citations", "")),
    ]

    for heading, content in heading_sections:
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_fill_color(240, 240, 240)
        pdf.cell(0, 8, heading, new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.ln(2)
        _pdf_write_text(
            pdf, content, 10, cjk_available=cjk_ok, method="multi_cell", w=0, h=5
        )
        pdf.ln(4)

    fu_sections = sections.get("followup_sections", [])
    for fus in fu_sections:
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_fill_color(240, 240, 240)
        heading = f"{5 + fus['round']}. Follow-up Round {fus['round']}"
        pdf.cell(0, 8, heading, new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.ln(2)
        _pdf_write_text(
            pdf, fus["content"], 10, cjk_available=cjk_ok, method="multi_cell", w=0, h=5
        )
        pdf.ln(4)

    return pdf.output()


def generate_placeholder_output(study_type, study, persona_names):
    header = "*** SIMULATED PLACEHOLDER — NOT REAL OUTPUT ***"
    if study_type == "synthetic_survey":
        return json.dumps(
            {
                "disclaimer": header,
                "study_title": study["title"],
                "study_type": "synthetic_survey",
                "summary": "This is a placeholder aggregated survey result. No real respondents were surveyed.",
                "sample_size": 200,
                "questions": [
                    {
                        "q": "How satisfied are you with the product?",
                        "results": {
                            "Very satisfied": "32%",
                            "Satisfied": "41%",
                            "Neutral": "15%",
                            "Dissatisfied": "8%",
                            "Very dissatisfied": "4%",
                        },
                    },
                    {
                        "q": "Would you recommend this product to a friend?",
                        "results": {
                            "Definitely yes": "28%",
                            "Probably yes": "35%",
                            "Not sure": "22%",
                            "Probably not": "10%",
                            "Definitely not": "5%",
                        },
                    },
                    {
                        "q": "What is your primary reason for using this product?",
                        "results": {
                            "Price": "25%",
                            "Quality": "30%",
                            "Convenience": "20%",
                            "Brand trust": "15%",
                            "Other": "10%",
                        },
                    },
                ],
            },
            indent=2,
        )
    elif study_type == "synthetic_idi":
        speakers = persona_names if persona_names else ["Respondent"]
        lines = [header, "", f"Study: {study['title']}", f"Type: Synthetic IDI", ""]
        for speaker in speakers[:3]:
            lines.extend(
                [
                    f"--- Interview with {speaker} ---",
                    "",
                    f"Moderator: Thank you for joining, {speaker}. Can you tell me about your experience?",
                    f"{speaker}: Sure. I've been using the product for about 6 months now. Overall it's been positive.",
                    "",
                    f"Moderator: What stands out most to you?",
                    f"{speaker}: The ease of use is the biggest factor. I don't have to think about it.",
                    "",
                    f"Moderator: Are there any areas for improvement?",
                    f"{speaker}: The pricing could be more transparent. Sometimes I'm not sure what I'm paying for.",
                    "",
                ]
            )
        return "\n".join(lines)
    elif study_type == "synthetic_focus_group":
        speakers = (
            persona_names
            if len(persona_names) >= 4
            else ["Participant A", "Participant B", "Participant C", "Participant D"]
        )
        lines = [
            header,
            "",
            f"Study: {study['title']}",
            f"Type: Synthetic Focus Group",
            "",
        ]
        lines.extend(
            [
                "Moderator: Welcome everyone. Let's start by discussing your first impressions.",
                "",
                f"{speakers[0]}: I was skeptical at first, but the onboarding was smooth.",
                f"{speakers[1]}: Same here. Though I had some confusion with the navigation.",
                f"{speakers[2]}: I actually found it intuitive from day one.",
                f"{speakers[3] if len(speakers) > 3 else 'Participant D'}: The design is clean but I wish there were more customization options.",
                "",
                "Moderator: Interesting. How about ongoing usage?",
                "",
                f"{speakers[0]}: I use it daily now. It's become part of my routine.",
                f"{speakers[1]}: Weekly for me. Mostly for the reporting features.",
                f"{speakers[2]}: I've tried alternatives but keep coming back.",
                f"{speakers[3] if len(speakers) > 3 else 'Participant D'}: The mobile experience could use work.",
                "",
                "Moderator: Any final thoughts?",
                "",
                f"{speakers[0]}: Keep improving the speed. That's my top ask.",
                f"{speakers[1]}: More integrations with other tools would help.",
                f"{speakers[2]}: Overall very satisfied.",
                f"{speakers[3] if len(speakers) > 3 else 'Participant D'}: Agree with what's been said.",
                "",
            ]
        )
        return "\n".join(lines)
    return json.dumps({"disclaimer": header, "error": "Unknown study type"})


def run_ben_qa(study_dict):
    output = study_dict.get("study_output") or ""
    study_type = study_dict.get("study_type") or ""
    fail_zero = {"Strong": 0, "Indicative": 0, "Exploratory": 0}

    if not output or not study_type:
        return {
            "decision": "FAIL",
            "notes": "Missing required fields: no output or no study_type.",
            "confidence_labels": fail_zero,
        }

    if study_type == "synthetic_survey":
        raw_sq = study_dict.get("survey_questions") or "[]"
        if isinstance(raw_sq, str):
            try:
                sq = json.loads(raw_sq)
            except (json.JSONDecodeError, TypeError):
                sq = []
        elif isinstance(raw_sq, list):
            sq = raw_sq
        else:
            sq = []
        sq = [q for q in sq if isinstance(q, str) and q.strip()]
        r_count = study_dict.get("respondent_count")
        q_count = study_dict.get("question_count")
        failures = []
        if q_count is None or not (1 <= int(q_count) <= 12):
            failures.append(f"question_count invalid ({q_count})")
        if r_count is None or not (1 <= int(r_count) <= 400):
            failures.append(f"respondent_count invalid ({r_count})")
        if not failures and len(sq) != int(q_count):
            failures.append(
                f"survey has {len(sq)} questions but question_count is {q_count}"
            )
        if len(sq) < 1 and not failures:
            failures.append("survey has no questions defined")
        study_id_debug = study_dict.get("id", "?")
        if failures:
            decision = "FAIL"
            print(
                f"QA_DEBUG survey study={study_id_debug} question_count={q_count} questions_len={len(sq)} decision={decision}"
            )
            return {
                "decision": decision,
                "notes": f"QA failed: {'; '.join(failures)}.",
                "confidence_labels": fail_zero,
            }
    elif study_type in ("synthetic_idi", "synthetic_focus_group"):
        study_id_debug = study_dict.get("id", "?")
        anchor_fields = {
            "business_problem": "Business Problem",
            "decision_to_support": "Decision to Support",
            "known_vs_unknown": "Known vs Unknown",
            "target_audience": "Target Audience",
            "study_fit": "Study Fit",
            "definition_useful_insight": "Definition of Useful Insight",
        }
        missing_anchors = []
        for field_key, field_label in anchor_fields.items():
            val = study_dict.get(field_key)
            if not val or not str(val).strip():
                missing_anchors.append(field_label)
        if missing_anchors:
            print(
                f"QA_DEBUG qual study={study_id_debug} missing_anchors={missing_anchors} decision=FAIL"
            )
            return {
                "decision": "FAIL",
                "notes": f"QA failed: missing research brief anchors: {', '.join(missing_anchors)}.",
                "confidence_labels": fail_zero,
            }
        personas = normalize_personas_used(study_dict.get("personas_used"))
        pc = len(personas)
        if study_type == "synthetic_idi" and (pc < 1 or pc > 3):
            print(
                f"QA_DEBUG qual study={study_id_debug} missing_anchors=[] persona_count={pc} decision=FAIL"
            )
            return {
                "decision": "FAIL",
                "notes": f"QA failed: IDI requires 1–3 personas, found {pc}.",
                "confidence_labels": fail_zero,
            }
        if study_type == "synthetic_focus_group" and (pc < 4 or pc > 6):
            print(
                f"QA_DEBUG qual study={study_id_debug} missing_anchors=[] persona_count={pc} decision=FAIL"
            )
            return {
                "decision": "FAIL",
                "notes": f"QA failed: Focus Group requires 4–6 personas, found {pc}.",
                "confidence_labels": fail_zero,
            }

    if study_type == "synthetic_survey":
        study_id_debug = study_dict.get("id", "?")

    study_id_val = str(study_dict.get("id", ""))
    gov_failures = []
    try:
        gov_conn = get_db()

        gt_row = gov_conn.execute(
            "SELECT * FROM grounding_traces WHERE trigger_event = 'study_executed' AND study_id = ?",
            (study_id_val,),
        ).fetchone()
        if not gt_row:
            gov_failures.append("Missing Grounding Trace for study_executed.")
        else:
            if (
                gt_row["admin_sources_configured"] == 1
                and gt_row["admin_sources_queried"] != 1
            ):
                gov_failures.append("Admin sources configured but not queried.")
            if (
                gt_row["admin_sources_configured"] == 1
                and gt_row["admin_sources_used_in_output"] == 0
                and not (gt_row["admin_source_reason_code"] or "").strip()
            ):
                gov_failures.append("Admin sources unused without reason code.")

        if study_type in ("synthetic_idi", "synthetic_focus_group"):
            personas = normalize_personas_used(study_dict.get("personas_used"))
    
            for pid in personas:
                p_row = gov_conn.execute(
                    "SELECT ai_model_provenance FROM personas WHERE persona_instance_id = ?",
                    (pid,),
                ).fetchone()
                
                if not p_row:
                    gov_failures.append(f"Persona {pid} not found in database.")
                    continue
                    
                prov = (p_row["ai_model_provenance"] or "").strip()
                if not prov:
                    gov_failures.append(f"Persona {pid} missing ai_model_provenance.")
                    continue

                pt_row = gov_conn.execute(
                    "SELECT id FROM grounding_traces WHERE trigger_event = 'persona_created' AND persona_id = ?",
                    (pid,),
                ).fetchone()
                if not pt_row:
                    gov_failures.append(
                        f"Missing Grounding Trace for persona_created (persona {pid})."
                    )

        gov_conn.close()
    except Exception as e:
        gov_failures.append(f"Governance check error: {e}")

    if gov_failures:
        summary = "; ".join(gov_failures)
        print(f"BEN_QA_DECISION=FAIL reason={summary}")
        return {
            "decision": "FAIL",
            "notes": f"Governance check failed: {summary}",
            "confidence_labels": fail_zero,
        }

    if "SIMULATED PLACEHOLDER" in output:
        if study_type == "synthetic_survey":
            try:
                parsed = json.loads(output)
                n_insights = len(parsed.get("questions", []))
            except (json.JSONDecodeError, TypeError):
                n_insights = 3
            print(
                f"QA_DEBUG survey study={study_id_debug} question_count={q_count} questions_len={len(sq)} decision=DOWNGRADE"
            )
        elif study_type == "synthetic_idi":
            n_insights = output.count("--- Interview with")
            if n_insights == 0:
                n_insights = 1
        elif study_type == "synthetic_focus_group":
            n_insights = output.count("Moderator:")
            if n_insights == 0:
                n_insights = 1
        else:
            n_insights = 3

        print(f"BEN_QA_DECISION=DOWNGRADE reason=placeholder output")
        return {
            "decision": "DOWNGRADE",
            "notes": "Output is placeholder; confidence downgraded.",
            "confidence_labels": {
                "Strong": 0,
                "Indicative": min(n_insights, 2),
                "Exploratory": max(0, n_insights - 2),
            },
        }

    if study_type == "synthetic_survey":
        try:
            parsed = json.loads(output)
            n_insights = len(parsed.get("questions", []))
        except (json.JSONDecodeError, TypeError):
            n_insights = 3
        print(
            f"QA_DEBUG survey study={study_id_debug} question_count={q_count} questions_len={len(sq)} decision=PASS"
        )
    else:
        n_insights = 3

    print(f"BEN_QA_DECISION=PASS reason=all checks passed")
    return {
        "decision": "PASS",
        "notes": "All checks passed. Output meets quality standards.",
        "confidence_labels": {
            "Strong": max(1, n_insights // 2),
            "Indicative": n_insights - max(1, n_insights // 2),
            "Exploratory": 0,
        },
    }


def ben_precheck(study, persona_count, persona_dossiers=None):
    failures = []
    st = study["study_type"] or ""

    if not st:
        failures.append("Study type is not set.")
        return failures

    if st == "synthetic_survey":
        rc = study["respondent_count"] or 0
        qc = study["question_count"] or 0
        sq = []
        if study["survey_questions"]:
            try:
                sq = json.loads(study["survey_questions"])
            except (json.JSONDecodeError, TypeError):
                sq = []
        if rc < 1 or rc > 400:
            failures.append("Respondent count must be between 1 and 400.")
        if qc < 1 or qc > 12:
            failures.append("Question count must be between 1 and 12.")
        if len(sq) != qc:
            failures.append(f"Survey has {len(sq)} questions but needs exactly {qc}.")
    elif st in ("synthetic_idi", "synthetic_focus_group"):
        anchor_labels = [
            ("business_problem", "Business Problem"),
            ("decision_to_support", "Decision to Support"),
            ("market_geography", "Market / Geography"),
            ("product_concept", "Product / Concept"),
            ("target_audience", "Target Audience"),
            ("definition_useful_insight", "Definition of Useful Insight"),
        ]
        for field, label in anchor_labels:
            val = get_v1a_value(study, field)
            if not val:
                failures.append(f"{label} is missing.")
        if st == "synthetic_idi":
            if persona_count < 1 or persona_count > 3:
                failures.append(
                    f"IDI requires 1–3 personas (currently {persona_count})."
                )
        else:
            if persona_count < 4 or persona_count > 6:
                failures.append(
                    f"Focus Group requires 4–6 personas (currently {persona_count})."
                )

        target_audience = get_v1a_value(study, "target_audience")
        market_geo = get_v1a_value(study, "market_geography")
        if target_audience and persona_dossiers:
            ta_lower = target_audience.lower()
            mg_lower = (market_geo or "").lower()
            for pd in persona_dossiers:
                p_name = pd.get("name", "Unknown")
                p_summary = (pd.get("persona_summary") or "").lower()
                p_demo = (pd.get("demographic_frame") or "").lower()
                p_context = (pd.get("contextual_constraints") or "").lower()
                p_text = f"{p_summary} {p_demo} {p_context}"
                if mg_lower and len(mg_lower) > 3:
                    geo_tokens = [t.strip() for t in mg_lower.replace(",", " ").split() if len(t.strip()) > 3]
                    geo_match = any(tok in p_text for tok in geo_tokens)
                    if not geo_match and geo_tokens:
                        failures.append(
                            f"Persona '{p_name}' may not match Market / Geography '{market_geo}'. "
                            "Review persona dossier for geographic fit."
                        )
    else:
        failures.append(f"Unknown study type: {st}")

    return failures


def auto_ben_precheck(conn, study, user_id):
    st = study.get("study_type") or ""
    if not st:
        return
    if study.get("status") != "draft":
        return

    study_dict = dict(study) if not isinstance(study, dict) else study
    study_id = study_dict["id"]

    raw_ids = normalize_personas_used(study_dict.get("personas_used"))
    persona_count = 0
    persona_dossiers = []
    for pid in raw_ids:
        p_row = conn.execute(
            "SELECT name, persona_summary, demographic_frame, contextual_constraints "
            "FROM personas WHERE persona_instance_id = ? AND user_id = ?",
            (pid, user_id),
        ).fetchone()
        if p_row:
            persona_count += 1
            persona_dossiers.append(dict(p_row))

    if st in ("synthetic_idi", "synthetic_focus_group"):
        if persona_count == 0:
            min_p = 1 if st == "synthetic_idi" else 4
            persona_count = min_p

    failures = ben_precheck(study_dict, persona_count, persona_dossiers)

    if failures:
        conn.execute(
            "UPDATE studies SET qa_status = 'precheck_failed', qa_notes = ? WHERE id = ?",
            (json.dumps(failures), study_id),
        )
    else:
        conn.execute(
            "UPDATE studies SET qa_status = 'precheck_passed', qa_notes = NULL WHERE id = ?",
            (study_id,),
        )
    conn.commit()
    study_dict["qa_status"] = "precheck_passed" if not failures else "precheck_failed"
    study_dict["qa_notes"] = json.dumps(failures) if failures else None
    print(f"AUTO_PRECHECK study={study_id} result={'FAIL' if failures else 'PASS'} failures={failures}", flush=True)


@app.route("/ready-for-qa/<int:study_id>", methods=["POST"])
def ready_for_qa(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("Unauthorized.")
    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Study not found or not in draft status.")

    auto_ben_precheck(conn, dict(study), user["id"])
    conn.close()
    return redirect(f"/?token={token}&configure={study_id}")


@app.route("/download-pdf/<int:study_id>")
def download_pdf(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("Unauthorized.")
    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ?",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Study not found.")
    if study["status"] not in ("completed", "qa_blocked", "terminated_system"):
        conn.close()
        return render_error(
            "Report is only available for completed or reviewed studies."
        )
    followup_rows = conn.execute(
        "SELECT * FROM followups WHERE study_id = ? ORDER BY followup_round ASC",
        (study_id,),
    ).fetchall()
    upload_names = [
        r["filename"]
        for r in conn.execute(
            """SELECT u.filename FROM study_documents sd
           JOIN user_uploads u ON u.id = sd.user_doc_id
           WHERE sd.study_id = ?""",
            (study_id,),
        ).fetchall()
    ]
    active_admin_names = [
        r["filename"]
        for r in conn.execute(
            "SELECT filename FROM admin_uploads WHERE status = 'active'"
        ).fetchall()
    ]
    conn.close()
    all_upload_names = upload_names + active_admin_names
    followups = [dict(f) for f in followup_rows]
    study_dict = dict(study)
    version_param = request.args.get("version")
    version = int(version_param) if version_param and version_param.isdigit() else None
    sections = build_structured_report(
        study_dict,
        followups=followups,
        version=version,
        uploaded_filenames=all_upload_names,
    )
    pdf_bytes = generate_report_pdf(study_dict, sections)
    report_version = sections.get("version", 1)
    safe_title = (
        "".join(
            c if c.isalnum() or c in (" ", "-", "_") else ""
            for c in (study_dict.get("title") or f"study_{study_id}")
        )
        .strip()
        .replace(" ", "_")
    )
    filename = f"{safe_title}_{study_id}_V{report_version}.pdf"
    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=filename,
    )


MAX_FOLLOWUP_ROUNDS = 2


def generate_followup_placeholder(study, user_question, round_num):
    st = study.get("study_type", "")
    header = "*** SIMULATED FOLLOW-UP PLACEHOLDER ***"
    if st == "synthetic_idi":
        return (
            f"{header}\n\n"
            f"Follow-up Round {round_num} for IDI study: {study.get('title', '')}\n"
            f"User asked: {user_question}\n\n"
            f"[Simulated IDI follow-up response]\n"
            f"The respondent provided additional context on this topic. "
            f"Key themes include deepening the original insights and uncovering "
            f"new angles related to the follow-up question.\n"
        )
    else:
        return (
            f"{header}\n\n"
            f"Follow-up Round {round_num} for Focus Group study: {study.get('title', '')}\n"
            f"User asked: {user_question}\n\n"
            f"[Simulated Focus Group follow-up response]\n"
            f"The group engaged with the follow-up topic. "
            f"Multiple participants offered perspectives that build on the "
            f"original discussion and introduce new considerations.\n"
        )


@app.route("/submit-followup/<int:study_id>", methods=["POST"])
def submit_followup(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("Unauthorized.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ?",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Study not found.")

    if study["status"] != "completed":
        conn.close()
        return render_error("Follow-ups are only allowed for completed studies.")

    if study["study_type"] not in ("synthetic_idi", "synthetic_focus_group"):
        conn.close()
        return render_error(
            "Follow-ups are only available for IDI and Focus Group studies."
        )

    existing_count = conn.execute(
        "SELECT COUNT(*) FROM followups WHERE study_id = ?",
        (study_id,),
    ).fetchone()[0]

    if existing_count >= MAX_FOLLOWUP_ROUNDS:
        conn.close()
        return render_error(
            f"Maximum of {MAX_FOLLOWUP_ROUNDS} follow-up rounds reached for this study. "
            f"No further follow-ups are allowed."
        )

    user_question = (request.form.get("followup_question") or "").strip()
    if not user_question:
        conn.close()
        return render_error("Follow-up question cannot be empty.")

    round_num = existing_count + 1
    study_dict = dict(study)
    followup_output = generate_followup_placeholder(
        study_dict, user_question, round_num
    )

    qa_input = dict(study_dict)
    qa_input["study_output"] = followup_output
    fu_qa = run_ben_qa(qa_input)
    fu_qa_status = fu_qa["decision"].lower()
    fu_qa_notes = fu_qa["notes"]

    conn.execute(
        """INSERT INTO followups (study_id, followup_round, user_question, generated_output, qa_status, qa_notes)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            study_id,
            round_num,
            user_question,
            followup_output,
            fu_qa_status,
            fu_qa_notes,
        ),
    )
    conn.commit()
    conn.close()
    return redirect(f"/?token={token}&view_output={study_id}")


def get_save_buttons(study):
    st = study["study_type"] or ""
    buttons = []

    if not st:
        bp = (study["business_problem"] or "").strip()
        ds = (study["decision_to_support"] or "").strip()
        if not bp:
            buttons.append(("business_problem", "Save as Business Problem"))
        if not ds:
            buttons.append(("decision_to_support", "Save as Decision to Support"))
        return buttons[:2]

    if st == "synthetic_survey":
        qc = study["question_count"] or 0
        sq = []
        if study["survey_questions"]:
            try:
                sq = json.loads(study["survey_questions"])
            except (json.JSONDecodeError, TypeError):
                sq = []
        if len(sq) < qc:
            buttons.append(("survey_question_append", "Save as Survey Question"))
        return buttons[:1]

    if st in ("synthetic_idi", "synthetic_focus_group"):
        anchor_fields = [
            ("business_problem", "Save as Business Problem"),
            ("decision_to_support", "Save as Decision to Support"),
            ("market_geography", "Save as Market / Geography"),
            ("product_concept", "Save as Product / Concept"),
            ("target_audience", "Save as Target Audience"),
            ("definition_useful_insight", "Save as Definition of Useful Insight"),
        ]
        study_dict = dict(study)
        for field, label in anchor_fields:
            val = get_v1a_value(study_dict, field)
            if not val:
                buttons.append((field, label))
                if len(buttons) >= 3:
                    break
        return buttons

    return []


def get_coaching_nudge(study, persona_count):
    st = study["study_type"] or ""

    if not st:
        bp = (study["business_problem"] or "").strip()
        ds = (study["decision_to_support"] or "").strip()
        if not bp:
            return "Let's start by defining your Business Problem. What challenge is your business facing that this research should address?"
        if not ds:
            return "Great, you've described the business problem. Now, what specific decision do you need this research to support?"
        return "Both discovery fields are filled! Review Mark's recommendation above and confirm a study type to continue."

    if st == "synthetic_survey":
        rc = study["respondent_count"] or 0
        qc = study["question_count"] or 0
        sq = []
        if study["survey_questions"]:
            try:
                sq = json.loads(study["survey_questions"])
            except (json.JSONDecodeError, TypeError):
                sq = []
        if rc < 1:
            return "Next up: set your respondent count. How many people should take this survey? (Use the Survey Configuration form above.)"
        if qc < 1:
            return "Now set the number of survey questions you'd like to ask. (Use the Survey Configuration form above.)"
        if len(sq) != qc:
            return f"You need exactly {qc} survey questions but have {len(sq)} so far. Add or update your questions using the form above."
        return "Your survey setup looks complete! You can click 'Ready for QA Review' in the side panel when you're ready."

    if st in ("synthetic_idi", "synthetic_focus_group"):
        anchors = [
            (
                "business_problem",
                "Business Problem",
                "What business challenge should this research address?",
            ),
            (
                "decision_to_support",
                "Decision to Support",
                "What specific decision will this research inform?",
            ),
            (
                "market_geography",
                "Market / Geography",
                "What market or geographic region is this study focused on?",
            ),
            (
                "product_concept",
                "Product / Concept",
                "What product, service, or concept is being researched?",
            ),
            (
                "target_audience",
                "Target Audience",
                "Who is the target audience for this research?",
            ),
            (
                "definition_useful_insight",
                "Definition of Useful Insight",
                "What would a useful insight look like for this study?",
            ),
        ]
        study_dict = dict(study)
        for field, label, prompt in anchors:
            val = get_v1a_value(study_dict, field)
            if not val:
                return f"Next missing item: {label}. {prompt} (Fill it in the Research Brief section above.)"
        if st == "synthetic_idi":
            if persona_count < 1 or persona_count > 3:
                return f"You have {persona_count} persona(s) attached. IDI requires 1–3 personas. Attach or remove personas above."
        else:
            if persona_count < 4 or persona_count > 6:
                return f"You have {persona_count} persona(s) attached. Focus Groups require 4–6 personas. Attach or remove personas above."
        return "Everything looks complete! You can click 'Ready for QA Review' in the side panel when you're ready."

    return "Thanks for your message! I've noted your input — we'll use this as we shape the study together."


@app.route("/save-chat-field/<int:study_id>", methods=["POST"])
def save_chat_field(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("Unauthorized.")
    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Study not found or not in draft status.")

    field = (request.form.get("field") or "").strip()

    all_msgs = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM chat_messages WHERE study_id = ? ORDER BY id ASC",
            (study_id,),
        ).fetchall()
    ]
    if parse_mark_proposal_or_none(all_msgs):
        conn.close()
        return render_error("Cannot use chat save when a proposal exists. Use the Confirm & Save button instead.")

    last_user_msg = conn.execute(
        "SELECT message_text FROM chat_messages WHERE study_id = ? AND sender = 'user' ORDER BY id DESC LIMIT 1",
        (study_id,),
    ).fetchone()
    if not last_user_msg:
        conn.close()
        return render_error("No user message to save.")
    value = last_user_msg["message_text"]

    valid_fields = {
        "business_problem",
        "decision_to_support",
        "market_geography",
        "product_concept",
        "target_audience",
        "definition_useful_insight",
        "survey_question_append",
    }
    if field not in valid_fields:
        conn.close()
        return render_error("Invalid field.")

    if field == "survey_question_append":
        sq = []
        if study["survey_questions"]:
            try:
                sq = json.loads(study["survey_questions"])
            except (json.JSONDecodeError, TypeError):
                sq = []
        qc = study["question_count"] or 0
        if len(sq) >= qc:
            conn.close()
            return render_error(
                f"Already have {len(sq)}/{qc} questions. Cannot add more."
            )
        sq.append(value)
        conn.execute(
            "UPDATE studies SET survey_questions = ? WHERE id = ?",
            (json.dumps(sq), study_id),
        )
        save_label = f"Survey Question #{len(sq)}"
    elif field in V1A_FIELD_MAP:
        set_v1a_value(conn, study_id, field, value)
        save_label = V1A_LABELS.get(field, field.replace("_", " ").title())
    else:
        conn.execute(
            f"UPDATE studies SET {field} = ? WHERE id = ?",
            (value, study_id),
        )
        save_label = V1A_LABELS.get(field, field.replace("_", " ").title())

    conn.execute(
        "INSERT INTO chat_messages (study_id, sender, message_text) VALUES (?, 'mark', ?)",
        (study_id, f"Saved your message as {save_label}."),
    )
    conn.commit()
    conn.close()
    return redirect(f"/?token={token}&configure={study_id}")


@app.route("/confirm-proposed-updates", methods=["POST"])
def confirm_proposed_updates():
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("Session expired. Please log in again.")
    study_id = request.form.get("study_id", type=int)
    field = request.form.get("field", "").strip()
    value = request.form.get("value", "").strip()
    if not study_id:
        return render_error("Missing required fields.")
    ok, err = policy_validate_for_save(field, value)
    if not ok:
        return render_error(err)
    conn = get_db()
    try:
        study = conn.execute(
            "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
            (study_id, user["id"]),
        ).fetchone()
        if not study:
            conn.close()
            return render_error("Study not found or not a draft.")
        policy_apply_save(conn, study_id, field, value)
        conn.commit()
        db_col = V1A_FIELD_MAP.get(field, field)
        persisted = conn.execute(
            f"SELECT {db_col} FROM studies WHERE id = ?",
            (study_id,),
        ).fetchone()
        assert (
            persisted
            and persisted[0]
            and persisted[0].strip()
        ), f"FATAL: Save failed for field '{field}' (value not persisted)"
    except Exception as e:
        print(f"CONFIRM_PROPOSED_ERROR study={study_id}: {e}", flush=True)
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return redirect(f"/?token={token}&configure={study_id}")


@app.route("/chat-status/<int:study_id>")
def chat_status(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return jsonify({"ready": False, "messages": []})
    conn = get_db()
    study = conn.execute(
        "SELECT id FROM studies WHERE id = ? AND user_id = ?",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return jsonify({"ready": False, "messages": []})
    pending = conn.execute(
        "SELECT COUNT(*) FROM chat_messages WHERE study_id = ? AND sender = 'mark' AND message_text = '⏳ Mark is thinking...'",
        (study_id,),
    ).fetchone()[0]
    msgs = conn.execute(
        "SELECT id, sender, message_text, timestamp_utc FROM chat_messages WHERE study_id = ? ORDER BY id",
        (study_id,),
    ).fetchall()
    conn.close()
    messages = [{"id": m["id"], "sender": m["sender"], "text": m["message_text"], "time": m["timestamp_utc"]} for m in msgs]
    return jsonify({"ready": pending == 0, "pending": pending, "messages": messages})


@app.route("/clear-chat-pending/<int:study_id>", methods=["POST"])
def clear_chat_pending(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return redirect(f"/?error=auth")
    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ?",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return redirect(f"/?token={token}&error=study_not_found")
    deleted = conn.execute(
        "DELETE FROM chat_messages WHERE study_id = ? AND sender='mark' "
        "AND message_text IN ('\u23f3 Mark is thinking...', '\u23f3 Mark is thinking...')",
        (study_id,),
    ).rowcount
    conn.commit()
    conn.close()
    print(f"CLEAR_CHAT_PENDING study={study_id} deleted={deleted}", flush=True)
    return redirect(f"/?token={token}&configure={study_id}")


@app.route("/send-chat/<int:study_id>", methods=["POST", "GET"])
def send_chat(study_id):
    import subprocess

    if request.method == "GET":
        token = get_token()
        return redirect(f"/?token={token}&configure={study_id}")

    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("Unauthorized.")

    try:
        return _send_chat_inner(study_id, token, user)
    except Exception as e:
        print(f"SEND_CHAT_CRASH study={study_id}: {e}", flush=True)
        try:
            conn_err = get_db()
            conn_err.execute(
                "INSERT INTO chat_messages (study_id, sender, message_text) VALUES (?, 'mark', ?)",
                (study_id, "Mark couldn't complete that request right now. Please try again."),
            )
            conn_err.commit()
            conn_err.close()
        except Exception:
            pass
        return redirect(f"/?token={token}&configure={study_id}")


def _send_chat_inner(study_id, token, user):
    import subprocess

    conn = get_db()
    conn.execute(
        "DELETE FROM chat_messages "
        "WHERE sender='mark' AND message_text='\u23f3 Mark is thinking...' "
        "AND timestamp_utc < datetime('now','-5 minutes')"
    )
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ?",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Study not found.")

    message_text = (request.form.get("message_text") or "").strip()
    if not message_text:
        conn.close()
        return redirect(f"/?token={token}&configure={study_id}")

    pending_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM chat_messages WHERE study_id = ? AND sender = 'mark' AND message_text = ?",
        (study_id, "\u23f3 Mark is thinking..."),
    ).fetchone()["cnt"]
    if pending_count > 0:
        conn.execute(
            "INSERT INTO chat_messages (study_id, sender, message_text) VALUES (?, 'mark', ?)",
            (study_id, "Mark is still working on the previous message. Please wait a moment and refresh."),
        )
        conn.commit()
        conn.close()
        return redirect(f"/?token={token}&configure={study_id}")

    try:
        conn.execute(
            "INSERT INTO chat_messages (study_id, sender, message_text) VALUES (?, 'user', ?)",
            (study_id, message_text),
        )

        persona_count = 0
        raw_ids = normalize_personas_used(study["personas_used"])
        for pid in raw_ids:
            if conn.execute(
                "SELECT 1 FROM personas WHERE persona_instance_id = ? AND user_id = ?",
                (pid, user["id"]),
            ).fetchone():
                persona_count += 1

        mc = {
            r["key"]: r["value"]
            for r in conn.execute("SELECT key, value FROM model_config").fetchall()
        }
        mark_model_id = mc.get("mark_model")
        study_dict = dict(study)
    except Exception as e:
        print(f"SEND_CHAT_DB_ERROR study={study_id}: {e}", flush=True)
        try:
            conn.close()
        except Exception:
            pass
        return redirect(f"/?token={token}&configure={study_id}")

    system_prompt = build_mark_system_message(study_dict, persona_count, study_id=study_id)

    chat_history = conn.execute(
        "SELECT sender, message_text FROM chat_messages WHERE study_id = ? ORDER BY id",
        (study_id,),
    ).fetchall()

    placeholder_text = "\u23f3 Mark is thinking..."
    cursor = conn.execute(
        "INSERT INTO chat_messages (study_id, sender, message_text) VALUES (?, 'mark', ?)",
        (study_id, placeholder_text),
    )
    placeholder_msg_id = cursor.lastrowid
    conn.commit()
    conn.close()

    fallback = get_coaching_nudge(study_dict, persona_count)

    has_creds = (
        os.environ.get("AI_INTEGRATIONS_OPENROUTER_BASE_URL")
        and os.environ.get("AI_INTEGRATIONS_OPENROUTER_API_KEY")
    )

    if mark_model_id and has_creds:
        import tempfile
        prompt_data = {
            "system_prompt": system_prompt,
            "chat_history": [
                {"role": "assistant" if m["sender"] == "mark" else "user", "content": m["message_text"]}
                for m in chat_history
                if m["message_text"] != placeholder_text
            ],
        }
        prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, dir="/tmp"
        )
        json.dump(prompt_data, prompt_file)
        prompt_file.close()

        worker_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mark_reply_worker.py")
        env = os.environ.copy()
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker.log")
        try:
            log_f = open(log_path, "a")
            subprocess.Popen(
                [sys.executable, worker_script,
                 str(study_id), str(placeholder_msg_id), mark_model_id, message_text, fallback, prompt_file.name],
                env=env,
                stdout=log_f,
                stderr=log_f,
            )
            log_f.close()
        except Exception as e:
            print(f"WORKER_SPAWN_ERROR: {e}", flush=True)
            try:
                log_f.close()
            except Exception:
                pass
            conn2 = get_db()
            conn2.execute(
                "UPDATE chat_messages SET message_text = ? WHERE id = ?",
                (fallback, placeholder_msg_id),
            )
            conn2.commit()
            conn2.close()
    else:
        if not has_creds:
            print(f"SEND_CHAT_NO_CREDS study={study_id}: skipping worker spawn", flush=True)
        conn2 = get_db()
        conn2.execute(
            "UPDATE chat_messages SET message_text = ? WHERE id = ?",
            (fallback, placeholder_msg_id),
        )
        conn2.commit()
        conn2.close()

    return redirect(f"/?token={token}&configure={study_id}")


@app.route("/run-study/<int:study_id>", methods=["POST"])
def run_study(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Draft study not found or already executed.")

    if study["qa_status"] != "precheck_passed":
        conn.close()
        return render_error(
            "You must pass Ben's QA precheck before running the study. Click 'Ready for QA Review' first."
        )

    study_type = study["study_type"]
    if not study_type:
        conn.close()
        return render_error("You must select a study type before running the study.")

    personas_used = normalize_personas_used(study["personas_used"])
    persona_count = len(personas_used)

    if study_type == "synthetic_survey":
        r_count = study["respondent_count"] or 100
        q_count = study["question_count"] or 8
        if r_count < 1 or r_count > 400:
            conn.close()
            return render_error("Survey respondent count must be between 1 and 400.")
        if q_count < 1 or q_count > 12:
            conn.close()
            return render_error("Survey question count must be between 1 and 12.")
        sq = json.loads(study["survey_questions"] or "[]")
        if len(sq) != q_count:
            conn.close()
            return render_error(
                f"Survey must have exactly {q_count} questions (currently {len(sq)}). Save your questions first."
            )
    elif study_type in ("synthetic_idi", "synthetic_focus_group"):
        anchor_missing = []
        for col, label in [
            ("business_problem", "Business Problem"),
            ("decision_to_support", "Decision to Support"),
            ("known_vs_unknown", "Known vs Unknown"),
            ("target_audience", "Target Audience"),
            ("study_fit", "Study Fit"),
            ("definition_useful_insight", "Definition of Useful Insight"),
        ]:
            val = (study[col] or "").strip()
            if not val:
                anchor_missing.append(label)
        if anchor_missing:
            conn.close()
            return render_error(
                f"Cannot run: the following Research Brief anchors are missing: {', '.join(anchor_missing)}"
            )

        if persona_count == 0:
            auto_n = 1 if study_type == "synthetic_idi" else 4
            try:
                mc = {
                    r["key"]: r["value"]
                    for r in conn.execute(
                        "SELECT key, value FROM model_config"
                    ).fetchall()
                }
                lisa_mid = mc.get("lisa_model", "")
                if not lisa_mid:
                    raise ValueError("lisa_model not configured in model_config")
                study_snapshot = dict(study)
                conn.close()
                generated = lisa_generate_personas(study_snapshot, auto_n, lisa_mid)
                conn = get_db()
                new_persona_ids = []
                for p_data in generated:
                    p_instance_id = f"P-{secrets.token_hex(4).upper()}"
                    p_persona_id = f"PID-{secrets.token_hex(4).upper()}"
                    provenance = f"lisa:{lisa_mid}; selection_method=auto"
                    conn.execute(
                        """INSERT INTO personas
                           (user_id, persona_id, version, persona_instance_id, name,
                            persona_summary, demographic_frame, psychographic_profile,
                            contextual_constraints, behavioural_tendencies,
                            ai_model_provenance, grounding_sources, confidence_and_limits)
                           VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            user["id"],
                            p_persona_id,
                            p_instance_id,
                            p_data.get("name", "Auto-Persona"),
                            p_data.get("persona_summary", ""),
                            p_data.get("demographic_frame", ""),
                            p_data.get("psychographic_profile", ""),
                            p_data.get("contextual_constraints", ""),
                            p_data.get("behavioural_tendencies", ""),
                            provenance,
                            p_data.get("grounding_sources", ""),
                            p_data.get("confidence_and_limits", ""),
                        ),
                    )
                    conn.execute(
                        """INSERT INTO grounding_traces
                           (study_id, persona_id, trigger_event,
                            admin_sources_configured, admin_sources_queried,
                            admin_sources_matched, admin_sources_used_in_output)
                           VALUES (?, ?, 'persona_created', 0, 0, 0, 0)""",
                        (str(study_id), p_instance_id),
                    )
                    new_persona_ids.append(p_instance_id)

                conn.execute(
                    "UPDATE studies SET personas_used = ? WHERE id = ?",
                    (json.dumps(new_persona_ids), study_id),
                )
                conn.commit()
                personas_used = new_persona_ids
                persona_count = len(personas_used)
                study = conn.execute(
                    "SELECT * FROM studies WHERE id = ?", (study_id,)
                ).fetchone()
                print(
                    f"AUTO_PERSONAS_GENERATED study={study_id} count={len(new_persona_ids)}"
                )
            except Exception as e:
                conn.close()
                print(f"AUTO_PERSONAS_FAILED study={study_id} reason={e}")
                return render_error(
                    f"Auto-persona generation failed: {e}. Please attach personas manually or contact admin."
                )

        if study_type == "synthetic_idi":
            if persona_count < 1:
                conn.close()
                return render_error("IDI requires at least 1 persona.")
            if persona_count > 3:
                conn.close()
                return render_error("IDI allows max 3 personas.")
        elif study_type == "synthetic_focus_group":
            if persona_count < 4:
                conn.close()
                return render_error("Focus Group requires at least 4 personas.")
            if persona_count > 6:
                conn.close()
                return render_error("Focus Group allows max 6 personas.")

    persona_names = []
    for pid in personas_used:
        p_row = conn.execute(
            "SELECT name FROM personas WHERE persona_instance_id = ?", (pid,)
        ).fetchone()
        if p_row:
            persona_names.append(p_row["name"])

    output = None
    if study_type == "synthetic_survey":
        try:
            mc = {
                r["key"]: r["value"]
                for r in conn.execute("SELECT key, value FROM model_config").fetchall()
            }
            lisa_model_id = mc.get("lisa_model", "")
            if not lisa_model_id:
                raise ValueError("lisa_model not configured")

            study_dict = dict(study)
            r_count = study_dict.get("respondent_count") or 100
            q_count = study_dict.get("question_count") or 8
            sq = json.loads(study_dict.get("survey_questions") or "[]")
            bp = (study_dict.get("business_problem") or "").strip()
            ta = (study_dict.get("target_audience") or "").strip()

            questions_text = "\n".join(f"  Q{i + 1}: {q}" for i, q in enumerate(sq))

            lisa_system = (
                "You are Lisa, a senior quantitative research analyst at Project Brainstorm. "
                "You generate synthetic survey results grounded in realistic market data.\n\n"
                "RULES:\n"
                "1. Output ONLY valid JSON. No markdown, no code fences, no commentary.\n"
                "2. The JSON must have this exact structure:\n"
                "{\n"
                '  "study_title": "<title>",\n'
                '  "study_type": "synthetic_survey",\n'
                '  "methodology": {\n'
                '    "respondent_count": <number>,\n'
                '    "target_audience": "<description>",\n'
                '    "limitations": ["<limitation1>", ...],\n'
                '    "sources": ["<source1>", ...]\n'
                "  },\n"
                '  "questions": [\n'
                "    {\n"
                '      "q": "<question text>",\n'
                '      "results": {"<option>": "<percent>%", ...}\n'
                "    }\n"
                "  ],\n"
                '  "top_findings": ["<finding1>", ...],\n'
                '  "risks_unknowns": ["<risk1>", ...]\n'
                "}\n"
                "3. Each question's result percentages must sum to 100%.\n"
                "4. Provide 3-5 top_findings and 2-4 risks_unknowns.\n"
                "5. limitations should mention this is AI-simulated, not real respondents.\n"
                "6. sources should list plausible grounding references.\n"
                "7. Be realistic and culturally grounded for Asia-Pacific markets where relevant."
            )

            oc_block, oc_keys = _extract_optional_context(study_dict)
            oc_section = f"\n{oc_block}\n" if oc_block else ""
            print(f"OPTIONAL_CONTEXT_INJECT_LISA_EXEC study={study_id} type=synthetic_survey included={'true' if oc_keys else 'false'} keys={','.join(oc_keys)}", flush=True)

            lisa_user = (
                f"Generate synthetic survey results for:\n"
                f"Title: {study_dict.get('title', 'Untitled Study')}\n"
                f"Business Problem: {bp or 'Not specified'}\n"
                f"Target Audience: {ta or 'Not specified'}\n"
                f"Respondent Count: {r_count}\n"
                f"Questions ({q_count}):\n{questions_text}\n"
                f"{oc_section}\n"
                f"Return ONLY the JSON object, nothing else."
            )

            conn.close()
            raw_llm = call_llm(
                lisa_model_id,
                [
                    {"role": "system", "content": lisa_system},
                    {"role": "user", "content": lisa_user},
                ],
                purpose="lisa_survey_execution",
            )
            conn = get_db()

            cleaned = raw_llm.strip()
            if cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[-1]
                if cleaned.endswith("```"):
                    cleaned = cleaned[:-3]
                cleaned = cleaned.strip()

            parsed = json.loads(cleaned)
            output = json.dumps(parsed, indent=2)
            app.logger.info(f"Lisa LLM survey output generated for study {study_id}")
        except Exception as e:
            app.logger.warning(f"Lisa LLM survey call failed, using placeholder: {e}")
            output = None
            try:
                conn.execute("SELECT 1")
            except Exception:
                conn = get_db()

    elif study_type in ("synthetic_idi", "synthetic_focus_group"):
        try:
            mc = {
                r["key"]: r["value"]
                for r in conn.execute("SELECT key, value FROM model_config").fetchall()
            }
            lisa_model_id = mc.get("lisa_model", "")
            if not lisa_model_id:
                raise ValueError("lisa_model not configured")

            study_dict = dict(study)
            brief_fields = [
                ("business_problem", "Business Problem"),
                ("decision_to_support", "Decision to Support"),
                ("known_vs_unknown", "Known vs Unknown"),
                ("target_audience", "Target Audience"),
                ("study_fit", "Study Fit"),
                ("definition_useful_insight", "Definition of Useful Insight"),
            ]
            brief_text = ""
            for field, label in brief_fields:
                val = (study_dict.get(field) or "").strip()
                brief_text += f"{label}: {val or 'Not specified'}\n"

            oc_block, oc_keys = _extract_optional_context(study_dict)
            if oc_block:
                brief_text += f"\n{oc_block}\n"
            print(f"OPTIONAL_CONTEXT_INJECT_LISA_EXEC study={study_id} type={study_type} included={'true' if oc_keys else 'false'} keys={','.join(oc_keys)}", flush=True)

            persona_dossiers = []
            for pid in personas_used:
                p_row = conn.execute(
                    "SELECT name, persona_summary, demographic_frame, psychographic_profile, "
                    "contextual_constraints, behavioural_tendencies FROM personas "
                    "WHERE persona_instance_id = ?",
                    (pid,),
                ).fetchone()
                if p_row:
                    persona_dossiers.append(
                        f"Name: {p_row['name']}\n"
                        f"  Summary: {p_row['persona_summary'][:300]}\n"
                        f"  Demographics: {p_row['demographic_frame'][:200]}\n"
                        f"  Psychographics: {p_row['psychographic_profile'][:200]}\n"
                        f"  Context: {p_row['contextual_constraints'][:200]}\n"
                        f"  Behaviour: {p_row['behavioural_tendencies'][:200]}"
                    )

            personas_block = (
                "\n\n".join(persona_dossiers)
                if persona_dossiers
                else "No persona dossiers available."
            )

            if study_type == "synthetic_idi":
                format_instruction = (
                    "Format: Individual in-depth interviews. Generate a separate interview "
                    "for EACH persona. Each interview should have 8-12 exchanges between "
                    "Moderator and the respondent. The respondent must speak in character "
                    "based on their dossier. Use the persona's actual name as the speaker label."
                )
            else:
                format_instruction = (
                    "Format: Group discussion with ALL personas present simultaneously. "
                    "Generate 15-20 exchanges. The moderator guides the discussion. "
                    "Each persona speaks in character based on their dossier. "
                    "Show natural group dynamics: agreements, disagreements, building on each other's points. "
                    "Use each persona's actual name as their speaker label."
                )

            lisa_system = (
                "You are Lisa, a senior qualitative research analyst at Project Brainstorm. "
                "You generate realistic synthetic qualitative research output.\n\n"
                "You must produce TWO clearly labeled sections in this exact order:\n\n"
                "TRANSCRIPT:\n"
                "(the full simulated transcript)\n\n"
                "FIRST-PASS FINDINGS MEMO:\n"
                "Key themes\n"
                "(list the major themes that emerged)\n\n"
                "Strong vs exploratory signals\n"
                "(classify which findings are robust vs tentative)\n\n"
                "Contradictions/tensions\n"
                "(note any conflicting views or internal tensions)\n\n"
                "Candidate insights mapped to brief\n"
                "(map findings back to the research brief anchors)\n\n"
                "Supporting excerpts\n"
                "(quote 3-5 key verbatims from the transcript)\n\n"
                "Limitations/unknowns\n"
                "(note limitations of synthetic qualitative research and remaining unknowns)\n\n"
                "RULES:\n"
                "1. Keep respondent voices distinct and grounded in their persona dossiers.\n"
                "2. Do NOT output JSON. Output plain text with the two labeled sections.\n"
                "3. Be culturally grounded for Asia-Pacific markets where relevant.\n"
                "4. Findings memo should be analytical and concise, not just a summary."
            )

            lisa_user = (
                f"Study: {study_dict.get('title', 'Untitled Study')}\n"
                f"Type: {'In-Depth Interview (IDI)' if study_type == 'synthetic_idi' else 'Focus Group'}\n\n"
                f"Research Brief:\n{brief_text}\n"
                f"{format_instruction}\n\n"
                f"Personas:\n{personas_block}"
            )

            conn.close()
            raw_llm = call_llm(
                lisa_model_id,
                [
                    {"role": "system", "content": lisa_system},
                    {"role": "user", "content": lisa_user},
                ],
                purpose="lisa_qual_execution",
            )
            conn = get_db()

            if raw_llm and raw_llm.strip():
                output = raw_llm.strip()
                print(f"LISA_QUAL=LLM study_id={study_id}")
                app.logger.info(f"LISA_QUAL=LLM for study {study_id}")
            else:
                raise ValueError("LLM returned empty output")
        except Exception as e:
            print(f"LISA_QUAL=FALLBACK study_id={study_id} reason={e}")
            app.logger.warning(f"LISA_QUAL=FALLBACK for study {study_id}: {e}")
            output = None
            try:
                conn.execute("SELECT 1")
            except Exception:
                conn = get_db()

    if output is None:
        output = generate_placeholder_output(study_type, dict(study), persona_names)

    study_data = dict(study)
    study_data["study_output"] = output

    create_grounding_trace(
        conn,
        trigger_event="study_executed",
        study_id=str(study_id),
    )
    conn.commit()
    print(f"TRACE_CREATED study_id={study_id} trigger=study_executed", flush=True)

    qa_result = run_ben_qa(study_data)
    qa_decision = qa_result["decision"]
    qa_notes = qa_result["notes"]
    confidence_summary = json.dumps(qa_result["confidence_labels"])

    if qa_decision == "FAIL":
        final_status = "qa_blocked"
        final_report = None
    elif qa_decision == "DOWNGRADE":
        final_status = "completed"
        cl = qa_result["confidence_labels"]
        final_report = (
            f"=== QA REVIEW: DOWNGRADE ===\n"
            f"Confidence Labels — Strong: {cl['Strong']}, Indicative: {cl['Indicative']}, Exploratory: {cl['Exploratory']}\n"
            f"Note: {qa_notes}\n"
            f"{'=' * 40}\n\n"
            f"{output}"
        )
    else:
        final_status = "completed"
        cl = qa_result["confidence_labels"]
        final_report = (
            f"=== QA REVIEW: PASS ===\n"
            f"Confidence Labels — Strong: {cl['Strong']}, Indicative: {cl['Indicative']}, Exploratory: {cl['Exploratory']}\n"
            f"Note: {qa_notes}\n"
            f"{'=' * 40}\n\n"
            f"{output}"
        )

    model_call_count = 2
    tokens_total = max(1, len(output) // 4)
    tokens_mark = int(tokens_total * 0.10)
    tokens_lisa = int(tokens_total * 0.80)
    tokens_ben = tokens_total - tokens_mark - tokens_lisa

    ceiling = BUDGET_CEILINGS.get(study_type, 300_000)
    termination_reason = None
    if tokens_total > ceiling:
        final_status = "terminated_system"
        termination_reason = "budget_exhaustion"
        final_report = None
        qa_decision = "FAIL"
        qa_notes = (
            f"Budget ceiling exceeded: {tokens_total} tokens > {ceiling} ceiling."
        )

    conn.execute(
        """UPDATE studies SET status = ?, study_output = ?, qa_status = ?, qa_notes = ?,
           confidence_summary = ?, final_report = ? WHERE id = ?""",
        (
            final_status,
            output,
            qa_decision.lower(),
            qa_notes,
            confidence_summary,
            final_report,
            study_id,
        ),
    )

    conn.execute(
        """INSERT INTO cost_telemetry
           (study_id, study_type, tokens_mark, tokens_lisa, tokens_ben, tokens_total,
            model_call_count, qa_retry_count, followup_round_count, status, termination_reason)
           VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)""",
        (
            study_id,
            study_type,
            tokens_mark,
            tokens_lisa,
            tokens_ben,
            tokens_total,
            model_call_count,
            final_status,
            termination_reason,
        ),
    )

    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, view_output=study_id))


@app.route("/save-survey-config/<int:study_id>", methods=["POST"])
def save_survey_config(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Draft study not found.")
    if study["study_type"] != "synthetic_survey":
        conn.close()
        return render_error("Survey config only applies to synthetic survey studies.")

    try:
        r_count = int(request.form.get("respondent_count", 100))
    except (ValueError, TypeError):
        r_count = 100
    try:
        q_count = int(request.form.get("question_count", 8))
    except (ValueError, TypeError):
        q_count = 8

    r_count = max(1, min(400, r_count))
    q_count = max(1, min(12, q_count))

    conn.execute(
        "UPDATE studies SET respondent_count = ?, question_count = ? WHERE id = ?",
        (r_count, q_count, study_id),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


@app.route("/save-survey-questions/<int:study_id>", methods=["POST"])
def save_survey_questions(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Draft study not found.")
    if study["study_type"] != "synthetic_survey":
        conn.close()
        return render_error("Survey questions only apply to synthetic survey studies.")

    q_count = study["question_count"] or 8
    questions = []
    for i in range(1, q_count + 1):
        q = (request.form.get(f"survey_q_{i}") or "").strip()
        if q:
            questions.append(q)

    if len(questions) != q_count:
        conn.close()
        return render_error(
            f"You must provide exactly {q_count} questions (got {len(questions)})."
        )

    conn.execute(
        "UPDATE studies SET survey_questions = ? WHERE id = ?",
        (json.dumps(questions), study_id),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


@app.route("/save-remaining-anchors/<int:study_id>", methods=["POST"])
def save_remaining_anchors(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Draft study not found.")
    if study["study_type"] not in ("synthetic_idi", "synthetic_focus_group"):
        conn.close()
        return render_error(
            "Remaining anchors only apply to IDI or Focus Group studies."
        )

    mg = (request.form.get("market_geography") or "").strip()
    pc = (request.form.get("product_concept") or "").strip()
    ta = (request.form.get("target_audience") or "").strip()
    dui = (request.form.get("definition_useful_insight") or "").strip()

    missing = []
    if not mg:
        missing.append("Market / Geography")
    if not pc:
        missing.append("Product / Concept")
    if not ta:
        missing.append("Target Audience")
    if not dui:
        missing.append("Definition of Useful Insight")

    if missing:
        conn.close()
        return render_error("Please complete the remaining required items.")

    conn.execute(
        """UPDATE studies SET study_fit = ?, known_vs_unknown = ?,
           target_audience = ?, definition_useful_insight = ? WHERE id = ?""",
        (mg, pc, ta, dui, study_id),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


@app.route("/save-optional-context/<int:study_id>", methods=["POST"])
def save_optional_context(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    conn = get_db()
    try:
        study = conn.execute(
            "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
            (study_id, user["id"]),
        ).fetchone()
        if not study:
            conn.close()
            return render_error("Draft study not found.")

        existing_brief = {}
        if study["survey_brief"]:
            try:
                existing_brief = json.loads(study["survey_brief"])
            except (json.JSONDecodeError, TypeError):
                existing_brief = {}

        existing_brief["optional_context"] = {
            "competitive_context": (request.form.get("competitive_context") or "").strip(),
            "cultural_sensitivities": (request.form.get("cultural_sensitivities") or "").strip(),
            "adoption_barriers": (request.form.get("adoption_barriers") or "").strip(),
            "risk_tolerance": (request.form.get("risk_tolerance") or "").strip(),
        }

        conn.execute(
            "UPDATE studies SET survey_brief = ? WHERE id = ?",
            (json.dumps(existing_brief), study_id),
        )
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


@app.route("/attach-persona/<int:study_id>", methods=["POST"])
def attach_persona(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Draft study not found.")

    study_type = study["study_type"] or ""
    if study_type == "synthetic_survey":
        conn.close()
        return render_error("Surveys do not use inspectable personas.")

    instance_id = (request.form.get("persona_instance_id") or "").strip()
    if not instance_id:
        conn.close()
        return render_error("Persona is required.")

    persona_row = conn.execute(
        "SELECT id FROM personas WHERE persona_instance_id = ? AND user_id = ?",
        (instance_id, user["id"]),
    ).fetchone()
    if not persona_row:
        conn.close()
        return render_error("Persona not found.")

    current = normalize_personas_used(study["personas_used"])
    if instance_id in current:
        conn.close()
        return render_error("Persona is already attached to this study.")

    max_personas = {"synthetic_idi": 3, "synthetic_focus_group": 6}
    limit = max_personas.get(study_type)
    if limit and len(current) >= limit:
        conn.close()
        label = "IDI" if study_type == "synthetic_idi" else "Focus Group"
        return render_error(f"{label} allows max {limit} personas.")

    current.append(instance_id)
    conn.execute(
        "UPDATE studies SET personas_used = ? WHERE id = ?",
        (json.dumps(current), study_id),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


@app.route("/detach-persona/<int:study_id>", methods=["POST"])
def detach_persona(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return render_error("Draft study not found.")

    instance_id = (request.form.get("persona_instance_id") or "").strip()

    current = normalize_personas_used(study["personas_used"])
    current = [p for p in current if p != instance_id]
    conn.execute(
        "UPDATE studies SET personas_used = ? WHERE id = ?",
        (json.dumps(current), study_id),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


@app.route("/create-persona", methods=["POST"])
def create_persona():
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    name = (request.form.get("name") or "").strip()
    if not name:
        return render_error("Persona name is required.", show_new_persona=True)

    import random as _random

    conn = get_db()
    pool_models = [
        r["model_id"]
        for r in conn.execute(
            "SELECT model_id FROM persona_model_pool WHERE status = 'active'"
        ).fetchall()
    ]
    if not pool_models:
        conn.close()
        return render_error(
            "Cannot create persona: no active models in the persona model pool. An admin must configure at least one pool model.",
            show_new_persona=True,
        )

    dossier = {}
    for field_key, field_label in PERSONA_DOSSIER_FIELDS:
        val = (request.form.get(field_key) or "").strip()
        if not val:
            conn.close()
            return render_error(
                f'"{field_label}" is required. All dossier fields must be filled.',
                show_new_persona=True,
            )
        dossier[field_key] = val

    selected_model = _random.choice(pool_models)
    print(f"PERSONA_MODEL_SELECTED={selected_model}")
    provenance = f"{dossier['ai_model_provenance']} [model={selected_model}, selection_method=random from pool]"

    new_instance_id = f"P-{secrets.token_hex(4).upper()}"

    conn.execute(
        """INSERT INTO personas
           (user_id, persona_id, version, persona_instance_id, name,
            persona_summary, demographic_frame, psychographic_profile,
            contextual_constraints, behavioural_tendencies,
            ai_model_provenance, grounding_sources, confidence_and_limits)
           VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            user["id"],
            new_instance_id,
            new_instance_id,
            name,
            dossier["persona_summary"],
            dossier["demographic_frame"],
            dossier["psychographic_profile"],
            dossier["contextual_constraints"],
            dossier["behavioural_tendencies"],
            provenance,
            dossier["grounding_sources"],
            dossier["confidence_and_limits"],
        ),
    )
    create_grounding_trace(
        conn, trigger_event="persona_created", persona_id=new_instance_id
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, view_persona=new_instance_id))


@app.route("/delete-persona/<path:instance_id>", methods=["POST"])
def delete_persona(instance_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    conn = get_db()
    if persona_used_in_completed_study(conn, instance_id, user["id"]):
        conn.close()
        return render_error(
            f"Cannot delete persona {instance_id}: it is used in a completed study and is immutable."
        )

    remove_persona_from_non_completed_studies(conn, user["id"], instance_id)
    conn.execute(
        "DELETE FROM personas WHERE persona_instance_id = ? AND user_id = ?",
        (instance_id, user["id"]),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/set-model-config", methods=["POST"])
def admin_set_model_config():
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")
    conn = get_db()
    active_models = {
        r["model_id"]
        for r in conn.execute(
            "SELECT model_id FROM allowed_models WHERE status = 'active'"
        ).fetchall()
    }
    for key in ("mark_model", "lisa_model", "ben_model"):
        val = (request.form.get(key) or "").strip()
        if val:
            if val not in active_models:
                conn.close()
                return render_error(
                    f"Model '{val}' is not in the active allowed models list."
                )
            conn.execute(
                "INSERT INTO model_config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?",
                (key, val, val),
            )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/add-allowed-model", methods=["POST"])
def admin_add_allowed_model():
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")
    model_id = (request.form.get("model_id") or "").strip()
    source = (request.form.get("source") or "replit_openrouter").strip()
    if not model_id:
        return render_error("Model ID is required.")
    conn = get_db()
    existing = conn.execute(
        "SELECT id FROM allowed_models WHERE model_id = ?", (model_id,)
    ).fetchone()
    if existing:
        conn.close()
        return render_error(f"Model '{model_id}' already exists in allowed models.")
    conn.execute(
        "INSERT INTO allowed_models (model_id, source, status) VALUES (?, ?, 'active')",
        (model_id, source),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/toggle-allowed-model/<int:model_db_id>", methods=["POST"])
def admin_toggle_allowed_model(model_db_id):
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")
    conn = get_db()
    row = conn.execute(
        "SELECT status FROM allowed_models WHERE id = ?", (model_db_id,)
    ).fetchone()
    if row:
        new_status = "disabled" if row["status"] == "active" else "active"
        conn.execute(
            "UPDATE allowed_models SET status = ? WHERE id = ?",
            (new_status, model_db_id),
        )
        conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/delete-allowed-model/<int:model_db_id>", methods=["POST"])
def admin_delete_allowed_model(model_db_id):
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")
    conn = get_db()
    conn.execute("DELETE FROM allowed_models WHERE id = ?", (model_db_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/add-pool-model", methods=["POST"])
def admin_add_pool_model():
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")
    model_id = (request.form.get("model_id") or "").strip()
    if not model_id:
        return render_error("Model ID is required.")
    conn = get_db()
    allowed = conn.execute(
        "SELECT id FROM allowed_models WHERE model_id = ? AND status = 'active'",
        (model_id,),
    ).fetchone()
    if not allowed:
        conn.close()
        return render_error(
            f"Model '{model_id}' must be in the active allowed models list before adding to persona pool."
        )
    existing = conn.execute(
        "SELECT id FROM persona_model_pool WHERE model_id = ?", (model_id,)
    ).fetchone()
    if existing:
        conn.close()
        return render_error(f"Model '{model_id}' is already in the persona model pool.")
    conn.execute(
        "INSERT INTO persona_model_pool (model_id, status) VALUES (?, 'active')",
        (model_id,),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/toggle-pool-model/<int:pool_id>", methods=["POST"])
def admin_toggle_pool_model(pool_id):
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")
    conn = get_db()
    row = conn.execute(
        "SELECT status FROM persona_model_pool WHERE id = ?", (pool_id,)
    ).fetchone()
    if row:
        new_status = "disabled" if row["status"] == "active" else "active"
        conn.execute(
            "UPDATE persona_model_pool SET status = ? WHERE id = ?",
            (new_status, pool_id),
        )
        conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/remove-pool-model/<int:pool_id>", methods=["POST"])
def admin_remove_pool_model(pool_id):
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")
    conn = get_db()
    conn.execute("DELETE FROM persona_model_pool WHERE id = ?", (pool_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/import-openrouter-models", methods=["POST"])
def admin_import_openrouter_models():
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")
    import urllib.request as ur

    try:
        req = ur.Request(
            "https://openrouter.ai/api/v1/models",
            headers={"User-Agent": "ProjectBrainstorm/1.0"},
        )
        resp = ur.urlopen(req, timeout=10)
        data = json.loads(resp.read().decode())
        models = data.get("data", [])
        conn = get_db()
        added = 0
        for m in models:
            mid = m.get("id", "")
            if mid:
                existing = conn.execute(
                    "SELECT id FROM allowed_models WHERE model_id = ?", (mid,)
                ).fetchone()
                if not existing:
                    conn.execute(
                        "INSERT INTO allowed_models (model_id, source, status) VALUES (?, 'openrouter', 'active')",
                        (mid,),
                    )
                    added += 1
        conn.commit()
        conn.close()
        return redirect(url_for("index", token=token))
    except Exception:
        return render_error(
            "Failed to fetch models from OpenRouter API. Please use the OpenRouter Models catalog link to browse model IDs manually."
        )


@app.route("/admin/model-health/run", methods=["POST"])
def admin_model_health_run():
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")
    result = run_model_health_check("manual")
    if request.headers.get("Accept") == "application/json":
        return jsonify(result)
    return redirect(url_for("index", token=token))


@app.route("/admin/model-health/status", methods=["GET"])
def admin_model_health_status():
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")
    conn = get_db()
    rows = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM model_health_status ORDER BY model_id"
        ).fetchall()
    ]
    last_check = conn.execute(
        "SELECT * FROM model_health_checks ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    return jsonify(
        {
            "models": rows,
            "last_check": dict(last_check) if last_check else None,
        }
    )


@app.route("/admin/weekly-qa-report/latest", methods=["GET"])
def admin_weekly_qa_report_latest():
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM weekly_qa_reports ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    if row:
        return jsonify(dict(row))
    return jsonify({"message": "No weekly QA reports yet."})


@app.route("/admin/add-source", methods=["POST"])
def admin_add_source():
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")

    url = (request.form.get("url") or "").strip()
    name = (request.form.get("source_name") or "").strip()
    city = (request.form.get("city") or "").strip() or None
    country = (request.form.get("country") or "").strip() or None
    language = (request.form.get("language") or "").strip() or "en"

    if not url or not name:
        return render_error("URL and Source Name are required.")

    conn = get_db()
    conn.execute(
        "INSERT INTO admin_web_sources (url, name, city, country, language, status) VALUES (?, ?, ?, ?, ?, 'active')",
        (url, name, city, country, language),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/toggle-source/<int:source_id>", methods=["POST"])
def admin_toggle_source(source_id):
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")

    conn = get_db()
    row = conn.execute(
        "SELECT status FROM admin_web_sources WHERE id = ?", (source_id,)
    ).fetchone()
    if row:
        new_status = "disabled" if row["status"] == "active" else "active"
        conn.execute(
            "UPDATE admin_web_sources SET status = ? WHERE id = ?",
            (new_status, source_id),
        )
        conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


@app.route("/admin/delete-source/<int:source_id>", methods=["POST"])
def admin_delete_source(source_id):
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")

    conn = get_db()
    conn.execute("DELETE FROM admin_web_sources WHERE id = ?", (source_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


def render_error(message, show_new_research=False, show_new_persona=False):
    token = get_token()
    user, is_admin = get_session_data(token)
    if not user and not is_admin:
        referrer = request.referrer or ""
        if "/login" in referrer or request.path == "/login":
            show_tab = "login"
        elif "/admin-login" in referrer or request.path == "/admin-login":
            show_tab = "login"
        else:
            show_tab = "signup"
        return render_template(
            "landing.html",
            error=message,
            show_auth_tab=show_tab,
            latest_blog_posts=get_latest_blog_posts(2),
        )
    pending_users = []
    all_users = []
    studies = []
    personas_list = []
    configure_study = None
    if is_admin:
        conn = get_db()
        pending_users = [
            dict(r)
            for r in conn.execute(
                "SELECT id, email, username, state, created_at FROM users WHERE state = 'pending' ORDER BY created_at DESC"
            ).fetchall()
        ]
        all_users = [
            dict(r)
            for r in conn.execute(
                "SELECT id, email, username, state, created_at FROM users ORDER BY created_at DESC"
            ).fetchall()
        ]
        conn.close()
    if user and user["state"] == "active":
        conn = get_db()
        studies = [
            dict(r)
            for r in conn.execute(
                "SELECT id, title, study_type, status, created_at, study_output, qa_status, confidence_summary, final_report FROM studies WHERE user_id = ? ORDER BY created_at DESC",
                (user["id"],),
            ).fetchall()
        ]
        personas_list = get_user_personas_list(conn, user["id"])
        conn.close()
    if not show_new_persona:
        show_new_persona = request.args.get("new_persona") == "1"
    admin_web_sources = []
    grounding_traces = []
    cost_telemetry_rows = []
    model_config = {}
    allowed_models_list = []
    persona_pool_list = []
    if is_admin:
        conn2 = get_db()
        admin_web_sources = get_admin_web_sources(conn2)
        grounding_traces = [
            dict(r)
            for r in conn2.execute(
                "SELECT * FROM grounding_traces ORDER BY id DESC LIMIT 50"
            ).fetchall()
        ]
        cost_telemetry_rows = [
            dict(r)
            for r in conn2.execute(
                "SELECT * FROM cost_telemetry ORDER BY id DESC LIMIT 50"
            ).fetchall()
        ]
        for row in conn2.execute("SELECT key, value FROM model_config").fetchall():
            model_config[row["key"]] = row["value"]
        allowed_models_list = [
            dict(r)
            for r in conn2.execute(
                "SELECT * FROM allowed_models ORDER BY model_id"
            ).fetchall()
        ]
        persona_pool_list = [
            dict(r)
            for r in conn2.execute(
                "SELECT * FROM persona_model_pool ORDER BY model_id"
            ).fetchall()
        ]
        conn2.close()
    all_blog_posts_err = []
    if is_admin:
        conn3 = get_db()
        all_blog_posts_err = [
            dict(r)
            for r in conn3.execute(
                "SELECT * FROM blog_posts ORDER BY is_pinned DESC, pinned_rank ASC, created_at DESC, id DESC"
            ).fetchall()
        ]
        conn3.close()
    return render_template(
        "index.html",
        user=user,
        is_admin=is_admin,
        pending_users=pending_users,
        all_users=all_users,
        studies=studies,
        token=token,
        error=message,
        new_research_step=None,
        new_research_type="",
        configure_study=configure_study,
        configure_study_personas=[],
        available_personas=[],
        study_type_limits=STUDY_TYPE_LIMITS,
        personas_list=personas_list,
        show_new_persona=show_new_persona,
        clone_source=None,
        view_persona=None,
        view_study_output=None,
        admin_web_sources=admin_web_sources,
        grounding_traces=grounding_traces,
        cost_telemetry_rows=cost_telemetry_rows,
        model_config=model_config,
        allowed_models_list=allowed_models_list,
        persona_pool_list=persona_pool_list,
        health_status_list=[],
        latest_weekly_report=None,
        latest_health_check=None,
        mark_recommendation="",
        mark_recommendation_label="",
        mark_recommendation_reason="",
        chat_messages=[],
        chat_save_buttons=[],
        proposed_update=None,
        studies_page=1,
        studies_q="",
        studies_total_pages=1,
        studies_total=0,
        personas_page=1,
        personas_q="",
        personas_total_pages=1,
        personas_total=0,
        usage_count=0,
        usage_limit=FREE_TIER_MONTHLY_LIMIT,
        usage_limit_reached=False,
        usage_window_start="",
        usage_window_end="",
        study_uploads=[],
        study_uploads_total_size=0,
        study_attachable_docs=[],
        upload_max_files=UPLOAD_MAX_FILES_PER_STUDY,
        upload_max_file_size_mb=UPLOAD_MAX_FILE_SIZE // (1024 * 1024),
        upload_max_total_mb=UPLOAD_MAX_TOTAL_PER_STUDY // (1024 * 1024),
        upload_allowed_extensions=", ".join(sorted(UPLOAD_ALLOWED_EXTENSIONS)),
        admin_uploads_list=[],
        all_blog_posts=all_blog_posts_err,
        docs_list=[],
        docs_page=1,
        docs_q="",
        docs_total_pages=1,
        docs_total=0,
        user_storage_used=0,
        user_storage_cap_mb=UPLOAD_USER_STORAGE_CAP // (1024 * 1024),
        user_storage_cap=UPLOAD_USER_STORAGE_CAP,
        latest_blog_posts=get_latest_blog_posts(2),
    )


@app.route("/admin/dev-run-study/<int:study_id>", methods=["POST"])
def admin_dev_run_study(study_id):
    token = get_token()
    admin_token = request.form.get("admin_token")
    if admin_token != os.environ.get("ADMIN_PASSWORD", "admin123"):
        return render_error("Admin access required.")

    conn = get_db()
    study = conn.execute("SELECT * FROM studies WHERE id = ?", (study_id,)).fetchone()
    if not study:
        conn.close()
        return render_error("Study not found.")

    output = "NON_PLACEHOLDER_OUTPUT\n\nThis is a real research output for QA PASS testing.\n\nInsight 1: Users prefer mobile experiences.\nInsight 2: Price sensitivity varies by age group.\nInsight 3: Brand loyalty is declining."

    study_data = dict(study)
    study_data["study_output"] = output

    create_grounding_trace(conn, trigger_event="study_executed", study_id=str(study_id))
    conn.commit()
    print(f"TRACE_CREATED study_id={study_id} trigger=study_executed", flush=True)

    qa_result = run_ben_qa(study_data)
    qa_decision = qa_result["decision"]
    qa_notes = qa_result["notes"]
    confidence_summary = json.dumps(qa_result["confidence_labels"])

    if qa_decision == "FAIL":
        final_status = "qa_blocked"
        final_report = None
    else:
        final_status = "completed"
        cl = qa_result["confidence_labels"]
        final_report = (
            f"=== QA REVIEW: {qa_decision} ===\n"
            f"Confidence Labels — Strong: {cl['Strong']}, Indicative: {cl['Indicative']}, Exploratory: {cl['Exploratory']}\n"
            f"Note: {qa_notes}\n"
            f"{'=' * 40}\n\n"
            f"{output}"
        )

    study_type = study["study_type"] or ""
    model_call_count = 2
    tokens_total = max(1, len(output) // 4)
    force_tokens = request.form.get("force_tokens_total")
    if force_tokens:
        try:
            tokens_total = int(force_tokens)
        except (ValueError, TypeError):
            pass
    tokens_mark = int(tokens_total * 0.10)
    tokens_lisa = int(tokens_total * 0.80)
    tokens_ben = tokens_total - tokens_mark - tokens_lisa

    ceiling = BUDGET_CEILINGS.get(study_type, 300_000)
    termination_reason = None
    if tokens_total > ceiling:
        final_status = "terminated_system"
        termination_reason = "budget_exhaustion"
        final_report = None
        qa_decision = "FAIL"
        qa_notes = (
            f"Budget ceiling exceeded: {tokens_total} tokens > {ceiling} ceiling."
        )

    conn.execute(
        """UPDATE studies SET status = ?, study_output = ?, qa_status = ?, qa_notes = ?,
           confidence_summary = ?, final_report = ? WHERE id = ?""",
        (
            final_status,
            output,
            qa_decision.lower(),
            qa_notes,
            confidence_summary,
            final_report,
            study_id,
        ),
    )

    conn.execute(
        """INSERT INTO cost_telemetry
           (study_id, study_type, tokens_mark, tokens_lisa, tokens_ben, tokens_total,
            model_call_count, qa_retry_count, followup_round_count, status, termination_reason)
           VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)""",
        (
            study_id,
            study_type,
            tokens_mark,
            tokens_lisa,
            tokens_ben,
            tokens_total,
            model_call_count,
            final_status,
            termination_reason,
        ),
    )

    conn.commit()
    conn.close()

    user, _ = get_session_data(token)
    if user:
        return redirect(url_for("index", token=token, view_output=study_id))
    return f"Study {study_id} executed with QA result: {qa_decision}"


@app.route("/admin/dev-inject-invalid-qual-study", methods=["POST"])
def admin_dev_inject_invalid_qual_study():
    admin_token = request.form.get("admin_token")
    if admin_token != os.environ.get("ADMIN_PASSWORD", "admin123"):
        return render_error("Admin access required.")

    study_type = request.form.get("study_type", "synthetic_idi")
    blank_field = request.form.get("blank_field", "business_problem")

    if study_type not in ("synthetic_idi", "synthetic_focus_group"):
        return render_error("Only synthetic_idi or synthetic_focus_group allowed.")

    anchors = {
        "business_problem": "Test BP",
        "decision_to_support": "Test DS",
        "known_vs_unknown": "Test KU",
        "target_audience": "Test TA",
        "study_fit": "Test SF",
        "definition_useful_insight": "Test DUI",
    }
    if blank_field in anchors:
        anchors[blank_field] = ""

    conn = get_db()
    admin_user = conn.execute("SELECT id FROM users LIMIT 1").fetchone()
    if not admin_user:
        conn.close()
        return render_error("No users in database.")

    conn.execute(
        """INSERT INTO studies (user_id, title, study_type, status,
           business_problem, decision_to_support, known_vs_unknown,
           target_audience, study_fit, definition_useful_insight, personas_used)
           VALUES (?, ?, ?, 'draft', ?, ?, ?, ?, ?, ?, '[]')""",
        (
            admin_user["id"],
            f"DEV_INVALID_{study_type}_{blank_field}",
            study_type,
            anchors["business_problem"],
            anchors["decision_to_support"],
            anchors["known_vs_unknown"],
            anchors["target_audience"],
            anchors["study_fit"],
            anchors["definition_useful_insight"],
        ),
    )
    conn.commit()
    study_id = conn.execute(
        "SELECT id FROM studies ORDER BY id DESC LIMIT 1"
    ).fetchone()["id"]
    conn.close()
    return f"DEV ONLY: Created invalid {study_type} study id={study_id} with blank field '{blank_field}'. Use /admin/dev-run-study/{study_id} to test QA."


@app.route("/admin/dev-cjk-pdf-test")
def admin_dev_cjk_pdf_test():
    token = get_token()
    user, is_admin = get_session_data(token)
    if not is_admin:
        return "Admin access required.", 403
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()
    cjk_ok = _register_cjk_fonts(pdf)
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 12, "CJK Font Rendering Test", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(8)
    test_lines = [
        ("Simplified Chinese", "简体中文：汉语、欢迎、测试"),
        ("Traditional Chinese", "繁體中文：漢語、歡迎、測試"),
        ("Japanese", "日本語：こんにちは世界、自転車"),
    ]
    for label, text in test_lines:
        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(0, 8, f"{label}:", new_x="LMARGIN", new_y="NEXT")
        _pdf_write_text(pdf, text, 12, cjk_available=cjk_ok, h=10)
        pdf.ln(4)
    pdf.ln(8)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(
        0,
        8,
        f"CJK fonts loaded: {'Yes' if cjk_ok else 'No'}",
        new_x="LMARGIN",
        new_y="NEXT",
    )
    pdf_bytes = pdf.output()
    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name="cjk_font_test.pdf",
    )


@app.route("/admin/export/studies.csv")
def admin_export_studies_csv():
    token = get_token()
    user, is_admin = get_session_data(token)
    if not is_admin:
        return "Admin access required.", 403
    import csv, io

    conn = get_db()
    rows = conn.execute("SELECT * FROM studies ORDER BY id").fetchall()
    cols = (
        [desc[0] for desc in conn.execute("SELECT * FROM studies LIMIT 1").description]
        if rows
        else []
    )
    conn.close()
    si = io.StringIO()
    w = csv.writer(si)
    if cols:
        w.writerow(cols)
    for r in rows:
        w.writerow([r[c] for c in cols])
    resp = app.make_response(si.getvalue())
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = "attachment; filename=studies.csv"
    return resp


@app.route("/admin/export/cost_telemetry.csv")
def admin_export_cost_telemetry_csv():
    token = get_token()
    user, is_admin = get_session_data(token)
    if not is_admin:
        return "Admin access required.", 403
    import csv, io

    conn = get_db()
    rows = conn.execute("SELECT * FROM cost_telemetry ORDER BY id").fetchall()
    cols = (
        [
            desc[0]
            for desc in conn.execute("SELECT * FROM cost_telemetry LIMIT 1").description
        ]
        if rows
        else []
    )
    conn.close()
    si = io.StringIO()
    w = csv.writer(si)
    if cols:
        w.writerow(cols)
    for r in rows:
        w.writerow([r[c] for c in cols])
    resp = app.make_response(si.getvalue())
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = "attachment; filename=cost_telemetry.csv"
    return resp


@app.route("/admin/export/grounding_traces.csv")
def admin_export_grounding_traces_csv():
    token = get_token()
    user, is_admin = get_session_data(token)
    if not is_admin:
        return "Admin access required.", 403
    import csv, io

    conn = get_db()
    rows = conn.execute("SELECT * FROM grounding_traces ORDER BY id").fetchall()
    cols = (
        [
            desc[0]
            for desc in conn.execute(
                "SELECT * FROM grounding_traces LIMIT 1"
            ).description
        ]
        if rows
        else []
    )
    conn.close()
    si = io.StringIO()
    w = csv.writer(si)
    if cols:
        w.writerow(cols)
    for r in rows:
        w.writerow([r[c] for c in cols])
    resp = app.make_response(si.getvalue())
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = "attachment; filename=grounding_traces.csv"
    return resp


_verification_codes = {}


@app.route("/verify-email", methods=["GET", "POST"])
def verify_email():
    token = get_token()
    user, is_admin = get_session_data(token)
    if not user or user["state"] != "active":
        return redirect(url_for("index"))

    user_id = user["id"]
    if request.method == "POST":
        entered = (request.form.get("code") or "").strip()
        expected = _verification_codes.get(user_id)
        if expected and entered == expected:
            conn = get_db()
            now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            conn.execute(
                "UPDATE users SET last_email_verification_timestamp = ? WHERE id = ?",
                (now_str, user_id),
            )
            conn.commit()
            conn.close()
            _verification_codes.pop(user_id, None)
            return redirect(url_for("index", token=token))
        return render_template(
            "verify_email.html",
            token=token,
            user=user,
            code=_verification_codes.get(user_id, ""),
            error="Invalid code. Please try again.",
        )

    import random

    code = str(random.randint(100000, 999999))
    _verification_codes[user_id] = code
    return render_template(
        "verify_email.html",
        token=token,
        user=user,
        code=code,
        error=None,
    )


@app.route("/account", methods=["GET", "POST"])
def manage_account():
    token = get_token()
    user, is_admin = get_session_data(token)
    if not user or user["state"] != "active":
        return redirect(url_for("index"))

    success_msg = None
    error_msg = None

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        company = (request.form.get("company") or "").strip()
        role = (request.form.get("role") or "").strip()
        location = (request.form.get("location") or "").strip()
        linkedin = (request.form.get("linkedin") or "").strip()

        conn = get_db()
        conn.execute(
            "UPDATE users SET name=?, company=?, role=?, location=?, linkedin=? WHERE id=?",
            (name, company, role, location, linkedin, user["id"]),
        )
        conn.commit()
        user = dict(
            conn.execute("SELECT * FROM users WHERE id = ?", (user["id"],)).fetchone()
        )
        conn.close()
        success_msg = "Profile updated successfully."

    return render_template(
        "account.html",
        token=token,
        user=user,
        success=success_msg,
        error=error_msg,
    )


@app.route("/change-password", methods=["POST"])
def change_password():
    token = get_token()
    user, is_admin = get_session_data(token)
    if not user or user["state"] != "active":
        return redirect(url_for("index"))

    current_pw = request.form.get("current_password") or ""
    new_pw = request.form.get("new_password") or ""

    if not check_password_hash(user["password_hash"], current_pw):
        return render_template(
            "account.html",
            token=token,
            user=user,
            success=None,
            error="Current password is incorrect.",
        )
    if len(new_pw) < 6 or len(new_pw) > 10:
        return render_template(
            "account.html",
            token=token,
            user=user,
            success=None,
            error="New password must be 6–10 characters.",
        )

    conn = get_db()
    conn.execute(
        "UPDATE users SET password_hash = ? WHERE id = ?",
        (generate_password_hash(new_pw), user["id"]),
    )
    conn.commit()
    conn.close()
    return render_template(
        "account.html",
        token=token,
        user=user,
        success="Password changed successfully.",
        error=None,
    )


@app.route("/admin/llm-smoke", methods=["POST"])
def admin_llm_smoke():
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return jsonify({"error": "Admin access required"}), 403

    model_id = request.args.get("model_id") or request.form.get("model_id") or ""
    if not model_id:
        return jsonify({"error": "model_id is required"}), 400

    try:
        result = call_llm(
            model_id,
            [{"role": "user", "content": "Reply with the single word OK"}],
            purpose="smoke_test",
        )
        return jsonify({"model_id": model_id, "status": "ok", "response": result[:500]})
    except Exception as e:
        return jsonify({"model_id": model_id, "status": "error", "error": str(e)[:500]})


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)
