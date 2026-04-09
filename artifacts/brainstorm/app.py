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
import sys
import secrets
import sqlite3
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

import re as _re_mod
from markupsafe import Markup

@app.template_filter("format_report_text")
def _format_report_text(text):
    if not text:
        return Markup("")
    lines = text.split("\n")
    html_parts = []
    in_list = False
    for line in lines:
        stripped = line.rstrip()
        is_bullet = stripped.lstrip().startswith("- ") or stripped.lstrip().startswith("• ") or stripped.lstrip().startswith("✓ ") or stripped.lstrip().startswith("✗ ")
        is_indented_bullet = len(stripped) > len(stripped.lstrip()) and is_bullet
        if is_bullet:
            if not in_list:
                html_parts.append("<ul class='report-list'>")
                in_list = True
            bullet_text = stripped.lstrip()
            for prefix in ["- ", "• ", "✓ ", "✗ "]:
                if bullet_text.startswith(prefix):
                    bullet_text = bullet_text[len(prefix):]
                    break
            if stripped.lstrip().startswith("✓"):
                html_parts.append(f"<li class='report-check'>✓ {Markup.escape(bullet_text)}</li>")
            elif stripped.lstrip().startswith("✗"):
                html_parts.append(f"<li class='report-cross'>✗ {Markup.escape(bullet_text)}</li>")
            else:
                html_parts.append(f"<li>{Markup.escape(bullet_text)}</li>")
        else:
            if in_list:
                html_parts.append("</ul>")
                in_list = False
            if not stripped:
                html_parts.append("<div class='report-spacer'></div>")
            elif stripped.endswith(":") and len(stripped) < 80:
                html_parts.append(f"<p class='report-subhead'>{Markup.escape(stripped)}</p>")
            elif _re_mod.match(r'^Finding \d+', stripped):
                html_parts.append(f"<p class='report-finding'>{Markup.escape(stripped)}</p>")
            elif stripped.startswith("Confidence:") or stripped.lstrip().startswith("Confidence:"):
                html_parts.append(f"<p class='report-confidence'>{Markup.escape(stripped)}</p>")
            else:
                html_parts.append(f"<p>{Markup.escape(stripped)}</p>")
    if in_list:
        html_parts.append("</ul>")
    return Markup("\n".join(html_parts))


@app.route("/__health")
def __health():
    return jsonify({"status": "ok", "service": "brainstorm_flask"})


VALID_LANGS = {"en", "zh-Hans", "zh-Hant", "ja", "ko", "th"}


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
    "study_status",
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
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "")

DM_FILE = os.path.join(os.path.dirname(__file__), "data", "messages.json")
DM_PAGE_SIZE = 5


def _load_dm_messages():
    try:
        with open(DM_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save_dm_messages(msgs):
    os.makedirs(os.path.dirname(DM_FILE), exist_ok=True)
    with open(DM_FILE, "w") as f:
        json.dump(msgs, f, indent=2)


def _dm_unread_count(user_id=None, is_admin=False):
    msgs = _load_dm_messages()
    if is_admin:
        return sum(1 for m in msgs if m.get("recipient_type") == "admin" and not m.get("read"))
    if user_id:
        return sum(1 for m in msgs if m.get("recipient_user_id") == user_id and m.get("recipient_type") == "user" and not m.get("read"))
    return 0


def _normalize_subject(subj):
    s = (subj or "").strip()
    while s.lower().startswith("re: "):
        s = s[4:].strip()
    return s


def _dm_inbox(user_id=None, is_admin=False, page=1):
    msgs = _load_dm_messages()
    relevant = []
    if is_admin:
        for m in msgs:
            if m.get("recipient_type") == "admin" or m.get("sender_type") == "admin":
                relevant.append(m)
    elif user_id:
        for m in msgs:
            if (m.get("recipient_user_id") == user_id and m.get("recipient_type") == "user"):
                relevant.append(m)
            elif (m.get("sender_type") == "user" and m.get("sender_id") == user_id):
                relevant.append(m)
    threads = {}
    for m in relevant:
        norm = _normalize_subject(m.get("subject", "")).lower()
        if is_admin:
            tuser = m.get("sender_id") if m.get("sender_type") == "user" else m.get("recipient_user_id")
            tkey = (norm, tuser)
        else:
            tkey = norm
        if tkey not in threads:
            threads[tkey] = {"subject": _normalize_subject(m.get("subject", "")) or "(no subject)", "messages": [], "latest_ts": "", "unread_count": 0, "user_name": "", "category": ""}
        threads[tkey]["messages"].append(m)
        ts = m.get("timestamp", "")
        if ts > threads[tkey]["latest_ts"]:
            threads[tkey]["latest_ts"] = ts
        if not m.get("read", True):
            if is_admin and m.get("recipient_type") == "admin":
                threads[tkey]["unread_count"] += 1
            elif not is_admin and m.get("recipient_type") == "user" and m.get("recipient_user_id") == user_id:
                threads[tkey]["unread_count"] += 1
        if m.get("sender_type") == "user" and m.get("sender_name"):
            threads[tkey]["user_name"] = m["sender_name"]
        if m.get("category") and not threads[tkey]["category"]:
            threads[tkey]["category"] = m["category"]
    for t in threads.values():
        t["messages"].sort(key=lambda x: x.get("timestamp", ""))
    thread_list = sorted(threads.values(), key=lambda t: t["latest_ts"], reverse=True)
    total = len(thread_list)
    total_pages = max(1, (total + DM_PAGE_SIZE - 1) // DM_PAGE_SIZE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * DM_PAGE_SIZE
    return thread_list[start:start + DM_PAGE_SIZE], page, total_pages, total


def _dm_latest_preview(user_id=None, is_admin=False):
    msgs = _load_dm_messages()
    if is_admin:
        filtered = [m for m in msgs if m.get("recipient_type") == "admin"]
    elif user_id:
        filtered = [m for m in msgs if m.get("recipient_user_id") == user_id and m.get("recipient_type") == "user"]
    else:
        return None
    filtered.sort(key=lambda m: m.get("timestamp", ""), reverse=True)
    return filtered[0] if filtered else None


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
TEST_USER_EMAIL = "test@admin.local"
TEST_USER_PASSWORD = "test123"
UNLIMITED_USER_IDS = set()

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
SURVEY_IMAGES_DIR = os.path.join(USER_UPLOADS_DIR, "survey_images")
os.makedirs(USER_UPLOADS_DIR, exist_ok=True)
os.makedirs(ADMIN_UPLOADS_DIR, exist_ok=True)
os.makedirs(SURVEY_IMAGES_DIR, exist_ok=True)

BLOG_IMAGE_MAX_SIZE = 300 * 1024
BLOG_IMAGE_ALLOWED = {"png", "jpg", "jpeg"}
BLOG_STATIC_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "static", "blog"
)
os.makedirs(BLOG_STATIC_DIR, exist_ok=True)


def call_llm(model_id, messages, purpose="", timeout_seconds=60):
    """Single wrapper for all LLM calls via Replit AI Integrations (OpenRouter)."""
    import openai as _openai
    import concurrent.futures

    base_url = os.environ.get("AI_INTEGRATIONS_OPENROUTER_BASE_URL")
    api_key = os.environ.get("AI_INTEGRATIONS_OPENROUTER_API_KEY")
    if not base_url or not api_key:
        raise NotImplementedError("LLM integration not connected yet")

    client = _openai.OpenAI(base_url=base_url, api_key=api_key, timeout=timeout_seconds)

    def _do_call():
        return client.chat.completions.create(
            model=model_id,
            messages=messages,
            max_tokens=8192,
        )

    wall_clock_limit = timeout_seconds + 15
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_do_call)
            resp = future.result(timeout=wall_clock_limit)
        return resp.choices[0].message.content or ""
    except concurrent.futures.TimeoutError:
        raise RuntimeError(f"LLM timeout for {model_id}: Wall-clock limit ({wall_clock_limit}s) exceeded")
    except _openai.APITimeoutError as e:
        raise RuntimeError(f"LLM timeout for {model_id}: {str(e)[:200]}")
    except _openai.APIError as e:
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
                    }
                    continue
                try:
                    result = call_llm(
                        mid,
                        [{"role": "user", "content": "Reply with the single word OK"}],
                        purpose="health_check",
                    )
                    if "ok" in (result or "").lower():
                        per_model[mid] = {"status": "pass", "error": None}
                    else:
                        per_model[mid] = {
                            "status": "fail",
                            "error": f"Unexpected response: {(result or '')[:300]}",
                        }
                except Exception as e:
                    per_model[mid] = {"status": "fail", "error": str(e)[:300]}

            if not config_valid:
                summary_status = "fail"
            elif any(v["status"] == "fail" for v in per_model.values()):
                summary_status = "fail"
            else:
                summary_status = "pass"

        conn = get_db()
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

        finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        details = {
            "config_errors": config_errors,
            "per_model": per_model,
            "models_checked": list(all_model_ids),
        }

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


def _get_healthy_pool_models(conn):
    pool_rows = conn.execute(
        "SELECT model_id FROM persona_model_pool WHERE status = 'active'"
    ).fetchall()
    non_gpt = [r["model_id"] for r in pool_rows if not is_gpt_family(r["model_id"])]

    if not non_gpt:
        return [], [], "no_eligible"

    health_rows = conn.execute(
        "SELECT model_id, status FROM model_health_status"
    ).fetchall()
    fail_set = {r["model_id"] for r in health_rows if r["status"] == "fail"}

    healthy = [m for m in non_gpt if m not in fail_set]
    excluded_fail = [m for m in non_gpt if m in fail_set]

    if not healthy and excluded_fail:
        return [], excluded_fail, "all_fail"

    return healthy, excluded_fail, "ok"


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
    dui_val = (study_dict.get("definition_useful_insight") or "").strip()
    if dui_val:
        oc["definition_useful_insight"] = dui_val
    fields = [
        ("definition_useful_insight", "Definition of Useful Insight"),
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


_PERSONA_REQUIRED_FIELDS = [
    "name", "persona_summary", "demographic_frame", "psychographic_profile",
    "contextual_constraints", "behavioural_tendencies", "grounding_sources",
    "confidence_and_limits",
]


def _safe_parse_persona_json(raw, study_id, model_id):
    if not raw or not raw.strip():
        raise ValueError("LLM returned empty response for persona generation")

    cleaned = raw.strip()
    if cleaned.startswith("```"):
        first_nl = cleaned.find("\n")
        if first_nl != -1:
            cleaned = cleaned[first_nl + 1:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    first_brace = cleaned.find("{")
    last_brace = cleaned.rfind("}")
    if first_brace != -1 and last_brace > first_brace:
        subset = cleaned[first_brace:last_brace + 1]
        try:
            return json.loads(subset)
        except json.JSONDecodeError:
            pass

        import re
        repaired = re.sub(r',\s*([\]}])', r'\1', subset)
        repaired = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', repaired)
        try:
            return json.loads(repaired)
        except json.JSONDecodeError as e:
            excerpt = subset[:200] if len(subset) > 200 else subset
            print(
                f"PERSONA_JSON_PARSE_FAIL study={study_id} model={model_id} "
                f"raw_len={len(raw)} excerpt={repr(excerpt)} err={e}",
                flush=True,
            )
            raise ValueError(f"Persona JSON parse failed after fallback: {e}")

    raise ValueError("No JSON object found in persona generation response")


def _validate_persona_list(parsed, n_required):
    if not isinstance(parsed, dict) or "personas" not in parsed:
        raise ValueError("Parsed output missing 'personas' key")
    personas = parsed["personas"]
    if not isinstance(personas, list) or len(personas) < 1:
        raise ValueError("'personas' is not a non-empty list")
    for i, p in enumerate(personas):
        if not isinstance(p, dict):
            raise ValueError(f"Persona {i} is not a dict")
        missing = [f for f in _PERSONA_REQUIRED_FIELDS if not p.get(f)]
        if missing:
            raise ValueError(f"Persona {i} missing fields: {', '.join(missing)}")
    if len(personas) < n_required:
        raise ValueError(f"Need {n_required} personas, got {len(personas)}")
    return personas


MLG_MAX_SNIPPETS_PER_TIER = 3
MLG_WEB_FETCH_TIMEOUT = 8
MLG_SEARCH_RESULTS_LIMIT = 5
MLG_SYNTHESIZER_MODEL = "google/gemini-2.0-flash-001"
MLG_MAX_SUMMARY_WORDS = 400
MLG_MAX_SUMMARY_CHARS = 3200
MLG_MIN_RELEVANCE_KEYWORDS = 1
MLG_MAX_SOURCES_SHOWN = 3
MLG_MAX_WIKIPEDIA_PER_BUNDLE = 1

_MLG_WIKIPEDIA_DOMAINS = {"wikipedia.org", "en.wikipedia.org", "ja.wikipedia.org",
    "zh.wikipedia.org", "ko.wikipedia.org", "th.wikipedia.org",
    "en.m.wikipedia.org", "ja.m.wikipedia.org", "zh.m.wikipedia.org",
    "ko.m.wikipedia.org", "th.m.wikipedia.org"}

_MLG_SECONDARY_AUTHORITATIVE_DOMAINS = {
    ".gov", ".gov.hk", ".gov.sg", ".gov.au", ".gov.uk", ".gov.jp", ".go.jp", ".go.th",
    ".edu", ".edu.hk", ".edu.sg", ".edu.au", ".ac.uk", ".ac.jp",
    ".int",
    "worldbank.org", "data.worldbank.org",
    "un.org", "unctad.org", "undp.org",
    "oecd.org", "oecd-ilibrary.org",
    "who.int", "imf.org",
    "stat.go.jp", "e-stat.go.jp",
    "census.gov", "bls.gov", "bea.gov",
    "ons.gov.uk", "abs.gov.au",
}


def _mlg_is_wikipedia(url):
    if not url:
        return False
    try:
        from urllib.parse import urlparse
        host = (urlparse(url).hostname or "").lower()
        return host in _MLG_WIKIPEDIA_DOMAINS or host.endswith(".wikipedia.org")
    except Exception:
        return "wikipedia.org" in (url or "").lower()


def _mlg_is_secondary_authoritative(url):
    if not url:
        return False
    try:
        from urllib.parse import urlparse
        host = (urlparse(url).hostname or "").lower()
        for dom in _MLG_SECONDARY_AUTHORITATIVE_DOMAINS:
            if dom.startswith("."):
                if host.endswith(dom):
                    return True
            else:
                if host == dom or host.endswith("." + dom):
                    return True
    except Exception:
        pass
    return False


def _mlg_tier1_user_uploads(conn, study_id, user_id):
    rows = conn.execute(
        """SELECT u.id, u.filename, u.retained_excerpt_text
           FROM study_documents sd
           JOIN user_uploads u ON u.id = sd.user_doc_id
           WHERE sd.study_id = ? AND u.user_id = ? AND u.status = 'active'""",
        (study_id, user_id),
    ).fetchall()
    sources = []
    snippets = []
    for r in rows:
        excerpt = (r["retained_excerpt_text"] or "").strip()
        if excerpt and len(excerpt) > 30:
            sources.append({
                "name": r["filename"],
                "url": "",
                "category": "Uploaded Document",
                "origin": "Uploaded Document",
            })
            snippets.append(f"[User Doc: {r['filename']}] {excerpt[:500]}")
    return sources, snippets


def _mlg_tier2_admin_uploads(conn):
    rows = conn.execute(
        "SELECT id, filename, storage_path FROM admin_uploads WHERE status = 'active'"
    ).fetchall()
    sources = []
    snippets = []
    for r in rows:
        storage_path = (r["storage_path"] or "").strip()
        excerpt = ""
        if storage_path and os.path.exists(storage_path):
            try:
                with open(storage_path, "r", errors="ignore") as f:
                    excerpt = f.read(2000).strip()
            except Exception:
                pass
        if excerpt and len(excerpt) > 30:
            sources.append({
                "name": r["filename"],
                "url": "",
                "category": "Uploaded Document",
                "origin": "Uploaded Document",
            })
            snippets.append(f"[Admin Doc: {r['filename']}] {excerpt[:500]}")
    return sources, snippets


def _mlg_build_relevance_keywords(study_dict):
    parts = []
    for key in ("target_audience", "study_fit"):
        val = (study_dict.get(key) or "").strip()
        if val:
            for word in val.lower().split():
                word = word.strip(".,;:!?\"'()[]{}")
                if len(word) >= 3 and word not in ("the", "and", "for", "with", "that", "this", "from", "are", "was"):
                    parts.append(word)
    return list(set(parts))


def _mlg_is_relevant(text, keywords, min_hits=MLG_MIN_RELEVANCE_KEYWORDS):
    if not keywords:
        return True
    text_lower = text.lower()
    hits = sum(1 for kw in keywords if kw in text_lower)
    return hits >= min_hits


def _mlg_is_safe_url(url):
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        host = (parsed.hostname or "").lower()
        if not host:
            return False
        blocked = ("localhost", "127.0.0.1", "0.0.0.0", "169.254.169.254", "[::1]", "metadata.google.internal")
        for b in blocked:
            if host == b or host.endswith("." + b):
                return False
        if host.startswith("10.") or host.startswith("192.168.") or host.startswith("172."):
            return False
        return True
    except Exception:
        return False


def _mlg_tier3_admin_web(admin_web_sources, relevance_keywords=None):
    import requests as _req
    import re as _re

    sources = []
    snippets = []
    attempted = 0
    matched = 0
    for src in admin_web_sources[:MLG_MAX_SNIPPETS_PER_TIER]:
        url = (src.get("url") or "").strip()
        name = src.get("name") or url
        if not url:
            continue
        if not _mlg_is_safe_url(url):
            print(f"MLG_TIER3_BLOCKED_URL url={url}", flush=True)
            continue
        attempted += 1
        try:
            resp = _req.get(url, timeout=MLG_WEB_FETCH_TIMEOUT, headers={"User-Agent": "ProjectBrainstorm/1.0"}, allow_redirects=False)
            resp.raise_for_status()
            text = resp.text[:3000]
            text = _re.sub(r"<[^>]+>", " ", text)
            text = _re.sub(r"\s+", " ", text).strip()
            if len(text) > 50 and _mlg_is_relevant(text, relevance_keywords):
                matched += 1
                sources.append({
                    "name": name,
                    "url": url,
                    "category": "News/Government/Census",
                    "origin": "Admin-Directed",
                })
                snippets.append(f"[Admin Web: {name}] {text[:500]}")
            elif len(text) > 50:
                print(f"MLG_TIER3_NOT_RELEVANT url={url}", flush=True)
        except Exception as e:
            print(f"MLG_TIER3_FETCH_FAIL url={url} err={e}", flush=True)
    return sources, snippets, attempted, matched


def _mlg_tier4_local_web(market_geo, lang_name, target_audience, relevance_keywords=None):
    try:
        from ddgs import DDGS
    except ImportError:
        try:
            from duckduckgo_search import DDGS
        except ImportError:
            print("MLG_TIER4_SKIP ddgs not installed", flush=True)
            return [], [], 0, 0

    queries = []
    geo_clean = (market_geo or "").strip()
    ta_clean = (target_audience or "").strip()
    if geo_clean and lang_name and lang_name != "English":
        queries.append(f"{geo_clean} demographics population socioeconomic")
        if ta_clean:
            queries.append(f"{ta_clean} {geo_clean} lifestyle work patterns")
    if not queries:
        return [], [], 0, 0

    sources = []
    snippets = []
    attempted = 0
    matched = 0
    try:
        ddgs = DDGS()
        for q in queries[:2]:
            attempted += 1
            try:
                results = list(ddgs.text(q, max_results=MLG_SEARCH_RESULTS_LIMIT))
                for r in results[:MLG_MAX_SNIPPETS_PER_TIER]:
                    body = (r.get("body") or "").strip()
                    title = (r.get("title") or "").strip()
                    href = (r.get("href") or "").strip()
                    if body and len(body) > 30 and _mlg_is_relevant(body + " " + title, relevance_keywords):
                        matched += 1
                        sources.append({
                            "name": title or href,
                            "url": href,
                            "category": "News/Market Data",
                            "origin": "Local Non-English",
                        })
                        snippets.append(f"[Local Web: {title}] {body[:400]}")
            except Exception as e:
                print(f"MLG_TIER4_SEARCH_FAIL query={q} err={e}", flush=True)
    except Exception as e:
        print(f"MLG_TIER4_INIT_FAIL err={e}", flush=True)
    return sources, snippets, attempted, matched


def _mlg_tier5_general_web(market_geo, target_audience, relevance_keywords=None):
    try:
        from ddgs import DDGS
    except ImportError:
        try:
            from duckduckgo_search import DDGS
        except ImportError:
            print("MLG_TIER5_SKIP ddgs not installed", flush=True)
            return [], [], 0, 0

    ta_clean = (target_audience or "").strip()
    geo_clean = (market_geo or "").strip()
    query = f"{ta_clean} {geo_clean} demographics socioeconomic population".strip()
    sources = []
    snippets = []
    attempted = 1
    matched = 0
    try:
        ddgs = DDGS()
        results = list(ddgs.text(query, max_results=MLG_SEARCH_RESULTS_LIMIT))
        for r in results[:MLG_MAX_SNIPPETS_PER_TIER]:
            body = (r.get("body") or "").strip()
            title = (r.get("title") or "").strip()
            href = (r.get("href") or "").strip()
            if body and len(body) > 30 and _mlg_is_relevant(body + " " + title, relevance_keywords):
                matched += 1
                sources.append({
                    "name": title or href,
                    "url": href,
                    "category": "News/Market Data",
                    "origin": "General Web",
                })
                snippets.append(f"[General Web: {title}] {body[:400]}")
    except Exception as e:
        print(f"MLG_TIER5_SEARCH_FAIL err={e}", flush=True)
    return sources, snippets, attempted, matched


def _mlg_synthesize_summary(raw_snippets, study_dict):
    if not raw_snippets:
        return ""
    combined = "\n".join(raw_snippets[:12])
    if len(combined) > 4000:
        combined = combined[:4000]
    market = (study_dict.get("study_fit") or "").strip()
    audience = (study_dict.get("target_audience") or "").strip()
    try:
        summary = call_llm(
            MLG_SYNTHESIZER_MODEL,
            [
                {
                    "role": "system",
                    "content": (
                        "You are a population-grounding assistant. Synthesize the following source excerpts "
                        "into a concise population-level context summary. "
                        "Rules:\n"
                        "1. Output ONLY population-level facts: demographics, socio-economic conditions, "
                        "cultural norms, lifestyle patterns, work/life realities, class dynamics, and "
                        "economic structure of the defined market segment.\n"
                        "2. Maximum 300 words. Be concise and factual.\n"
                        "3. Do NOT copy text verbatim — distill and paraphrase.\n"
                        "4. Do NOT add opinions or speculation.\n"
                        "5. Do NOT include source URLs or citations — just the synthesized facts.\n"
                        "6. Do NOT reference any product, brand, category, or commercial context. "
                        "Focus exclusively on who these people are, not what they buy."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Market/Geography: {market}\n"
                        f"Target Audience (Demographics / Socio-economics): {audience}\n\n"
                        f"Source excerpts to synthesize:\n{combined}"
                    ),
                },
            ],
            purpose="mlg_grounding_synthesis",
            timeout_seconds=30,
        )
        result = (summary or "").strip()
        words = result.split()
        if len(words) > MLG_MAX_SUMMARY_WORDS:
            result = " ".join(words[:MLG_MAX_SUMMARY_WORDS])
        if len(result) > MLG_MAX_SUMMARY_CHARS:
            result = result[:MLG_MAX_SUMMARY_CHARS]
        return result
    except Exception as e:
        print(f"MLG_SYNTHESIZE_FAIL err={e}", flush=True)
        return ""


_MLG_REPUTABLE_DOMAINS = {
    ".gov", ".gov.hk", ".gov.sg", ".gov.au", ".gov.uk", ".gov.jp", ".go.jp", ".go.th",
    ".edu", ".edu.hk", ".edu.sg", ".edu.au", ".ac.uk", ".ac.jp",
    ".int",
}

_MLG_LOW_QUALITY_PATTERNS = [
    "how-to-start", "how to start", "affiliate", "listicle",
    "best-vending-machine", "top-10", "top-5",
    "/blog/how-to", "/blog/best-", "/blog/top-",
    "shopify.com/blog", "entrepreneur.com/starting",
    "bizfluent.com", "wikihow.com",
]


def _mlg_score_source(source):
    from urllib.parse import urlparse

    origin = (source.get("origin") or "").strip()
    url = (source.get("url") or "").strip()
    name = (source.get("name") or "").strip()
    score = 50

    if origin == "Uploaded Document":
        score = 95
    elif origin == "Admin-Directed":
        score = 90
    elif origin == "Local Non-English":
        score = 60
    elif origin == "General Web":
        score = 40

    is_wiki = _mlg_is_wikipedia(url)
    is_authoritative = _mlg_is_secondary_authoritative(url)

    if url:
        try:
            host = urlparse(url).hostname or ""
            host_lower = host.lower()
            if is_wiki:
                score -= 25
                source["_source_class"] = "tertiary_calibration"
            elif is_authoritative:
                score += 25
                source["_source_class"] = "secondary_authoritative"
            else:
                for rep in _MLG_REPUTABLE_DOMAINS:
                    if host_lower.endswith(rep):
                        score += 15
                        break
            url_lower = url.lower()
            name_lower = name.lower()
            for pat in _MLG_LOW_QUALITY_PATTERNS:
                if pat in url_lower or pat in name_lower:
                    score -= 30
                    break
        except Exception:
            pass

    return max(0, min(100, score))


def _mlg_select_top_sources(all_sources, max_shown=MLG_MAX_SOURCES_SHOWN):
    if not all_sources:
        return []
    scored = []
    for s in all_sources:
        scored.append((s, _mlg_score_source(s)))
    scored.sort(key=lambda x: x[1], reverse=True)
    selected = []
    wiki_count = 0
    for s, sc in scored:
        if len(selected) >= max_shown:
            break
        url = (s.get("url") or "").strip()
        if _mlg_is_wikipedia(url):
            if wiki_count >= MLG_MAX_WIKIPEDIA_PER_BUNDLE:
                continue
            wiki_count += 1
        s["_quality_score"] = sc
        selected.append(s)
    return selected


def _mlg_format_grounding_sources_text(sources):
    if not sources:
        return "No live sources retrieved; persona relies on model priors and/or uploaded documents."
    lines = []
    for s in sources:
        name = s.get("name", "Unknown")
        url = s.get("url", "")
        cat = s.get("category", "")
        origin = s.get("origin", "")
        source_class = s.get("_source_class", "")
        entry = f"- {name}"
        if url:
            entry += f" ({url})"
        if source_class == "tertiary_calibration":
            entry += " [Tertiary Calibration Only]"
        elif cat:
            entry += f" [{cat}]"
        if origin:
            entry += f" Origin: {origin}"
        lines.append(entry)
    return "\n".join(lines)


def _mlg_build_reason_code(tier_stats):
    parts = []
    for tier_name in ["user_uploads", "admin_uploads", "admin_web", "local_web", "general_web"]:
        t = tier_stats.get(tier_name, {})
        found = t.get("found", 0)
        attempted = t.get("attempted", 0)
        matched = t.get("matched", 0)
        if tier_name in ("user_uploads", "admin_uploads"):
            if found > 0:
                parts.append(f"{tier_name}:found={found}")
            else:
                parts.append(f"{tier_name}:none")
        else:
            if attempted > 0:
                parts.append(f"{tier_name}:attempted={attempted},matched={matched}")
            else:
                parts.append(f"{tier_name}:skipped")
    return ";".join(parts)


def mlg_retrieve_for_persona(study_dict, study_id, user_id):
    all_sources = []
    all_snippets = []
    tier_stats = {}
    relevance_kw = _mlg_build_relevance_keywords(study_dict)

    conn = get_db()
    try:
        t1_sources, t1_snippets = _mlg_tier1_user_uploads(conn, study_id, user_id)
        tier_stats["user_uploads"] = {"found": len(t1_sources)}
        all_sources.extend(t1_sources[:MLG_MAX_SNIPPETS_PER_TIER])
        all_snippets.extend(t1_snippets[:MLG_MAX_SNIPPETS_PER_TIER])

        t2_sources, t2_snippets = _mlg_tier2_admin_uploads(conn)
        tier_stats["admin_uploads"] = {"found": len(t2_sources)}
        all_sources.extend(t2_sources[:MLG_MAX_SNIPPETS_PER_TIER])
        all_snippets.extend(t2_snippets[:MLG_MAX_SNIPPETS_PER_TIER])

        admin_web_sources = get_admin_web_sources(conn, status_filter="active")
    finally:
        conn.close()

    t3_sources, t3_snippets, t3_attempted, t3_matched = _mlg_tier3_admin_web(admin_web_sources, relevance_kw)
    tier_stats["admin_web"] = {"attempted": t3_attempted, "matched": t3_matched}
    all_sources.extend(t3_sources[:MLG_MAX_SNIPPETS_PER_TIER])
    all_snippets.extend(t3_snippets[:MLG_MAX_SNIPPETS_PER_TIER])

    market_geo = (study_dict.get("study_fit") or "").strip()
    target_aud = (study_dict.get("target_audience") or "").strip()
    _g_admin_srcs = admin_web_sources
    _, lang_name = infer_market_language(market_geo, _g_admin_srcs)

    t4_sources, t4_snippets, t4_attempted, t4_matched = _mlg_tier4_local_web(
        market_geo, lang_name, target_aud, relevance_kw
    )
    tier_stats["local_web"] = {"attempted": t4_attempted, "matched": t4_matched}
    all_sources.extend(t4_sources[:MLG_MAX_SNIPPETS_PER_TIER])
    all_snippets.extend(t4_snippets[:MLG_MAX_SNIPPETS_PER_TIER])

    t5_sources, t5_snippets, t5_attempted, t5_matched = _mlg_tier5_general_web(
        market_geo, target_aud, relevance_kw
    )
    tier_stats["general_web"] = {"attempted": t5_attempted, "matched": t5_matched}
    all_sources.extend(t5_sources[:MLG_MAX_SNIPPETS_PER_TIER])
    all_snippets.extend(t5_snippets[:MLG_MAX_SNIPPETS_PER_TIER])

    synthesized_summary = _mlg_synthesize_summary(all_snippets, study_dict)

    grounding_used = bool(synthesized_summary)
    if grounding_used:
        top_sources = _mlg_select_top_sources(all_sources, MLG_MAX_SOURCES_SHOWN)
        grounding_sources_text = _mlg_format_grounding_sources_text(top_sources)
        shown_scores = [(s.get("name","?"), s.get("_quality_score", 0)) for s in top_sources]
        print(
            f"MLG_QUALITY_FILTER study={study_id} total_retrieved={len(all_sources)} "
            f"shown={len(top_sources)} scores={shown_scores}",
            flush=True,
        )
    elif all_sources:
        top_sources = []
        grounding_sources_text = (
            "Sources were retrieved but could not be synthesized into grounding context; "
            "persona relies on model priors and/or uploaded documents."
        )
    else:
        top_sources = []
        grounding_sources_text = (
            "No live sources retrieved; persona relies on model priors and/or uploaded documents."
        )

    reason_code = _mlg_build_reason_code(tier_stats)
    if not grounding_used and all_sources:
        reason_code += ";synthesis:failed"
    reason_code += f";shown={len(top_sources)}/{len(all_sources)}"

    print(
        f"MLG_RETRIEVE_DONE study={study_id} total_sources={len(all_sources)} "
        f"grounding_used={grounding_used} summary_len={len(synthesized_summary)} reason={reason_code}",
        flush=True,
    )

    return {
        "sources": top_sources,
        "synthesized_summary": synthesized_summary,
        "grounding_sources_text": grounding_sources_text,
        "grounding_used": grounding_used,
        "tier_stats": tier_stats,
        "reason_code": reason_code,
        "admin_web_configured": len(admin_web_sources),
    }


CTX_CITATION_MODEL = "gpt-4o-mini"
CTX_CITATION_MAX_SOURCES = 5


def retrieve_context_citations(product_concept, business_problem):
    """Retrieve context citations via OpenAI Responses API with web_search_preview.

    Uses Product/Concept and Business Problem ONLY (never Market/Geography,
    Target Audience demographics, or Competitive Context).  All search,
    retrieval, and ranking is delegated to the model — the application only
    parses and labels returned citations as Context (NOT evidence).
    """
    import openai as _openai

    base_url = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")
    api_key = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
    if not base_url or not api_key:
        print("CTX_CITATION_SKIP reason=openai_not_configured", flush=True)
        return []

    pc = (product_concept or "").strip()
    bp = (business_problem or "").strip()
    if not pc and not bp:
        print("CTX_CITATION_SKIP reason=no_inputs", flush=True)
        return []

    topic_parts = []
    if pc:
        topic_parts.append(f"Product/Concept: {pc}")
    if bp:
        topic_parts.append(f"Business Problem: {bp}")
    topic_block = "\n".join(topic_parts)

    prompt = (
        "You are a media research assistant. Your task is to find 3 to 5 recent, "
        "reputable journalism articles (news reports, investigative pieces, feature "
        "articles, or analytical coverage) that are relevant to the following product "
        "or business context.\n\n"
        "PURPOSE: These articles will be used to understand what real-world media "
        "narratives consumers may have encountered that could shape their reactions "
        "to this product or business concept. This is for reaction realism in market "
        "research simulations.\n\n"
        "PREFERENCES:\n"
        "- Prefer independent, professionally edited journalism from reputable outlets\n"
        "- Prefer reporting, analysis, investigation, and feature coverage\n"
        "- Deprioritize encyclopedic summaries (e.g. Wikipedia), vendor/brand content, "
        "press releases, and promotional material\n"
        "- Focus on narratives consumers would plausibly have encountered\n\n"
        f"TOPIC:\n{topic_block}\n\n"
        "For each article found, provide:\n"
        "1. The article title\n"
        "2. A one-sentence summary of the article's relevance\n\n"
        "Return between 3 and 5 articles. If fewer than 3 relevant journalism "
        "articles exist, return what you find."
    )

    try:
        client = _openai.OpenAI(base_url=base_url, api_key=api_key, timeout=45)
        response = client.responses.create(
            model=CTX_CITATION_MODEL,
            tools=[{"type": "web_search_preview"}],
            input=prompt,
        )

        context_sources = []
        seen_urls = set()

        for item in response.output:
            if item.type == "message":
                for content_block in item.content:
                    if content_block.type == "output_text":
                        for annotation in getattr(content_block, "annotations", []):
                            if getattr(annotation, "type", "") == "url_citation":
                                url = getattr(annotation, "url", "")
                                title = getattr(annotation, "title", "") or url
                                if url and url not in seen_urls:
                                    seen_urls.add(url)
                                    context_sources.append({
                                        "title": title,
                                        "url": url,
                                        "citation_class": "context",
                                    })

        context_sources = context_sources[:CTX_CITATION_MAX_SOURCES]
        print(
            f"CTX_CITATION_DONE count={len(context_sources)} "
            f"urls={[s['url'][:60] for s in context_sources]}",
            flush=True,
        )
        return context_sources

    except Exception as e:
        print(f"CTX_CITATION_FAIL err={e}", flush=True)
        return []


def lisa_generate_personas(study_dict, n, lisa_model_id, grounding_summary=""):
    _pop_market = (study_dict.get("study_fit") or "").strip()
    _pop_audience = (study_dict.get("target_audience") or "").strip()

    _interp_product = (study_dict.get("known_vs_unknown") or "").strip()
    _interp_bp = (study_dict.get("business_problem") or "").strip()
    _interp_ds = (study_dict.get("decision_to_support") or "").strip()
    _interp_dui = (study_dict.get("definition_useful_insight") or "").strip()

    study_type_label = (
        "In-Depth Interview (IDI)"
        if study_dict.get("study_type") == "synthetic_idi"
        else "Focus Group"
    )

    base_json_instruction = (
        "Return STRICT JSON only — no markdown, no commentary, no code fences.\n"
        "Schema:\n"
        '{"personas": [{"name": "...", "persona_summary": "...", "demographic_frame": "...", '
        '"psychographic_profile": "...", "contextual_constraints": "...", '
        '"behavioural_tendencies": "...", "grounding_sources": "...", '
        '"confidence_and_limits": "..."}]}\n\n'
    )

    _persona_geo = _pop_market
    _persona_admin_srcs = []
    try:
        _p_conn = get_db()
        _persona_admin_srcs = get_admin_web_sources(_p_conn, status_filter="active")
        _p_conn.close()
    except Exception:
        pass
    _p_lang_code, _p_lang_name = infer_market_language(_persona_geo, _persona_admin_srcs)
    _persona_lang_rule = ""
    if _p_lang_code != "en":
        _persona_lang_rule = (
            f"\nLANGUAGE: Write persona dossier content (persona_summary, demographic_frame, "
            f"psychographic_profile, contextual_constraints, behavioural_tendencies, grounding_sources, "
            f"confidence_and_limits) in {_p_lang_name}, as appropriate for the {_persona_geo} market. "
            f"Keep JSON keys in English.\n"
        )

    system_prompt = (
        "You are Lisa, a senior qualitative research analyst at Project Brainstorm.\n\n"
        "GROUNDING MODE: Population-Grounded Discovery\n\n"
        "CORE PRINCIPLE: Personas represent who exists in the market segment, "
        "not who already cares about the product. Find people first. "
        "Expose them to the idea later.\n\n"
        "SCOPE SEPARATION (NON-NEGOTIABLE):\n"
        "- Population grounding answers: 'Who exists in this market segment?'\n"
        "- Interpretive context answers: 'How might these people interpret or react to an idea?'\n"
        "These are different concerns and MUST NOT be conflated.\n\n"
        "FIELD RULES:\n"
        "- POPULATION-DEFINING (use for identity, background, worldview): "
        "Market / Geography, Target Audience (Demographics / Socio-economics)\n"
        "- INTERPRETATION-ONLY (use ONLY for framing reactions/attitudes AFTER identity is set): "
        "Product / Concept, Business Problem, Decision to Support\n"
        "- Product / Concept MUST NOT influence whether a person exists, their baseline "
        "category affinity, or inclusion/exclusion from the population.\n\n"
        + base_json_instruction
        + "PERSONA GENERATION RULES:\n"
        f"1. Generate exactly {n} persona(s).\n"
        "2. Each persona must be distinct in demographics, psychographics, and behaviour.\n"
        "3. Base identity, background, and worldview on Market / Geography and "
        "Target Audience ONLY.\n"
        "4. Do NOT assume prior interest in or familiarity with the Product / Concept.\n"
        "5. Personas may be indifferent, skeptical, unaware, confused, or negative "
        "toward the product. This is expected and correct.\n"
        "6. Do NOT optimize for positive or commercially favourable reactions.\n"
        "7. Each persona name must be culturally and socially plausible for the target "
        "market and language. Use full given name and family name as appropriate.\n"
        "8. Each field must be substantive (50-200 words), not placeholder text.\n"
        "9. Product / Concept may influence what the person notices, how they interpret "
        "value or risk, and what questions or doubts they have — but MUST NOT influence "
        "whether the person exists or their baseline category affinity.\n"
        "10. Diversity, neutrality, and realism are required.\n"
        + _persona_lang_rule
    )

    if grounding_summary:
        system_prompt += (
            "\nGROUNDING CONTEXT (population-level facts synthesized from live sources — "
            "use to inform demographic realism and socio-economic accuracy, do NOT copy verbatim):\n"
            + grounding_summary
        )

    population_section = (
        "--- POPULATION-DEFINING INPUTS (use for identity) ---\n"
        f"Market / Geography: {_pop_market or 'Not specified'}\n"
        f"Target Audience (Demographics / Socio-economics): {_pop_audience or 'Not specified'}\n"
    )

    interpretation_section = (
        "--- INTERPRETATION-ONLY CONTEXT (use ONLY for framing reactions, NOT for identity) ---\n"
        f"Product / Concept: {_interp_product or 'Not specified'}\n"
        f"Business Problem: {_interp_bp or 'Not specified'}\n"
        f"Decision to Support: {_interp_ds or 'Not specified'}\n"
    )
    if _interp_dui:
        interpretation_section += f"Definition of Useful Insight: {_interp_dui}\n"

    oc_block, oc_keys = _extract_optional_context(study_dict)
    if oc_block:
        interpretation_section += f"\n{oc_block}\n"
    study_title = study_dict.get("title", "Untitled Study")
    study_id = study_dict.get("id", "?")
    print(f'OPTIONAL_CONTEXT_INJECT_LISA_PERSONA study_title="{study_title}" included={"true" if oc_keys else "false"} keys={",".join(oc_keys)}', flush=True)

    user_prompt = (
        f"Study: {study_title}\n"
        f"Type: {study_type_label}\n\n"
        f"{population_section}\n"
        f"{interpretation_section}\n"
        f"Generate {n} persona(s) for this study."
    )

    for attempt in range(2):
        attempt_label = "initial" if attempt == 0 else "retry"
        current_system = system_prompt
        if attempt == 1:
            current_system = (
                system_prompt +
                "\n\nCRITICAL: Your previous response had invalid JSON. "
                "You MUST return valid JSON with NO trailing commas, NO comments, "
                "NO markdown fences. Start with { and end with }. "
                "Ensure all strings are properly escaped."
            )
            print(f"PERSONA_GEN_RETRY study={study_id} model={lisa_model_id}", flush=True)

        try:
            raw = call_llm(
                lisa_model_id,
                [
                    {"role": "system", "content": current_system},
                    {"role": "user", "content": user_prompt},
                ],
                purpose="lisa_auto_persona_generation",
                timeout_seconds=120,
            )
        except RuntimeError as e:
            print(
                f"PERSONA_GEN_TIMEOUT study={study_id} attempt={attempt_label} "
                f"model={lisa_model_id} err={e}",
                flush=True,
            )
            print(
                f"PERSONA_TIMEOUT_NO_RETRY study={study_id} model={lisa_model_id} "
                f"reason=timeout_is_non_recoverable",
                flush=True,
            )
            raise

        try:
            parsed = _safe_parse_persona_json(raw, study_id, lisa_model_id)
            personas = _validate_persona_list(parsed, n)
            print(
                f"PERSONA_GEN_OK study={study_id} attempt={attempt_label} "
                f"model={lisa_model_id} count={len(personas)}",
                flush=True,
            )
            return personas[:n]
        except (ValueError, json.JSONDecodeError) as e:
            print(
                f"PERSONA_GEN_FAIL study={study_id} attempt={attempt_label} "
                f"model={lisa_model_id} raw_len={len(raw) if raw else 0} err={e}",
                flush=True,
            )
            if attempt == 0:
                continue
            raise ValueError(
                "Auto-persona generation failed due to invalid generator output. "
                "Please try again or attach personas manually."
            )


def _check_and_fix_name_plausibility(personas, study_dict, lisa_model_id):
    market_geo = (study_dict.get("study_fit") or "").strip()
    target_aud = (study_dict.get("target_audience") or "").strip()
    if not market_geo and not target_aud:
        return personas
    study_id = study_dict.get("id", "?")

    names_list = [p.get("name", "") for p in personas]
    names_str = ", ".join(f'"{n}"' for n in names_list)

    try:
        check_result = call_llm(
            MLG_SYNTHESIZER_MODEL,
            [
                {
                    "role": "system",
                    "content": (
                        "You are a cultural naming expert. Evaluate whether each persona name is "
                        "culturally and socially plausible for the specified market.\n"
                        "Return STRICT JSON only — no markdown, no commentary.\n"
                        'Schema: {"results": [{"name": "...", "plausible": true/false, "better_name": "..."}]}\n'
                        "Rules:\n"
                        "1. A name is plausible if it sounds like a real person's name in that market/culture.\n"
                        "2. Single-token generic words (e.g., 'Man', 'Wing', 'Chun' alone) are NOT plausible "
                        "as full names in Chinese-speaking markets — they need a family name.\n"
                        "3. If plausible=false, provide a better_name that IS plausible for that market.\n"
                        "4. Do NOT apply rigid formatting rules. Judge naturally.\n"
                        "5. If plausible=true, set better_name to the same name."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Market/Geography: {market_geo}\n"
                        f"Target Audience: {target_aud}\n"
                        f"Names to check: {names_str}"
                    ),
                },
            ],
            purpose="persona_name_plausibility_check",
            timeout_seconds=30,
        )
    except Exception as e:
        print(f"NAME_PLAUSIBILITY_CHECK_FAIL study={study_id} err={e}", flush=True)
        return personas

    try:
        cleaned = (check_result or "").strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        parsed = json.loads(cleaned)
        results = parsed.get("results", [])
        if not isinstance(results, list) or len(results) != len(personas):
            print(f"NAME_PLAUSIBILITY_PARSE_MISMATCH study={study_id} expected={len(personas)} got={len(results)}", flush=True)
            return personas

        any_fixed = False
        for i, r in enumerate(results):
            if not r.get("plausible", True):
                better = (r.get("better_name") or "").strip()
                if better and better != names_list[i]:
                    old_name = personas[i]["name"]
                    personas[i]["name"] = better
                    any_fixed = True
                    print(
                        f"NAME_PLAUSIBILITY_FIX study={study_id} idx={i} "
                        f"old={old_name} new={better} market={market_geo}",
                        flush=True,
                    )
        if not any_fixed:
            print(f"NAME_PLAUSIBILITY_ALL_OK study={study_id} count={len(personas)}", flush=True)
    except (json.JSONDecodeError, TypeError, KeyError) as e:
        print(f"NAME_PLAUSIBILITY_PARSE_FAIL study={study_id} err={e}", flush=True)

    return personas


def _detect_actual_content_language(personas, expected_lang_code):
    if expected_lang_code == "en":
        return "en"
    sample_text = ""
    for p in personas:
        for fld in ("persona_summary", "demographic_frame", "psychographic_profile"):
            val = p.get(fld, "")
            if isinstance(val, str) and len(val) > 30:
                sample_text += val + " "
                if len(sample_text) > 400:
                    break
        if len(sample_text) > 400:
            break
    if not sample_text:
        return expected_lang_code
    ascii_chars = sum(1 for c in sample_text if c.isascii() and c.isalpha())
    total_alpha = sum(1 for c in sample_text if c.isalpha())
    if total_alpha == 0:
        return expected_lang_code
    ascii_ratio = ascii_chars / total_alpha
    if ascii_ratio > 0.85:
        return "en"
    return expected_lang_code


_JSON_FIELD_LABELS = {
    "age": "Age", "gender": "Gender", "location": "Location",
    "education": "Education", "income_level": "Income Level",
    "family_structure": "Family Structure", "occupation": "Occupation",
    "personality": "Personality", "interests": "Interests",
    "values": "Values", "lifestyle": "Lifestyle", "attitudes": "Attitudes",
    "market_segment": "Market Segment", "competitive_context": "Competitive Context",
    "adoption_barriers": "Adoption Barriers", "cultural_considerations": "Cultural Considerations",
    "shopping_habits": "Shopping Habits", "product_preferences": "Product Preferences",
    "brand_loyalty": "Brand Loyalty", "social_influence": "Social Influence",
    "decision_making": "Decision Making", "media_consumption": "Media Consumption",
    "motivations": "Motivations", "pain_points": "Pain Points",
    "trigger_events": "Trigger Events", "goals": "Goals",
}


def _json_to_prose(json_str):
    try:
        obj = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None
    lines = []
    for k, v in obj.items():
        label = _JSON_FIELD_LABELS.get(k, k.replace("_", " ").title())
        val = str(v).strip() if v else ""
        if val:
            lines.append(f"{label}: {val}")
    if not lines:
        return None
    return ". ".join(lines) + "."


def _fix_json_structured_fields(personas, study_id="?"):
    prose_fields = [
        "demographic_frame", "psychographic_profile",
        "contextual_constraints", "behavioural_tendencies",
        "confidence_and_limits",
    ]
    any_fixed = False
    for i, p in enumerate(personas):
        for fld in prose_fields:
            val = p.get(fld, "")
            if not isinstance(val, str):
                continue
            stripped = val.strip()
            if stripped.startswith("{") and stripped.endswith("}"):
                prose = _json_to_prose(stripped)
                if prose:
                    p[fld] = prose
                    any_fixed = True
    if any_fixed:
        print(f"JSON_TO_PROSE_FIX study={study_id} fixed_personas={sum(1 for p in personas for f in prose_fields if not (p.get(f,'') or '').strip().startswith('{'))}", flush=True)
    return personas


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
    limit = 999999 if user_id in UNLIMITED_USER_IDS else FREE_TIER_MONTHLY_LIMIT
    return (
        count,
        limit,
        window_start.strftime("%Y-%m-%d"),
        window_end.strftime("%Y-%m-%d"),
    )


STUDY_TYPE_LIMITS = {
    "synthetic_survey": {"max_questions": 12, "max_respondents": 400},
    "synthetic_idi": {"min_personas": 1, "max_personas": 1},
    "synthetic_focus_group": {"min_personas": 4, "max_personas": 6},
}

VALID_SURVEY_Q_TYPES = ("likert", "mc", "ab", "ab_image", "range", "open")
AB_IMAGE_LIMITS = {
    "max_ab_image_questions": 6,
    "max_images_per_survey": 12,
    "max_size_bytes": 500 * 1024,
    "allowed_types": ("image/jpeg", "image/png"),
    "allowed_extensions": (".jpg", ".jpeg", ".png"),
}
# NOTE: max_size_bytes and allowed_types are enforced at upload time
# when file upload infrastructure is added (future prompt). Currently
# ab_image questions store text references only; extension checks are
# applied in _validate_survey_questions().


def _normalize_survey_question(q):
    if isinstance(q, str):
        return {"type": "open", "prompt": q.strip(), "max_words": 50}
    if not isinstance(q, dict):
        return None
    q["prompt"] = (q.get("prompt") or "").strip()
    q["type"] = (q.get("type") or "open").strip().lower()
    if q["type"] not in VALID_SURVEY_Q_TYPES:
        q["type"] = "open"
    if "options" in q and isinstance(q["options"], list):
        q["options"] = [str(o).strip() for o in q["options"] if str(o).strip()]
    q["max_words"] = 50
    if "min" in q:
        try:
            q["min"] = float(q["min"])
        except (ValueError, TypeError):
            q["min"] = 0
    if "max" in q:
        try:
            q["max"] = float(q["max"])
        except (ValueError, TypeError):
            q["max"] = 100
    if "images" in q and isinstance(q["images"], dict):
        q["images"] = {k: str(v).strip() for k, v in q["images"].items()}
    return q


def _get_q_prompt(q):
    if isinstance(q, dict):
        return (q.get("prompt") or "").strip()
    if isinstance(q, str):
        return q.strip()
    return ""


def _validate_survey_questions(questions):
    errors = []
    if not isinstance(questions, list) or len(questions) == 0:
        return ["At least 1 survey question is required."]
    if len(questions) > 12:
        return ["Maximum 12 survey questions allowed."]

    ab_image_count = 0
    total_images = 0

    for i, q in enumerate(questions, 1):
        if not isinstance(q, dict):
            errors.append(f"Q{i}: invalid question format.")
            continue
        qtype = q.get("type", "")
        prompt = (q.get("prompt") or "").strip()
        if not prompt:
            errors.append(f"Q{i}: prompt text is required.")
        if qtype not in VALID_SURVEY_Q_TYPES:
            errors.append(f"Q{i}: invalid type '{qtype}'. Must be one of: {', '.join(VALID_SURVEY_Q_TYPES)}.")
            continue

        opts = q.get("options") or []

        if qtype == "likert":
            if not isinstance(opts, list) or len(opts) != 5:
                errors.append(f"Q{i} (likert): must have exactly 5 options.")
        elif qtype == "mc":
            if not isinstance(opts, list) or len(opts) < 3:
                errors.append(f"Q{i} (mc): must have at least 3 options.")
            elif len(opts) > 8:
                errors.append(f"Q{i} (mc): cannot have more than 8 options.")
        elif qtype == "ab":
            if not isinstance(opts, list) or len(opts) != 2:
                errors.append(f"Q{i} (ab): must have exactly 2 options.")
        elif qtype == "ab_image":
            ab_image_count += 1
            images = q.get("images") or {}
            if not isinstance(images, dict) or set(images.keys()) != {"A", "B"}:
                errors.append(f"Q{i} (ab_image): images must contain exactly keys 'A' and 'B'.")
            else:
                ref_a = (images.get("A") or "").strip()
                ref_b = (images.get("B") or "").strip()
                if not ref_a or not ref_b:
                    errors.append(f"Q{i} (ab_image): both image A and image B must be uploaded.")
                else:
                    for label, ref in [("A", ref_a), ("B", ref_b)]:
                        if "." in ref:
                            ext = ("." + ref.rsplit(".", 1)[-1]).lower()
                            if ext not in AB_IMAGE_LIMITS["allowed_extensions"]:
                                errors.append(f"Q{i} (ab_image): image {label} must be JPG or PNG (got '{ext}').")
                        fpath = os.path.join(SURVEY_IMAGES_DIR, os.path.basename(ref))
                        if not os.path.isfile(fpath):
                            errors.append(f"Q{i} (ab_image): image {label} file not found. Please re-upload.")
                total_images += 2
        elif qtype == "range":
            r_min = q.get("min")
            r_max = q.get("max")
            if r_min is None or r_max is None:
                errors.append(f"Q{i} (range): min and max bounds are required.")
            else:
                try:
                    if float(r_min) >= float(r_max):
                        errors.append(f"Q{i} (range): min must be less than max.")
                except (ValueError, TypeError):
                    errors.append(f"Q{i} (range): min and max must be numeric.")
        elif qtype == "open":
            mw = q.get("max_words")
            if mw is None:
                q["max_words"] = 50
            else:
                try:
                    mw = int(mw)
                    if mw < 1 or mw > 500:
                        errors.append(f"Q{i} (open): max_words must be between 1 and 500.")
                except (ValueError, TypeError):
                    errors.append(f"Q{i} (open): max_words must be a number.")

    if ab_image_count > AB_IMAGE_LIMITS["max_ab_image_questions"]:
        errors.append(f"Maximum {AB_IMAGE_LIMITS['max_ab_image_questions']} ab_image questions allowed per survey.")
    if total_images > AB_IMAGE_LIMITS["max_images_per_survey"]:
        errors.append(f"Maximum {AB_IMAGE_LIMITS['max_images_per_survey']} stimulus images allowed per survey.")

    return errors


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
}

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
    test_row = conn.execute("SELECT id FROM users WHERE email = ?", (TEST_USER_EMAIL,)).fetchone()
    if not test_row:
        from werkzeug.security import generate_password_hash as _gph
        conn.execute(
            "INSERT INTO users (email, username, password_hash, state) VALUES (?, ?, ?, 'active')",
            (TEST_USER_EMAIL, "TestAdmin", _gph(TEST_USER_PASSWORD)),
        )
        conn.commit()
        test_row = conn.execute("SELECT id FROM users WHERE email = ?", (TEST_USER_EMAIL,)).fetchone()
    if test_row:
        UNLIMITED_USER_IDS.add(test_row["id"])
    conn.commit()
    conn.close()


def migrate_db(conn):
    cols = [row[1] for row in conn.execute("PRAGMA table_info(personas)").fetchall()]
    if "persona_instance_id" not in cols:
        conn.execute("ALTER TABLE personas ADD COLUMN persona_instance_id TEXT")
    if "content_language" not in cols:
        conn.execute("ALTER TABLE personas ADD COLUMN content_language TEXT")
    if "translated_content" not in cols:
        conn.execute("ALTER TABLE personas ADD COLUMN translated_content TEXT")

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
    if "output_language" not in study_cols:
        conn.execute("ALTER TABLE studies ADD COLUMN output_language TEXT")
    if "translated_output" not in study_cols:
        conn.execute("ALTER TABLE studies ADD COLUMN translated_output TEXT")
    if "exec_grounding_data" not in study_cols:
        conn.execute("ALTER TABLE studies ADD COLUMN exec_grounding_data TEXT")

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
    if user and user["id"] in UNLIMITED_USER_IDS:
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


def get_user_personas_list(conn, user_id, user_lang=None):
    rows = conn.execute(
        "SELECT persona_instance_id, name, created_at, content_language, translated_content FROM personas WHERE user_id = ? ORDER BY id DESC",
        (user_id,),
    ).fetchall()
    result = []
    for r in rows:
        item = {"persona_instance_id": r["persona_instance_id"], "name": r["name"], "created_at": r["created_at"]}
        if user_lang and user_lang != (r["content_language"] or "en") and r["translated_content"]:
            try:
                _tc = json.loads(r["translated_content"])
                if isinstance(_tc, dict) and _tc.get("lang") == user_lang:
                    _tname = _tc.get("fields", {}).get("name")
                    if not _tname and _tc.get("fields", {}).get("persona_summary"):
                        _tname = _extract_translated_name(_tc["fields"]["persona_summary"], r["name"])
                    if _tname:
                        item["translated_name"] = _tname
            except (json.JSONDecodeError, TypeError):
                pass
        result.append(item)
    return result


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


GEOGRAPHY_LANGUAGE_MAP = {
    "hong kong": ("zh-Hant", "Traditional Chinese (Cantonese)"),
    "hk": ("zh-Hant", "Traditional Chinese (Cantonese)"),
    "macau": ("zh-Hant", "Traditional Chinese (Cantonese)"),
    "macao": ("zh-Hant", "Traditional Chinese (Cantonese)"),
    "taiwan": ("zh-Hant", "Traditional Chinese (Mandarin)"),
    "china": ("zh-Hans", "Simplified Chinese (Mandarin)"),
    "mainland china": ("zh-Hans", "Simplified Chinese (Mandarin)"),
    "prc": ("zh-Hans", "Simplified Chinese (Mandarin)"),
    "japan": ("ja", "Japanese"),
    "south korea": ("ko", "Korean"),
    "korea": ("ko", "Korean"),
    "thailand": ("th", "Thai"),
    "vietnam": ("vi", "Vietnamese"),
    "indonesia": ("id", "Bahasa Indonesia"),
    "malaysia": ("ms", "Bahasa Melayu"),
    "philippines": ("tl", "Filipino/Tagalog"),
    "singapore": ("en", "English"),
    "australia": ("en", "English"),
    "new zealand": ("en", "English"),
    "india": ("en", "English"),
    "united states": ("en", "English"),
    "usa": ("en", "English"),
    "united kingdom": ("en", "English"),
    "uk": ("en", "English"),
    "canada": ("en", "English"),
}

SOURCE_LANG_MAP = {
    "cn": "zh-Hant",
    "zh": "zh-Hans",
    "zh-hans": "zh-Hans",
    "zh-hant": "zh-Hant",
    "ja": "ja",
    "ko": "ko",
    "th": "th",
    "vi": "vi",
    "id": "id",
    "ms": "ms",
    "en": "en",
}

LANG_CODE_TO_NAME = {
    "en": "English",
    "zh-Hans": "Simplified Chinese",
    "zh-Hant": "Traditional Chinese",
    "ja": "Japanese",
    "ko": "Korean",
    "th": "Thai",
    "vi": "Vietnamese",
    "id": "Bahasa Indonesia",
    "ms": "Bahasa Melayu",
    "tl": "Filipino/Tagalog",
}


def infer_market_language(study_fit_text, admin_sources=None):
    geo = (study_fit_text or "").strip().lower()
    geo_lang_code = None
    geo_lang_name = None
    for keyword, (code, name) in GEOGRAPHY_LANGUAGE_MAP.items():
        if keyword in geo:
            geo_lang_code = code
            geo_lang_name = name
            break

    source_lang_code = None
    if admin_sources:
        lang_counts = {}
        for src in admin_sources:
            src_lang = (src.get("language") or "en").strip().lower()
            normalized = SOURCE_LANG_MAP.get(src_lang, src_lang)
            if normalized != "en":
                lang_counts[normalized] = lang_counts.get(normalized, 0) + 1
        if lang_counts:
            source_lang_code = max(lang_counts, key=lang_counts.get)

    if geo_lang_code and geo_lang_code != "en":
        return geo_lang_code, geo_lang_name
    if source_lang_code and source_lang_code != "en":
        return source_lang_code, LANG_CODE_TO_NAME.get(source_lang_code, source_lang_code)
    if geo_lang_code == "en":
        return "en", "English"
    return "en", "English"


def create_grounding_trace(conn, trigger_event, study_id=None, persona_id=None, mlg_data=None):
    if mlg_data:
        admin_cfg_count = mlg_data.get("admin_web_configured", 0)
        tier_stats = mlg_data.get("tier_stats", {})
        admin_web_stats = tier_stats.get("admin_web", {})
        admin_queried_count = admin_web_stats.get("attempted", 0)
        admin_matched_count = admin_web_stats.get("matched", 0)
        grounding_used = mlg_data.get("grounding_used", False)
        flag_configured = 1 if admin_cfg_count > 0 else 0
        flag_queried = 1 if admin_queried_count > 0 else 0
        flag_matched = 1 if admin_matched_count > 0 else 0
        flag_used = 1 if (admin_matched_count > 0 and grounding_used) else 0
        reason_code = mlg_data.get("reason_code", "")
    else:
        active_sources = get_admin_web_sources(conn, status_filter="active")
        has_active = len(active_sources) > 0
        flag_configured = 1 if has_active else 0
        flag_queried = 1 if has_active else 0
        flag_matched = 0
        flag_used = 0
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
            flag_configured,
            flag_queried,
            flag_matched,
            flag_used,
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

    dm_view = request.args.get("view") or ""
    dm_messages_list = []
    dm_page = 1
    dm_total_pages = 1
    dm_total = 0
    dm_unread_count = 0
    dm_latest_preview = None

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

        dm_unread_count = _dm_unread_count(is_admin=True)
        dm_latest_preview = _dm_latest_preview(is_admin=True)
        _dm_pg = int(request.args.get("dm_page") or 1)
        dm_messages_list, dm_page, dm_total_pages, dm_total = _dm_inbox(is_admin=True, page=_dm_pg)

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
        dm_unread_count = _dm_unread_count(user_id=user["id"])
        dm_latest_preview = _dm_latest_preview(user_id=user["id"])
        if dm_view == "messages":
            _dm_pg = int(request.args.get("dm_page") or 1)
            dm_messages_list, dm_page, dm_total_pages, dm_total = _dm_inbox(user_id=user["id"], page=_dm_pg)

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
                    "SELECT id, title, study_type, status, created_at, study_output, qa_status, confidence_summary, final_report, output_language FROM studies WHERE user_id = ? AND title LIKE ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
                    (user["id"], f"%{studies_q}%", PAGE_SIZE, s_offset),
                ).fetchall()
            ]
        else:
            studies = [
                dict(r)
                for r in conn.execute(
                    "SELECT id, title, study_type, status, created_at, study_output, qa_status, confidence_summary, final_report, output_language FROM studies WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
                    (user["id"], PAGE_SIZE, s_offset),
                ).fetchall()
            ]

        configure_id = request.args.get("configure")
        if configure_id:
            _any_study = conn.execute(
                "SELECT id, status FROM studies WHERE id = ? AND user_id = ?",
                (configure_id, user["id"]),
            ).fetchone()
            if _any_study and _any_study["status"] != "draft":
                conn.close()
                return redirect(url_for("index", token=token, view_output=configure_id))
            row = conn.execute(
                "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
                (configure_id, user["id"]),
            ).fetchone()
            if row:
                configure_study = dict(row)
                if configure_study.get("survey_questions"):
                    try:
                        configure_study["survey_questions_list"] = json.loads(configure_study["survey_questions"])
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
                _cfg_ulang = request.cookies.get("pb_lang", "en")
                if _cfg_ulang not in VALID_LANGS:
                    _cfg_ulang = "en"
                all_personas = get_user_personas_list(conn, user["id"], user_lang=_cfg_ulang)
                attached_ids = set(cleaned_ids)
                available_personas = [
                    p
                    for p in all_personas
                    if p["persona_instance_id"] not in attached_ids
                ]

                chat_messages = [
                    dict(r)
                    for r in conn.execute(
                        "SELECT * FROM chat_messages WHERE study_id = ? ORDER BY id ASC",
                        (configure_study["id"],),
                    ).fetchall()
                ]

                chat_save_buttons = get_save_buttons(configure_study)
                chat_save_buttons = []

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
        _p_sel = "persona_instance_id, name, created_at, content_language, translated_content"
        if personas_q:
            _p_rows = conn.execute(
                f"SELECT {_p_sel} FROM personas WHERE user_id = ? AND (persona_instance_id LIKE ? OR name LIKE ?) ORDER BY id DESC LIMIT ? OFFSET ?",
                (user["id"], f"%{personas_q}%", f"%{personas_q}%", PAGE_SIZE, p_offset),
            ).fetchall()
        else:
            _p_rows = conn.execute(
                f"SELECT {_p_sel} FROM personas WHERE user_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
                (user["id"], PAGE_SIZE, p_offset),
            ).fetchall()
        _p_user_lang = request.cookies.get("pb_lang", "en")
        if _p_user_lang not in VALID_LANGS:
            _p_user_lang = "en"
        personas_list = []
        for _pr in _p_rows:
            _pi = {"persona_instance_id": _pr["persona_instance_id"], "name": _pr["name"], "created_at": _pr["created_at"]}
            _pcl = _pr["content_language"] or "en"
            if _pcl != _p_user_lang and _pr["translated_content"]:
                try:
                    _ptc = json.loads(_pr["translated_content"])
                    if isinstance(_ptc, dict) and _ptc.get("lang") == _p_user_lang:
                        _ptn = _ptc.get("fields", {}).get("name")
                        if not _ptn and _ptc.get("fields", {}).get("persona_summary"):
                            _ptn = _extract_translated_name(_ptc["fields"]["persona_summary"], _pr["name"])
                        if _ptn:
                            _pi["translated_name"] = _ptn
                except (json.JSONDecodeError, TypeError):
                    pass
            personas_list.append(_pi)

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
                _p_content_lang = view_persona.get("content_language") or "en"
                _p_user_lang = request.cookies.get("pb_lang", "en")
                if _p_user_lang not in VALID_LANGS:
                    _p_user_lang = "en"
                view_persona["needs_translation"] = (_p_content_lang != _p_user_lang)
                view_persona["content_lang_name"] = LANG_CODE_TO_NAME.get(_p_content_lang, _p_content_lang)
                view_persona["user_lang_name"] = LANG_CODE_TO_NAME.get(_p_user_lang, _p_user_lang)
                view_persona["user_lang"] = _p_user_lang
                view_persona["translated_fields"] = None
                if view_persona["needs_translation"] and view_persona.get("translated_content"):
                    try:
                        _tc = json.loads(view_persona["translated_content"])
                        if isinstance(_tc, dict) and _tc.get("lang") == _p_user_lang:
                            view_persona["translated_fields"] = _tc.get("fields", {})
                    except (json.JSONDecodeError, TypeError):
                        pass

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
                _persona_data_for_report = []
                if view_study_output.get("personas_used"):
                    try:
                        _pids = json.loads(view_study_output["personas_used"]) if isinstance(view_study_output["personas_used"], str) else view_study_output["personas_used"]
                    except (json.JSONDecodeError, TypeError):
                        _pids = []
                    if _pids:
                        _ph = ",".join("?" for _ in _pids)
                        _persona_data_for_report = [
                            dict(r) for r in conn.execute(
                                f"SELECT persona_id, persona_instance_id, name, persona_summary, demographic_frame, psychographic_profile, contextual_constraints, behavioural_tendencies FROM personas WHERE (persona_instance_id IN ({_ph}) OR persona_id IN ({_ph})) AND user_id = ? ORDER BY name",
                                _pids + _pids + [user["id"]],
                            ).fetchall()
                        ]
                view_study_output["report_sections"] = build_structured_report(
                    view_study_output,
                    followups=view_study_output["followups"],
                    version=report_version,
                    uploaded_filenames=all_upload_names,
                    persona_data=_persona_data_for_report,
                )
                if view_study_output.get("study_type") == "synthetic_survey":
                    _sq_raw = view_study_output.get("survey_questions") or "[]"
                    try:
                        view_study_output["survey_questions_parsed"] = json.loads(_sq_raw) if isinstance(_sq_raw, str) else _sq_raw
                    except (json.JSONDecodeError, TypeError):
                        view_study_output["survey_questions_parsed"] = []

                _out_lang = view_study_output.get("output_language") or "en"
                _user_lang = request.cookies.get("pb_lang", "en")
                if _user_lang not in VALID_LANGS:
                    _user_lang = "en"
                view_study_output["user_lang"] = _user_lang
                view_study_output["user_lang_name"] = LANG_CODE_TO_NAME.get(_user_lang, _user_lang)
                view_study_output["output_lang_name"] = LANG_CODE_TO_NAME.get(_out_lang, _out_lang)

                _translated_raw = view_study_output.get("translated_output")
                view_study_output["translation_cached"] = False
                view_study_output["translated_text"] = ""
                view_study_output["translated_sections"] = {}
                _trans_display_lang = _user_lang
                if _translated_raw:
                    try:
                        _tdata = json.loads(_translated_raw)
                        if isinstance(_tdata, dict) and _tdata.get("sections"):
                            _cached_lang = _tdata.get("lang", "en")
                            if _cached_lang == _user_lang or _cached_lang != _out_lang:
                                view_study_output["translation_cached"] = True
                                view_study_output["translated_text"] = _tdata.get("text", "")
                                view_study_output["translated_sections"] = _tdata.get("sections", {})
                                _trans_display_lang = _cached_lang
                    except (json.JSONDecodeError, TypeError):
                        pass
                view_study_output["show_translate"] = (_out_lang != "en") or (_out_lang != _user_lang)
                view_study_output["translation_lang_name"] = LANG_CODE_TO_NAME.get(_trans_display_lang, _trans_display_lang)

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
        health_map={hs['model_id']: hs['status'] for hs in health_status_list},
        latest_weekly_report=latest_weekly_report,
        latest_health_check=latest_health_check,
        mark_recommendation=mark_recommendation,
        mark_recommendation_label=mark_recommendation_label,
        mark_recommendation_reason=mark_recommendation_reason,
        chat_messages=chat_messages,
        chat_save_buttons=chat_save_buttons,
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
        dm_view=dm_view,
        dm_messages_list=dm_messages_list,
        dm_page=dm_page,
        dm_total_pages=dm_total_pages,
        dm_total=dm_total,
        dm_unread_count=dm_unread_count,
        dm_admin_unread=_dm_unread_count(is_admin=True) if is_admin else 0,
        dm_latest_preview=dm_latest_preview,
        admin_email=ADMIN_EMAIL,
        monthly_study_limit=FREE_TIER_MONTHLY_LIMIT,
        test_user_email=TEST_USER_EMAIL,
        test_user_password=TEST_USER_PASSWORD,
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


@app.route("/survey-image/<path:filename>")
def serve_survey_image(filename):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return "Unauthorized", 403
    safe = os.path.basename(filename)
    parts = safe.split("_")
    if len(parts) < 2:
        return "Forbidden", 403
    raw_id = parts[0]
    if raw_id.startswith("s"):
        raw_id = raw_id[1:]
    try:
        study_id = int(raw_id)
    except (ValueError, TypeError):
        return "Forbidden", 403
    conn = get_db()
    study = conn.execute(
        "SELECT id FROM studies WHERE id = ? AND user_id = ?",
        (study_id, user["id"]),
    ).fetchone()
    conn.close()
    if not study:
        return "Forbidden", 403
    return send_from_directory(SURVEY_IMAGES_DIR, safe)


@app.route("/upload-survey-image/<int:study_id>/<int:q_index>/<side>", methods=["POST"])
def upload_survey_image(study_id, q_index, side):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return jsonify({"ok": False, "error": "You must be an active user."}), 403

    if side not in ("A", "B"):
        return jsonify({"ok": False, "error": "Side must be 'A' or 'B'."}), 400

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return jsonify({"ok": False, "error": "Draft study not found."}), 404
    if study["study_type"] != "synthetic_survey":
        conn.close()
        return jsonify({"ok": False, "error": "Survey images only apply to synthetic survey studies."}), 400
    conn.close()

    f = request.files.get("image")
    if not f or not f.filename:
        return jsonify({"ok": False, "error": "No image file provided."}), 400

    orig = f.filename.strip()
    ext = ""
    if "." in orig:
        ext = ("." + orig.rsplit(".", 1)[-1]).lower()
    if ext not in AB_IMAGE_LIMITS["allowed_extensions"]:
        return jsonify({"ok": False, "error": f"Only JPG/PNG images are allowed (got '{ext}')."}), 400

    data = f.read()
    if len(data) > AB_IMAGE_LIMITS["max_size_bytes"]:
        max_kb = AB_IMAGE_LIMITS["max_size_bytes"] // 1024
        return jsonify({"ok": False, "error": f"Image exceeds {max_kb}KB limit ({len(data) // 1024}KB)."}), 400

    import uuid as _uuid
    safe_name = f"s{study_id}_q{q_index}_{side}_{_uuid.uuid4().hex[:8]}{ext}"
    dest = os.path.join(SURVEY_IMAGES_DIR, safe_name)
    with open(dest, "wb") as out:
        out.write(data)

    return jsonify({"ok": True, "filename": safe_name, "side": side, "size": len(data)})


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


@app.route("/send-message", methods=["POST"])
def send_message():
    token = get_token()
    user, is_admin = get_session_data(token)
    ajax = _is_ajax(request)

    subject = (request.form.get("subject") or "").strip()
    body = (request.form.get("body") or "").strip()
    category = (request.form.get("category") or "").strip()

    if not subject or len(subject) > 30:
        err = "Subject is required (max 30 characters)."
        if ajax:
            return jsonify({"ok": False, "error": err}), 400
        return render_error(err)
    if not body or len(body) > 300:
        err = "Body is required (max 300 characters)."
        if ajax:
            return jsonify({"ok": False, "error": err}), 400
        return render_error(err)

    msgs = _load_dm_messages()
    new_msg = {
        "id": secrets.token_hex(8),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "subject": subject,
        "body": body,
        "category": category if category else "",
        "read": False,
    }

    if is_admin:
        recipient_user_id = request.form.get("recipient_user_id")
        if not recipient_user_id:
            err = "Recipient user is required."
            if ajax:
                return jsonify({"ok": False, "error": err}), 400
            return render_error(err)
        new_msg["sender_type"] = "admin"
        new_msg["sender_id"] = None
        new_msg["recipient_type"] = "user"
        new_msg["recipient_user_id"] = int(recipient_user_id)
    elif user and user["state"] == "active":
        new_msg["sender_type"] = "user"
        new_msg["sender_id"] = user["id"]
        new_msg["sender_name"] = user.get("username") or user.get("email") or f"User #{user['id']}"
        new_msg["recipient_type"] = "admin"
        new_msg["recipient_user_id"] = None
    else:
        err = "You must be logged in to send messages."
        if ajax:
            return jsonify({"ok": False, "error": err}), 403
        return render_error(err)

    msgs.append(new_msg)
    _save_dm_messages(msgs)

    if ajax:
        return jsonify({"ok": True, "message": "Message sent.", "msg": new_msg})

    if is_admin:
        return redirect(url_for("index", token=token, msg_sent="1"))
    return redirect(url_for("index", token=token, view="messages", msg_sent="1"))


@app.route("/mark-message-read", methods=["POST"])
def mark_message_read():
    token = get_token()
    user, is_admin = get_session_data(token)
    ajax = _is_ajax(request)

    msg_id = request.form.get("msg_id") or ""
    if not msg_id:
        if ajax:
            return jsonify({"ok": False, "error": "Missing message ID."}), 400
        return render_error("Missing message ID.")

    msgs = _load_dm_messages()
    found = False
    for m in msgs:
        if m.get("id") == msg_id:
            if is_admin and m.get("recipient_type") == "admin":
                m["read"] = True
                found = True
            elif user and m.get("recipient_user_id") == user["id"] and m.get("recipient_type") == "user":
                m["read"] = True
                found = True
            break

    if found:
        _save_dm_messages(msgs)

    if ajax:
        return jsonify({"ok": True})

    if is_admin:
        return redirect(url_for("index", token=token))
    return redirect(url_for("index", token=token, view="messages"))


@app.route("/admin/change-password", methods=["POST"])
def admin_change_password():
    global ADMIN_PASSWORD
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")

    current_pw = (request.form.get("current_password") or "").strip()
    new_pw = (request.form.get("new_password") or "").strip()
    confirm_pw = (request.form.get("confirm_password") or "").strip()

    if current_pw != ADMIN_PASSWORD:
        return render_error("Current password is incorrect.")
    if not new_pw or len(new_pw) < 6:
        return render_error("New password must be at least 6 characters.")
    if new_pw != confirm_pw:
        return render_error("New passwords do not match.")

    ADMIN_PASSWORD = new_pw
    os.environ["ADMIN_PASSWORD"] = new_pw
    return redirect(url_for("index", token=token))


@app.route("/admin/update-quota", methods=["POST"])
def admin_update_quota():
    global FREE_TIER_MONTHLY_LIMIT
    token = get_token()
    _, is_admin = get_session_data(token)
    ajax = _is_ajax(request)
    if not is_admin:
        if ajax:
            return jsonify({"ok": False, "error": "Admin access required."}), 403
        return render_error("Admin access required.")

    try:
        new_limit = int(request.form.get("monthly_limit", "").strip())
    except (ValueError, AttributeError):
        if ajax:
            return jsonify({"ok": False, "error": "Invalid number."}), 400
        return render_error("Invalid number for monthly limit.")
    if new_limit < 1 or new_limit > 9999:
        if ajax:
            return jsonify({"ok": False, "error": "Limit must be between 1 and 9999."}), 400
        return render_error("Monthly limit must be between 1 and 9999.")

    FREE_TIER_MONTHLY_LIMIT = new_limit
    if ajax:
        return jsonify({"ok": True})
    return redirect(url_for("index", token=token))


@app.route("/admin/set-email", methods=["POST"])
def admin_set_email():
    global ADMIN_EMAIL
    token = get_token()
    _, is_admin = get_session_data(token)
    if not is_admin:
        return render_error("Admin access required.")

    email = (request.form.get("admin_email") or "").strip()
    ADMIN_EMAIL = email
    os.environ["ADMIN_EMAIL"] = email
    return redirect(url_for("index", token=token))


RESEARCH_BRIEF_FIELDS = [
    ("business_problem", "Business Problem"),
    ("decision_to_support", "Decision to Support"),
    ("known_vs_unknown", "Product / Concept"),
    ("target_audience", "Target Audience"),
    ("study_fit", "Market / Geography"),
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

    _dup_conn = get_db()
    _dup = _dup_conn.execute(
        "SELECT id FROM studies WHERE user_id = ? AND LOWER(title) = LOWER(?)",
        (user["id"], title),
    ).fetchone()
    _dup_conn.close()
    if _dup:
        return render_error(f'A study titled "{title}" already exists. Please choose a different title.')
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
            or not top_hypotheses
        ):
            return render_error(
                "All survey brief fields are required (Decision to Support, Target Audience, Top Hypotheses)."
            )

        try:
            respondent_count = int(request.form.get("respondent_count", 100))
        except (ValueError, TypeError):
            respondent_count = 100
        try:
            question_count = int(request.form.get("question_count", 8))
        except (ValueError, TypeError):
            question_count = 8
        respondent_count = max(25, min(100, respondent_count))
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
                    f'"{field_label}" is required. All 5 anchors must be filled.',
                )
            brief[field_key] = val
        definition_useful_insight_val = (request.form.get("definition_useful_insight") or "").strip()

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
                definition_useful_insight_val,
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

    _dup_conn2 = get_db()
    _dup2 = _dup_conn2.execute(
        "SELECT id FROM studies WHERE user_id = ? AND LOWER(title) = LOWER(?)",
        (user["id"], title),
    ).fetchone()
    _dup_conn2.close()
    if _dup2:
        return render_error(f'A study titled "{title}" already exists. Please choose a different title.')

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
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "You must be an active user."}), 403
        return render_error("You must be an active user.")

    field = request.form.get("field", "").strip()
    value = (request.form.get("value") or "").strip()

    if field not in ("business_problem", "decision_to_support"):
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Invalid discovery field."}), 400
        return render_error("Invalid discovery field.")
    if not value:
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Value cannot be empty."}), 400
        return render_error("Value cannot be empty.")
    if len(value) > 300:
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Value must be 300 characters or fewer."}), 400
        return render_error("Value must be 300 characters or fewer.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Study not found or not in draft status."}), 404
        return render_error("Study not found or not in draft status.")

    conn.execute(f"UPDATE studies SET {field} = ? WHERE id = ?", (value, study_id))
    conn.commit()

    if _is_ajax(request):
        study = conn.execute("SELECT * FROM studies WHERE id = ?", (study_id,)).fetchone()
        study_dict = dict(study)
        auto_ben_precheck(conn, study_dict, user["id"])
        personas_used = normalize_personas_used(study_dict.get("personas_used"))
        precheck = _build_precheck_state(study_dict, len(personas_used))
        qa_status = study_dict.get("qa_status", "")
        qa_failures = []
        if qa_status == "precheck_failed" and study_dict.get("qa_notes"):
            try:
                qa_failures = json.loads(study_dict["qa_notes"])
            except (json.JSONDecodeError, TypeError):
                qa_failures = []
        resp = {
            "ok": True, "field": field, "value": value,
            "precheck": precheck,
            "qa_status": qa_status, "qa_failures": qa_failures,
        }
        bp_val = (study_dict.get("business_problem") or "").strip()
        ds_val = (study_dict.get("decision_to_support") or "").strip()
        if bp_val and ds_val and not study_dict.get("study_type"):
            rec_type, rec_label, rec_reason = get_mark_recommendation(bp_val, ds_val)
            resp["mark_recommendation"] = {
                "type": rec_type,
                "label": rec_label,
                "reason": rec_reason,
                "study_id": study_id,
            }
        conn.close()
        return jsonify(resp)

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
    ("known_vs_unknown", "Product / Concept"),
    ("target_audience", "Target Audience"),
    ("study_fit", "Market / Geography"),
]


def _parse_lisa_memo(raw_text):
    transcript = ""
    memo = ""
    memo_marker = "FIRST-PASS FINDINGS MEMO:"
    idx = raw_text.find(memo_marker)
    if idx >= 0:
        transcript = raw_text[:idx].strip()
        memo = raw_text[idx + len(memo_marker):].strip()
    else:
        transcript = raw_text.strip()
    if transcript.upper().startswith("TRANSCRIPT:"):
        transcript = transcript[len("TRANSCRIPT:"):].strip()
    memo_sections = {}
    if memo:
        current_key = None
        current_lines = []
        for line in memo.split("\n"):
            stripped = line.strip().lower()
            if stripped.startswith("key themes"):
                if current_key:
                    memo_sections[current_key] = "\n".join(current_lines).strip()
                current_key = "key_themes"
                current_lines = []
            elif stripped.startswith("strong vs exploratory"):
                if current_key:
                    memo_sections[current_key] = "\n".join(current_lines).strip()
                current_key = "strong_vs_exploratory"
                current_lines = []
            elif stripped.startswith("contradictions") or stripped.startswith("contradiction"):
                if current_key:
                    memo_sections[current_key] = "\n".join(current_lines).strip()
                current_key = "contradictions"
                current_lines = []
            elif stripped.startswith("candidate insights"):
                if current_key:
                    memo_sections[current_key] = "\n".join(current_lines).strip()
                current_key = "candidate_insights"
                current_lines = []
            elif stripped.startswith("supporting excerpts"):
                if current_key:
                    memo_sections[current_key] = "\n".join(current_lines).strip()
                current_key = "supporting_excerpts"
                current_lines = []
            elif stripped.startswith("limitations") or stripped.startswith("limitation"):
                if current_key:
                    memo_sections[current_key] = "\n".join(current_lines).strip()
                current_key = "limitations"
                current_lines = []
            else:
                current_lines.append(line)
        if current_key:
            memo_sections[current_key] = "\n".join(current_lines).strip()
    return transcript, memo, memo_sections


def build_structured_report(
    study, followups=None, version=None, uploaded_filenames=None, persona_data=None
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

    study_type_reason_map = {
        "synthetic_survey": "A Synthetic Survey was selected to quantitatively measure attitudes, preferences, and behavioural intent across a defined respondent panel. This method is suited for studies requiring structured, comparable data points.",
        "synthetic_idi": "A Synthetic IDI (In-Depth Interview) was selected to explore individual perspectives in depth. This method surfaces nuanced reasoning, emotional drivers, and personal narratives that structured surveys cannot capture.",
        "synthetic_focus_group": "A Synthetic Focus Group was selected to capture group dynamics, social influence, and emergent consensus or disagreement. This method reveals how opinions shift when exposed to peer perspectives.",
    }
    why_lines = [study_type_reason_map.get(st, f"Study type: {st_label}")]
    bp = (study.get("business_problem") or "").strip()
    ds = (study.get("decision_to_support") or "").strip()
    if bp:
        why_lines.append(f"Business Problem: {bp}")
    if ds:
        why_lines.append(f"Decision to Support: {ds}")
    sections["why_study_type"] = "\n".join(why_lines)

    studied_lines = [f"Study Title: {title}", f"Study Type: {st_label}"]
    if st == "synthetic_survey":
        rc = study.get("respondent_count") or 0
        qc = study.get("question_count") or 0
        studied_lines.append(f"Respondent Count: {rc}")
        studied_lines.append(f"Question Count: {qc}")
        sq = []
        if study.get("survey_questions"):
            try:
                sq = [q for q in json.loads(study["survey_questions"]) if q is not None]
            except (json.JSONDecodeError, TypeError):
                sq = []
        if sq:
            studied_lines.append("Questions:")
            for i, q in enumerate(sq, 1):
                studied_lines.append(f"  {i}. {_get_q_prompt(q)}")
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

    transcript_text = ""
    memo_sections_parsed = {}
    findings_lines = []
    surprised_lines = []

    if is_placeholder:
        findings_lines.append("[Placeholder findings — not based on real data]")

    if st == "synthetic_survey" and output_raw:
        try:
            clean_json = output_raw.strip()
            if clean_json.startswith("```"):
                clean_json = clean_json.split("\n", 1)[-1]
                if clean_json.endswith("```"):
                    clean_json = clean_json[:-3]
                clean_json = clean_json.strip()
            brace_idx = clean_json.find("{")
            if brace_idx > 0:
                clean_json = clean_json[brace_idx:]
            last_brace = clean_json.rfind("}")
            if last_brace >= 0 and last_brace < len(clean_json) - 1:
                clean_json = clean_json[: last_brace + 1]
            survey_data = json.loads(clean_json)
            sq_meta = []
            if study.get("survey_questions"):
                try:
                    sq_meta = json.loads(study["survey_questions"]) if isinstance(study["survey_questions"], str) else study["survey_questions"]
                    if not isinstance(sq_meta, list):
                        sq_meta = []
                except (json.JSONDecodeError, TypeError):
                    sq_meta = []
            if isinstance(survey_data, dict) and "questions" in survey_data:
                for qi, qobj in enumerate(survey_data["questions"], 1):
                    q_text = qobj.get("q", f"Question {qi}")
                    q_type = qobj.get("type", "")
                    conf_label = "Indicative" if confidence else "Exploratory"
                    if confidence and confidence.get("Strong", 0) > 0 and qi == 1:
                        conf_label = "Strong"
                    findings_lines.append(f"Finding {qi} [{conf_label}]: {q_text}")
                    if confidence:
                        findings_lines.append(f"  Confidence: {conf_label} — based on QA assessment of response consistency and grounding coverage.")
                    if q_type == "ab_image" and qi - 1 < len(sq_meta) and sq_meta[qi - 1]:
                        imgs = (sq_meta[qi - 1] or {}).get("images") or {}
                        if imgs.get("A"):
                            findings_lines.append(f"  Image A: {imgs['A']}")
                        if imgs.get("B"):
                            findings_lines.append(f"  Image B: {imgs['B']}")
                    results = qobj.get("results", {})
                    for k, v in results.items():
                        if q_type == "ab_image":
                            img_ref = ""
                            if k == "A" and qi - 1 < len(sq_meta) and sq_meta[qi - 1]:
                                img_ref = f" ({(sq_meta[qi - 1] or {{}}).get('images', {{}}).get('A', '')})"
                            elif k == "B" and qi - 1 < len(sq_meta) and sq_meta[qi - 1]:
                                img_ref = f" ({(sq_meta[qi - 1] or {{}}).get('images', {{}}).get('B', '')})"
                            findings_lines.append(f"  - {k}{img_ref}: {v}")
                        else:
                            findings_lines.append(f"  - {k}: {v}")
                    findings_lines.append("")
                if survey_data.get("top_findings"):
                    findings_lines.append("Top Findings:")
                    for tf in survey_data["top_findings"]:
                        findings_lines.append(f"  - {tf}")
                    findings_lines.append("")
                if survey_data.get("risks_unknowns"):
                    findings_lines.append("Risks & Unknowns:")
                    for ru in survey_data["risks_unknowns"]:
                        findings_lines.append(f"  - {ru}")
                    findings_lines.append("")
        except (json.JSONDecodeError, TypeError, IndexError):
            findings_lines.append(
                "Raw output available but could not be parsed into structured findings."
            )
    elif st in ("synthetic_idi", "synthetic_focus_group") and output_raw:
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

        transcript_text, memo_raw, memo_sections_parsed = _parse_lisa_memo(clean_output)

        conf_summary_line = ""
        if confidence:
            strong_n = confidence.get("Strong", 0)
            ind_n = confidence.get("Indicative", 0)
            exp_n = confidence.get("Exploratory", 0)
            conf_summary_line = f"Confidence Assessment: Strong={strong_n}, Indicative={ind_n}, Exploratory={exp_n}"

        if memo_sections_parsed.get("key_themes"):
            findings_lines.append("Key Themes (from First-Pass Findings Memo):")
            if conf_summary_line:
                findings_lines.append(f"  {conf_summary_line}")
            theme_lines = [l.strip() for l in memo_sections_parsed["key_themes"].split("\n") if l.strip()]
            for ti, tl in enumerate(theme_lines, 1):
                conf_label = "Indicative"
                if confidence and confidence.get("Strong", 0) >= ti:
                    conf_label = "Strong"
                elif confidence and confidence.get("Exploratory", 0) > 0 and ti > (confidence.get("Strong", 0) + confidence.get("Indicative", 0)):
                    conf_label = "Exploratory"
                findings_lines.append(f"  Finding {ti} [{conf_label}]: {tl}")
                findings_lines.append(f"    Confidence: {conf_label} — based on QA assessment of response consistency and grounding coverage.")
            findings_lines.append("")

        if memo_sections_parsed.get("strong_vs_exploratory"):
            findings_lines.append("Signal Strength:")
            for line in memo_sections_parsed["strong_vs_exploratory"].split("\n"):
                if line.strip():
                    conf_tag = ""
                    low = line.strip().lower()
                    if "strong" in low:
                        conf_tag = " [Strong]"
                    elif "exploratory" in low:
                        conf_tag = " [Exploratory]"
                    else:
                        conf_tag = " [Indicative]"
                    findings_lines.append(f"  {line.strip()}{conf_tag}")
            findings_lines.append("")

        if memo_sections_parsed.get("contradictions"):
            findings_lines.append("Contradictions and Tensions:")
            for line in memo_sections_parsed["contradictions"].split("\n"):
                if line.strip():
                    findings_lines.append(f"  {line.strip()}")
            findings_lines.append("")

        if memo_sections_parsed.get("candidate_insights"):
            findings_lines.append("Candidate Insights Mapped to Brief:")
            for line in memo_sections_parsed["candidate_insights"].split("\n"):
                if line.strip():
                    findings_lines.append(f"  {line.strip()}")
            findings_lines.append("")

        if memo_sections_parsed.get("supporting_excerpts"):
            findings_lines.append("Supporting Excerpts:")
            for line in memo_sections_parsed["supporting_excerpts"].split("\n"):
                if line.strip():
                    findings_lines.append(f"  {line.strip()}")
            findings_lines.append("")

        if not memo_sections_parsed:
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
                    findings_lines.append(f"  Confidence: {conf_label} — based on QA assessment of response consistency and grounding coverage.")
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
                findings_lines.append(f"  Confidence: Exploratory — no structured memo available for confidence classification.")
                for line in clean_output.strip().split("\n")[:10]:
                    if line.strip():
                        findings_lines.append(f"  {line.strip()}")
                findings_lines.append("")

        if memo_sections_parsed.get("contradictions"):
            surprised_lines.append("Contradictions and Tensions:")
            for line in memo_sections_parsed["contradictions"].split("\n"):
                if line.strip():
                    surprised_lines.append(f"  {line.strip()}")
    elif output_raw:
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
        if clean_output.strip():
            findings_lines.append(f"Finding 1 [Exploratory]: Study output summary")
            for line in clean_output.strip().split("\n")[:10]:
                if line.strip():
                    findings_lines.append(f"  {line.strip()}")
            findings_lines.append("")

    if not findings_lines:
        findings_lines.append("No findings available.")
    sections["key_findings"] = "\n".join(findings_lines)

    if not surprised_lines:
        surprised_lines.append("No unexpected findings identified in this study.")
    sections["what_surprised_us"] = "\n".join(surprised_lines)

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
    if memo_sections_parsed.get("limitations"):
        risk_lines.append("")
        risk_lines.append("Limitations (from First-Pass Findings Memo):")
        for line in memo_sections_parsed["limitations"].split("\n"):
            if line.strip():
                risk_lines.append(f"  {line.strip()}")
    sections["risks_limits"] = "\n".join(risk_lines)

    _exec_gd = None
    _exec_gd_raw = study.get("exec_grounding_data")
    if _exec_gd_raw:
        try:
            _exec_gd = json.loads(_exec_gd_raw) if isinstance(_exec_gd_raw, str) else _exec_gd_raw
        except (json.JSONDecodeError, TypeError):
            pass

    grounding_lines = []
    covered = []
    not_covered = []
    for field, label in ANCHOR_FIELDS:
        val = (study.get(field) or "").strip()
        if val:
            covered.append(label)
        else:
            not_covered.append(label)
    if covered:
        grounding_lines.append(f"Anchors provided ({len(covered)}/{len(ANCHOR_FIELDS)}):")
        for c in covered:
            grounding_lines.append(f"  \u2713 {c}")
    if not_covered:
        grounding_lines.append(f"Anchors not provided ({len(not_covered)}):")
        for nc in not_covered:
            grounding_lines.append(f"  \u2717 {nc}")
    grounding_lines.append("")
    grounding_lines.append("Grounding Mode: Population-Grounded Discovery")
    grounding_lines.append("Population grounding serves as calibration context \u2014 informing persona realism,")
    grounding_lines.append("cultural tone, and feasibility constraints. It is NOT evidentiary support for")
    grounding_lines.append("specific claims or findings.")
    grounding_lines.append("")
    if len(covered) == len(ANCHOR_FIELDS):
        grounding_lines.append("Full grounding coverage achieved. All research brief anchors were defined before execution.")
    elif len(covered) >= 4:
        grounding_lines.append("Partial grounding coverage. Most anchors were defined but some gaps remain which may limit insight quality.")
    else:
        grounding_lines.append("Low grounding coverage. Several anchors were missing, which may significantly limit the quality and actionability of findings.")

    grounding_lines.append("")
    grounding_lines.append("Execution-Level Calibration:")
    if _exec_gd and _exec_gd.get("grounding_used"):
        _eg_sources = _exec_gd.get("sources", [])
        _eg_ts = _exec_gd.get("tier_stats", {})
        _eg_local = _eg_ts.get("local_web", {})
        _eg_general = _eg_ts.get("general_web", {})
        _total_retrieved = _eg_local.get("matched", 0) + _eg_general.get("matched", 0)
        grounding_lines.append(f"  Population calibration context was provided to the execution model.")
        grounding_lines.append(f"  Sources retrieved: {_total_retrieved} (shown: {len(_eg_sources)})")
        if _eg_sources:
            grounding_lines.append("  Calibration sources (used for realism, NOT cited as evidence):")
            for _egs in _eg_sources[:MLG_MAX_SOURCES_SHOWN]:
                _egs_title = _egs.get("title", "Untitled")
                _egs_url = _egs.get("url", "")
                _egs_class = _egs.get("source_class", "")
                _egs_label = " [Tertiary Calibration Only]" if _egs_class == "tertiary_calibration" else ""
                if _egs_url:
                    grounding_lines.append(f"    - {_egs_title} ({_egs_url}){_egs_label}")
                else:
                    grounding_lines.append(f"    - {_egs_title}{_egs_label}")
    else:
        grounding_lines.append("  No execution-level calibration context was available for this study.")

    sections["grounding_coverage"] = "\n".join(grounding_lines)

    source_lines = [
        "IMPORTANT: All findings in this report are simulated and exploratory.",
        "They were generated by AI-driven synthetic personas, not real human participants.",
        "No findings should be treated as externally validated evidence.",
        "",
        "Methodology:",
        "  AI Model: LLM-generated synthetic research output",
        "  Persona Profiles: Generated from user-defined demographic and psychographic parameters",
        "  Grounding Mode: Population-Grounded Discovery (calibration context only, not evidentiary support)",
    ]
    if study.get("personas_used"):
        try:
            pids = (
                json.loads(study["personas_used"])
                if isinstance(study["personas_used"], str)
                else study["personas_used"]
            )
            if pids:
                source_lines.append(f"  Personas Used: {', '.join(pids)}")
        except (json.JSONDecodeError, TypeError):
            pass
    if uploaded_filenames:
        source_lines.append("")
        source_lines.append("User-Uploaded Reference Documents:")
        for uf in uploaded_filenames:
            source_lines.append(f"  - {uf}")
    source_lines.append("")
    source_lines.append("Note: Population grounding sources (web searches, demographic data) were used")
    source_lines.append("as calibration context for persona realism and are NOT cited as evidence for")
    source_lines.append("any specific finding. Only objective, verifiable population-level facts")
    source_lines.append("(e.g., regulations, demographic statistics, historical events) may be cited")
    source_lines.append("as external evidence.")
    if _exec_gd and _exec_gd.get("grounding_used") and _exec_gd.get("sources"):
        source_lines.append("")
        source_lines.append("Execution Calibration Sources (NOT evidence \u2014 used for population realism only):")
        for _egs in _exec_gd["sources"][:MLG_MAX_SOURCES_SHOWN]:
            _egs_title = _egs.get("title", "Untitled")
            _egs_url = _egs.get("url", "")
            _egs_class = _egs.get("source_class", "")
            _egs_label = " [Tertiary Calibration Only]" if _egs_class == "tertiary_calibration" else ""
            if _egs_url:
                source_lines.append(f"  - {_egs_title} ({_egs_url}){_egs_label}")
            else:
                source_lines.append(f"  - {_egs_title}{_egs_label}")
    source_lines.append("")
    sections["sources_citations"] = "\n".join(source_lines)

    ctx_src_lines = []
    _ctx_sources_data = (_exec_gd or {}).get("context_sources", [])
    if _ctx_sources_data:
        ctx_src_lines.append("Context Sources (NOT Evidence)")
        ctx_src_lines.append("")
        ctx_src_lines.append("The following media articles were retrieved to provide context for consumer")
        ctx_src_lines.append("reaction realism. They represent narratives consumers may have encountered.")
        ctx_src_lines.append("They are NOT evidence, NOT validation, and do NOT support any specific finding.")
        ctx_src_lines.append("")
        for _csi, _csv in enumerate(_ctx_sources_data, 1):
            _csv_title = _csv.get("title", "Untitled")
            _csv_url = _csv.get("url", "")
            if _csv_url:
                ctx_src_lines.append(f"  {_csi}. {_csv_title}")
                ctx_src_lines.append(f"     {_csv_url}")
            else:
                ctx_src_lines.append(f"  {_csi}. {_csv_title}")
        ctx_src_lines.append("")
        ctx_src_lines.append("These sources were used for context calibration only and must not be")
        ctx_src_lines.append("cited as evidence or validation for any finding in this report.")
        ctx_src_lines.append("Context sources did not influence population definition, persona")
        ctx_src_lines.append("selection, or persona creation. They were retrieved after population")
        ctx_src_lines.append("grounding was complete and were used solely to inform reaction realism.")
    sections["context_sources"] = "\n".join(ctx_src_lines)

    persona_summary_lines = []
    if persona_data:
        for p in persona_data:
            p_name = p.get("name", "Unknown")
            p_id = p.get("persona_id", "")
            persona_summary_lines.append(f"{p_name} ({p_id})")
            if p.get("persona_summary"):
                persona_summary_lines.append(f"  {p['persona_summary']}")
            attrs = []
            if p.get("demographic_frame"):
                attrs.append(("Demographics", p["demographic_frame"]))
            if p.get("psychographic_profile"):
                attrs.append(("Psychographics", p["psychographic_profile"]))
            if p.get("contextual_constraints"):
                attrs.append(("Context", p["contextual_constraints"]))
            if p.get("behavioural_tendencies"):
                attrs.append(("Behaviour", p["behavioural_tendencies"]))
            for attr_label, attr_val in attrs:
                short = attr_val.strip().replace("\n", " ")
                if len(short) > 200:
                    short = short[:197] + "..."
                persona_summary_lines.append(f"  - {attr_label}: {short}")
            persona_summary_lines.append("")
    if persona_summary_lines:
        sections["persona_summaries"] = "\n".join(persona_summary_lines)
    else:
        sections["persona_summaries"] = ""

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

    if st in ("synthetic_idi", "synthetic_focus_group") and transcript_text:
        qa_s = study.get("qa_status") or ""
        if qa_s in ("pass", "downgrade"):
            sections["transcript_appendix"] = transcript_text

    sections["version"] = version
    sections["max_version"] = max_version

    return sections


CJK_FONT_PATH = os.path.join(
    os.path.dirname(__file__), "fonts", "NotoSansCJK-Regular.ttc"
)
THAI_FONT_PATH = os.path.join(
    os.path.dirname(__file__), "fonts", "NotoSansThai.ttf"
)
TRAD_MARKERS = set("漢歡測臺體繁廣")
SIMP_MARKERS = set("汉欢测台体简")


def _has_non_ascii(text):
    for ch in text:
        if ord(ch) > 127:
            return True
    return False


def _has_thai(text):
    for ch in text:
        cp = ord(ch)
        if 0x0E00 <= cp <= 0x0E7F:
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
            or 0xAC00 <= cp <= 0xD7AF
        ):
            return True
    return False


def _has_hangul(text):
    for ch in text:
        cp = ord(ch)
        if 0xAC00 <= cp <= 0xD7AF or 0x1100 <= cp <= 0x11FF or 0x3130 <= cp <= 0x318F:
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
    if _has_hangul(text):
        return "CJK_JP"
    chars = set(text)
    if chars & TRAD_MARKERS:
        return "CJK_TC"
    if chars & SIMP_MARKERS:
        return "CJK_SC"
    return "CJK_TC"


def _register_cjk_fonts(pdf):
    cjk_ok = False
    if os.path.exists(CJK_FONT_PATH):
        try:
            pdf.add_font("CJK_JP", fname=CJK_FONT_PATH, collection_font_number=0)
            pdf.add_font("CJK_SC", fname=CJK_FONT_PATH, collection_font_number=2)
            pdf.add_font("CJK_TC", fname=CJK_FONT_PATH, collection_font_number=3)
            cjk_ok = True
        except Exception:
            pass
    if os.path.exists(THAI_FONT_PATH):
        try:
            pdf.add_font("Thai", fname=THAI_FONT_PATH)
        except Exception:
            pass
    return cjk_ok


def _pick_unicode_font(text):
    if _has_thai(text):
        return "Thai"
    return _pick_cjk_font(text)


def _pdf_set_font(pdf, text, size, style="", cjk_available=True):
    if cjk_available and (_has_cjk(text) or _has_thai(text)):
        font_name = _pick_unicode_font(text)
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

    font_name = _pick_unicode_font(text)
    if font_name == "Thai":
        fallback_order = ["Thai", "CJK_JP", "CJK_SC", "CJK_TC"]
    else:
        fallback_order = ["CJK_JP", "CJK_SC", "CJK_TC"]
        try:
            fallback_order.remove(font_name)
        except ValueError:
            pass
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


def generate_report_pdf(study, sections, translated_sections=None, translation_lang_name=None):
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

    if translated_sections and translation_lang_name:
        output_lang_name = LANG_CODE_TO_NAME.get(study.get("output_language", "en"), "Original")
        pdf.set_font("Helvetica", "I", 9)
        pdf.set_text_color(80, 80, 80)
        pdf.cell(
            0, 6,
            f"Bilingual report: {output_lang_name} (original) + {translation_lang_name} (translated)",
            new_x="LMARGIN", new_y="NEXT", align="C",
        )
        pdf.set_text_color(0, 0, 0)
        pdf.ln(2)

    title_text = study.get("title", "Untitled Study")
    _pdf_write_text(pdf, title_text, 14, style="B", cjk_available=cjk_ok, h=10)
    st_label = STUDY_TYPE_LABELS.get(
        study.get("study_type", ""), study.get("study_type", "Unknown")
    )
    status_line = f"Type: {st_label}  |  Status: {study.get('status', 'unknown')}"
    _pdf_write_text(pdf, status_line, 10, cjk_available=cjk_ok, h=6)
    pdf.ln(6)

    _ts = translated_sections or {}

    heading_sections = [
        ("1. Executive Summary", sections.get("executive_summary", ""), None),
        ("2. Why This Study Type Was Chosen", sections.get("why_study_type", ""), None),
        ("3. What Was Studied", sections.get("what_was_studied", ""), None),
        ("4. Key Findings", sections.get("key_findings", ""), _ts.get("key_findings")),
        ("5. What Surprised Us", sections.get("what_surprised_us", ""), _ts.get("what_surprised_us")),
        ("6. Risks, Limits, and Unknowns", sections.get("risks_limits", ""), _ts.get("risks_limits")),
        ("7. Grounding Coverage Summary", sections.get("grounding_coverage", ""), _ts.get("grounding_coverage")),
        ("8. Sources and Citations", sections.get("sources_citations", ""), _ts.get("sources_citations")),
        ("8.5. Context Sources (NOT Evidence)", sections.get("context_sources", ""), _ts.get("context_sources")),
        ("9. Persona Summaries", sections.get("persona_summaries", ""), _ts.get("persona_summaries")),
    ]

    for item in heading_sections:
        heading, content, trans = item
        if not content:
            continue
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_fill_color(240, 240, 240)
        pdf.cell(0, 8, heading, new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.ln(2)
        _pdf_write_text(
            pdf, content, 10, cjk_available=cjk_ok, method="multi_cell", w=0, h=5
        )
        pdf.ln(2)

        if trans and translation_lang_name:
            pdf.set_draw_color(66, 133, 244)
            pdf.set_fill_color(235, 243, 254)
            x_start = pdf.get_x()
            y_start = pdf.get_y()
            pdf.rect(x_start, y_start, pdf.w - pdf.l_margin - pdf.r_margin, 7, style="DF")
            pdf.set_font("Helvetica", "BI", 10)
            pdf.set_text_color(30, 80, 180)
            pdf.cell(0, 7, f"  Translated ({translation_lang_name})", new_x="LMARGIN", new_y="NEXT")
            pdf.set_draw_color(0, 0, 0)
            pdf.ln(2)
            pdf.set_text_color(30, 80, 180)
            _pdf_write_text(
                pdf, trans, 10, cjk_available=cjk_ok, method="multi_cell", w=0, h=5
            )
            pdf.set_text_color(0, 0, 0)
            pdf.set_fill_color(240, 240, 240)

        pdf.ln(4)

    fu_sections = sections.get("followup_sections", [])
    for fus in fu_sections:
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_fill_color(240, 240, 240)
        heading = f"{9 + fus['round']}. Follow-up Round {fus['round']}"
        pdf.cell(0, 8, heading, new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.ln(2)
        _pdf_write_text(
            pdf, fus["content"], 10, cjk_available=cjk_ok, method="multi_cell", w=0, h=5
        )
        pdf.ln(4)

    if study.get("study_type") == "synthetic_survey":
        sq_meta = []
        if study.get("survey_questions"):
            try:
                sq_meta = json.loads(study["survey_questions"]) if isinstance(study["survey_questions"], str) else study["survey_questions"]
                if not isinstance(sq_meta, list):
                    sq_meta = []
            except (json.JSONDecodeError, TypeError):
                sq_meta = []
        ab_img_items = []
        for qi, sq in enumerate(sq_meta, 1):
            if sq and isinstance(sq, dict) and sq.get("type") == "ab_image" and sq.get("images"):
                ab_img_items.append((qi, sq))
        if ab_img_items:
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 14)
            pdf.cell(0, 10, "Appendix: A/B Image Reference", new_x="LMARGIN", new_y="NEXT")
            pdf.ln(4)
            for qi, sq in ab_img_items:
                prompt = sq.get("prompt") or f"Question {qi}"
                imgs = sq.get("images") or {}
                pdf.set_font("Helvetica", "B", 11)
                _pdf_write_text(pdf, f"Q{qi}: {prompt}", 11, style="B", cjk_available=cjk_ok, h=6)
                pdf.ln(2)
                for side_label in ("A", "B"):
                    fname = imgs.get(side_label, "")
                    pdf.set_font("Helvetica", "", 10)
                    pdf.cell(0, 5, f"  Image {side_label}: {fname}", new_x="LMARGIN", new_y="NEXT")
                    if fname:
                        fpath = os.path.join(SURVEY_IMAGES_DIR, os.path.basename(fname))
                        if os.path.isfile(fpath):
                            try:
                                img_x = pdf.get_x() + 10
                                pdf.image(fpath, x=img_x, w=40)
                                pdf.ln(2)
                            except Exception:
                                pdf.cell(0, 5, "    (thumbnail could not be embedded)", new_x="LMARGIN", new_y="NEXT")
                        else:
                            pdf.cell(0, 5, "    (file not found on disk)", new_x="LMARGIN", new_y="NEXT")
                pdf.ln(4)

    if sections.get("transcript_appendix"):
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 10, "Appendix: Transcript", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(4)
        pdf.set_font("Helvetica", "", 9)
        _pdf_write_text(
            pdf, sections["transcript_appendix"], 9, cjk_available=cjk_ok, method="multi_cell", w=0, h=4
        )

    if sections.get("translated_transcript") and translation_lang_name:
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 10, f"Appendix: Translated Transcript ({translation_lang_name})", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(4)
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(30, 80, 180)
        _pdf_write_text(
            pdf, sections["translated_transcript"], 9, cjk_available=cjk_ok, method="multi_cell", w=0, h=4
        )
        pdf.set_text_color(0, 0, 0)

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
        sq = [q for q in sq if (isinstance(q, str) and q.strip()) or (isinstance(q, dict) and _get_q_prompt(q))]
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
            "known_vs_unknown": "Product / Concept",
            "target_audience": "Target Audience",
            "study_fit": "Market / Geography",
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
        if study_type == "synthetic_idi" and pc > 1:
            print(
                f"QA_DEBUG qual study={study_id_debug} missing_anchors=[] persona_count={pc} decision=FAIL"
            )
            return {
                "decision": "FAIL",
                "notes": f"QA failed: IDI requires exactly 1 persona, found {pc}.",
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

        _qa_egd_raw = study_dict.get("exec_grounding_data")
        if _qa_egd_raw:
            try:
                _qa_egd = json.loads(_qa_egd_raw) if isinstance(_qa_egd_raw, str) else _qa_egd_raw
                _qa_egd_sources = _qa_egd.get("sources", [])
                _qa_wiki_sources = [s for s in _qa_egd_sources if _mlg_is_wikipedia(s.get("url", ""))]
                _qa_non_wiki = [s for s in _qa_egd_sources if not _mlg_is_wikipedia(s.get("url", ""))]
                if len(_qa_wiki_sources) > MLG_MAX_WIKIPEDIA_PER_BUNDLE:
                    gov_failures.append(
                        f"Wikipedia over-represented in execution grounding: "
                        f"{len(_qa_wiki_sources)} Wikipedia sources (max {MLG_MAX_WIKIPEDIA_PER_BUNDLE})."
                    )
                if len(_qa_wiki_sources) > 0 and len(_qa_non_wiki) == 0 and len(_qa_egd_sources) > 1:
                    gov_failures.append(
                        "Wikipedia is the sole grounding source when secondary authoritative sources should be available."
                    )

                _qa_ctx_sources = _qa_egd.get("context_sources", [])
                if _qa_ctx_sources:
                    _ctx_urls_set = {cs.get("url", "") for cs in _qa_ctx_sources if cs.get("url")}
                    _grnd_urls_set = {s.get("url", "") for s in _qa_egd_sources if s.get("url")}
                    _overlap = _ctx_urls_set & _grnd_urls_set
                    if _overlap:
                        gov_failures.append(
                            f"Context sources overlap with population grounding sources ({len(_overlap)} URLs). "
                            "Context and grounding must be separate."
                        )
                    for _cqs in _qa_ctx_sources:
                        _cq_class = (_cqs.get("citation_class") or "").strip().lower()
                        if _cq_class != "context":
                            gov_failures.append(
                                f"Context source '{_cqs.get('title', '?')}' has citation_class='{_cq_class or '(missing)'}' "
                                "instead of 'context'."
                            )
            except (json.JSONDecodeError, TypeError):
                pass

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
                    "SELECT id, mlg_data FROM grounding_traces WHERE trigger_event = 'persona_created' AND persona_id = ?",
                    (pid,),
                ).fetchone()
                if not pt_row:
                    gov_failures.append(
                        f"Missing Grounding Trace for persona_created (persona {pid})."
                    )
                elif pt_row["mlg_data"]:
                    try:
                        _pt_mlg = json.loads(pt_row["mlg_data"]) if isinstance(pt_row["mlg_data"], str) else pt_row["mlg_data"]
                        if _pt_mlg.get("context_sources"):
                            gov_failures.append(
                                f"Persona {pid} grounding trace contains context_sources — "
                                "context citations must NOT influence persona selection/creation."
                            )
                    except (json.JSONDecodeError, TypeError):
                        pass

        _excluded_fields = ["known_vs_unknown", "business_problem", "competitive_context"]
        if gt_row:
            _gt_reason = (gt_row["admin_source_reason_code"] or "").lower()
            for _ef in _excluded_fields:
                if _ef in _gt_reason:
                    gov_failures.append(
                        f"Grounding trace reason_code references excluded field '{_ef}' — "
                        "population grounding must use only Market/Geography + Target Audience."
                    )

        _output_lower = output.lower()
        _citation_markers = [
            "according to grounding",
            "grounding sources confirm",
            "grounding data shows",
            "as shown by population grounding",
            "population grounding proves",
            "grounding evidence",
        ]
        for _cm in _citation_markers:
            if _cm in _output_lower:
                gov_failures.append(
                    f"Output implies external evidence via grounding ('{_cm}'). "
                    "Population grounding is calibration context, not evidentiary support."
                )
                break

        _ctx_evidence_markers = [
            "context sources confirm",
            "context sources prove",
            "as evidenced by context",
            "context evidence shows",
            "validated by context sources",
            "context sources validate",
            "supported by context sources",
            "supported by media",
        ]
        for _cem in _ctx_evidence_markers:
            if _cem in _output_lower:
                gov_failures.append(
                    f"Output treats context sources as evidence ('{_cem}'). "
                    "Context sources are for reaction realism only, NOT evidentiary support."
                )
                break

        import re as _re_mod
        _ctx_ref_combined = (
            r"i (?:read|saw|heard|noticed) (?:somewhere|recently|that|about|in the news)"
            r"|(?:news|media|article|report)s? (?:say|said|mention|suggest|show|indicate)"
            r"|according to (?:a |an |the )?(?:news|media|article|report|story)"
        )
        _qa_egd_raw_ctx = study_dict.get("exec_grounding_data")
        _qa_had_ctx_sources = False
        if _qa_egd_raw_ctx:
            try:
                _qa_egd_ctx = json.loads(_qa_egd_raw_ctx) if isinstance(_qa_egd_raw_ctx, str) else _qa_egd_raw_ctx
                _qa_had_ctx_sources = bool(_qa_egd_ctx.get("context_sources"))
            except (json.JSONDecodeError, TypeError):
                pass
        if _qa_had_ctx_sources:
            _ctx_tag_norm = "[context — not evidence]"
            _ctx_tag_alt = "[context – not evidence]"
            _output_lines = output.split("\n")
            _unlabeled_count = 0
            for _ol_idx, _ol in enumerate(_output_lines):
                _ol_lower = _ol.lower()
                if _re_mod.search(_ctx_ref_combined, _ol_lower):
                    _nearby_text = "\n".join(
                        _output_lines[max(0, _ol_idx):min(len(_output_lines), _ol_idx + 3)]
                    ).lower()
                    if _ctx_tag_norm not in _nearby_text and _ctx_tag_alt not in _nearby_text:
                        _unlabeled_count += 1
            if _unlabeled_count > 0:
                gov_failures.append(
                    f"Output contains {_unlabeled_count} inline media/context reference(s) without the required "
                    "[Context — NOT evidence] label. Each inline context reference must carry this tag."
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
        required_anchors = [
            ("business_problem", "Business Problem"),
            ("decision_to_support", "Decision to Support"),
            ("market_geography", "Market / Geography"),
            ("product_concept", "Product / Concept"),
            ("target_audience", "Target Audience"),
        ]
        for field, label in required_anchors:
            val = get_v1a_value(study, field)
            if not val:
                failures.append(f"{label} is missing.")
        rc = study["respondent_count"] or 0
        qc = study["question_count"] or 0
        sq = []
        if study["survey_questions"]:
            try:
                sq = [q for q in json.loads(study["survey_questions"]) if q is not None]
            except (json.JSONDecodeError, TypeError):
                sq = []
        if rc < 25 or rc > 100:
            failures.append("Respondent count must be between 25 and 100.")
        if qc < 1 or qc > 12:
            failures.append("Question count must be between 1 and 12.")
        if len(sq) != qc:
            failures.append(f"Survey has {len(sq)} questions but needs exactly {qc}.")
        for qi, q in enumerate(sq, 1):
            if isinstance(q, dict) and q.get("type") == "ab_image":
                imgs = q.get("images") or {}
                for side_label in ("A", "B"):
                    ref = (imgs.get(side_label) or "").strip()
                    if not ref:
                        failures.append(f"Q{qi} (ab_image): image {side_label} not uploaded.")
                    else:
                        fpath = os.path.join(SURVEY_IMAGES_DIR, os.path.basename(ref))
                        if not os.path.isfile(fpath):
                            failures.append(f"Q{qi} (ab_image): image {side_label} file missing. Please re-upload.")
    elif st in ("synthetic_idi", "synthetic_focus_group"):
        anchor_labels = [
            ("business_problem", "Business Problem"),
            ("decision_to_support", "Decision to Support"),
            ("market_geography", "Market / Geography"),
            ("product_concept", "Product / Concept"),
            ("target_audience", "Target Audience"),
        ]
        for field, label in anchor_labels:
            val = get_v1a_value(study, field)
            if not val:
                failures.append(f"{label} is missing.")
        if st == "synthetic_idi":
            if persona_count > 1:
                failures.append(
                    f"IDI requires exactly 1 persona (currently {persona_count})."
                )
        else:
            if persona_count > 6:
                failures.append(
                    f"Focus Group allows max 6 personas (currently {persona_count})."
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
        min_p = 1 if st == "synthetic_idi" else 4
        if persona_count < min_p:
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
    _pdf_persona_data = []
    _pdf_study_dict = dict(study)
    if _pdf_study_dict.get("personas_used"):
        try:
            _pdf_pids = json.loads(_pdf_study_dict["personas_used"]) if isinstance(_pdf_study_dict["personas_used"], str) else _pdf_study_dict["personas_used"]
        except (json.JSONDecodeError, TypeError):
            _pdf_pids = []
        if _pdf_pids:
            _pdf_ph = ",".join("?" for _ in _pdf_pids)
            _pdf_persona_data = [
                dict(r) for r in conn.execute(
                    f"SELECT persona_id, persona_instance_id, name, persona_summary, demographic_frame, psychographic_profile, contextual_constraints, behavioural_tendencies FROM personas WHERE (persona_instance_id IN ({_pdf_ph}) OR persona_id IN ({_pdf_ph})) AND user_id = ? ORDER BY name",
                    _pdf_pids + _pdf_pids + [user["id"]],
                ).fetchall()
            ]
    conn.close()
    all_upload_names = upload_names + active_admin_names
    followups = [dict(f) for f in followup_rows]
    study_dict = _pdf_study_dict
    version_param = request.args.get("version")
    version = int(version_param) if version_param and version_param.isdigit() else None
    sections = build_structured_report(
        study_dict,
        followups=followups,
        version=version,
        uploaded_filenames=all_upload_names,
        persona_data=_pdf_persona_data,
    )

    _pdf_trans_sections = None
    _pdf_trans_lang_name = None
    _user_lang = request.cookies.get("pb_lang", "en")
    _output_lang = study_dict.get("output_language") or "en"
    _trans_raw = study_dict.get("translated_output")
    if _trans_raw:
        try:
            _tdata = json.loads(_trans_raw)
            if isinstance(_tdata, dict) and _tdata.get("sections"):
                _cached_lang = _tdata.get("lang", "en")
                if _cached_lang == _user_lang or _cached_lang != _output_lang:
                    _pdf_trans_sections = _tdata["sections"]
                    _pdf_trans_lang_name = LANG_CODE_TO_NAME.get(_cached_lang, _cached_lang)
                    if _tdata.get("text"):
                        sections["translated_transcript"] = _tdata["text"]
        except (json.JSONDecodeError, TypeError):
            pass

    pdf_bytes = generate_report_pdf(study_dict, sections, translated_sections=_pdf_trans_sections, translation_lang_name=_pdf_trans_lang_name)
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
                sq = [q for q in json.loads(study["survey_questions"]) if q is not None]
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
                sq = [q for q in json.loads(study["survey_questions"]) if q is not None]
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
        ]
        study_dict = dict(study)
        for field, label, prompt in anchors:
            val = get_v1a_value(study_dict, field)
            if not val:
                return f"Next missing item: {label}. {prompt} (Fill it in the Research Brief section above.)"
        if st == "synthetic_idi":
            if persona_count > 1:
                return f"You have {persona_count} persona(s) attached. IDI requires exactly 1 persona. Remove extras above."
        else:
            if persona_count > 6:
                return f"You have {persona_count} persona(s) attached. Focus Groups allow max 6 personas. Remove extras above."
        return "Everything looks complete! You can click 'Ready for QA Review' in the side panel when you're ready."

    return "Thanks for your message! I've noted your input — we'll use this as we shape the study together."


def _sync_translate_study_output(study_id, user_id, target_lang, output_lang):
    if output_lang == target_lang:
        return
    target_lang_name = LANG_CODE_TO_NAME.get(target_lang, target_lang)
    source_lang_name = LANG_CODE_TO_NAME.get(output_lang, output_lang)

    conn = get_db()
    study_dict = dict(conn.execute("SELECT * FROM studies WHERE id = ? AND user_id = ?", (study_id, user_id)).fetchone())
    existing = study_dict.get("translated_output")
    if existing:
        try:
            existing_data = json.loads(existing)
            if isinstance(existing_data, dict) and existing_data.get("lang") == target_lang and existing_data.get("sections"):
                conn.close()
                print(f"SYNC_TRANSLATE_SKIP study={study_id} reason=already_cached", flush=True)
                return
        except (json.JSONDecodeError, TypeError):
            pass

    raw_output = study_dict.get("final_report") or study_dict.get("study_output") or ""
    if not raw_output.strip():
        conn.close()
        return

    mc = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM model_config").fetchall()}
    translate_model = mc.get("mark_model", "")
    if not translate_model:
        conn.close()
        print(f"SYNC_TRANSLATE_SKIP study={study_id} reason=no_model", flush=True)
        return

    followup_rows = conn.execute("SELECT * FROM followups WHERE study_id = ? ORDER BY followup_round ASC", (study_id,)).fetchall()
    followups = [dict(f) for f in followup_rows]
    _personas_used_raw = study_dict.get("personas_used")
    _persona_data_for_report = []
    if _personas_used_raw:
        try:
            _pids = json.loads(_personas_used_raw) if isinstance(_personas_used_raw, str) else _personas_used_raw
            if _pids:
                _ph = ",".join("?" for _ in _pids)
                _persona_data_for_report = [
                    dict(r) for r in conn.execute(
                        f"SELECT persona_id, persona_instance_id, name, persona_summary, demographic_frame, psychographic_profile, contextual_constraints, behavioural_tendencies FROM personas WHERE (persona_instance_id IN ({_ph}) OR persona_id IN ({_ph})) AND user_id = ? ORDER BY name",
                        _pids + _pids + [user_id],
                    ).fetchall()
                ]
        except (json.JSONDecodeError, TypeError):
            pass

    report = build_structured_report(study_dict, followups=followups, uploaded_filenames=[], persona_data=_persona_data_for_report)

    sections_to_translate = {}
    for skey in ("key_findings", "what_surprised_us", "risks_limits",
                 "grounding_coverage", "sources_citations", "context_sources", "persona_summaries"):
        val = report.get(skey) or ""
        if val.strip():
            sections_to_translate[skey] = val
    for fus in report.get("followup_sections", []):
        fkey = f"followup_{fus['round']}"
        if fus.get("content", "").strip():
            sections_to_translate[fkey] = fus["content"]

    conn.close()

    translate_system = (
        f"You are a professional translator specializing in market research documents. "
        f"Translate the following research output from {source_lang_name} to {target_lang_name}. "
        f"Translate EVERYTHING including direct quotes, speaker dialogue, and supporting excerpts — "
        f"do NOT leave any quoted speech in the original language. "
        f"Preserve all formatting, section headers, speaker labels, and structure exactly. "
        f"Do not add commentary or explanations. "
        f"Translate the content faithfully, maintaining the analytical tone and research terminology."
    )

    try:
        translated = call_llm(
            translate_model,
            [{"role": "system", "content": translate_system}, {"role": "user", "content": raw_output}],
            purpose="sync_translate_study_output",
            timeout_seconds=120,
        )
    except Exception as e:
        print(f"SYNC_TRANSLATE_FAIL study={study_id} stage=full_output err={e}", flush=True)
        return

    if not translated or not translated.strip():
        print(f"SYNC_TRANSLATE_FAIL study={study_id} stage=full_output err=empty_result", flush=True)
        return

    translated_text = translated.strip()
    translated_sections = {}

    section_system = (
        f"You are a professional translator specializing in market research documents. "
        f"Translate the following text from {source_lang_name} to {target_lang_name}. "
        f"Translate EVERYTHING including direct quotes, speaker dialogue, and supporting excerpts — "
        f"do NOT leave any quoted speech in the original language. "
        f"Preserve all formatting, line breaks, bullet points, and structure exactly. "
        f"Do not add commentary or explanations. Output ONLY the translated text."
    )
    for skey, sval in sections_to_translate.items():
        try:
            section_translated = call_llm(
                translate_model,
                [{"role": "system", "content": section_system}, {"role": "user", "content": sval}],
                purpose="sync_translate_study_section",
                timeout_seconds=90,
            )
            if section_translated and section_translated.strip():
                translated_sections[skey] = section_translated.strip()
        except Exception as e:
            print(f"SYNC_TRANSLATE_SECTION_FAIL study={study_id} key={skey} err={e}", flush=True)

    translation_data = json.dumps({"lang": target_lang, "text": translated_text, "sections": translated_sections})
    conn = get_db()
    conn.execute("UPDATE studies SET translated_output = ? WHERE id = ? AND user_id = ?", (translation_data, study_id, user_id))
    conn.commit()
    conn.close()
    print(f"SYNC_TRANSLATE_DONE study={study_id} from={output_lang} to={target_lang} sections={len(translated_sections)}", flush=True)


def _sync_translate_personas(study_id, user_id, target_lang):
    conn = get_db()
    study_row = conn.execute("SELECT personas_used FROM studies WHERE id = ? AND user_id = ?", (study_id, user_id)).fetchone()
    if not study_row:
        conn.close()
        return
    personas_used_raw = study_row["personas_used"]
    if not personas_used_raw:
        conn.close()
        return
    try:
        pids = json.loads(personas_used_raw) if isinstance(personas_used_raw, str) else personas_used_raw
    except (json.JSONDecodeError, TypeError):
        conn.close()
        return
    if not pids:
        conn.close()
        return

    mc = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM model_config").fetchall()}
    translate_model = mc.get("mark_model", "")
    if not translate_model:
        conn.close()
        print(f"SYNC_TRANSLATE_PERSONAS_SKIP study={study_id} reason=no_model", flush=True)
        return

    _ph = ",".join("?" for _ in pids)
    persona_rows = conn.execute(
        f"SELECT persona_instance_id, name, content_language, translated_content, persona_summary, demographic_frame, psychographic_profile, contextual_constraints, behavioural_tendencies, confidence_and_limits FROM personas WHERE persona_instance_id IN ({_ph}) AND user_id = ?",
        pids + [user_id],
    ).fetchall()
    conn.close()

    target_lang_name = LANG_CODE_TO_NAME.get(target_lang, target_lang)
    translate_fields = ["name", "persona_summary", "demographic_frame", "psychographic_profile",
                        "contextual_constraints", "behavioural_tendencies", "confidence_and_limits"]

    section_system_tpl = (
        "You are a professional translator. Translate the following persona dossier sections "
        "from {source} to {target}. "
        "Preserve the === section_name === headers exactly as-is (in English). "
        "Translate only the content within each section. "
        "Maintain professional market research tone."
    )

    for pr in persona_rows:
        p_dict = dict(pr)
        pid = p_dict["persona_instance_id"]
        content_lang = p_dict.get("content_language") or "en"
        if content_lang == target_lang:
            continue
        existing = p_dict.get("translated_content")
        if existing:
            try:
                ex_data = json.loads(existing)
                if isinstance(ex_data, dict) and ex_data.get("lang") == target_lang:
                    continue
            except (json.JSONDecodeError, TypeError):
                pass

        source_lang_name = LANG_CODE_TO_NAME.get(content_lang, content_lang)
        original_text = ""
        for f in translate_fields:
            val = p_dict.get(f, "") or ""
            original_text += f"=== {f} ===\n{val}\n\n"

        if not original_text.strip():
            continue

        section_system = section_system_tpl.format(source=source_lang_name, target=target_lang_name)
        try:
            translated_raw = call_llm(
                translate_model,
                [{"role": "system", "content": section_system}, {"role": "user", "content": original_text}],
                purpose="sync_translate_persona",
                timeout_seconds=90,
            )
        except Exception as e:
            print(f"SYNC_TRANSLATE_PERSONA_FAIL study={study_id} pid={pid} err={e}", flush=True)
            continue

        if not translated_raw or not translated_raw.strip():
            continue

        translated_fields = {}
        for f in translate_fields:
            marker = f"=== {f} ==="
            idx = translated_raw.find(marker)
            if idx >= 0:
                start = idx + len(marker)
                next_marker_idx = translated_raw.find("===", start + 1)
                if next_marker_idx > 0:
                    section_text = translated_raw[start:next_marker_idx].strip()
                    if section_text.startswith("\n"):
                        section_text = section_text[1:]
                    last_newline = section_text.rfind("\n")
                    if last_newline > 0 and section_text[last_newline:].strip() == "":
                        section_text = section_text[:last_newline].strip()
                else:
                    section_text = translated_raw[start:].strip()
                translated_fields[f] = section_text

        translation_data = json.dumps({"lang": target_lang, "fields": translated_fields})
        conn2 = get_db()
        conn2.execute("UPDATE personas SET translated_content = ? WHERE persona_instance_id = ? AND user_id = ?", (translation_data, pid, user_id))
        conn2.commit()
        conn2.close()
        print(f"SYNC_TRANSLATE_PERSONA_DONE study={study_id} pid={pid} from={content_lang} to={target_lang}", flush=True)


@app.route("/translate-output/<int:study_id>", methods=["POST"])
def translate_output(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return jsonify({"ok": False, "error": "Unauthorized."}), 401

    target_lang = request.form.get("target_lang", "en")
    if target_lang not in VALID_LANGS:
        target_lang = "en"

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ?",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        return jsonify({"ok": False, "error": "Study not found."}), 404

    study_dict = dict(study)
    output_lang = study_dict.get("output_language") or "en"

    if output_lang == target_lang:
        conn.close()
        return jsonify({"ok": True, "skip": True, "reason": "Output already in target language."})

    existing = study_dict.get("translated_output")
    if existing:
        try:
            existing_data = json.loads(existing)
            if isinstance(existing_data, dict) and existing_data.get("lang") == target_lang and existing_data.get("sections"):
                conn.close()
                return jsonify({
                    "ok": True, "translated": existing_data.get("text", ""),
                    "sections": existing_data.get("sections", {}),
                    "lang": target_lang, "cached": True,
                })
        except (json.JSONDecodeError, TypeError):
            pass

    raw_output = study_dict.get("final_report") or study_dict.get("study_output") or ""
    if not raw_output.strip():
        conn.close()
        return jsonify({"ok": False, "error": "No study output to translate."}), 400

    target_lang_name = LANG_CODE_TO_NAME.get(target_lang, target_lang)
    source_lang_name = LANG_CODE_TO_NAME.get(output_lang, output_lang)

    mc = {
        r["key"]: r["value"]
        for r in conn.execute("SELECT key, value FROM model_config").fetchall()
    }
    translate_model = mc.get("mark_model", "")
    if not translate_model:
        conn.close()
        return jsonify({"ok": False, "error": "No model configured for translation."}), 500

    followup_rows = conn.execute(
        "SELECT * FROM followups WHERE study_id = ? ORDER BY followup_round ASC",
        (study_id,),
    ).fetchall()
    followups = [dict(f) for f in followup_rows]

    all_upload_names = []
    _personas_used_raw = study_dict.get("personas_used")
    _persona_data_for_report = []
    if _personas_used_raw:
        try:
            _pids = json.loads(_personas_used_raw) if isinstance(_personas_used_raw, str) else _personas_used_raw
            if _pids:
                _ph = ",".join("?" for _ in _pids)
                _persona_data_for_report = [
                    dict(r) for r in conn.execute(
                        f"SELECT persona_id, persona_instance_id, name, persona_summary, demographic_frame, psychographic_profile, contextual_constraints, behavioural_tendencies FROM personas WHERE (persona_instance_id IN ({_ph}) OR persona_id IN ({_ph})) AND user_id = ? ORDER BY name",
                        _pids + _pids + [user["id"]],
                    ).fetchall()
                ]
        except (json.JSONDecodeError, TypeError):
            pass

    report = build_structured_report(
        study_dict, followups=followups,
        uploaded_filenames=all_upload_names,
        persona_data=_persona_data_for_report,
    )

    sections_to_translate = {}
    for skey in ("key_findings", "what_surprised_us", "risks_limits",
                 "grounding_coverage", "sources_citations", "context_sources", "persona_summaries"):
        val = report.get(skey) or ""
        if val.strip():
            sections_to_translate[skey] = val
    for fus in report.get("followup_sections", []):
        fkey = f"followup_{fus['round']}"
        if fus.get("content", "").strip():
            sections_to_translate[fkey] = fus["content"]

    conn.close()

    translate_system = (
        f"You are a professional translator specializing in market research documents. "
        f"Translate the following research output from {source_lang_name} to {target_lang_name}. "
        f"Translate EVERYTHING including direct quotes, speaker dialogue, and supporting excerpts — "
        f"do NOT leave any quoted speech in the original language. "
        f"Preserve all formatting, section headers, speaker labels, and structure exactly. "
        f"Do not add commentary or explanations. "
        f"Translate the content faithfully, maintaining the analytical tone and research terminology."
    )

    try:
        translated = call_llm(
            translate_model,
            [
                {"role": "system", "content": translate_system},
                {"role": "user", "content": raw_output},
            ],
            purpose="translate_study_output",
            timeout_seconds=120,
        )
    except Exception as e:
        app.logger.warning(f"Translation failed for study {study_id}: {e}")
        return jsonify({"ok": False, "error": f"Translation failed: {str(e)[:200]}"}), 500

    if not translated or not translated.strip():
        return jsonify({"ok": False, "error": "Translation returned empty result."}), 500

    translated_text = translated.strip()
    translated_sections = {}

    section_system = (
        f"You are a professional translator specializing in market research documents. "
        f"Translate the following text from {source_lang_name} to {target_lang_name}. "
        f"Translate EVERYTHING including direct quotes, speaker dialogue, and supporting excerpts — "
        f"do NOT leave any quoted speech in the original language. "
        f"Preserve all formatting, line breaks, bullet points, and structure exactly. "
        f"Do not add commentary or explanations. Output ONLY the translated text."
    )
    for skey, sval in sections_to_translate.items():
        try:
            section_translated = call_llm(
                translate_model,
                [
                    {"role": "system", "content": section_system},
                    {"role": "user", "content": sval},
                ],
                purpose="translate_study_section",
                timeout_seconds=90,
            )
            if section_translated and section_translated.strip():
                translated_sections[skey] = section_translated.strip()
        except Exception as e:
            app.logger.warning(f"Section translation failed for study {study_id} key={skey}: {e}")

    translation_data = json.dumps({
        "lang": target_lang,
        "text": translated_text,
        "sections": translated_sections,
    })

    conn = get_db()
    conn.execute(
        "UPDATE studies SET translated_output = ? WHERE id = ? AND user_id = ?",
        (translation_data, study_id, user["id"]),
    )
    conn.commit()
    conn.close()

    print(f"TRANSLATE study_id={study_id} from={output_lang} to={target_lang} sections={len(translated_sections)}", flush=True)
    return jsonify({
        "ok": True, "translated": translated_text,
        "sections": translated_sections,
        "lang": target_lang, "cached": False,
    })


def _extract_translated_name(translated_summary, original_name):
    if not translated_summary:
        return original_name
    _pronouns = {"he", "she", "they", "it", "his", "her", "this", "that", "the"}
    first_line = translated_summary.strip().split("\n")[0]
    for sep in [" is ", " is a ", ", a ", ", an "]:
        idx = first_line.find(sep)
        if idx > 0:
            candidate = first_line[:idx].strip().rstrip(",").strip()
            if 1 < len(candidate) < 60 and not candidate.startswith("(") and candidate.lower() not in _pronouns:
                return candidate
    return original_name


@app.route("/translate-persona/<persona_pid>", methods=["POST"])
def translate_persona(persona_pid):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return jsonify({"ok": False, "error": "Unauthorized."}), 401

    target_lang = request.form.get("target_lang", "en")
    if target_lang not in VALID_LANGS:
        target_lang = "en"

    conn = get_db()
    persona = conn.execute(
        "SELECT * FROM personas WHERE persona_instance_id = ? AND user_id = ?",
        (persona_pid, user["id"]),
    ).fetchone()
    if not persona:
        conn.close()
        return jsonify({"ok": False, "error": "Persona not found."}), 404

    p_dict = dict(persona)
    content_lang = p_dict.get("content_language") or "en"
    if content_lang == target_lang:
        conn.close()
        return jsonify({"ok": True, "skip": True})

    existing = p_dict.get("translated_content")
    if existing:
        try:
            ex_data = json.loads(existing)
            if isinstance(ex_data, dict) and ex_data.get("lang") == target_lang:
                _ex_fields = ex_data.get("fields", {})
                if "name" not in _ex_fields and _ex_fields.get("persona_summary"):
                    _ex_fields["name"] = _extract_translated_name(_ex_fields["persona_summary"], p_dict.get("name", ""))
                    ex_data["fields"] = _ex_fields
                    conn.execute(
                        "UPDATE personas SET translated_content = ? WHERE persona_instance_id = ? AND user_id = ?",
                        (json.dumps(ex_data), persona_pid, user["id"]),
                    )
                    conn.commit()
                conn.close()
                return jsonify({"ok": True, "fields": _ex_fields, "lang": target_lang, "cached": True})
        except (json.JSONDecodeError, TypeError):
            pass

    translate_fields = ["name", "persona_summary", "demographic_frame", "psychographic_profile",
                        "contextual_constraints", "behavioural_tendencies",
                        "confidence_and_limits"]
    original_text = ""
    for f in translate_fields:
        val = p_dict.get(f, "") or ""
        original_text += f"=== {f} ===\n{val}\n\n"

    if not original_text.strip():
        conn.close()
        return jsonify({"ok": False, "error": "No persona content to translate."}), 400

    mc = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM model_config").fetchall()}
    translate_model = mc.get("mark_model", "")
    if not translate_model:
        conn.close()
        return jsonify({"ok": False, "error": "No model configured."}), 500

    target_lang_name = LANG_CODE_TO_NAME.get(target_lang, target_lang)
    source_lang_name = LANG_CODE_TO_NAME.get(content_lang, content_lang)

    translate_system = (
        f"You are a professional translator. Translate the following persona dossier sections "
        f"from {source_lang_name} to {target_lang_name}. "
        f"Preserve the === section_name === headers exactly as-is (in English). "
        f"Translate only the content within each section. "
        f"Maintain professional market research tone."
    )

    conn.close()
    try:
        translated_raw = call_llm(
            translate_model,
            [{"role": "system", "content": translate_system}, {"role": "user", "content": original_text}],
            purpose="translate_persona",
            timeout_seconds=90,
        )
    except Exception as e:
        return jsonify({"ok": False, "error": f"Translation failed: {str(e)[:200]}"}), 500

    if not translated_raw or not translated_raw.strip():
        return jsonify({"ok": False, "error": "Translation returned empty."}), 500

    translated_fields = {}
    for f in translate_fields:
        marker = f"=== {f} ==="
        idx = translated_raw.find(marker)
        if idx >= 0:
            start = idx + len(marker)
            next_marker_idx = translated_raw.find("===", start + 1)
            if next_marker_idx > 0:
                section_text = translated_raw[start:next_marker_idx].strip()
                if section_text.startswith("\n"):
                    section_text = section_text[1:]
                last_newline = section_text.rfind("\n")
                if last_newline > 0 and section_text[last_newline:].strip() == "":
                    section_text = section_text[:last_newline].strip()
            else:
                section_text = translated_raw[start:].strip()
            translated_fields[f] = section_text

    translation_data = json.dumps({"lang": target_lang, "fields": translated_fields})
    conn = get_db()
    conn.execute(
        "UPDATE personas SET translated_content = ? WHERE persona_instance_id = ? AND user_id = ?",
        (translation_data, persona_pid, user["id"]),
    )
    conn.commit()
    conn.close()

    print(f"TRANSLATE_PERSONA pid={persona_pid} from={content_lang} to={target_lang}", flush=True)
    return jsonify({"ok": True, "fields": translated_fields, "lang": target_lang, "cached": False})


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
        "survey_question_append",
    }
    if field not in valid_fields:
        conn.close()
        return render_error("Invalid field.")

    if field == "survey_question_append":
        sq = []
        if study["survey_questions"]:
            try:
                sq = [q for q in json.loads(study["survey_questions"]) if q is not None]
            except (json.JSONDecodeError, TypeError):
                sq = []
        qc = study["question_count"] or 0
        if len(sq) >= qc:
            conn.close()
            return render_error(
                f"Already have {len(sq)}/{qc} questions. Cannot add more."
            )
        sq.append({"type": "open", "prompt": value, "max_words": 50})
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

    conn = get_db()
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

    anchor_keys = [
        ("business_problem", "Business Problem"),
        ("decision_to_support", "Decision to Support"),
        ("market_geography", "Market / Geography"),
        ("product_concept", "Product / Concept"),
        ("target_audience", "Target Audience"),
    ]
    field_statuses = {}
    for key, label in anchor_keys:
        val = get_v1a_value(study_dict, key)
        field_statuses[key] = "saved" if val else "empty"

    study_type_val = study_dict.get("study_type") or ""
    bp_status = field_statuses["business_problem"]
    ds_status = field_statuses["decision_to_support"]
    all_saved = all(s == "saved" for s in field_statuses.values())

    if bp_status == "empty" or ds_status == "empty":
        study_phase = "discovery"
    elif not study_type_val:
        study_phase = "discovery"
    elif all_saved:
        study_phase = "ready_for_QA"
    else:
        study_phase = "brief"

    snapshot_lines = [f"Study Title: {study_dict.get('title', 'Untitled')}"]
    snapshot_lines.append(f"Study Type: {study_type_val or 'Not yet selected'}")
    snapshot_lines.append(f"Study Phase: {study_phase}")
    snapshot_lines.append("Research Brief field status (read-only):")
    for key, label in anchor_keys:
        snapshot_lines.append(f"  {label}: [{field_statuses[key]}]")
    if study_type_val == "synthetic_idi":
        personas_required = 1
    elif study_type_val == "synthetic_focus_group":
        personas_required = 4
    else:
        personas_required = 0
    snapshot_lines.append(f"Personas attached: {persona_count}")
    if personas_required > 0:
        snapshot_lines.append(f"Personas required: {personas_required} (Lisa fills gaps automatically)")
    oc_block, oc_keys = _extract_optional_context(study_dict)
    if oc_block:
        snapshot_lines.append("")
        snapshot_lines.append(oc_block)
    study_snapshot = "\n".join(snapshot_lines)
    status_summary = ",".join(f"{k}={v}" for k, v in field_statuses.items())
    print(f"MARK_FIELD_STATUS study={study_id} phase={study_phase} {status_summary}", flush=True)
    print(f"OPTIONAL_CONTEXT_INJECT_MARK study={study_id} included={'true' if oc_keys else 'false'} keys={','.join(oc_keys)}", flush=True)

    system_prompt = (
        "You are Mark, the Market Intelligence Copilot for Project Brainstorm.\n\n"
        "Project Brainstorm is an AI-native market research platform that helps users frame business problems "
        "and run disciplined, governed market research simulations (Synthetic Survey, Synthetic IDI, Synthetic Focus Group) "
        "to support real business decisions.\n\n"
        "Project Brainstorm is NOT:\n"
        "- a general-purpose chatbot\n"
        "- an academic research tutor\n"
        "- a prediction or forecasting engine\n"
        "- a brainstorming partner for unrelated topics\n\n"
        "Your role is to ORCHESTRATE research setup. You lead the process. The user provides inputs. "
        "You decide what is required next.\n\n"
        "Authority & behavior rules:\n"
        "1) Be professional, direct, and succinct.\n"
        "2) Do NOT explain generic research theory.\n"
        "3) Ask exactly ONE targeted next question per turn.\n"
        "4) Always move the study toward execution.\n"
        "5) If the conversation loops or is vague: summarize in one sentence and propose the next action.\n"
        "6) Do not invent details or speculate.\n\n"
        "Response constraints:\n"
        "- Replies must be 100 words or fewer.\n"
        "- One question only (unless all fields are [saved], in which case state completion instead).\n"
        "- Do NOT include any 'Suggested saves' section or footer in your reply.\n"
        "- End after your single question.\n\n"
        "Field awareness rules (MANDATORY):\n"
        "You receive field STATUS only (empty / saved), NOT field values.\n"
        "- You MAY reference which fields are missing and explain why they matter.\n"
        "- You MAY explain why a specific field is needed next.\n"
        "- You MAY encourage completion in clear, professional language.\n"
        "- You MAY maintain urgency when nearing completion.\n"
        "- You MUST NOT propose updates or draft text for a field.\n"
        "- You MUST NOT summarize chat content 'for saving'.\n"
        "- You MUST NOT ask the user to save, click save, or mention save buttons.\n"
        "- You MUST NOT mention UI persistence mechanics or buttons.\n"
        "- You MUST NOT infer field completion from chat content alone.\n"
        "- A field is complete ONLY when its status shows [saved].\n\n"
        "PHASE GATE (MANDATORY):\n"
        "If Study Type is 'Not yet selected':\n"
        "  a) If Business Problem is [empty]: ask ONLY for Business Problem.\n"
        "  b) Else if Decision to Support is [empty]: ask ONLY for Decision to Support.\n"
        "  c) Else (both BP and Decision are [saved]): recommend ONE study type "
        "(Synthetic Survey vs Synthetic IDI vs Synthetic Focus Group) based on the business problem, "
        "then ask the user to confirm their preferred study type. "
        "Do NOT proceed to any other questions.\n"
        "  d) You MUST NOT ask about Market/Geography, Product/Concept, Target Audience, "
        "or Definition of Useful Insight until Study Type is selected.\n\n"
        "Current study state:\n"
        + study_snapshot
        + "\n\n"
        "V1A required anchors for IDI / Focus Group:\n"
        "- Business Problem\n"
        "- Decision to Support\n"
        "- Market / Geography\n"
        "- Product / Concept\n"
        "- Target Audience\n\n"
        "Important:\n"
        "- Do NOT ask the user to fill 'Market / Geography' as a field; YOU explain fit when recommending a study type.\n"
        "- Do NOT treat 'Product / Concept' as required; if it appears, frame it as a hypothesis only.\n\n"
        "Task rules:\n"
        "1) Briefly acknowledge the user's latest message.\n"
        "2) Identify the single most important missing/unclear item based on field status.\n"
        "3) Ask ONE precise question to resolve it.\n\n"
        "Completion rule:\n"
        "When ALL required fields show [saved], say: "
        "'The Research Brief is complete. You can proceed to QA.'"
    )

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

    if mark_model_id:
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
        conn2 = get_db()
        conn2.execute(
            "UPDATE chat_messages SET message_text = ? WHERE id = ?",
            (fallback, placeholder_msg_id),
        )
        conn2.commit()
        conn2.close()

    return redirect(f"/?token={token}&configure={study_id}")


@app.route("/study-status/<int:study_id>")
def study_status(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user:
        return jsonify({"ok": False, "error": "Auth required."}), 403
    conn = get_db()
    row = conn.execute(
        "SELECT status, qa_status FROM studies WHERE id = ? AND user_id = ?",
        (study_id, user["id"]),
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({"ok": False, "error": "Not found."}), 404
    done = row["status"] in ("completed", "qa_blocked")
    return jsonify({"ok": True, "status": row["status"], "qa_status": row["qa_status"], "done": done})


@app.route("/run-study/<int:study_id>", methods=["POST"])
def run_study(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    ajax = _is_ajax(request)
    if not user or user["state"] != "active":
        if ajax:
            return jsonify({"ok": False, "error": "You must be an active user."}), 403
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        any_study = conn.execute(
            "SELECT id, status FROM studies WHERE id = ? AND user_id = ?",
            (study_id, user["id"]),
        ).fetchone()
        conn.close()
        if any_study:
            st = any_study["status"]
            if st == "running":
                if ajax:
                    return jsonify({"ok": True, "running": True, "message": "Study is already running."}), 200
                return redirect(url_for("index", token=token, view_output=study_id))
            if st in ("completed", "qa_blocked"):
                if ajax:
                    return jsonify({"ok": True, "already_done": True, "message": "Study already completed."}), 200
                return redirect(url_for("index", token=token, view_output=study_id))
        if ajax:
            return jsonify({"ok": False, "error": "Draft study not found or already executed."}), 404
        return render_error("Draft study not found or already executed.")

    if study["qa_status"] != "precheck_passed":
        conn.close()
        if ajax:
            return jsonify({"ok": False, "error": "Precheck has not passed yet."}), 400
        return render_error(
            "You must pass Ben's QA precheck before running the study. Click 'Ready for QA Review' first."
        )

    study_type = study["study_type"]
    if not study_type:
        conn.close()
        if ajax:
            return jsonify({"ok": False, "error": "You must select a study type before running the study."}), 400
        return render_error("You must select a study type before running the study.")

    conn.execute(
        "UPDATE studies SET status = 'running' WHERE id = ? AND status = 'draft'",
        (study_id,),
    )
    conn.commit()

    def _reset_to_draft():
        try:
            c2 = get_db()
            c2.execute("UPDATE studies SET status = 'draft' WHERE id = ? AND status = 'running'", (study_id,))
            c2.commit()
            c2.close()
        except Exception:
            pass

    personas_used = normalize_personas_used(study["personas_used"])
    persona_count = len(personas_used)

    if study_type == "synthetic_survey":
        r_count = study["respondent_count"] or 100
        q_count = study["question_count"] or 8
        if r_count < 25 or r_count > 100:
            conn.close()
            _reset_to_draft()
            if ajax:
                return jsonify({"ok": False, "error": "Survey respondent count must be between 25 and 100."}), 400
            return render_error("Survey respondent count must be between 25 and 100.")
        if q_count < 1 or q_count > 12:
            conn.close()
            _reset_to_draft()
            if ajax:
                return jsonify({"ok": False, "error": "Survey question count must be between 1 and 12."}), 400
            return render_error("Survey question count must be between 1 and 12.")
        sq = [q for q in json.loads(study["survey_questions"] or "[]") if q is not None]
        if len(sq) != q_count:
            conn.close()
            _reset_to_draft()
            if ajax:
                return jsonify({"ok": False, "error": f"Survey must have exactly {q_count} questions (currently {len(sq)})."}), 400
            return render_error(
                f"Survey must have exactly {q_count} questions (currently {len(sq)}). Save your questions first."
            )
    elif study_type in ("synthetic_idi", "synthetic_focus_group"):
        anchor_missing = []
        for col, label in [
            ("business_problem", "Business Problem"),
            ("decision_to_support", "Decision to Support"),
            ("known_vs_unknown", "Product / Concept"),
            ("target_audience", "Target Audience"),
            ("study_fit", "Market / Geography"),
        ]:
            val = (study[col] or "").strip()
            if not val:
                anchor_missing.append(label)
        if anchor_missing:
            conn.close()
            _reset_to_draft()
            msg = f"Cannot run: the following Research Brief anchors are missing: {', '.join(anchor_missing)}"
            if ajax:
                return jsonify({"ok": False, "error": msg}), 400
            return render_error(msg)

        max_allowed = 1 if study_type == "synthetic_idi" else 6
        _run_geo = (study["study_fit"] or "").strip()
        _run_admin_srcs = get_admin_web_sources(conn, status_filter="active")
        _persona_content_lang, _ = infer_market_language(_run_geo, _run_admin_srcs)
        needed = max_allowed - persona_count
        if needed > 0:
            auto_n = needed
            try:
                import random as _rng
                healthy_models, excluded_fail, pool_status = _get_healthy_pool_models(conn)
                if pool_status == "no_eligible":
                    conn.close()
                    _reset_to_draft()
                    msg_ne = "No eligible persona generation models in pool (GPT-family excluded). An admin must add non-GPT models to the persona model pool."
                    if ajax:
                        return jsonify({"ok": False, "error": msg_ne}), 500
                    return render_error(msg_ne)
                if pool_status == "all_fail":
                    conn.close()
                    _reset_to_draft()
                    msg_af = "All eligible persona models failed their last health check: " + ", ".join(excluded_fail) + ". An admin must run a health check or fix the model pool before running a study."
                    if ajax:
                        return jsonify({"ok": False, "error": msg_af}), 500
                    return render_error(msg_af)
                if excluded_fail:
                    fail_warning = "Models excluded from persona pool (health check FAIL): " + ", ".join(excluded_fail)
                    print(f"PERSONA_POOL_FAIL_EXCLUSION study={study_id} excluded={excluded_fail}", flush=True)
                    existing_notes = (study["qa_notes"] if "qa_notes" in study.keys() else "") or ""
                    try:
                        notes_list = json.loads(existing_notes) if existing_notes else []
                    except (json.JSONDecodeError, TypeError):
                        notes_list = []
                    if not isinstance(notes_list, list):
                        notes_list = []
                    notes_list.append(fail_warning)
                    conn.execute(
                        "UPDATE studies SET qa_notes = ? WHERE id = ?",
                        (json.dumps(notes_list), study_id),
                    )
                    conn.commit()
                _rng.shuffle(healthy_models)
                generated = None
                persona_gen_model = None
                study_snapshot = dict(study)
                conn.close()

                mlg_data = None
                try:
                    mlg_data = mlg_retrieve_for_persona(study_snapshot, study_id, user["id"])
                    print(f"MLG_COMPLETE study={study_id} sources={len(mlg_data.get('sources', []))} reason={mlg_data.get('reason_code', '')}", flush=True)
                except Exception as _mlg_err:
                    print(f"MLG_FAILED study={study_id} err={_mlg_err}", flush=True)
                    mlg_data = {
                        "sources": [],
                        "synthesized_summary": "",
                        "grounding_sources_text": "No live sources retrieved; persona relies on model priors and/or uploaded documents.",
                        "tier_stats": {
                            "user_uploads": {"found": 0},
                            "admin_uploads": {"found": 0},
                            "admin_web": {"attempted": 0, "matched": 0},
                            "local_web": {"attempted": 0, "matched": 0},
                            "general_web": {"attempted": 0, "matched": 0},
                        },
                        "reason_code": "user_uploads:none;admin_uploads:none;admin_web:error;local_web:error;general_web:error",
                        "admin_web_configured": 0,
                    }

                _mlg_summary = (mlg_data or {}).get("synthesized_summary", "")
                models_to_try = healthy_models[:3]
                for _model_idx, _try_model in enumerate(models_to_try):
                    persona_gen_model = _try_model
                    print(f"PERSONA_POOL_MODEL_SELECTED={persona_gen_model} study={study_id} pool_attempt={_model_idx+1}/{len(models_to_try)}", flush=True)
                    try:
                        generated = lisa_generate_personas(study_snapshot, auto_n, persona_gen_model, grounding_summary=_mlg_summary)
                        break
                    except Exception as _model_err:
                        print(f"PERSONA_MODEL_FAILED study={study_id} model={persona_gen_model} err={_model_err}", flush=True)
                        if _model_idx < len(models_to_try) - 1:
                            print(f"PERSONA_TRYING_NEXT_MODEL study={study_id}", flush=True)
                            continue
                        raise
                try:
                    generated = _check_and_fix_name_plausibility(generated, study_snapshot, persona_gen_model)
                except Exception as _np_err:
                    print(f"NAME_PLAUSIBILITY_OUTER_FAIL study={study_id} err={_np_err}", flush=True)
                try:
                    generated = _fix_json_structured_fields(generated, study_id=study_id)
                except Exception as _jsf_err:
                    print(f"JSON_TO_PROSE_OUTER_FAIL study={study_id} err={_jsf_err}", flush=True)
                _actual_lang = _detect_actual_content_language(generated, _persona_content_lang)
                if _actual_lang != _persona_content_lang:
                    print(f"LANG_DETECT_OVERRIDE study={study_id} expected={_persona_content_lang} actual={_actual_lang}", flush=True)
                    _persona_content_lang = _actual_lang
                conn = get_db()
                new_persona_ids = []
                _mlg_grounding_text = (mlg_data or {}).get("grounding_sources_text", "")
                def _pstr(val):
                    if val is None:
                        return ""
                    if isinstance(val, dict):
                        prose = _json_to_prose(json.dumps(val))
                        return prose if prose else json.dumps(val)
                    if isinstance(val, list):
                        return json.dumps(val)
                    return str(val)

                if len(generated) > auto_n:
                    print(f"PERSONA_GEN_CLIP study={study_id} generated={len(generated)} auto_n={auto_n}", flush=True)
                    generated = generated[:auto_n]
                for p_data in generated:
                    p_instance_id = f"P-{secrets.token_hex(4).upper()}"
                    p_persona_id = f"PID-{secrets.token_hex(4).upper()}"
                    provenance = f"persona_model={persona_gen_model}; selection_method=random_pool; orchestrated_by=Lisa"
                    conn.execute(
                        """INSERT INTO personas
                           (user_id, persona_id, version, persona_instance_id, name,
                            persona_summary, demographic_frame, psychographic_profile,
                            contextual_constraints, behavioural_tendencies,
                            ai_model_provenance, grounding_sources, confidence_and_limits,
                            content_language)
                           VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            user["id"],
                            p_persona_id,
                            p_instance_id,
                            _pstr(p_data.get("name", "Auto-Persona")),
                            _pstr(p_data.get("persona_summary", "")),
                            _pstr(p_data.get("demographic_frame", "")),
                            _pstr(p_data.get("psychographic_profile", "")),
                            _pstr(p_data.get("contextual_constraints", "")),
                            _pstr(p_data.get("behavioural_tendencies", "")),
                            provenance,
                            _mlg_grounding_text if _mlg_grounding_text else _pstr(p_data.get("grounding_sources", "")),
                            _pstr(p_data.get("confidence_and_limits", "")),
                            _persona_content_lang,
                        ),
                    )
                    create_grounding_trace(
                        conn,
                        trigger_event="persona_created",
                        study_id=str(study_id),
                        persona_id=p_instance_id,
                        mlg_data=mlg_data,
                    )
                    new_persona_ids.append(p_instance_id)

                all_persona_ids = list(personas_used) + new_persona_ids
                if len(all_persona_ids) > max_allowed:
                    print(f"PERSONA_TOTAL_CLIP study={study_id} total={len(all_persona_ids)} max={max_allowed}", flush=True)
                    all_persona_ids = all_persona_ids[:max_allowed]
                conn.execute(
                    "UPDATE studies SET personas_used = ? WHERE id = ?",
                    (json.dumps(all_persona_ids), study_id),
                )
                conn.commit()
                personas_used = all_persona_ids
                persona_count = len(personas_used)
                study = conn.execute(
                    "SELECT * FROM studies WHERE id = ?", (study_id,)
                ).fetchone()
                print(
                    f"AUTO_PERSONAS_GENERATED study={study_id} count={len(new_persona_ids)}"
                )
            except Exception as e:
                conn.close()
                _reset_to_draft()
                print(f"AUTO_PERSONAS_FAILED study={study_id} reason={e}", flush=True)
                msg_ap = "Auto-persona generation failed due to invalid generator output. Please try again or attach personas manually."
                if ajax:
                    return jsonify({"ok": False, "error": msg_ap}), 500
                return render_error(msg_ap)

        if study_type == "synthetic_idi":
            if persona_count > 1:
                conn.close()
                _reset_to_draft()
                if ajax:
                    return jsonify({"ok": False, "error": "IDI requires exactly 1 persona. Remove extras before running."}), 400
                return render_error("IDI requires exactly 1 persona. Remove extras before running.")
        elif study_type == "synthetic_focus_group":
            if persona_count < 4:
                conn.close()
                _reset_to_draft()
                if ajax:
                    return jsonify({"ok": False, "error": "Focus Group requires at least 4 personas."}), 400
                return render_error("Focus Group requires at least 4 personas.")
            if persona_count > 6:
                conn.close()
                _reset_to_draft()
                if ajax:
                    return jsonify({"ok": False, "error": "Focus Group allows max 6 personas."}), 400
                return render_error("Focus Group allows max 6 personas.")

    persona_names = []
    for pid in personas_used:
        p_row = conn.execute(
            "SELECT name FROM personas WHERE persona_instance_id = ?", (pid,)
        ).fetchone()
        if p_row:
            persona_names.append(p_row["name"])

    return _run_study_execute(conn, study, study_type, personas_used, persona_names, study_id, user, token, ajax)


def _run_study_execute(conn, study, study_type, personas_used, persona_names, study_id, user, token, ajax):
    _active_conn = [conn]
    try:
        return _run_study_core(_active_conn, study, study_type, personas_used, persona_names, study_id, user, token, ajax)
    except Exception as exc:
        print(f"RUN_STUDY_UNHANDLED study={study_id} error={exc}", flush=True)
        try:
            c2 = get_db()
            c2.execute("UPDATE studies SET status = 'draft' WHERE id = ? AND status = 'running'", (study_id,))
            c2.commit()
            c2.close()
        except Exception:
            pass
        if ajax:
            return jsonify({"ok": False, "error": "An unexpected error occurred during study execution. Please try again or contact admin."}), 500
        return render_error(f"Unexpected execution error: {exc}")
    finally:
        try:
            _active_conn[0].close()
        except Exception:
            pass


def _run_study_core(_active_conn, study, study_type, personas_used, persona_names, study_id, user, token, ajax):
    conn = _active_conn[0]
    _study_geo = (dict(study).get("study_fit") or "").strip()
    _core_admin_srcs = get_admin_web_sources(conn, status_filter="active")
    _study_lang_code, _study_lang_name = infer_market_language(_study_geo, _core_admin_srcs)

    _exec_grounding_summary = ""
    _exec_mlg_data = None
    try:
        _exec_study_snapshot = dict(study)
        _exec_user_id = user["id"] if isinstance(user, dict) else user.get("id", "")
        conn.close()
        _exec_mlg_data = mlg_retrieve_for_persona(_exec_study_snapshot, study_id, _exec_user_id)
        _exec_grounding_summary = (_exec_mlg_data or {}).get("synthesized_summary", "")
        print(f"MLG_EXEC_COMPLETE study={study_id} sources={len((_exec_mlg_data or {}).get('sources', []))}", flush=True)
        conn = get_db()
        _active_conn[0] = conn
    except Exception as _mlg_exec_err:
        print(f"MLG_EXEC_FAILED study={study_id} err={_mlg_exec_err}", flush=True)
        try:
            conn.execute("SELECT 1")
        except Exception:
            conn = get_db()
            _active_conn[0] = conn

    _exec_context_sources = []
    try:
        _ctx_study_snap = dict(study)
        _ctx_pc = (_ctx_study_snap.get("known_vs_unknown") or "").strip()
        _ctx_bp = (_ctx_study_snap.get("business_problem") or "").strip()
        conn.close()
        _exec_context_sources = retrieve_context_citations(_ctx_pc, _ctx_bp)
        conn = get_db()
        _active_conn[0] = conn
    except Exception as _ctx_err:
        print(f"CTX_CITATION_EXEC_FAILED study={study_id} err={_ctx_err}", flush=True)
        try:
            conn.execute("SELECT 1")
        except Exception:
            conn = get_db()
            _active_conn[0] = conn

    _pop_cal_block = ""
    if _exec_grounding_summary.strip():
        _pop_cal_block = (
            "\n\n=== Population Calibration Context (NOT evidence) ===\n"
            "The following population-level context is provided ONLY to calibrate tone, "
            "feasibility, and cultural realism. It must NOT be cited as evidence, quoted "
            "verbatim, or referenced in findings.\n\n"
            f"{_exec_grounding_summary}\n"
            "=== End Population Calibration Context ===\n"
        )

    _ctx_sources_block = ""
    if _exec_context_sources:
        _ctx_lines = [
            "\n\n=== Context Sources [Context — NOT evidence] ===",
            "The following media articles represent narratives consumers may have encountered.",
            "They provide CONTEXT for reaction realism ONLY. They are NOT evidence, NOT",
            "validation, and must NOT be cited as supporting any specific finding.",
            "",
            "INLINE CITATION RULES (MANDATORY):",
            "- You may reference at most 1-2 of these in persona dialogue where a persona",
            "  would plausibly have encountered the narrative (e.g. 'I read somewhere that...').",
            "- Every such inline reference MUST be immediately followed by the exact tag:",
            "  [Context — NOT evidence]",
            "- Do NOT omit this tag. Any inline reference to media narratives without the",
            "  explicit [Context — NOT evidence] label is a violation.",
            "- Never use context sources for personal preferences or emotional reactions.",
            "- Never describe context sources as 'evidence', 'proof', 'validation', or",
            "  'support' for any finding.",
            "",
        ]
        for _cs in _exec_context_sources:
            _ctx_lines.append(f"- {_cs.get('title', 'Untitled')} ({_cs.get('url', '')})")
        _ctx_lines.append("=== End Context Sources ===")
        _ctx_sources_block = "\n".join(_ctx_lines)

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
            sq = [q for q in json.loads(study_dict.get("survey_questions") or "[]") if q is not None]
            bp = (study_dict.get("business_problem") or "").strip()
            ds = (study_dict.get("decision_to_support") or "").strip()
            mg = (study_dict.get("study_fit") or "").strip()
            pc = (study_dict.get("known_vs_unknown") or "").strip()
            ta = (study_dict.get("target_audience") or "").strip()
            dui = (study_dict.get("definition_useful_insight") or "").strip()

            _lang_instruction = ""
            if _study_lang_code != "en":
                _lang_instruction = (
                    f"\n7. LANGUAGE: Produce all text content (top_findings, risks_unknowns, limitations, "
                    f"target_audience description, open-ended sample_responses and themes) in {_study_lang_name}. "
                    f"Keep JSON keys in English. This study targets the {mg} market."
                )

            q_lines = []
            for i, q in enumerate(sq):
                nq = _normalize_survey_question(q)
                if not nq:
                    continue
                qtype = nq.get("type", "open")
                prompt = _get_q_prompt(nq)
                line = f"  Q{i + 1} [{qtype}]: {prompt}"
                if qtype in ("likert", "mc", "ab") and nq.get("options"):
                    line += f"\n    Options: {' | '.join(nq['options'])}"
                elif qtype == "ab_image":
                    imgs = nq.get("images") or {}
                    line += f"\n    Image A: {imgs.get('A', '?')} | Image B: {imgs.get('B', '?')}"
                elif qtype == "range":
                    line += f"\n    Range: {nq.get('min', 0)} to {nq.get('max', 100)}"
                elif qtype == "open":
                    line += f"\n    Max words: {nq.get('max_words', 50)}"
                q_lines.append(line)
            questions_text = "\n".join(q_lines)

            lisa_system = (
                "You are Lisa, a senior quantitative research analyst at Project Brainstorm. "
                "You generate synthetic survey results.\n\n"
                "GROUNDING MODE: Population-Grounded Discovery\n"
                "Core principle: Respondents represent who EXISTS in the market, not who likes the product.\n\n"
                "SCOPE SEPARATION (MANDATORY):\n"
                "- Population-defining inputs (Market/Geography + Target Audience) calibrate tone, "
                "feasibility, and cultural constraints. They are calibration context, NOT evidentiary support.\n"
                "- Interpretation-only inputs (Product/Concept, Business Problem, Competitive Context) "
                "are discussion stimuli ONLY. They must NEVER bias responses toward positive reception.\n"
                "- Respondents may be neutral, indifferent, unaware, skeptical, or negative about the product.\n"
                "- Do NOT cite population grounding as evidence or attribute findings to grounding sources.\n"
                "- Do NOT reference population calibration context verbatim or quote it in findings.\n\n"
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
                '    "sources": ["AI-simulated synthetic respondents (not real participants)"]\n'
                "  },\n"
                '  "questions": [\n'
                "    {\n"
                '      "q": "<question text>",\n'
                '      "type": "<likert|mc|ab|ab_image|range|open>",\n'
                '      "results": { ... }\n'
                "    }\n"
                "  ],\n"
                '  "top_findings": ["<finding1>", ...],\n'
                '  "risks_unknowns": ["<risk1>", ...]\n'
                "}\n\n"
                "TYPE-SPECIFIC OUTPUT RULES:\n"
                "- likert/mc/ab: results is {\"<option>\": \"<percent>%\", ...}, percentages must sum to 100%.\n"
                "- ab_image: results is {\"A\": \"<percent>%\", \"B\": \"<percent>%\"}, must sum to 100%. No explanatory text.\n"
                "- range: results is {\"mean\": <number>, \"median\": <number>, \"std_dev\": <number>}, all within min/max bounds.\n"
                "- open: results is {\"themes\": [\"<theme1>\", ...], \"sample_responses\": [\"<resp1>\", ...]}, "
                "each sample response must be <= max_words words.\n\n"
                "3. Provide 3-5 top_findings and 2-4 risks_unknowns.\n"
                "4. limitations MUST state this is AI-simulated with synthetic respondents, not real participants.\n"
                "5. sources MUST be [\"AI-simulated synthetic respondents (not real participants)\"]. "
                "Do NOT list population grounding references as sources.\n"
                "6. Be realistic and culturally grounded for Asia-Pacific markets where relevant.\n"
                "7. All findings are simulated and exploratory. Do NOT imply external evidentiary support."
                + _lang_instruction
            )

            oc_block, oc_keys = _extract_optional_context(study_dict)
            oc_section = f"\n{oc_block}\n" if oc_block else ""
            print(f"OPTIONAL_CONTEXT_INJECT_LISA_EXEC study={study_id} type=synthetic_survey lang={_study_lang_code} included={'true' if oc_keys else 'false'} keys={','.join(oc_keys)}", flush=True)

            lisa_user = (
                f"Generate synthetic survey results for:\n"
                f"Title: {study_dict.get('title', 'Untitled Study')}\n"
                f"Business Problem: {bp or 'Not specified'}\n"
                f"Decision to Support: {ds or 'Not specified'}\n"
                f"Market / Geography: {mg or 'Not specified'}\n"
                f"Product / Concept: {pc or 'Not specified'}\n"
                f"Target Audience: {ta or 'Not specified'}\n"
                f"{_pop_cal_block}"
                f"{_ctx_sources_block}"
                f"Definition of Useful Insight: {dui or 'Not specified'}\n"
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
            _active_conn[0] = conn

            cleaned = raw_llm.strip()
            if cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[-1]
                if cleaned.endswith("```"):
                    cleaned = cleaned[:-3]
                cleaned = cleaned.strip()
            brace_idx = cleaned.find("{")
            if brace_idx > 0:
                cleaned = cleaned[brace_idx:]
            last_brace = cleaned.rfind("}")
            if last_brace >= 0 and last_brace < len(cleaned) - 1:
                cleaned = cleaned[: last_brace + 1]

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
                _active_conn[0] = conn

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
                ("known_vs_unknown", "Product / Concept"),
                ("target_audience", "Target Audience"),
                ("study_fit", "Market / Geography"),
            ]
            brief_text = ""
            for field, label in brief_fields:
                val = (study_dict.get(field) or "").strip()
                brief_text += f"{label}: {val or 'Not specified'}\n"

            _qual_lang_instruction = ""
            if _study_lang_code != "en":
                _qual_lang_instruction = (
                    f"\n5. LANGUAGE: Conduct the transcript in {_study_lang_name}, with respondents speaking naturally "
                    f"in {_study_lang_name} as appropriate for the {_study_geo} market. The Moderator may speak in {_study_lang_name} too. "
                    f"Keep section headers (TRANSCRIPT, FIRST-PASS FINDINGS MEMO, Key themes, etc.) in English for parseability. "
                    f"The Findings Memo analysis text should also be in {_study_lang_name}."
                )

            oc_block, oc_keys = _extract_optional_context(study_dict)
            if oc_block:
                brief_text += f"\n{oc_block}\n"
            print(f"OPTIONAL_CONTEXT_INJECT_LISA_EXEC study={study_id} type={study_type} lang={_study_lang_code} included={'true' if oc_keys else 'false'} keys={','.join(oc_keys)}", flush=True)

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
                "GROUNDING MODE: Population-Grounded Discovery\n"
                "Core principle: Personas represent who EXISTS in the market, not who likes the product.\n\n"
                "SCOPE SEPARATION (MANDATORY):\n"
                "- Population-defining inputs (Market/Geography + Target Audience) calibrate tone, "
                "feasibility, and cultural constraints. They are calibration context, NOT evidentiary support.\n"
                "- Interpretation-only inputs (Product/Concept, Business Problem, Competitive Context) "
                "are discussion stimuli ONLY. They must NEVER bias persona responses toward positive "
                "reception, awareness, or preference.\n"
                "- Product/Concept may be introduced ONLY as a topic for discussion. Respondents may be "
                "neutral, indifferent, unaware, skeptical, or negative about the product.\n"
                "- Competitive brands may surface ONLY if (a) raised organically by personas based on "
                "their lived context, OR (b) explicitly probed by moderator instructions.\n"
                "- Do NOT force competitor mentions from competitive context fields.\n"
                "- Do NOT cite population grounding as evidence or attribute findings to grounding sources.\n"
                "- Do NOT reference population calibration context verbatim or quote it in findings.\n\n"
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
                "4. Findings memo should be analytical and concise, not just a summary.\n"
                "5. All findings are simulated and exploratory. Do NOT imply external evidentiary support "
                "unless citing a specific, verifiable, objective population-level fact (e.g., regulation, "
                "demographic statistic, historical event)."
                + _qual_lang_instruction
            )

            lisa_user = (
                f"Study: {study_dict.get('title', 'Untitled Study')}\n"
                f"Type: {'In-Depth Interview (IDI)' if study_type == 'synthetic_idi' else 'Focus Group'}\n\n"
                f"Research Brief:\n{brief_text}\n"
                f"{_pop_cal_block}\n"
                f"{_ctx_sources_block}\n"
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
            _active_conn[0] = conn

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
                _active_conn[0] = conn

    if output is None:
        output = generate_placeholder_output(study_type, dict(study), persona_names)

    study_data = dict(study)
    study_data["study_output"] = output

    create_grounding_trace(
        conn,
        trigger_event="study_executed",
        study_id=str(study_id),
        mlg_data=_exec_mlg_data,
    )
    conn.commit()
    print(f"TRACE_CREATED study_id={study_id} trigger=study_executed lang={_study_lang_code} mlg_wired={'true' if _exec_mlg_data else 'false'}", flush=True)

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

    _exec_grounding_json = None
    if _exec_mlg_data or _exec_context_sources:
        try:
            _eg_save = {
                "sources": [
                    {"title": s.get("name", s.get("title", "")), "url": s.get("url", ""), "score": s.get("_quality_score", s.get("score", 0)), "source_class": s.get("_source_class", "")}
                    for s in ((_exec_mlg_data or {}).get("sources") or [])
                ],
                "synthesized_summary": ((_exec_mlg_data or {}).get("synthesized_summary") or "")[:500],
                "reason_code": (_exec_mlg_data or {}).get("reason_code", ""),
                "grounding_used": (_exec_mlg_data or {}).get("grounding_used", False),
                "tier_stats": (_exec_mlg_data or {}).get("tier_stats", {}),
                "context_sources": _exec_context_sources or [],
            }
            _exec_grounding_json = json.dumps(_eg_save)
        except Exception:
            pass

    conn.execute(
        """UPDATE studies SET status = ?, study_output = ?, qa_status = ?, qa_notes = ?,
           confidence_summary = ?, final_report = ?, output_language = ?,
           exec_grounding_data = ? WHERE id = ?""",
        (
            final_status,
            output,
            qa_decision.lower(),
            qa_notes,
            confidence_summary,
            final_report,
            _study_lang_code,
            _exec_grounding_json,
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
    _active_conn[0] = get_db()

    if final_status == "completed":
        _user_iface_lang = request.cookies.get("pb_lang", "en")
        if _user_iface_lang not in VALID_LANGS:
            _user_iface_lang = "en"
        _translate_target = _user_iface_lang if _user_iface_lang != _study_lang_code else ("en" if _study_lang_code != "en" else None)
        if _translate_target:
            print(f"SYNC_TRANSLATE_START study={study_id} output_lang={_study_lang_code} target_lang={_translate_target}", flush=True)
            try:
                _sync_translate_personas(study_id, user["id"], _translate_target)
            except Exception as _tp_err:
                print(f"SYNC_TRANSLATE_PERSONAS_ERR study={study_id} err={_tp_err}", flush=True)
            try:
                _sync_translate_study_output(study_id, user["id"], _translate_target, _study_lang_code)
            except Exception as _to_err:
                print(f"SYNC_TRANSLATE_OUTPUT_ERR study={study_id} err={_to_err}", flush=True)

    conn = _active_conn[0]

    if ajax:
        result = {
            "ok": True,
            "study_id": study_id,
            "study_type": study_type,
            "final_status": final_status,
            "qa_decision": qa_decision,
            "qa_notes": qa_notes,
            "study_output": output,
            "final_report": final_report,
        }
        try:
            result["confidence_labels"] = json.loads(confidence_summary)
        except (json.JSONDecodeError, TypeError):
            result["confidence_labels"] = None
        return jsonify(result)

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

    r_count = max(25, min(100, r_count))
    q_count = max(1, min(12, q_count))

    conn.execute(
        "UPDATE studies SET respondent_count = ?, question_count = ? WHERE id = ?",
        (r_count, q_count, study_id),
    )
    conn.commit()

    study = conn.execute("SELECT * FROM studies WHERE id = ?", (study_id,)).fetchone()
    study_dict = dict(study)
    auto_ben_precheck(conn, study_dict, user["id"])

    ajax = _is_ajax(request)
    if ajax:
        study = conn.execute("SELECT * FROM studies WHERE id = ?", (study_id,)).fetchone()
        study_dict = dict(study)
        personas_used = normalize_personas_used(study_dict.get("personas_used"))
        precheck = _build_precheck_state(study_dict, len(personas_used))
        qa_status = study_dict.get("qa_status", "")
        qa_failures = []
        if qa_status == "precheck_failed" and study_dict.get("qa_notes"):
            try:
                qa_failures = json.loads(study_dict["qa_notes"])
            except (json.JSONDecodeError, TypeError):
                qa_failures = []
        conn.close()
        return jsonify({
            "ok": True,
            "precheck": precheck,
            "qa_status": qa_status,
            "qa_failures": qa_failures,
        })

    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


@app.route("/save-survey-questions/<int:study_id>", methods=["POST"])
def save_survey_questions(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    ajax = _is_ajax(request)
    if not user or user["state"] != "active":
        if ajax:
            return jsonify({"ok": False, "error": "You must be an active user."}), 403
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        if ajax:
            return jsonify({"ok": False, "error": "Draft study not found."}), 404
        return render_error("Draft study not found.")
    if study["study_type"] != "synthetic_survey":
        conn.close()
        if ajax:
            return jsonify({"ok": False, "error": "Survey questions only apply to synthetic survey studies."}), 400
        return render_error("Survey questions only apply to synthetic survey studies.")

    q_count = study["question_count"] or 8

    raw_json = request.form.get("survey_questions_json", "").strip()
    if raw_json:
        try:
            questions = json.loads(raw_json)
        except (json.JSONDecodeError, TypeError):
            conn.close()
            if ajax:
                return jsonify({"ok": False, "error": "Invalid JSON for survey questions."}), 400
            return render_error("Invalid JSON for survey questions.")
        if not isinstance(questions, list):
            conn.close()
            if ajax:
                return jsonify({"ok": False, "error": "Survey questions must be a JSON array."}), 400
            return render_error("Survey questions must be a JSON array.")
        normalized = []
        for q in questions:
            nq = _normalize_survey_question(q)
            if nq:
                normalized.append(nq)
        questions = normalized
    else:
        questions = []
        for i in range(1, q_count + 1):
            prompt = (request.form.get(f"survey_q_{i}") or "").strip()
            qtype = (request.form.get(f"survey_qtype_{i}") or "open").strip()
            if not prompt:
                continue
            qobj = {"type": qtype, "prompt": prompt}
            if qtype in ("likert", "mc", "ab"):
                opts_raw = (request.form.get(f"survey_qopts_{i}") or "").strip()
                qobj["options"] = [o.strip() for o in opts_raw.split("|") if o.strip()] if opts_raw else []
            elif qtype == "ab_image":
                img_a = (request.form.get(f"survey_qimg_a_{i}") or "").strip()
                img_b = (request.form.get(f"survey_qimg_b_{i}") or "").strip()
                qobj["images"] = {"A": img_a, "B": img_b}
            elif qtype == "range":
                try:
                    qobj["min"] = float(request.form.get(f"survey_qmin_{i}", 0))
                    qobj["max"] = float(request.form.get(f"survey_qmax_{i}", 100))
                except (ValueError, TypeError):
                    qobj["min"] = 0
                    qobj["max"] = 100
            elif qtype == "open":
                try:
                    qobj["max_words"] = int(request.form.get(f"survey_qmaxw_{i}", 50))
                except (ValueError, TypeError):
                    qobj["max_words"] = 50
            questions.append(qobj)

    if len(questions) != q_count:
        conn.close()
        err = f"You must provide exactly {q_count} questions (got {len(questions)})."
        if ajax:
            return jsonify({"ok": False, "error": err}), 400
        return render_error(err)

    validation_errors = _validate_survey_questions(questions)
    if validation_errors:
        conn.close()
        err = " ".join(validation_errors)
        if ajax:
            return jsonify({"ok": False, "error": err, "validation_errors": validation_errors}), 400
        return render_error(err)

    conn.execute(
        "UPDATE studies SET survey_questions = ? WHERE id = ?",
        (json.dumps(questions), study_id),
    )
    conn.commit()

    if ajax:
        study = conn.execute("SELECT * FROM studies WHERE id = ?", (study_id,)).fetchone()
        study_dict = dict(study)
        auto_ben_precheck(conn, study_dict, user["id"])
        personas_used = normalize_personas_used(study_dict.get("personas_used"))
        precheck = _build_precheck_state(study_dict, len(personas_used))
        qa_status = study_dict.get("qa_status", "")
        qa_failures = []
        if qa_status == "precheck_failed" and study_dict.get("qa_notes"):
            try:
                qa_failures = json.loads(study_dict["qa_notes"])
            except (json.JSONDecodeError, TypeError):
                qa_failures = []
        conn.close()
        return jsonify({
            "ok": True,
            "questions": questions,
            "precheck": precheck,
            "qa_status": qa_status,
            "qa_failures": qa_failures,
        })

    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


@app.route("/save-survey-question/<int:study_id>/<int:q_index>", methods=["POST"])
def save_survey_question(study_id, q_index):
    token = get_token()
    user, _ = get_session_data(token)
    ajax = _is_ajax(request)
    if not user or user["state"] != "active":
        if ajax:
            return jsonify({"ok": False, "error": "You must be an active user."}), 403
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        if ajax:
            return jsonify({"ok": False, "error": "Draft study not found."}), 404
        return render_error("Draft study not found.")
    if study["study_type"] != "synthetic_survey":
        conn.close()
        if ajax:
            return jsonify({"ok": False, "error": "Survey questions only apply to synthetic survey studies."}), 400
        return render_error("Survey questions only apply to synthetic survey studies.")

    q_count = study["question_count"] or 8
    if q_index < 1 or q_index > q_count:
        conn.close()
        if ajax:
            return jsonify({"ok": False, "error": f"Question index must be between 1 and {q_count}."}), 400
        return render_error(f"Question index must be between 1 and {q_count}.")

    try:
        existing = json.loads(study["survey_questions"] or "[]")
    except (json.JSONDecodeError, TypeError):
        existing = []
    while len(existing) < q_count:
        existing.append(None)

    raw_json = request.form.get("question_json", "").strip()
    if not raw_json:
        conn.close()
        if ajax:
            return jsonify({"ok": False, "error": "No question data provided."}), 400
        return render_error("No question data provided.")

    try:
        qobj = json.loads(raw_json)
    except (json.JSONDecodeError, TypeError):
        conn.close()
        if ajax:
            return jsonify({"ok": False, "error": "Invalid JSON for question."}), 400
        return render_error("Invalid JSON for question.")

    nq = _normalize_survey_question(qobj)
    if not nq or not nq.get("prompt"):
        conn.close()
        if ajax:
            return jsonify({"ok": False, "error": "Question prompt is required."}), 400
        return render_error("Question prompt is required.")

    qtype = nq.get("type", "open")
    q_errors = []
    if qtype == "likert":
        opts = nq.get("options") or []
        if not isinstance(opts, list) or len(opts) != 5:
            q_errors.append("Likert must have exactly 5 labels.")
    elif qtype == "mc":
        opts = nq.get("options") or []
        if not isinstance(opts, list) or len(opts) < 3:
            q_errors.append("Multiple Choice must have at least 3 options.")
        elif len(opts) > 8:
            q_errors.append("Multiple Choice cannot have more than 8 options.")
    elif qtype == "ab":
        opts = nq.get("options") or []
        if not isinstance(opts, list) or len(opts) != 2:
            q_errors.append("A/B Choice must have exactly 2 options.")
    elif qtype == "ab_image":
        imgs = nq.get("images") or {}
        if not isinstance(imgs, dict) or set(imgs.keys()) != {"A", "B"}:
            q_errors.append("A/B Image must have exactly keys 'A' and 'B'.")
        else:
            ref_a = (imgs.get("A") or "").strip()
            ref_b = (imgs.get("B") or "").strip()
            if not ref_a or not ref_b:
                q_errors.append("A/B Image: both images must be uploaded.")
    elif qtype == "range":
        r_min = nq.get("min")
        r_max = nq.get("max")
        if r_min is not None and r_max is not None and r_min >= r_max:
            q_errors.append("Range: min must be less than max.")
    if q_errors:
        conn.close()
        err_msg = "; ".join(q_errors)
        if ajax:
            return jsonify({"ok": False, "error": err_msg}), 400
        return render_error(err_msg)

    existing[q_index - 1] = nq

    conn.execute(
        "UPDATE studies SET survey_questions = ? WHERE id = ?",
        (json.dumps(existing), study_id),
    )
    conn.commit()

    study = conn.execute("SELECT * FROM studies WHERE id = ?", (study_id,)).fetchone()
    study_dict = dict(study)
    auto_ben_precheck(conn, study_dict, user["id"])
    study = conn.execute("SELECT * FROM studies WHERE id = ?", (study_id,)).fetchone()
    study_dict = dict(study)
    personas_used = normalize_personas_used(study_dict.get("personas_used"))
    precheck = _build_precheck_state(study_dict, len(personas_used))
    qa_status = study_dict.get("qa_status", "")
    qa_failures = []
    if qa_status == "precheck_failed" and study_dict.get("qa_notes"):
        try:
            qa_failures = json.loads(study_dict["qa_notes"])
        except (json.JSONDecodeError, TypeError):
            qa_failures = []
    saved_count = len([q for q in existing if q is not None])
    conn.close()

    if ajax:
        return jsonify({
            "ok": True,
            "q_index": q_index,
            "question": nq,
            "saved_count": saved_count,
            "precheck": precheck,
            "qa_status": qa_status,
            "qa_failures": qa_failures,
        })
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

    updates = []
    params = []
    if mg:
        updates.append("study_fit = ?")
        params.append(mg)
    if pc:
        updates.append("known_vs_unknown = ?")
        params.append(pc)
    if ta:
        updates.append("target_audience = ?")
        params.append(ta)

    if not updates:
        conn.close()
        return render_error("Please fill in at least one anchor to save.")

    params.append(study_id)
    conn.execute(
        f"UPDATE studies SET {', '.join(updates)} WHERE id = ?",
        tuple(params),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


def _is_ajax(req):
    return (req.headers.get("X-Requested-With") == "XMLHttpRequest"
            or "application/json" in (req.headers.get("Accept") or ""))


def _build_precheck_state(study_dict, persona_count):
    st = study_dict.get("study_type") or ""
    has_bp = bool((study_dict.get("business_problem") or "").strip())
    has_ds = bool((study_dict.get("decision_to_support") or "").strip())
    has_mg = bool((study_dict.get("study_fit") or "").strip())
    has_pc = bool((study_dict.get("known_vs_unknown") or "").strip())
    has_ta = bool((study_dict.get("target_audience") or "").strip())
    has_dui = bool((study_dict.get("definition_useful_insight") or "").strip())
    if st == "synthetic_idi":
        p_min = 1
    elif st == "synthetic_focus_group":
        p_min = 4
    else:
        p_min = 0
    p_complete = persona_count >= p_min
    p_gap = max(0, p_min - persona_count)

    survey_rc = 0
    survey_qc = 0
    survey_sq_count = 0
    if st == "synthetic_survey":
        survey_rc = study_dict.get("respondent_count") or 0
        survey_qc = study_dict.get("question_count") or 0
        sq_raw = study_dict.get("survey_questions") or "[]"
        try:
            sq_list = json.loads(sq_raw) if isinstance(sq_raw, str) else sq_raw
            survey_sq_count = len([q for q in sq_list if q is not None]) if isinstance(sq_list, list) else 0
        except (json.JSONDecodeError, TypeError):
            survey_sq_count = 0

    return {
        "has_bp": has_bp, "has_ds": has_ds, "has_mg": has_mg,
        "has_pc": has_pc, "has_ta": has_ta, "has_dui": has_dui,
        "all_anchors": has_bp and has_ds and has_mg and has_pc and has_ta,
        "persona_count": persona_count, "personas_min": p_min,
        "personas_complete": p_complete, "personas_gap": p_gap,
        "study_type": st,
        "survey_rc": survey_rc, "survey_qc": survey_qc,
        "survey_sq_count": survey_sq_count,
    }


@app.route("/save-single-anchor/<int:study_id>", methods=["POST"])
def save_single_anchor(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "You must be an active user."}), 403
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Draft study not found."}), 404
        return render_error("Draft study not found.")

    anchor_key = (request.form.get("anchor_key") or "").strip()
    anchor_value = (request.form.get("anchor_value") or "").strip()

    allowed_keys = {
        "market_geography": "study_fit",
        "product_concept": "known_vs_unknown",
        "target_audience": "target_audience",
    }

    if anchor_key not in allowed_keys:
        conn.close()
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Invalid anchor key."}), 400
        return render_error("Invalid anchor key.")
    if not anchor_value:
        conn.close()
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Anchor value cannot be empty."}), 400
        return render_error("Anchor value cannot be empty.")
    if len(anchor_value) > 300:
        conn.close()
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Anchor value must be 300 characters or fewer."}), 400
        return render_error("Anchor value must be 300 characters or fewer.")

    db_col = allowed_keys[anchor_key]
    conn.execute(
        f"UPDATE studies SET {db_col} = ? WHERE id = ?",
        (anchor_value, study_id),
    )
    conn.commit()

    if _is_ajax(request):
        study = conn.execute("SELECT * FROM studies WHERE id = ?", (study_id,)).fetchone()
        study_dict = dict(study)
        auto_ben_precheck(conn, study_dict, user["id"])
        personas_used = normalize_personas_used(study_dict.get("personas_used"))
        precheck = _build_precheck_state(study_dict, len(personas_used))
        qa_status = study_dict.get("qa_status", "")
        qa_failures = []
        if qa_status == "precheck_failed" and study_dict.get("qa_notes"):
            try:
                qa_failures = json.loads(study_dict["qa_notes"])
            except (json.JSONDecodeError, TypeError):
                qa_failures = []
        conn.close()
        return jsonify({
            "ok": True, "anchor_key": anchor_key, "anchor_value": anchor_value,
            "precheck": precheck,
            "qa_status": qa_status, "qa_failures": qa_failures,
        })

    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


OPTIONAL_CONTEXT_FIELDS = {
    "definition_useful_insight": "Definition of Useful Insight",
    "competitive_context": "Competitive context",
    "cultural_sensitivities": "Cultural sensitivities",
    "adoption_barriers": "Adoption barriers",
    "risk_tolerance": "Risk tolerance",
}


@app.route("/save-optional-context-field/<int:study_id>", methods=["POST"])
def save_optional_context_field(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "You must be an active user."}), 403
        return render_error("You must be an active user.")

    field_key = (request.form.get("field_key") or "").strip()
    field_value = (request.form.get("field_value") or "").strip()

    if field_key not in OPTIONAL_CONTEXT_FIELDS:
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Invalid field key."}), 400
        return render_error("Invalid optional context field.")
    if len(field_value) > 300:
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Field value must be 300 characters or fewer."}), 400
        return render_error("Field value must be 300 characters or fewer.")

    conn = get_db()
    try:
        study = conn.execute(
            "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
            (study_id, user["id"]),
        ).fetchone()
        if not study:
            conn.close()
            if _is_ajax(request):
                return jsonify({"ok": False, "error": "Draft study not found."}), 404
            return render_error("Draft study not found.")

        if field_key == "definition_useful_insight":
            conn.execute(
                "UPDATE studies SET definition_useful_insight = ? WHERE id = ?",
                (field_value, study_id),
            )
            conn.commit()
        else:
            existing_brief = {}
            if study["survey_brief"]:
                try:
                    existing_brief = json.loads(study["survey_brief"])
                except (json.JSONDecodeError, TypeError):
                    existing_brief = {}

            oc = existing_brief.get("optional_context", {})
            if not isinstance(oc, dict):
                oc = {}
            oc[field_key] = field_value
            existing_brief["optional_context"] = oc

            conn.execute(
                "UPDATE studies SET survey_brief = ? WHERE id = ?",
                (json.dumps(existing_brief), study_id),
            )
            conn.commit()
    except Exception as e:
        if _is_ajax(request):
            return jsonify({"ok": False, "error": f"Save failed: {e}"}), 500
        return render_error(f"Save failed: {e}")
    finally:
        conn.close()

    if _is_ajax(request):
        return jsonify({
            "ok": True,
            "field_key": field_key,
            "field_value": field_value,
            "field_label": OPTIONAL_CONTEXT_FIELDS[field_key],
        })

    return redirect(url_for("index", token=token, configure=study_id))


@app.route("/attach-persona/<int:study_id>", methods=["POST"])
def attach_persona(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "You must be an active user."}), 403
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Draft study not found."}), 404
        return render_error("Draft study not found.")

    study_type = study["study_type"] or ""
    if study_type == "synthetic_survey":
        conn.close()
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Surveys do not use inspectable personas."}), 400
        return render_error("Surveys do not use inspectable personas.")

    instance_id = (request.form.get("persona_instance_id") or "").strip()
    if not instance_id:
        conn.close()
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Persona is required."}), 400
        return render_error("Persona is required.")

    persona_row = conn.execute(
        "SELECT id FROM personas WHERE persona_instance_id = ? AND user_id = ?",
        (instance_id, user["id"]),
    ).fetchone()
    if not persona_row:
        conn.close()
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Persona not found."}), 404
        return render_error("Persona not found.")

    current = normalize_personas_used(study["personas_used"])
    if instance_id in current:
        conn.close()
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Persona is already attached to this study."}), 400
        return render_error("Persona is already attached to this study.")

    max_personas = {"synthetic_idi": 1, "synthetic_focus_group": 6}
    limit = max_personas.get(study_type)
    if limit and len(current) >= limit:
        conn.close()
        label = "IDI" if study_type == "synthetic_idi" else "Focus Group"
        if _is_ajax(request):
            return jsonify({"ok": False, "error": f"{label} allows max {limit} personas."}), 400
        return render_error(f"{label} allows max {limit} personas.")

    current.append(instance_id)
    conn.execute(
        "UPDATE studies SET personas_used = ? WHERE id = ?",
        (json.dumps(current), study_id),
    )
    conn.commit()

    if _is_ajax(request):
        persona_name = ""
        p_row = conn.execute(
            "SELECT name FROM personas WHERE persona_instance_id = ? AND user_id = ?",
            (instance_id, user["id"]),
        ).fetchone()
        if p_row:
            persona_name = p_row["name"]
        study = conn.execute("SELECT * FROM studies WHERE id = ?", (study_id,)).fetchone()
        study_dict = dict(study)
        auto_ben_precheck(conn, study_dict, user["id"])
        precheck = _build_precheck_state(study_dict, len(current))
        qa_status = study_dict.get("qa_status", "")
        qa_failures = []
        if qa_status == "precheck_failed" and study_dict.get("qa_notes"):
            try:
                qa_failures = json.loads(study_dict["qa_notes"])
            except (json.JSONDecodeError, TypeError):
                qa_failures = []
        conn.close()
        return jsonify({
            "ok": True, "action": "attached",
            "persona_id": instance_id, "persona_name": persona_name,
            "persona_count": len(current), "precheck": precheck,
            "qa_status": qa_status, "qa_failures": qa_failures,
        })

    conn.close()
    return redirect(url_for("index", token=token, configure=study_id))


@app.route("/detach-persona/<int:study_id>", methods=["POST"])
def detach_persona(study_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "You must be an active user."}), 403
        return render_error("You must be an active user.")

    conn = get_db()
    study = conn.execute(
        "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
        (study_id, user["id"]),
    ).fetchone()
    if not study:
        conn.close()
        if _is_ajax(request):
            return jsonify({"ok": False, "error": "Draft study not found."}), 404
        return render_error("Draft study not found.")

    instance_id = (request.form.get("persona_instance_id") or "").strip()

    current = normalize_personas_used(study["personas_used"])
    current = [p for p in current if p != instance_id]
    conn.execute(
        "UPDATE studies SET personas_used = ? WHERE id = ?",
        (json.dumps(current), study_id),
    )
    conn.commit()

    if _is_ajax(request):
        study = conn.execute("SELECT * FROM studies WHERE id = ?", (study_id,)).fetchone()
        study_dict = dict(study)
        auto_ben_precheck(conn, study_dict, user["id"])
        precheck = _build_precheck_state(study_dict, len(current))
        qa_status = study_dict.get("qa_status", "")
        qa_failures = []
        if qa_status == "precheck_failed" and study_dict.get("qa_notes"):
            try:
                qa_failures = json.loads(study_dict["qa_notes"])
            except (json.JSONDecodeError, TypeError):
                qa_failures = []
        conn.close()
        return jsonify({
            "ok": True, "action": "detached",
            "persona_id": instance_id,
            "persona_count": len(current), "precheck": precheck,
            "qa_status": qa_status, "qa_failures": qa_failures,
        })

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
    healthy_models, excluded_fail, pool_status = _get_healthy_pool_models(conn)
    if pool_status == "no_eligible":
        conn.close()
        return render_error(
            "Cannot create persona: no eligible models in pool (GPT-family excluded). An admin must add non-GPT models to the persona model pool.",
            show_new_persona=True,
        )
    if pool_status == "all_fail":
        conn.close()
        return render_error(
            "Cannot create persona: all eligible models failed their last health check ("
            + ", ".join(excluded_fail) + "). "
            "An admin must run a health check or fix the model pool.",
            show_new_persona=True,
        )
    if excluded_fail:
        print(f"PERSONA_CREATE_FAIL_EXCLUSION excluded={excluded_fail}", flush=True)

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

    selected_model = _random.choice(healthy_models)
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


@app.route("/_bapi/personas-list", methods=["GET"])
def api_personas_list():
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return jsonify({"ok": False, "error": "Not authenticated."}), 403

    page = max(1, int(request.args.get("page", "1") or "1"))
    q = (request.args.get("q") or "").strip()
    ps = 10

    conn = get_db()
    if q:
        total = conn.execute(
            "SELECT COUNT(*) FROM personas WHERE user_id = ? AND (persona_instance_id LIKE ? OR name LIKE ?)",
            (user["id"], f"%{q}%", f"%{q}%"),
        ).fetchone()[0]
    else:
        total = conn.execute(
            "SELECT COUNT(*) FROM personas WHERE user_id = ?",
            (user["id"],),
        ).fetchone()[0]

    total_pages = max(1, (total + ps - 1) // ps)
    page = min(page, total_pages)
    offset = (page - 1) * ps

    _sel_cols = "persona_instance_id, name, created_at, content_language, translated_content"
    if q:
        rows = conn.execute(
            f"SELECT {_sel_cols} FROM personas WHERE user_id = ? AND (persona_instance_id LIKE ? OR name LIKE ?) ORDER BY id DESC LIMIT ? OFFSET ?",
            (user["id"], f"%{q}%", f"%{q}%", ps, offset),
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT {_sel_cols} FROM personas WHERE user_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
            (user["id"], ps, offset),
        ).fetchall()

    _user_lang = request.cookies.get("pb_lang", "en")
    if _user_lang not in VALID_LANGS:
        _user_lang = "en"
    conn.close()

    personas = []
    for r in rows:
        p_item = {"persona_instance_id": r[0], "name": r[1], "created_at": r[2]}
        _p_clang = r[3] or "en"
        if _p_clang != _user_lang and r[4]:
            try:
                _tc = json.loads(r[4])
                if isinstance(_tc, dict) and _tc.get("lang") == _user_lang:
                    _tname = _tc.get("fields", {}).get("name")
                    if not _tname and _tc.get("fields", {}).get("persona_summary"):
                        _tname = _extract_translated_name(_tc["fields"]["persona_summary"], r[1])
                    if _tname:
                        p_item["translated_name"] = _tname
            except (json.JSONDecodeError, TypeError):
                pass
        personas.append(p_item)
    return jsonify({
        "ok": True,
        "personas": personas,
        "page": page,
        "total_pages": total_pages,
        "total": total,
        "q": q,
    })


@app.route("/_bapi/persona-detail/<path:instance_id>", methods=["GET"])
def api_persona_detail(instance_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return jsonify({"ok": False, "error": "Not authenticated."}), 403

    conn = get_db()
    row = conn.execute(
        "SELECT * FROM personas WHERE persona_instance_id = ? AND user_id = ?",
        (instance_id, user["id"]),
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({"ok": False, "error": "Persona not found."}), 404

    p = dict(row)
    prov_raw = p.get("ai_model_provenance") or ""
    model_name = ""
    if "persona_model=" in prov_raw:
        model_name = prov_raw.split("persona_model=")[1].split(";")[0]

    content_lang = p.get("content_language") or "en"
    user_lang = request.cookies.get("pb_lang", "en")
    if user_lang not in VALID_LANGS:
        user_lang = "en"
    needs_translation = content_lang != user_lang

    translated_fields = None
    translated_name = None
    if needs_translation and p.get("translated_content"):
        try:
            _tc = json.loads(p["translated_content"])
            if isinstance(_tc, dict) and _tc.get("lang") == user_lang:
                translated_fields = _tc.get("fields", {})
                translated_name = translated_fields.get("name")
                if not translated_name and translated_fields.get("persona_summary"):
                    translated_name = _extract_translated_name(translated_fields["persona_summary"], p.get("name", ""))
        except (json.JSONDecodeError, TypeError):
            pass

    return jsonify({
        "ok": True,
        "persona": {
            "name": p.get("name", ""),
            "persona_instance_id": p.get("persona_instance_id", ""),
            "created_at": p.get("created_at", ""),
            "persona_summary": p.get("persona_summary", ""),
            "demographic_frame": p.get("demographic_frame", ""),
            "psychographic_profile": p.get("psychographic_profile", ""),
            "contextual_constraints": p.get("contextual_constraints", ""),
            "behavioural_tendencies": p.get("behavioural_tendencies", ""),
            "model_name": model_name,
            "provenance": prov_raw,
            "grounding_sources": p.get("grounding_sources", ""),
            "confidence_and_limits": p.get("confidence_and_limits", ""),
            "content_language": content_lang,
            "needs_translation": needs_translation,
            "user_lang": user_lang,
            "user_lang_name": LANG_CODE_TO_NAME.get(user_lang, user_lang),
            "content_lang_name": LANG_CODE_TO_NAME.get(content_lang, content_lang),
            "translated_fields": translated_fields,
            "translated_name": translated_name,
        },
    })


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
                "SELECT id, title, study_type, status, created_at, study_output, qa_status, confidence_summary, final_report, output_language FROM studies WHERE user_id = ? ORDER BY created_at DESC",
                (user["id"],),
            ).fetchall()
        ]
        _ul = request.cookies.get("pb_lang", "en")
        if _ul not in VALID_LANGS:
            _ul = "en"
        personas_list = get_user_personas_list(conn, user["id"], user_lang=_ul)
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
        health_map={},
        latest_weekly_report=None,
        latest_health_check=None,
        mark_recommendation="",
        mark_recommendation_label="",
        mark_recommendation_reason="",
        chat_messages=[],
        chat_save_buttons=[],
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
        dm_unread_count=0,
        dm_messages_list=[],
        dm_page=1,
        dm_total_pages=1,
        dm_total=0,
        dm_view=None,
        dm_admin_unread=0,
        dm_latest_preview=None,
        admin_email=ADMIN_EMAIL,
        monthly_study_limit=FREE_TIER_MONTHLY_LIMIT,
        test_user_email=TEST_USER_EMAIL,
        test_user_password=TEST_USER_PASSWORD,
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

    _dev_geo = (study_data.get("study_fit") or "").strip()
    _dev_admin_srcs = get_admin_web_sources(conn, status_filter="active")
    _dev_lang_code, _dev_lang_name = infer_market_language(_dev_geo, _dev_admin_srcs)

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
           confidence_summary = ?, final_report = ?, output_language = ? WHERE id = ?""",
        (
            final_status,
            output,
            qa_decision.lower(),
            qa_notes,
            confidence_summary,
            final_report,
            _dev_lang_code,
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
    _dev_user_id = study["user_id"]
    conn.close()

    if final_status == "completed":
        _user_iface_lang = request.cookies.get("pb_lang", "en")
        if _user_iface_lang not in VALID_LANGS:
            _user_iface_lang = "en"
        _translate_target = _user_iface_lang if _user_iface_lang != _dev_lang_code else ("en" if _dev_lang_code != "en" else None)
        if _translate_target:
            print(f"SYNC_TRANSLATE_START study={study_id} output_lang={_dev_lang_code} target_lang={_translate_target}", flush=True)
            try:
                _sync_translate_personas(study_id, _dev_user_id, _translate_target)
            except Exception as _tp_err:
                print(f"SYNC_TRANSLATE_PERSONAS_ERR study={study_id} err={_tp_err}", flush=True)
            try:
                _sync_translate_study_output(study_id, _dev_user_id, _translate_target, _dev_lang_code)
            except Exception as _to_err:
                print(f"SYNC_TRANSLATE_OUTPUT_ERR study={study_id} err={_to_err}", flush=True)

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


@app.route("/admin/export/users.csv")
def admin_export_users_csv():
    token = get_token()
    user, is_admin = get_session_data(token)
    if not is_admin:
        return "Admin access required.", 403
    import csv, io

    conn = get_db()
    rows = conn.execute("SELECT id, email, username, state, created_at, name, company, role, location FROM users ORDER BY id").fetchall()
    conn.close()
    cols = ["id", "email", "username", "state", "created_at", "name", "company", "role", "location"]
    si = io.StringIO()
    w = csv.writer(si)
    w.writerow(cols)
    for r in rows:
        w.writerow([r[c] for c in cols])
    resp = app.make_response(si.getvalue())
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = "attachment; filename=users.csv"
    return resp


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
