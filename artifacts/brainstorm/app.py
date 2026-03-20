"""
app.py — Project Brainstorm V1 (PROMPT 1–7 + PRD fixes + branching flow)

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

Rules: See brainstorm_v1_replit_singlepage_pack/00_FROZEN_RULES_FROM_PRD.md
"""

import json
import os
import secrets
import sqlite3
from datetime import datetime

from flask import (
    Flask,
    redirect,
    render_template,
    request,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-fallback-key")

DB_PATH = os.path.join(os.path.dirname(__file__), "brainstorm.db")

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin123")

VALID_STUDY_STATUSES = [
    "draft", "in_progress", "qa_blocked",
    "terminated_system", "terminated_user", "completed",
]

VALID_STUDY_TYPES = ["synthetic_survey", "synthetic_idi", "synthetic_focus_group"]

STUDY_TYPE_LIMITS = {
    "synthetic_survey": {"max_questions": 12, "max_respondents": 400},
    "synthetic_idi": {"min_personas": 1, "max_personas": 3},
    "synthetic_focus_group": {"min_personas": 4, "max_personas": 6},
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
    ("ai_model_provenance", "AI Model Provenance (provider family + model id + selection method)"),
    ("grounding_sources", "Grounding Sources (list)"),
    ("confidence_and_limits", "Confidence and Limits"),
]


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
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
            created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
        )
    """)
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

    study_cols = [row[1] for row in conn.execute("PRAGMA table_info(studies)").fetchall()]
    if "personas_used" not in study_cols:
        conn.execute("ALTER TABLE studies ADD COLUMN personas_used TEXT NOT NULL DEFAULT '[]'")
    if "study_output" not in study_cols:
        conn.execute("ALTER TABLE studies ADD COLUMN study_output TEXT")
    if "respondent_count" not in study_cols:
        conn.execute("ALTER TABLE studies ADD COLUMN respondent_count INTEGER DEFAULT 100")
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
        user = conn.execute("SELECT * FROM users WHERE id = ?", (row["user_id"],)).fetchone()
        if user:
            user = dict(user)
    conn.close()
    return user, bool(row["is_admin"])


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
            study_id, persona_id, trigger_event,
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

    pending_users = []
    all_users = []
    studies = []
    personas_list = []
    view_persona = None
    configure_study = None
    configure_study_personas = []
    available_personas = []
    clone_source = None
    view_study_output = None
    admin_web_sources = []
    grounding_traces = []
    cost_telemetry_rows = []

    if is_admin:
        conn = get_db()
        pending_users = [dict(r) for r in conn.execute(
            "SELECT id, email, username, state, created_at FROM users WHERE state = 'pending' ORDER BY created_at DESC"
        ).fetchall()]
        all_users = [dict(r) for r in conn.execute(
            "SELECT id, email, username, state, created_at FROM users ORDER BY created_at DESC"
        ).fetchall()]
        admin_web_sources = get_admin_web_sources(conn)
        grounding_traces = [dict(r) for r in conn.execute(
            "SELECT * FROM grounding_traces ORDER BY id DESC LIMIT 50"
        ).fetchall()]
        cost_telemetry_rows = [dict(r) for r in conn.execute(
            "SELECT * FROM cost_telemetry ORDER BY id DESC LIMIT 50"
        ).fetchall()]
        conn.close()

    if user and user["state"] == "active":
        conn = get_db()
        studies = [dict(r) for r in conn.execute(
            "SELECT id, title, study_type, status, created_at, study_output, qa_status, confidence_summary, final_report FROM studies WHERE user_id = ? ORDER BY created_at DESC",
            (user["id"],),
        ).fetchall()]
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
                        configure_study["survey_questions_list"] = json.loads(configure_study["survey_questions"])
                    except (json.JSONDecodeError, TypeError):
                        configure_study["survey_questions_list"] = []
                else:
                    configure_study["survey_questions_list"] = []
                if configure_study.get("survey_brief"):
                    try:
                        configure_study["survey_brief_data"] = json.loads(configure_study["survey_brief"])
                    except (json.JSONDecodeError, TypeError):
                        configure_study["survey_brief_data"] = {}
                else:
                    configure_study["survey_brief_data"] = {}
                raw_ids = normalize_personas_used(configure_study.get("personas_used"))
                cleaned_ids = []
                for pid in raw_ids:
                    p_row = conn.execute(
                        "SELECT name FROM personas WHERE persona_instance_id = ? AND user_id = ?",
                        (pid, user["id"]),
                    ).fetchone()
                    if p_row:
                        configure_study_personas.append({"id": pid, "name": p_row["name"], "exists": True})
                        cleaned_ids.append(pid)
                    else:
                        pass
                if len(cleaned_ids) != len(raw_ids):
                    conn.execute(
                        "UPDATE studies SET personas_used = ? WHERE id = ?",
                        (json.dumps(cleaned_ids), configure_study["id"]),
                    )
                    conn.commit()
                    print(f"Cleaned dangling persona references from study {configure_study['id']}")
                all_personas = get_user_personas_list(conn, user["id"])
                attached_ids = set(cleaned_ids)
                available_personas = [p for p in all_personas if p["persona_instance_id"] not in attached_ids]

        personas_list = get_user_personas_list(conn, user["id"])

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
            if row and (row["study_output"] or row["status"] == "qa_blocked"):
                view_study_output = dict(row)
                if view_study_output.get("confidence_summary"):
                    try:
                        view_study_output["confidence_parsed"] = json.loads(view_study_output["confidence_summary"])
                    except (json.JSONDecodeError, TypeError):
                        view_study_output["confidence_parsed"] = None
                else:
                    view_study_output["confidence_parsed"] = None

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
    )


@app.route("/signup", methods=["POST"])
def signup():
    email = (request.form.get("email") or "").strip().lower()
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""

    if not email or not username or not password:
        return render_error("All fields are required.")
    if len(password) < 6:
        return render_error("Password must be at least 6 characters.")

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

    title = (request.form.get("title") or "").strip()
    study_type = (request.form.get("study_type") or "").strip()

    if not title:
        return render_error("Title is required.")
    if study_type not in VALID_STUDY_TYPES:
        return render_error("Please select a valid study type.")

    if study_type == "synthetic_survey":
        decision_to_support = (request.form.get("decision_to_support") or "").strip()
        target_audience = (request.form.get("target_audience") or "").strip()
        definition_useful_insight = (request.form.get("definition_useful_insight") or "").strip()
        top_hypotheses = (request.form.get("top_hypotheses") or "").strip()
        if not decision_to_support or not target_audience or not definition_useful_insight or not top_hypotheses:
            return render_error("All survey brief fields are required (Decision to Support, Target Audience, Definition of Useful Insight, Top Hypotheses).")

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

        survey_brief = json.dumps({
            "decision_to_support": decision_to_support,
            "target_audience": target_audience,
            "definition_useful_insight": definition_useful_insight,
            "top_hypotheses": top_hypotheses,
        })

        conn = get_db()
        conn.execute(
            """INSERT INTO studies
               (user_id, title, status, study_type, respondent_count, question_count,
                survey_brief, survey_questions)
               VALUES (?, ?, 'draft', ?, ?, ?, ?, ?)""",
            (user["id"], title, study_type, respondent_count, question_count,
             survey_brief, json.dumps(survey_questions)),
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
                user["id"], title, study_type,
                brief["business_problem"], brief["decision_to_support"],
                brief["known_vs_unknown"], brief["target_audience"],
                brief["study_fit"], brief["definition_useful_insight"],
            ),
        )
        conn.commit()
        conn.close()
        return redirect(url_for("index", token=token))


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


def generate_placeholder_output(study_type, study, persona_names):
    header = "*** SIMULATED PLACEHOLDER — NOT REAL OUTPUT ***"
    if study_type == "synthetic_survey":
        return json.dumps({
            "disclaimer": header,
            "study_title": study["title"],
            "study_type": "synthetic_survey",
            "summary": "This is a placeholder aggregated survey result. No real respondents were surveyed.",
            "sample_size": 200,
            "questions": [
                {
                    "q": "How satisfied are you with the product?",
                    "results": {"Very satisfied": "32%", "Satisfied": "41%", "Neutral": "15%", "Dissatisfied": "8%", "Very dissatisfied": "4%"},
                },
                {
                    "q": "Would you recommend this product to a friend?",
                    "results": {"Definitely yes": "28%", "Probably yes": "35%", "Not sure": "22%", "Probably not": "10%", "Definitely not": "5%"},
                },
                {
                    "q": "What is your primary reason for using this product?",
                    "results": {"Price": "25%", "Quality": "30%", "Convenience": "20%", "Brand trust": "15%", "Other": "10%"},
                },
            ],
        }, indent=2)
    elif study_type == "synthetic_idi":
        speakers = persona_names if persona_names else ["Respondent"]
        lines = [header, "", f"Study: {study['title']}", f"Type: Synthetic IDI", ""]
        for speaker in speakers[:3]:
            lines.extend([
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
            ])
        return "\n".join(lines)
    elif study_type == "synthetic_focus_group":
        speakers = persona_names if len(persona_names) >= 4 else ["Participant A", "Participant B", "Participant C", "Participant D"]
        lines = [header, "", f"Study: {study['title']}", f"Type: Synthetic Focus Group", ""]
        lines.extend([
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
        ])
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
            failures.append(f"survey has {len(sq)} questions but question_count is {q_count}")
        if len(sq) < 1 and not failures:
            failures.append("survey has no questions defined")
        study_id_debug = study_dict.get("id", "?")
        if failures:
            decision = "FAIL"
            print(f"QA_DEBUG survey study={study_id_debug} question_count={q_count} questions_len={len(sq)} decision={decision}")
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
            print(f"QA_DEBUG qual study={study_id_debug} missing_anchors={missing_anchors} decision=FAIL")
            return {
                "decision": "FAIL",
                "notes": f"QA failed: missing research brief anchors: {', '.join(missing_anchors)}.",
                "confidence_labels": fail_zero,
            }
        personas = normalize_personas_used(study_dict.get("personas_used"))
        pc = len(personas)
        if study_type == "synthetic_idi" and (pc < 1 or pc > 3):
            print(f"QA_DEBUG qual study={study_id_debug} missing_anchors=[] persona_count={pc} decision=FAIL")
            return {
                "decision": "FAIL",
                "notes": f"QA failed: IDI requires 1–3 personas, found {pc}.",
                "confidence_labels": fail_zero,
            }
        if study_type == "synthetic_focus_group" and (pc < 4 or pc > 6):
            print(f"QA_DEBUG qual study={study_id_debug} missing_anchors=[] persona_count={pc} decision=FAIL")
            return {
                "decision": "FAIL",
                "notes": f"QA failed: Focus Group requires 4–6 personas, found {pc}.",
                "confidence_labels": fail_zero,
            }

    if study_type == "synthetic_survey":
        study_id_debug = study_dict.get("id", "?")

    if "SIMULATED PLACEHOLDER" in output:
        if study_type == "synthetic_survey":
            try:
                parsed = json.loads(output)
                n_insights = len(parsed.get("questions", []))
            except (json.JSONDecodeError, TypeError):
                n_insights = 3
            print(f"QA_DEBUG survey study={study_id_debug} question_count={q_count} questions_len={len(sq)} decision=DOWNGRADE")
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

        return {
            "decision": "DOWNGRADE",
            "notes": "Output is placeholder; confidence downgraded.",
            "confidence_labels": {"Strong": 0, "Indicative": min(n_insights, 2), "Exploratory": max(0, n_insights - 2)},
        }

    if study_type == "synthetic_survey":
        try:
            parsed = json.loads(output)
            n_insights = len(parsed.get("questions", []))
        except (json.JSONDecodeError, TypeError):
            n_insights = 3
        print(f"QA_DEBUG survey study={study_id_debug} question_count={q_count} questions_len={len(sq)} decision=PASS")
    else:
        n_insights = 3

    return {
        "decision": "PASS",
        "notes": "All checks passed. Output meets quality standards.",
        "confidence_labels": {"Strong": max(1, n_insights // 2), "Indicative": n_insights - max(1, n_insights // 2), "Exploratory": 0},
    }


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
        if len(sq) < 1:
            conn.close()
            return render_error("Survey must have at least 1 question.")
        if len(sq) > 12:
            conn.close()
            return render_error("Survey allows max 12 questions.")
    elif study_type == "synthetic_idi":
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

    output = generate_placeholder_output(study_type, dict(study), persona_names)

    study_data = dict(study)
    study_data["study_output"] = output

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
        qa_notes = f"Budget ceiling exceeded: {tokens_total} tokens > {ceiling} ceiling."

    conn.execute(
        """UPDATE studies SET status = ?, study_output = ?, qa_status = ?, qa_notes = ?,
           confidence_summary = ?, final_report = ? WHERE id = ?""",
        (final_status, output, qa_decision.lower(), qa_notes, confidence_summary, final_report, study_id),
    )

    conn.execute(
        """INSERT INTO cost_telemetry
           (study_id, study_type, tokens_mark, tokens_lisa, tokens_ben, tokens_total,
            model_call_count, qa_retry_count, followup_round_count, status, termination_reason)
           VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)""",
        (study_id, study_type, tokens_mark, tokens_lisa, tokens_ben, tokens_total,
         model_call_count, final_status, termination_reason),
    )

    create_grounding_trace(conn, trigger_event="study_executed", study_id=str(study_id))
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

    dossier = {}
    for field_key, field_label in PERSONA_DOSSIER_FIELDS:
        val = (request.form.get(field_key) or "").strip()
        if not val:
            return render_error(
                f'"{field_label}" is required. All dossier fields must be filled.',
                show_new_persona=True,
            )
        dossier[field_key] = val

    new_instance_id = f"P-{secrets.token_hex(4).upper()}"

    conn = get_db()
    conn.execute(
        """INSERT INTO personas
           (user_id, persona_id, version, persona_instance_id, name,
            persona_summary, demographic_frame, psychographic_profile,
            contextual_constraints, behavioural_tendencies,
            ai_model_provenance, grounding_sources, confidence_and_limits)
           VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            user["id"], new_instance_id, new_instance_id, name,
            dossier["persona_summary"], dossier["demographic_frame"],
            dossier["psychographic_profile"], dossier["contextual_constraints"],
            dossier["behavioural_tendencies"], dossier["ai_model_provenance"],
            dossier["grounding_sources"], dossier["confidence_and_limits"],
        ),
    )
    create_grounding_trace(conn, trigger_event="persona_created", persona_id=new_instance_id)
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
    row = conn.execute("SELECT status FROM admin_web_sources WHERE id = ?", (source_id,)).fetchone()
    if row:
        new_status = "disabled" if row["status"] == "active" else "active"
        conn.execute("UPDATE admin_web_sources SET status = ? WHERE id = ?", (new_status, source_id))
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
    pending_users = []
    all_users = []
    studies = []
    personas_list = []
    configure_study = None
    if is_admin:
        conn = get_db()
        pending_users = [dict(r) for r in conn.execute(
            "SELECT id, email, username, state, created_at FROM users WHERE state = 'pending' ORDER BY created_at DESC"
        ).fetchall()]
        all_users = [dict(r) for r in conn.execute(
            "SELECT id, email, username, state, created_at FROM users ORDER BY created_at DESC"
        ).fetchall()]
        conn.close()
    if user and user["state"] == "active":
        conn = get_db()
        studies = [dict(r) for r in conn.execute(
            "SELECT id, title, study_type, status, created_at, study_output, qa_status, confidence_summary, final_report FROM studies WHERE user_id = ? ORDER BY created_at DESC",
            (user["id"],),
        ).fetchall()]
        personas_list = get_user_personas_list(conn, user["id"])
        conn.close()
    if not show_new_persona:
        show_new_persona = request.args.get("new_persona") == "1"
    admin_web_sources = []
    grounding_traces = []
    cost_telemetry_rows = []
    if is_admin:
        conn2 = get_db()
        admin_web_sources = get_admin_web_sources(conn2)
        grounding_traces = [dict(r) for r in conn2.execute(
            "SELECT * FROM grounding_traces ORDER BY id DESC LIMIT 50"
        ).fetchall()]
        cost_telemetry_rows = [dict(r) for r in conn2.execute(
            "SELECT * FROM cost_telemetry ORDER BY id DESC LIMIT 50"
        ).fetchall()]
        conn2.close()
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
        qa_notes = f"Budget ceiling exceeded: {tokens_total} tokens > {ceiling} ceiling."

    conn.execute(
        """UPDATE studies SET status = ?, study_output = ?, qa_status = ?, qa_notes = ?,
           confidence_summary = ?, final_report = ? WHERE id = ?""",
        (final_status, output, qa_decision.lower(), qa_notes, confidence_summary, final_report, study_id),
    )

    conn.execute(
        """INSERT INTO cost_telemetry
           (study_id, study_type, tokens_mark, tokens_lisa, tokens_ben, tokens_total,
            model_call_count, qa_retry_count, followup_round_count, status, termination_reason)
           VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)""",
        (study_id, study_type, tokens_mark, tokens_lisa, tokens_ben, tokens_total,
         model_call_count, final_status, termination_reason),
    )

    create_grounding_trace(conn, trigger_event="study_executed", study_id=str(study_id))
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
        (admin_user["id"], f"DEV_INVALID_{study_type}_{blank_field}",
         study_type,
         anchors["business_problem"], anchors["decision_to_support"],
         anchors["known_vs_unknown"], anchors["target_audience"],
         anchors["study_fit"], anchors["definition_useful_insight"]),
    )
    conn.commit()
    study_id = conn.execute("SELECT id FROM studies ORDER BY id DESC LIMIT 1").fetchone()["id"]
    conn.close()
    return f"DEV ONLY: Created invalid {study_type} study id={study_id} with blank field '{blank_field}'. Use /admin/dev-run-study/{study_id} to test QA."


@app.route("/admin/export/studies.csv")
def admin_export_studies_csv():
    token = get_token()
    user, is_admin = get_session_data(token)
    if not is_admin:
        return "Admin access required.", 403
    import csv, io
    conn = get_db()
    rows = conn.execute("SELECT * FROM studies ORDER BY id").fetchall()
    cols = [desc[0] for desc in conn.execute("SELECT * FROM studies LIMIT 1").description] if rows else []
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
    cols = [desc[0] for desc in conn.execute("SELECT * FROM cost_telemetry LIMIT 1").description] if rows else []
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
    cols = [desc[0] for desc in conn.execute("SELECT * FROM grounding_traces LIMIT 1").description] if rows else []
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


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
