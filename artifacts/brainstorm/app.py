"""
app.py — Project Brainstorm V1 (PROMPT 1–6)

Single-page Flask app with SQLite.
Sections: Login/Signup | Pending Approval | Active Dashboard | Admin Panel

PROMPT 2: studies table, study list on dashboard, "New Research" button.
PROMPT 3: Research Brief form with 6 required anchors; saving creates a draft study.
PROMPT 4: Study type selector + limits enforcement.
PROMPT 5: Personas — immutable once saved, clone-as-new, no versioning.
PROMPT 6: Grounding Trace logging + Admin-Directed Web Sources.

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
    admin_web_sources = []
    grounding_traces = []

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
        conn.close()

    if user and user["state"] == "active":
        conn = get_db()
        studies = [dict(r) for r in conn.execute(
            "SELECT id, title, study_type, status, created_at FROM studies WHERE user_id = ? ORDER BY created_at DESC",
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
                available_personas = get_user_personas_list(conn, user["id"])

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

        conn.close()

    return render_template(
        "index.html",
        user=user,
        is_admin=is_admin,
        pending_users=pending_users,
        all_users=all_users,
        studies=studies,
        token=token,
        show_new_research=request.args.get("new_research") == "1",
        configure_study=configure_study,
        configure_study_personas=configure_study_personas,
        available_personas=available_personas,
        study_type_limits=STUDY_TYPE_LIMITS,
        personas_list=personas_list,
        show_new_persona=request.args.get("new_persona") == "1",
        clone_source=clone_source,
        view_persona=view_persona,
        admin_web_sources=admin_web_sources,
        grounding_traces=grounding_traces,
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
    if not title:
        return render_error("Title is required.", show_new_research=True)

    brief = {}
    for field_key, field_label in RESEARCH_BRIEF_FIELDS:
        val = (request.form.get(field_key) or "").strip()
        if not val:
            return render_error(
                f'"{field_label}" is required. All 6 anchors must be filled.',
                show_new_research=True,
            )
        brief[field_key] = val

    conn = get_db()
    conn.execute(
        """INSERT INTO studies
           (user_id, title, status, business_problem, decision_to_support,
            known_vs_unknown, target_audience, study_fit, definition_useful_insight)
           VALUES (?, ?, 'draft', ?, ?, ?, ?, ?, ?)""",
        (
            user["id"], title,
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

    conn.execute(
        "UPDATE studies SET study_type = ? WHERE id = ?",
        (study_type, study_id),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


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
        return redirect(url_for("index", token=token, configure=study_id))

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
            "SELECT id, title, study_type, status, created_at FROM studies WHERE user_id = ? ORDER BY created_at DESC",
            (user["id"],),
        ).fetchall()]
        personas_list = get_user_personas_list(conn, user["id"])
        conn.close()
    if not show_new_research:
        show_new_research = request.args.get("new_research") == "1"
    if not show_new_persona:
        show_new_persona = request.args.get("new_persona") == "1"
    admin_web_sources = []
    grounding_traces = []
    if is_admin:
        conn2 = get_db()
        admin_web_sources = get_admin_web_sources(conn2)
        grounding_traces = [dict(r) for r in conn2.execute(
            "SELECT * FROM grounding_traces ORDER BY id DESC LIMIT 50"
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
        show_new_research=show_new_research,
        configure_study=configure_study,
        configure_study_personas=[],
        available_personas=[],
        study_type_limits=STUDY_TYPE_LIMITS,
        personas_list=personas_list,
        show_new_persona=show_new_persona,
        clone_source=None,
        view_persona=None,
        admin_web_sources=admin_web_sources,
        grounding_traces=grounding_traces,
    )


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
