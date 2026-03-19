"""
app.py — Project Brainstorm V1 (PROMPT 1–5)

Single-page Flask app with SQLite.
Sections: Login/Signup | Pending Approval | Active Dashboard | Admin Panel

PROMPT 2: studies table, study list on dashboard, "New Research" button.
PROMPT 3: Research Brief form with 6 required anchors; saving creates a draft study.
PROMPT 4: Study type selector + limits enforcement.
PROMPT 5: Personas — create, save, version (v1, v2…), view (read-only), delete.

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
    conn.commit()
    conn.close()


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


def get_user_personas_summary(conn, user_id):
    rows = conn.execute(
        """SELECT persona_id, name, MAX(version) as latest_version, created_at
           FROM personas WHERE user_id = ?
           GROUP BY persona_id ORDER BY MAX(id) DESC""",
        (user_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_user_all_persona_versions(conn, user_id):
    rows = conn.execute(
        "SELECT persona_id, version, name FROM personas WHERE user_id = ? ORDER BY persona_id, version",
        (user_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def persona_used_in_completed_study(conn, persona_id, version, user_id):
    rows = conn.execute(
        "SELECT id, persona_versions_used FROM studies WHERE user_id = ? AND status = 'completed'",
        (user_id,),
    ).fetchall()
    for row in rows:
        used = json.loads(row["persona_versions_used"] or "[]")
        for entry in used:
            if entry.get("persona_id") == persona_id and entry.get("version") == version:
                return True
    return False


@app.route("/")
def index():
    token = get_token()
    user, is_admin = get_session_data(token)

    pending_users = []
    all_users = []
    studies = []
    personas_summary = []
    view_persona = None
    configure_study = None
    configure_study_personas = []
    available_persona_versions = []

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
        configure_id = request.args.get("configure")
        if configure_id:
            row = conn.execute(
                "SELECT * FROM studies WHERE id = ? AND user_id = ? AND status = 'draft'",
                (configure_id, user["id"]),
            ).fetchone()
            if row:
                configure_study = dict(row)
                configure_study_personas = json.loads(configure_study.get("persona_versions_used") or "[]")
                available_persona_versions = get_user_all_persona_versions(conn, user["id"])

        personas_summary = get_user_personas_summary(conn, user["id"])

        view_pid = request.args.get("view_persona")
        view_ver = request.args.get("ver")
        if view_pid:
            if view_ver:
                row = conn.execute(
                    "SELECT * FROM personas WHERE persona_id = ? AND version = ? AND user_id = ?",
                    (view_pid, view_ver, user["id"]),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM personas WHERE persona_id = ? AND user_id = ? ORDER BY version DESC LIMIT 1",
                    (view_pid, user["id"]),
                ).fetchone()
            if row:
                view_persona = dict(row)
                all_versions = conn.execute(
                    "SELECT version, created_at FROM personas WHERE persona_id = ? AND user_id = ? ORDER BY version DESC",
                    (view_pid, user["id"]),
                ).fetchall()
                view_persona["all_versions"] = [dict(v) for v in all_versions]

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
        available_persona_versions=available_persona_versions,
        study_type_limits=STUDY_TYPE_LIMITS,
        personas_summary=personas_summary,
        show_new_persona=request.args.get("new_persona") == "1",
        new_version_of=request.args.get("new_version_of"),
        view_persona=view_persona,
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

    pid = (request.form.get("persona_id") or "").strip()
    ver = request.form.get("version", "").strip()
    if not pid or not ver:
        conn.close()
        return render_error("Persona ID and version are required.")

    persona_row = conn.execute(
        "SELECT id FROM personas WHERE persona_id = ? AND version = ? AND user_id = ?",
        (pid, int(ver), user["id"]),
    ).fetchone()
    if not persona_row:
        conn.close()
        return render_error("Persona version not found.")

    current = json.loads(study["persona_versions_used"] or "[]")
    for entry in current:
        if entry["persona_id"] == pid and entry["version"] == int(ver):
            conn.close()
            return redirect(url_for("index", token=token, configure=study_id))

    current.append({"persona_id": pid, "version": int(ver)})
    conn.execute(
        "UPDATE studies SET persona_versions_used = ? WHERE id = ?",
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

    pid = (request.form.get("persona_id") or "").strip()
    ver = request.form.get("version", "").strip()

    current = json.loads(study["persona_versions_used"] or "[]")
    current = [e for e in current if not (e["persona_id"] == pid and e["version"] == int(ver))]
    conn.execute(
        "UPDATE studies SET persona_versions_used = ? WHERE id = ?",
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

    new_version_of = (request.form.get("new_version_of") or "").strip()

    conn = get_db()
    if new_version_of:
        existing = conn.execute(
            "SELECT MAX(version) as max_ver FROM personas WHERE persona_id = ? AND user_id = ?",
            (new_version_of, user["id"]),
        ).fetchone()
        if not existing or existing["max_ver"] is None:
            conn.close()
            return render_error("Original persona not found.")
        next_version = existing["max_ver"] + 1
        persona_id = new_version_of
    else:
        persona_id = f"P-{secrets.token_hex(4).upper()}"
        next_version = 1

    conn.execute(
        """INSERT INTO personas
           (user_id, persona_id, version, name,
            persona_summary, demographic_frame, psychographic_profile,
            contextual_constraints, behavioural_tendencies,
            ai_model_provenance, grounding_sources, confidence_and_limits)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            user["id"], persona_id, next_version, name,
            dossier["persona_summary"], dossier["demographic_frame"],
            dossier["psychographic_profile"], dossier["contextual_constraints"],
            dossier["behavioural_tendencies"], dossier["ai_model_provenance"],
            dossier["grounding_sources"], dossier["confidence_and_limits"],
        ),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token, view_persona=persona_id, ver=next_version))


@app.route("/delete-persona/<persona_id>", methods=["POST"])
def delete_persona(persona_id):
    token = get_token()
    user, _ = get_session_data(token)
    if not user or user["state"] != "active":
        return render_error("You must be an active user.")

    conn = get_db()
    versions = conn.execute(
        "SELECT version FROM personas WHERE persona_id = ? AND user_id = ?",
        (persona_id, user["id"]),
    ).fetchall()
    for v in versions:
        if persona_used_in_completed_study(conn, persona_id, v["version"], user["id"]):
            conn.close()
            return render_error(
                f'Cannot delete persona {persona_id}: version {v["version"]} is used in a completed study and is immutable.'
            )

    conn.execute(
        "DELETE FROM personas WHERE persona_id = ? AND user_id = ?",
        (persona_id, user["id"]),
    )
    conn.commit()
    conn.close()
    return redirect(url_for("index", token=token))


def render_error(message, show_new_research=False, show_new_persona=False):
    token = get_token()
    user, is_admin = get_session_data(token)
    pending_users = []
    all_users = []
    studies = []
    personas_summary = []
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
        personas_summary = get_user_personas_summary(conn, user["id"])
        conn.close()
    if not show_new_research:
        show_new_research = request.args.get("new_research") == "1"
    if not show_new_persona:
        show_new_persona = request.args.get("new_persona") == "1"
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
        available_persona_versions=[],
        study_type_limits=STUDY_TYPE_LIMITS,
        personas_summary=personas_summary,
        show_new_persona=show_new_persona,
        new_version_of=request.args.get("new_version_of"),
        view_persona=None,
    )


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
