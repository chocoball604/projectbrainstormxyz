"""
app.py — Project Brainstorm V1 (PROMPT 1 + PROMPT 2 + PROMPT 3)

Single-page Flask app with SQLite.
Sections: Login/Signup | Pending Approval | Active Dashboard | Admin Panel

PROMPT 2: studies table, study list on dashboard, "New Research" button.
PROMPT 3: Research Brief form with 6 required anchors; saving creates a draft study.

Rules: See brainstorm_v1_replit_singlepage_pack/00_FROZEN_RULES_FROM_PRD.md
"""

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
            created_at              TEXT    NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id)
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


@app.route("/")
def index():
    token = get_token()
    user, is_admin = get_session_data(token)

    pending_users = []
    all_users = []
    studies = []

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


def render_error(message, show_new_research=False):
    token = get_token()
    user, is_admin = get_session_data(token)
    pending_users = []
    all_users = []
    studies = []
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
        conn.close()
    if not show_new_research:
        show_new_research = request.args.get("new_research") == "1"
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
    )


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
