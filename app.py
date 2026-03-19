"""
app.py — Project Brainstorm V1 (PROMPT 1: Simplest runnable app)

Single-page Flask app with SQLite.
Sections: Login/Signup | Pending Approval | Active Dashboard | Admin Panel

Rules: See brainstorm_v1_replit_singlepage_pack/00_FROZEN_RULES_FROM_PRD.md
"""

import os
import sqlite3
from datetime import datetime
from functools import wraps

from flask import (
    Flask,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

# ── App setup ────────────────────────────────────────────────────────────────

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-fallback-key")

DB_PATH = os.path.join(os.path.dirname(__file__), "brainstorm.db")

# Admin password from environment variable (hardcoded in .env for V1)
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin123")

# ── Database helpers ─────────────────────────────────────────────────────────


def get_db():
    """Open a database connection. Rows behave like dicts."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    """Create the users table if it does not exist."""
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            email        TEXT    UNIQUE NOT NULL,
            username     TEXT    NOT NULL,
            password_hash TEXT   NOT NULL,
            state        TEXT    NOT NULL DEFAULT 'pending',
            created_at   TEXT    NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


def get_current_user():
    """Return the current logged-in user dict, or None."""
    user_id = session.get("user_id")
    if not user_id:
        return None
    conn = get_db()
    row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    conn.close()
    if row is None:
        session.clear()
        return None
    return dict(row)


# ── Routes ────────────────────────────────────────────────────────────────────


@app.route("/")
def index():
    """Serve the single-page app. All state switching happens in the template."""
    user = get_current_user()
    is_admin = session.get("is_admin", False)

    # If admin is logged in, fetch pending users
    pending_users = []
    all_users = []
    if is_admin:
        conn = get_db()
        pending_users = [dict(r) for r in conn.execute(
            "SELECT id, email, username, state, created_at FROM users WHERE state = 'pending' ORDER BY created_at DESC"
        ).fetchall()]
        all_users = [dict(r) for r in conn.execute(
            "SELECT id, email, username, state, created_at FROM users ORDER BY created_at DESC"
        ).fetchall()]
        conn.close()

    return render_template(
        "index.html",
        user=user,
        is_admin=is_admin,
        pending_users=pending_users,
        all_users=all_users,
    )


@app.route("/signup", methods=["POST"])
def signup():
    """Register a new user. New users start as state=pending."""
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

    # Log the user in immediately after signup
    user_row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    conn.close()
    session["user_id"] = user_row["id"]
    session["is_admin"] = False
    return redirect(url_for("index"))


@app.route("/login", methods=["POST"])
def login():
    """Log in an existing user."""
    email = (request.form.get("email") or "").strip().lower()
    password = request.form.get("password") or ""

    if not email or not password:
        return render_error("Email and password are required.")

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    conn.close()

    if not user or not check_password_hash(user["password_hash"], password):
        return render_error("Invalid email or password.")

    session["user_id"] = user["id"]
    session["is_admin"] = False
    return redirect(url_for("index"))


@app.route("/admin-login", methods=["POST"])
def admin_login():
    """Log in as Admin using the hardcoded password from .env."""
    password = request.form.get("admin_password") or ""
    if password != ADMIN_PASSWORD:
        return render_error("Invalid admin password.")

    # Clear any user session, set admin flag
    session.clear()
    session["is_admin"] = True
    return redirect(url_for("index"))


@app.route("/logout", methods=["POST"])
def logout():
    """Log out the current user or admin."""
    session.clear()
    return redirect(url_for("index"))


@app.route("/admin/approve/<int:user_id>", methods=["POST"])
def admin_approve(user_id):
    """Admin action: approve a pending user (pending -> active)."""
    if not session.get("is_admin"):
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
    return redirect(url_for("index"))


@app.route("/admin/disable/<int:user_id>", methods=["POST"])
def admin_disable(user_id):
    """Admin action: disable a user."""
    if not session.get("is_admin"):
        return render_error("Admin access required.")

    conn = get_db()
    conn.execute("UPDATE users SET state = 'disabled' WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("index"))


def render_error(message):
    """Render the index page with an error message."""
    user = get_current_user()
    is_admin = session.get("is_admin", False)
    pending_users = []
    all_users = []
    if is_admin:
        conn = get_db()
        pending_users = [dict(r) for r in conn.execute(
            "SELECT id, email, username, state, created_at FROM users WHERE state = 'pending' ORDER BY created_at DESC"
        ).fetchall()]
        all_users = [dict(r) for r in conn.execute(
            "SELECT id, email, username, state, created_at FROM users ORDER BY created_at DESC"
        ).fetchall()]
        conn.close()
    return render_template(
        "index.html",
        user=user,
        is_admin=is_admin,
        pending_users=pending_users,
        all_users=all_users,
        error=message,
    )


# ── Startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
