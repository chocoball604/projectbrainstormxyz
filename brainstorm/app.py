"""
app.py — Project Brainstorm V1, Flask single-page web app.

Architecture: One Flask server serves both the single HTML page (GET /)
and a set of JSON API endpoints (POST/GET /api/...).
The frontend (index.html) uses vanilla JS + fetch() to call the API.

Rules: See 00_FROZEN_RULES_FROM_PRD.md before modifying this file.
"""

import json
import os
import secrets
from datetime import datetime
from functools import wraps

from flask import (
    Flask,
    jsonify,
    render_template,
    request,
    session,
)
from werkzeug.security import check_password_hash, generate_password_hash

from database import get_db, init_db, row_to_dict, rows_to_list

# ── App setup ────────────────────────────────────────────────────────────────

app = Flask(__name__)

# Secret key for sessions. In production, set SECRET_KEY env var.
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))

# ── Helpers ───────────────────────────────────────────────────────────────────


def ok(data=None, **kwargs):
    """Return a JSON success response."""
    payload = {"ok": True}
    if data is not None:
        payload["data"] = data
    payload.update(kwargs)
    return jsonify(payload)


def err(message, status=400):
    """Return a JSON error response."""
    return jsonify({"ok": False, "error": message}), status


def login_required(f):
    """Decorator: user must be logged in."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            return err("Not logged in", 401)
        return f(*args, **kwargs)
    return decorated


def active_required(f):
    """Decorator: user must be logged in AND have status=active."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            return err("Not logged in", 401)
        conn = get_db()
        user = row_to_dict(
            conn.execute("SELECT * FROM users WHERE id=?", (session["user_id"],)).fetchone()
        )
        conn.close()
        if not user or user["status"] != "active":
            return err("Account not yet approved", 403)
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    """Decorator: user must be logged in AND have role=admin."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            return err("Not logged in", 401)
        conn = get_db()
        user = row_to_dict(
            conn.execute("SELECT * FROM users WHERE id=?", (session["user_id"],)).fetchone()
        )
        conn.close()
        if not user or user["role"] != "admin":
            return err("Admin access required", 403)
        return f(*args, **kwargs)
    return decorated


def log_grounding_trace(conn, study_run_id, event_type, qa_verdict=None, payload=None):
    """
    Write one row to grounding_traces.
    Per FROZEN_RULES rule #17 and #15: QA result must be logged before saving study status.
    payload must be a dict (will be JSON-serialized).
    """
    if payload is None:
        payload = {}
    conn.execute(
        """
        INSERT INTO grounding_traces
            (study_run_id, event_type, qa_verdict, payload, is_placeholder)
        VALUES (?, ?, ?, ?, 1)
        """,
        (study_run_id, event_type, qa_verdict, json.dumps(payload)),
    )


def log_cost_telemetry(conn, study_run_id, study_type):
    """
    Write a PLACEHOLDER cost telemetry row.
    Per FROZEN_RULES rule #20: costs are PLACEHOLDER until real billing is added.
    """
    # PLACEHOLDER cost map — sourced from budget_limits.yaml
    placeholder_costs = {"survey": 5.00, "idi": 15.00, "focus_group": 25.00}
    cost = placeholder_costs.get(study_type, 5.00)
    conn.execute(
        """
        INSERT INTO cost_telemetry
            (study_run_id, study_type, tokens_input, tokens_output,
             estimated_cost_usd, model_name, is_placeholder, notes)
        VALUES (?, ?, ?, ?, ?, ?, 1, ?)
        """,
        (
            study_run_id,
            study_type,
            1000,   # PLACEHOLDER token count
            500,    # PLACEHOLDER token count
            cost,
            None,   # No real model used
            "PLACEHOLDER — no real AI was called in V1",
        ),
    )


# ── Front-end route ───────────────────────────────────────────────────────────


@app.route("/")
def index():
    """Serve the single-page app."""
    return render_template("index.html")


# ── Auth API ──────────────────────────────────────────────────────────────────


@app.route("/api/auth/register", methods=["POST"])
def register():
    """
    Register a new user.
    Per FROZEN_RULES rules #1, #2: new users start as status=pending.
    """
    data = request.get_json() or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or not password:
        return err("Email and password are required")
    if len(password) < 6:
        return err("Password must be at least 6 characters")

    conn = get_db()
    existing = conn.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
    if existing:
        conn.close()
        return err("Email already registered")

    hashed = generate_password_hash(password)
    # Per FROZEN_RULES rule #5: first user gets status=active (but not admin)
    user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    status = "active" if user_count == 0 else "pending"

    conn.execute(
        "INSERT INTO users (email, password, role, status) VALUES (?, ?, 'user', ?)",
        (email, hashed, status),
    )
    conn.commit()
    user_id = conn.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()[0]
    conn.close()

    session["user_id"] = user_id
    session["email"] = email
    return ok({"user_id": user_id, "email": email, "status": status})


@app.route("/api/auth/login", methods=["POST"])
def login():
    """Log in an existing user."""
    data = request.get_json() or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or not password:
        return err("Email and password are required")

    conn = get_db()
    user = row_to_dict(conn.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone())
    conn.close()

    if not user or not check_password_hash(user["password"], password):
        return err("Invalid email or password")

    session["user_id"] = user["id"]
    session["email"] = user["email"]
    return ok({
        "user_id": user["id"],
        "email": user["email"],
        "status": user["status"],
        "role": user["role"],
    })


@app.route("/api/auth/logout", methods=["POST"])
def logout():
    """Log out the current user."""
    session.clear()
    return ok()


@app.route("/api/auth/me", methods=["GET"])
def me():
    """Return the currently logged-in user's info, or 401 if not logged in."""
    if "user_id" not in session:
        return err("Not logged in", 401)
    conn = get_db()
    user = row_to_dict(
        conn.execute(
            "SELECT id, email, role, status, created_at FROM users WHERE id=?",
            (session["user_id"],),
        ).fetchone()
    )
    conn.close()
    if not user:
        session.clear()
        return err("User not found", 401)
    return ok(user)


# ── Admin API ─────────────────────────────────────────────────────────────────


@app.route("/api/admin/pending", methods=["GET"])
@admin_required
def admin_pending():
    """Return all users with status=pending. Admin only."""
    conn = get_db()
    users = rows_to_list(
        conn.execute(
            "SELECT id, email, role, status, created_at FROM users WHERE status='pending'"
        ).fetchall()
    )
    conn.close()
    return ok(users)


@app.route("/api/admin/users", methods=["GET"])
@admin_required
def admin_all_users():
    """Return all users. Admin only."""
    conn = get_db()
    users = rows_to_list(
        conn.execute(
            "SELECT id, email, role, status, created_at FROM users ORDER BY created_at DESC"
        ).fetchall()
    )
    conn.close()
    return ok(users)


@app.route("/api/admin/approve/<int:user_id>", methods=["POST"])
@admin_required
def admin_approve(user_id):
    """
    Promote a user from pending → active. Admin only.
    Per FROZEN_RULES rule #3.
    """
    conn = get_db()
    user = row_to_dict(conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone())
    if not user:
        conn.close()
        return err("User not found", 404)
    if user["status"] == "active":
        conn.close()
        return err("User is already active")

    conn.execute("UPDATE users SET status='active' WHERE id=?", (user_id,))
    conn.commit()
    conn.close()
    return ok({"approved": True, "user_id": user_id})


@app.route("/api/admin/promote/<int:user_id>", methods=["POST"])
@admin_required
def admin_promote_to_admin(user_id):
    """Promote a user to admin role. Admin only."""
    conn = get_db()
    conn.execute("UPDATE users SET role='admin', status='active' WHERE id=?", (user_id,))
    conn.commit()
    conn.close()
    return ok({"promoted": True, "user_id": user_id})


# ── Research Briefs API ───────────────────────────────────────────────────────


@app.route("/api/briefs", methods=["GET"])
@active_required
def list_briefs():
    """Return all research briefs for the current user."""
    conn = get_db()
    briefs = rows_to_list(
        conn.execute(
            "SELECT * FROM research_briefs WHERE user_id=? ORDER BY created_at DESC",
            (session["user_id"],),
        ).fetchall()
    )
    conn.close()
    return ok(briefs)


@app.route("/api/briefs", methods=["POST"])
@active_required
def create_brief():
    """
    Create a new Research Brief.
    Per FROZEN_RULES rules #6, #7, #8: exactly 6 answers, all required, none blank.
    """
    data = request.get_json() or {}
    required_keys = ["q1", "q2", "q3", "q4", "q5", "q6"]

    # Check all 6 answers are present and non-blank
    for key in required_keys:
        val = (data.get(key) or "").strip()
        if not val:
            return err(f"Answer to {key.upper()} is required")

    conn = get_db()
    conn.execute(
        """
        INSERT INTO research_briefs (user_id, q1, q2, q3, q4, q5, q6)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session["user_id"],
            data["q1"].strip(),
            data["q2"].strip(),
            data["q3"].strip(),
            data["q4"].strip(),
            data["q5"].strip(),
            data["q6"].strip(),
        ),
    )
    conn.commit()
    brief_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    brief = row_to_dict(conn.execute("SELECT * FROM research_briefs WHERE id=?", (brief_id,)).fetchone())
    conn.close()
    return ok(brief), 201


@app.route("/api/briefs/<int:brief_id>", methods=["GET"])
@active_required
def get_brief(brief_id):
    """Return a single research brief. Users can only see their own."""
    conn = get_db()
    brief = row_to_dict(
        conn.execute(
            "SELECT * FROM research_briefs WHERE id=? AND user_id=?",
            (brief_id, session["user_id"]),
        ).fetchone()
    )
    conn.close()
    if not brief:
        return err("Brief not found", 404)
    return ok(brief)


# ── Studies API ───────────────────────────────────────────────────────────────


ALLOWED_STUDY_TYPES = {"survey", "idi", "focus_group"}


@app.route("/api/studies", methods=["GET"])
@active_required
def list_studies():
    """Return all studies for the current user, newest first."""
    conn = get_db()
    studies = rows_to_list(
        conn.execute(
            """
            SELECT s.*, b.q1 as brief_objective
            FROM studies s
            JOIN research_briefs b ON s.brief_id = b.id
            WHERE s.user_id=?
            ORDER BY s.created_at DESC
            """,
            (session["user_id"],),
        ).fetchall()
    )
    conn.close()
    return ok(studies)


@app.route("/api/studies", methods=["POST"])
@active_required
def create_study():
    """
    Create a new study (starts as DRAFT).
    Per FROZEN_RULES rules #9, #10, #11.
    """
    data = request.get_json() or {}
    title = (data.get("title") or "").strip()
    brief_id = data.get("brief_id")
    study_type = (data.get("study_type") or "").strip().lower()

    if not title:
        return err("Title is required")
    if not brief_id:
        return err("brief_id is required")
    if study_type not in ALLOWED_STUDY_TYPES:
        return err(f"study_type must be one of: {', '.join(ALLOWED_STUDY_TYPES)}")

    conn = get_db()
    # Verify brief belongs to this user
    brief = conn.execute(
        "SELECT id FROM research_briefs WHERE id=? AND user_id=?",
        (brief_id, session["user_id"]),
    ).fetchone()
    if not brief:
        conn.close()
        return err("Research brief not found", 404)

    conn.execute(
        "INSERT INTO studies (user_id, brief_id, title, study_type) VALUES (?, ?, ?, ?)",
        (session["user_id"], brief_id, title, study_type),
    )
    conn.commit()
    study_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    study = row_to_dict(conn.execute("SELECT * FROM studies WHERE id=?", (study_id,)).fetchone())
    conn.close()
    return ok(study), 201


@app.route("/api/studies/<int:study_id>", methods=["GET"])
@active_required
def get_study(study_id):
    """Return a single study with its runs. Users can only see their own."""
    conn = get_db()
    study = row_to_dict(
        conn.execute(
            "SELECT * FROM studies WHERE id=? AND user_id=?",
            (study_id, session["user_id"]),
        ).fetchone()
    )
    if not study:
        conn.close()
        return err("Study not found", 404)

    runs = rows_to_list(
        conn.execute(
            "SELECT * FROM study_runs WHERE study_id=? ORDER BY started_at DESC",
            (study_id,),
        ).fetchall()
    )
    brief = row_to_dict(
        conn.execute(
            "SELECT * FROM research_briefs WHERE id=?", (study["brief_id"],)
        ).fetchone()
    )
    conn.close()

    study["runs"] = runs
    study["brief"] = brief
    return ok(study)


# ── Study Runs API ────────────────────────────────────────────────────────────


@app.route("/api/studies/<int:study_id>/run", methods=["POST"])
@active_required
def start_run(study_id):
    """
    Start a new study run (PLACEHOLDER — no real AI is called).
    Per FROZEN_RULES rule #12: transitions study to status=running.
    Per FROZEN_RULES rule #17: logs a grounding trace for run_started.
    Per FROZEN_RULES rule #18: logs cost telemetry (PLACEHOLDER).
    """
    conn = get_db()
    study = row_to_dict(
        conn.execute(
            "SELECT * FROM studies WHERE id=? AND user_id=?",
            (study_id, session["user_id"]),
        ).fetchone()
    )
    if not study:
        conn.close()
        return err("Study not found", 404)

    # Create a new run record
    conn.execute(
        "INSERT INTO study_runs (study_id, status) VALUES (?, 'running')",
        (study_id,),
    )
    conn.commit()
    run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Log grounding trace: run_started
    log_grounding_trace(
        conn,
        run_id,
        "run_started",
        payload={
            "study_type": study["study_type"],
            "note": "PLACEHOLDER — no real AI call made",
        },
    )

    # Log a placeholder completion trace immediately (no real async in V1)
    log_grounding_trace(
        conn,
        run_id,
        "placeholder_output",
        payload={
            "message": "Simulated study run complete. Results are PLACEHOLDER.",
            "study_type": study["study_type"],
        },
    )

    # Log PLACEHOLDER cost telemetry
    log_cost_telemetry(conn, run_id, study["study_type"])

    # Update study status to "running"
    conn.execute("UPDATE studies SET status='running' WHERE id=?", (study_id,))
    # Update run status to completed (placeholder — instant in V1)
    conn.execute(
        "UPDATE study_runs SET status='completed', finished_at=datetime('now') WHERE id=?",
        (run_id,),
    )
    conn.commit()

    run = row_to_dict(conn.execute("SELECT * FROM study_runs WHERE id=?", (run_id,)).fetchone())
    conn.close()
    return ok(run), 201


@app.route("/api/runs/<int:run_id>/qa", methods=["POST"])
@active_required
def set_qa_result(run_id):
    """
    Set the QA verdict for a study run.
    Per FROZEN_RULES rule #14: only PASS, DOWNGRADE, FAIL are allowed.
    Per FROZEN_RULES rule #15: QA result logged to grounding_trace BEFORE study status update.
    Per FROZEN_RULES rule #13: can only QA a run in running or completed status.
    """
    data = request.get_json() or {}
    verdict = (data.get("verdict") or "").strip().upper()

    if verdict not in ("PASS", "DOWNGRADE", "FAIL"):
        return err("verdict must be PASS, DOWNGRADE, or FAIL")

    conn = get_db()
    # Fetch the run and verify ownership via the study
    run = row_to_dict(
        conn.execute(
            """
            SELECT r.*, s.user_id, s.id as study_id, s.study_type
            FROM study_runs r
            JOIN studies s ON r.study_id = s.id
            WHERE r.id=? AND s.user_id=?
            """,
            (run_id, session["user_id"]),
        ).fetchone()
    )
    if not run:
        conn.close()
        return err("Run not found", 404)
    if run["status"] not in ("running", "completed"):
        conn.close()
        return err("Run must be in running or completed status to QA")

    # Per FROZEN_RULES rule #15: log grounding trace FIRST, then update status
    log_grounding_trace(
        conn,
        run_id,
        "qa_result",
        qa_verdict=verdict,
        payload={"verdict": verdict, "study_type": run["study_type"]},
    )

    # Update run with QA result
    conn.execute("UPDATE study_runs SET qa_result=? WHERE id=?", (verdict, run_id))

    # Update parent study status based on verdict
    new_study_status = {
        "PASS": "completed",
        "DOWNGRADE": "completed_downgrade",
        "FAIL": "failed",
    }[verdict]
    conn.execute(
        "UPDATE studies SET status=? WHERE id=?",
        (new_study_status, run["study_id"]),
    )
    conn.commit()

    updated_run = row_to_dict(
        conn.execute("SELECT * FROM study_runs WHERE id=?", (run_id,)).fetchone()
    )
    conn.close()
    return ok(updated_run)


# ── Telemetry API ─────────────────────────────────────────────────────────────


@app.route("/api/runs/<int:run_id>/telemetry", methods=["GET"])
@active_required
def get_run_telemetry(run_id):
    """Return grounding traces and cost telemetry for a specific run."""
    conn = get_db()
    # Verify ownership
    run = conn.execute(
        """
        SELECT r.* FROM study_runs r
        JOIN studies s ON r.study_id = s.id
        WHERE r.id=? AND s.user_id=?
        """,
        (run_id, session["user_id"]),
    ).fetchone()
    if not run:
        conn.close()
        return err("Run not found", 404)

    traces = rows_to_list(
        conn.execute(
            "SELECT * FROM grounding_traces WHERE study_run_id=? ORDER BY created_at",
            (run_id,),
        ).fetchall()
    )
    costs = rows_to_list(
        conn.execute(
            "SELECT * FROM cost_telemetry WHERE study_run_id=? ORDER BY created_at",
            (run_id,),
        ).fetchall()
    )
    conn.close()
    return ok({"traces": traces, "costs": costs})


@app.route("/api/telemetry/export", methods=["GET"])
@active_required
def export_telemetry():
    """
    Export all cost telemetry for the current user as JSON.
    Per FROZEN_RULES rule #19.
    """
    conn = get_db()
    records = rows_to_list(
        conn.execute(
            """
            SELECT ct.*, sr.study_id, s.title as study_title, s.study_type
            FROM cost_telemetry ct
            JOIN study_runs sr ON ct.study_run_id = sr.id
            JOIN studies s ON sr.study_id = s.id
            WHERE s.user_id=?
            ORDER BY ct.created_at DESC
            """,
            (session["user_id"],),
        ).fetchall()
    )
    conn.close()
    return ok(records)


# ── Startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Initialize the database tables when the app starts
    init_db()
    port = int(os.environ.get("PORT", 5000))
    # debug=True gives auto-reload during development
    app.run(host="0.0.0.0", port=port, debug=True)
