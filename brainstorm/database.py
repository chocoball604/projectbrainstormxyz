"""
database.py — SQLite setup and helpers for Project Brainstorm V1.

We use Python's built-in sqlite3 module (no ORM) to keep things simple.
All data persists in brainstorm.db in the same directory as this file.
"""

import sqlite3
import os

# Path to the SQLite database file
DB_PATH = os.path.join(os.path.dirname(__file__), "brainstorm.db")


def get_db():
    """Open a database connection with row_factory so rows behave like dicts."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # lets us do row["column_name"]
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    """Create all tables if they don't exist. Safe to call multiple times."""
    conn = get_db()
    c = conn.cursor()

    # ---- users ----
    # role: "user" or "admin"
    # status: "pending" (waiting for approval) or "active" (can use app)
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            email     TEXT    UNIQUE NOT NULL,
            password  TEXT    NOT NULL,
            role      TEXT    NOT NULL DEFAULT 'user',
            status    TEXT    NOT NULL DEFAULT 'pending',
            created_at TEXT   NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # ---- research_briefs ----
    # 6 required questions per FROZEN_RULES rule #7
    c.execute("""
        CREATE TABLE IF NOT EXISTS research_briefs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL REFERENCES users(id),
            q1         TEXT    NOT NULL,
            q2         TEXT    NOT NULL,
            q3         TEXT    NOT NULL,
            q4         TEXT    NOT NULL,
            q5         TEXT    NOT NULL,
            q6         TEXT    NOT NULL,
            created_at TEXT    NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # ---- studies ----
    # study_type: "survey", "idi", or "focus_group"
    # status: "draft", "running", "completed", "completed_downgrade", "failed"
    c.execute("""
        CREATE TABLE IF NOT EXISTS studies (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL REFERENCES users(id),
            brief_id   INTEGER NOT NULL REFERENCES research_briefs(id),
            title      TEXT    NOT NULL,
            study_type TEXT    NOT NULL,
            status     TEXT    NOT NULL DEFAULT 'draft',
            created_at TEXT    NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # ---- study_runs ----
    # Each time a study is "run", a new run record is created
    # qa_result: "PASS", "DOWNGRADE", "FAIL", or NULL (not yet QA'd)
    c.execute("""
        CREATE TABLE IF NOT EXISTS study_runs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id   INTEGER NOT NULL REFERENCES studies(id),
            status     TEXT    NOT NULL DEFAULT 'running',
            qa_result  TEXT,
            started_at TEXT    NOT NULL DEFAULT (datetime('now')),
            finished_at TEXT
        )
    """)

    # ---- grounding_traces ----
    # Stores one trace row per event (run_started, qa_result, etc.)
    # payload is stored as a JSON string
    c.execute("""
        CREATE TABLE IF NOT EXISTS grounding_traces (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            study_run_id   INTEGER NOT NULL REFERENCES study_runs(id),
            event_type     TEXT    NOT NULL,
            qa_verdict     TEXT,
            payload        TEXT    NOT NULL DEFAULT '{}',
            is_placeholder INTEGER NOT NULL DEFAULT 1,
            created_at     TEXT    NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # ---- cost_telemetry ----
    # All cost values are PLACEHOLDER (is_placeholder=1) in V1
    c.execute("""
        CREATE TABLE IF NOT EXISTS cost_telemetry (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            study_run_id        INTEGER NOT NULL REFERENCES study_runs(id),
            study_type          TEXT    NOT NULL,
            tokens_input        INTEGER,
            tokens_output       INTEGER,
            estimated_cost_usd  REAL    NOT NULL,
            model_name          TEXT,
            is_placeholder      INTEGER NOT NULL DEFAULT 1,
            notes               TEXT,
            created_at          TEXT    NOT NULL DEFAULT (datetime('now'))
        )
    """)

    conn.commit()
    conn.close()
    print("Database initialized at:", DB_PATH)


def row_to_dict(row):
    """Convert a sqlite3.Row to a plain dict (for JSON serialization)."""
    if row is None:
        return None
    return dict(row)


def rows_to_list(rows):
    """Convert a list of sqlite3.Row objects to a list of dicts."""
    return [dict(r) for r in rows]
