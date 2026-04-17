"""Step 1 telemetry table init + writer (Prompt 3).

Telemetry events are append-only. The writer opens its own short-lived
sqlite connection so it never holds the DB open across LLM / HTTP calls.
"""

import os
import sqlite3

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "brainstorm.db")

EVENT_TYPES = (
    "pattern_triggered",
    "template_applied",
    "quick_action_used",
    "rewrite_count_at_save",
)


def init_step1_telemetry(conn):
    """Create the telemetry table if missing. Caller manages the connection."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS step1_telemetry_events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            occurred_at     TEXT    NOT NULL DEFAULT (datetime('now')),
            study_id        INTEGER,
            user_id         INTEGER,
            session_id      TEXT,
            event_type      TEXT    NOT NULL,
            field           TEXT,
            pattern_id      TEXT,
            template_id     TEXT,
            quick_action    TEXT,
            length_bucket   TEXT,
            count_value     INTEGER,
            extra_json      TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_step1_tel_study "
        "ON step1_telemetry_events (study_id, occurred_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_step1_tel_event "
        "ON step1_telemetry_events (event_type, occurred_at)"
    )


def length_bucket(text):
    n = len((text or "").strip())
    if n < 60:
        return "s"
    if n <= 300:
        return "m"
    return "l"


def _open():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def record_step1_event(
    event_type,
    study_id=None,
    user_id=None,
    session_id=None,
    field=None,
    pattern_id=None,
    template_id=None,
    quick_action=None,
    length_bucket_value=None,
    count_value=None,
    extra_json=None,
):
    """Open -> insert -> close. Never raises out."""
    if event_type not in EVENT_TYPES:
        print(f"STEP1_TEL_BAD_EVENT_TYPE: {event_type}", flush=True)
        return
    conn = None
    try:
        conn = _open()
        conn.execute(
            "INSERT INTO step1_telemetry_events "
            "(study_id, user_id, session_id, event_type, field, pattern_id, "
            " template_id, quick_action, length_bucket, count_value, extra_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                study_id, user_id, session_id, event_type, field, pattern_id,
                template_id, quick_action, length_bucket_value, count_value,
                extra_json,
            ),
        )
        conn.commit()
    except Exception as e:
        print(f"STEP1_TEL_WRITE_ERROR event={event_type} study={study_id} err={e}",
              flush=True)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
