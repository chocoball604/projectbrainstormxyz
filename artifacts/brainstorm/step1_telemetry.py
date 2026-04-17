"""Step 1 telemetry table init + writer (Prompt 3).

Telemetry events are append-only. The writer opens its own short-lived
sqlite connection so it never holds the DB open across LLM / HTTP calls.

Schema column ``created_at`` is the canonical timestamp. Older databases
with ``occurred_at`` are migrated in-place by ``init_step1_telemetry``.
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


def _existing_columns(conn, table):
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def init_step1_telemetry(conn):
    """Create the telemetry table if missing. Caller manages the connection.

    Also migrates legacy ``occurred_at`` column to ``created_at``.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS step1_telemetry_events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
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
    cols = _existing_columns(conn, "step1_telemetry_events")
    if "created_at" not in cols and "occurred_at" in cols:
        try:
            conn.execute(
                "ALTER TABLE step1_telemetry_events "
                "RENAME COLUMN occurred_at TO created_at"
            )
        except sqlite3.OperationalError as e:
            print(f"STEP1_TEL_MIGRATE_ERROR: {e}", flush=True)
    try:
        conn.execute("DROP INDEX IF EXISTS idx_step1_tel_study")
        conn.execute("DROP INDEX IF EXISTS idx_step1_tel_event")
    except sqlite3.OperationalError:
        pass
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_step1_tel_study "
        "ON step1_telemetry_events (study_id, created_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_step1_tel_event "
        "ON step1_telemetry_events (event_type, created_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_step1_tel_session "
        "ON step1_telemetry_events (session_id, event_type)"
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


def count_session_quick_actions(session_id, quick_action_prefix, field=None):
    """Count ``quick_action_used`` rows for a session, scoped to Step 1.

    Used by ``save_discovery`` to compute session-scoped rewrite counts.
    Returns 0 on any error or when ``session_id`` is empty.
    """
    if not session_id:
        return 0
    conn = None
    try:
        conn = _open()
        sql = (
            "SELECT COUNT(*) FROM step1_telemetry_events "
            "WHERE event_type = 'quick_action_used' AND session_id = ? "
            "AND quick_action LIKE ?"
        )
        params = [session_id, quick_action_prefix + "%"]
        if field:
            sql += " AND field = ?"
            params.append(field)
        row = conn.execute(sql, tuple(params)).fetchone()
        return int(row[0]) if row else 0
    except Exception as e:
        print(f"STEP1_TEL_COUNT_ERROR err={e}", flush=True)
        return 0
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
