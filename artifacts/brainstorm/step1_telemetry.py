"""Step 1 telemetry table init + writer (Prompts 3 & 4).

Prompt 3 defines the *semantics* (event types, ``pattern_id`` /
``template_id`` values, what counts as a rewrite). Prompt 4 is mechanics
only: this module is the storage + writer layer that records those
signals in a counts-only, append-only, non-adaptive manner.

Non-blocking contract (Prompt 4 §4)
-----------------------------------
- Each call to ``record_step1_event`` opens a short-lived sqlite
  connection, executes a single INSERT, commits, and closes. No LLM,
  HTTP, or email call is ever made while the connection is open.
- The writer never raises out into the caller. All exceptions are
  caught and logged with a ``STEP1_TEL_*`` prefix to stderr.
- A soft latency log (``STEP1_TEL_SLOW_WRITE``) is emitted when a
  single write exceeds ``STEP1_TELEMETRY_SLOW_MS`` (default 250 ms).
  This is observational only — there are no retries, fallbacks, or
  control-flow effects.
- Callers must ``conn.commit()`` on their own request connection
  *before* calling ``record_step1_event``; otherwise this writer's
  second connection waits on the busy_timeout. (Regression test:
  ``test_step1_library.TelemetryLockOrderingTests``.)

Kill switch (Prompt 4 §3)
-------------------------
- ``STEP1_TELEMETRY_ENABLED`` env var (default ``"1"``). When set to
  ``"0"``, ``"false"``, ``"no"``, or ``"off"`` (case-insensitive),
  ``record_step1_event`` returns immediately without opening the DB
  and ``summarize_recent`` returns its zero-state dict. The schema is
  still created on startup so flipping the switch back on is seamless.
- ``count_session_quick_actions`` continues to read regardless: a
  disabled-then-re-enabled run must not double-count rewrites that
  were already persisted in earlier sessions.

Session scope (Prompt 4 §2 — fallback in use)
---------------------------------------------
- We use the **per-study, per-page-load** fallback that Prompt 4 §2
  explicitly permits: ``step1_session_id`` is generated as ``uuid4().hex``
  at study creation in ``app.py`` and threaded through every form
  POST. A future upgrade to a true "first-edit-of-BP-or-DS to
  Save-checkpoint OR navigate-away" lifecycle would require a
  ``/step1-session/start|end`` endpoint pair plus client timing; that
  upgrade is captured as a follow-up and is out of scope for Prompt 4.

Schema
------
``created_at`` is the canonical timestamp. Older databases with
``occurred_at`` are migrated in-place by ``init_step1_telemetry``.
The table is **append-only**; no code path may issue ``UPDATE`` or
``DELETE`` against it (regression-tested by
``test_step1_telemetry_infra.SchemaDisciplineTests``).
"""

import os
import sqlite3
import time

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "brainstorm.db")

_DISABLED_VALUES = frozenset({"0", "false", "no", "off"})


def telemetry_enabled():
    """Return True when the kill switch is **not** engaged.

    Read the env var on every call (cheap, and lets tests flip it via
    ``mock.patch.dict(os.environ, ...)`` without re-importing).
    """
    raw = (os.environ.get("STEP1_TELEMETRY_ENABLED") or "1").strip().lower()
    return raw not in _DISABLED_VALUES


def _slow_ms_threshold():
    """Soft latency budget in ms. ``0`` is allowed (always emit the
    marker — useful for tests). Negative or non-numeric → default 250.
    """
    raw = os.environ.get("STEP1_TELEMETRY_SLOW_MS")
    if raw is None or raw == "":
        return 250
    try:
        v = int(raw)
        return v if v >= 0 else 250
    except (TypeError, ValueError):
        return 250

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
    """Open -> insert -> close. Never raises out.

    Honors the ``STEP1_TELEMETRY_ENABLED`` kill switch and emits a soft
    ``STEP1_TEL_SLOW_WRITE`` log when the bounded INSERT exceeds the
    ``STEP1_TELEMETRY_SLOW_MS`` budget. See module docstring for the
    full non-blocking contract.
    """
    if not telemetry_enabled():
        return
    if event_type not in EVENT_TYPES:
        print(f"STEP1_TEL_BAD_EVENT_TYPE: {event_type}", flush=True)
        return
    conn = None
    started = time.monotonic()
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
        elapsed_ms = int((time.monotonic() - started) * 1000)
        slow_ms = _slow_ms_threshold()
        if elapsed_ms >= slow_ms:
            print(
                f"STEP1_TEL_SLOW_WRITE event={event_type} study={study_id} "
                f"elapsed_ms={elapsed_ms} budget_ms={slow_ms}",
                flush=True,
            )


def summarize_recent(days=7, top_n=10, qa_page=1, tpl_page=1, page_size=20):
    """Aggregate Step 1 telemetry over the last ``days`` days.

    Opens a short-lived connection, executes a handful of grouped SELECTs,
    and closes immediately so we never hold the DB open during render.
    Returns a dict of plain Python primitives suitable for templating.

    The two potentially-large tabular sections (quick_actions and
    top_templates) are paginated with ``qa_page`` / ``tpl_page`` and a
    shared ``page_size``. Total row counts are returned alongside each
    paged slice so the template can render Prev/Next controls.
    """
    try:
        days_int = max(1, min(int(days), 365))
    except (TypeError, ValueError):
        days_int = 7
    try:
        top_int = max(1, min(int(top_n), 50))
    except (TypeError, ValueError):
        top_int = 10
    try:
        page_size_int = max(5, min(int(page_size), 100))
    except (TypeError, ValueError):
        page_size_int = 20
    try:
        qa_page_int = max(1, int(qa_page))
    except (TypeError, ValueError):
        qa_page_int = 1
    try:
        tpl_page_int = max(1, int(tpl_page))
    except (TypeError, ValueError):
        tpl_page_int = 1

    cutoff_sql = f"datetime('now', '-{days_int} days')"
    enabled = telemetry_enabled()
    out = {
        "days": days_int,
        "top_n": top_int,
        "page_size": page_size_int,
        "total_events": 0,
        "events_by_type": [],
        "patterns_by_field": {},
        "quick_actions": {"rows": [], "page": qa_page_int,
                          "page_size": page_size_int, "total": 0,
                          "total_pages": 1, "has_prev": False, "has_next": False},
        "rewrite_distribution": [],
        "top_templates": {"rows": [], "page": tpl_page_int,
                          "page_size": page_size_int, "total": 0,
                          "total_pages": 1, "has_prev": False, "has_next": False},
        "telemetry_enabled": enabled,
    }

    if not enabled:
        return out

    conn = None
    try:
        conn = _open()
        out["total_events"] = int(conn.execute(
            f"SELECT COUNT(*) FROM step1_telemetry_events "
            f"WHERE created_at >= {cutoff_sql}"
        ).fetchone()[0])

        out["events_by_type"] = [
            {"event_type": r[0], "count": int(r[1])}
            for r in conn.execute(
                f"SELECT event_type, COUNT(*) FROM step1_telemetry_events "
                f"WHERE created_at >= {cutoff_sql} "
                f"GROUP BY event_type ORDER BY 2 DESC"
            ).fetchall()
        ]

        rows = conn.execute(
            f"SELECT field, pattern_id, COUNT(*) AS c FROM step1_telemetry_events "
            f"WHERE event_type = 'pattern_triggered' "
            f"  AND created_at >= {cutoff_sql} "
            f"  AND pattern_id IS NOT NULL "
            f"GROUP BY field, pattern_id ORDER BY field, c DESC"
        ).fetchall()
        patterns_by_field = {}
        for field, pattern_id, c in rows:
            key = field or "(unknown)"
            bucket = patterns_by_field.setdefault(key, [])
            if len(bucket) < top_int:
                bucket.append({"pattern_id": pattern_id, "count": int(c)})
        out["patterns_by_field"] = patterns_by_field

        qa_total = int(conn.execute(
            f"SELECT COUNT(*) FROM (SELECT 1 FROM step1_telemetry_events "
            f" WHERE event_type = 'quick_action_used' "
            f"   AND created_at >= {cutoff_sql} "
            f"   AND quick_action IS NOT NULL "
            f" GROUP BY quick_action, field)"
        ).fetchone()[0])
        qa_total_pages = max(1, (qa_total + page_size_int - 1) // page_size_int)
        qa_page_clamped = min(qa_page_int, qa_total_pages)
        qa_offset = (qa_page_clamped - 1) * page_size_int
        qa_rows = [
            {"quick_action": r[0], "field": r[1] or "(any)", "count": int(r[2])}
            for r in conn.execute(
                f"SELECT quick_action, field, COUNT(*) FROM step1_telemetry_events "
                f"WHERE event_type = 'quick_action_used' "
                f"  AND created_at >= {cutoff_sql} "
                f"  AND quick_action IS NOT NULL "
                f"GROUP BY quick_action, field "
                f"ORDER BY 3 DESC, quick_action, field LIMIT ? OFFSET ?",
                (page_size_int, qa_offset),
            ).fetchall()
        ]
        out["quick_actions"] = {
            "rows": qa_rows,
            "page": qa_page_clamped,
            "page_size": page_size_int,
            "total": qa_total,
            "total_pages": qa_total_pages,
            "has_prev": qa_page_clamped > 1,
            "has_next": qa_page_clamped < qa_total_pages,
        }

        out["rewrite_distribution"] = [
            {"length_bucket": r[0] or "(none)",
             "rewrites": int(r[1]) if r[1] is not None else 0,
             "count": int(r[2])}
            for r in conn.execute(
                f"SELECT length_bucket, count_value, COUNT(*) "
                f"FROM step1_telemetry_events "
                f"WHERE event_type = 'rewrite_count_at_save' "
                f"  AND created_at >= {cutoff_sql} "
                f"GROUP BY length_bucket, count_value "
                f"ORDER BY length_bucket, count_value"
            ).fetchall()
        ]

        tpl_total = int(conn.execute(
            f"SELECT COUNT(*) FROM (SELECT 1 FROM step1_telemetry_events "
            f" WHERE event_type = 'template_applied' "
            f"   AND created_at >= {cutoff_sql} "
            f"   AND template_id IS NOT NULL "
            f" GROUP BY template_id, field)"
        ).fetchone()[0])
        tpl_total_pages = max(1, (tpl_total + page_size_int - 1) // page_size_int)
        tpl_page_clamped = min(tpl_page_int, tpl_total_pages)
        tpl_offset = (tpl_page_clamped - 1) * page_size_int
        tpl_rows = [
            {"template_id": r[0], "field": r[1] or "(any)", "count": int(r[2])}
            for r in conn.execute(
                f"SELECT template_id, field, COUNT(*) FROM step1_telemetry_events "
                f"WHERE event_type = 'template_applied' "
                f"  AND created_at >= {cutoff_sql} "
                f"  AND template_id IS NOT NULL "
                f"GROUP BY template_id, field "
                f"ORDER BY 3 DESC, template_id, field LIMIT ? OFFSET ?",
                (page_size_int, tpl_offset),
            ).fetchall()
        ]
        out["top_templates"] = {
            "rows": tpl_rows,
            "page": tpl_page_clamped,
            "page_size": page_size_int,
            "total": tpl_total,
            "total_pages": tpl_total_pages,
            "has_prev": tpl_page_clamped > 1,
            "has_next": tpl_page_clamped < tpl_total_pages,
        }
    except Exception as e:
        print(f"STEP1_TEL_SUMMARY_ERROR err={e}", flush=True)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return out


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
