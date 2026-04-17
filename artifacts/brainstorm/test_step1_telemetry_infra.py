"""Prompt 4 — Step 1 Telemetry Infrastructure regression tests.

These tests prove the *mechanical* invariants of the telemetry layer
without exercising any UX or Mark behavior:

1. ``KillSwitchTests`` — ``STEP1_TELEMETRY_ENABLED=0`` makes
   ``record_step1_event`` a clean no-op (no rows written, no errors,
   no DB open) while ``summarize_recent`` returns a zero-state dict
   with ``telemetry_enabled=False``.
2. ``NeverRaisesTests`` — the writer swallows every error path
   (missing table, bad event type, bad DB path) without raising.
3. ``SchemaDisciplineTests`` — no source file in
   ``artifacts/brainstorm`` issues ``UPDATE``/``DELETE`` against
   ``step1_telemetry_events`` (append-only).
4. ``HookCallSitesPresentTests`` — smoke check that the four hook
   call sites from Prompts 1–3 are still attached at the expected
   files (regression for accidental removal).
5. ``SoftLatencyLogTests`` — when the per-write budget is exceeded
   the writer prints ``STEP1_TEL_SLOW_WRITE`` (observational only —
   no exception, no return-value change).

Run with::

    cd artifacts/brainstorm && python -m unittest test_step1_telemetry_infra
"""

from __future__ import annotations

import io
import os
import re
import sqlite3
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout, redirect_stderr
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import step1_telemetry as tel  # noqa: E402


def _fresh_db():
    """Create a tmp sqlite file with the telemetry schema initialised."""
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    conn = sqlite3.connect(tmp.name)
    tel.init_step1_telemetry(conn)
    conn.commit()
    conn.close()
    return tmp.name


def _row_count(db_path):
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM step1_telemetry_events"
        ).fetchone()
        return int(row[0])
    finally:
        conn.close()


class KillSwitchTests(unittest.TestCase):
    """Prompt 4 §3 — disable cleanly without breaking Step 1 flow."""

    def test_disabled_writes_zero_rows(self):
        db = _fresh_db()
        try:
            with mock.patch.object(tel, "DB_PATH", db), \
                    mock.patch.dict(os.environ,
                                    {"STEP1_TELEMETRY_ENABLED": "0"}):
                # Fire one of every event type — none should land.
                tel.record_step1_event(
                    "pattern_triggered", study_id=1, user_id=1,
                    session_id="s", field="business_problem",
                    pattern_id="BP_VAGUE_INTENT", length_bucket_value="s",
                )
                tel.record_step1_event(
                    "quick_action_used", study_id=1, user_id=1,
                    session_id="s", field="business_problem",
                    quick_action="REWRITE_PROBLEM",
                )
                tel.record_step1_event(
                    "template_applied", study_id=1, user_id=1,
                    session_id="s", field="business_problem",
                    template_id="BP_GAP_HYPOTHESIS",
                )
                tel.record_step1_event(
                    "rewrite_count_at_save", study_id=1, user_id=1,
                    session_id="s", field="business_problem",
                    length_bucket_value="m", count_value=2,
                )
            self.assertEqual(_row_count(db), 0)
        finally:
            os.unlink(db)

    def test_disabled_summary_is_zero_state(self):
        db = _fresh_db()
        try:
            # Pre-populate one row to prove summarize ignores existing
            # rows when the switch is off.
            with mock.patch.object(tel, "DB_PATH", db):
                tel.record_step1_event(
                    "pattern_triggered", study_id=1, user_id=1,
                    session_id="s", field="business_problem",
                    pattern_id="BP_VAGUE_INTENT", length_bucket_value="s",
                )
                self.assertEqual(_row_count(db), 1)

                with mock.patch.dict(os.environ,
                                     {"STEP1_TELEMETRY_ENABLED": "0"}):
                    summary = tel.summarize_recent(days=7)
            self.assertFalse(summary["telemetry_enabled"])
            self.assertEqual(summary["total_events"], 0)
            self.assertEqual(summary["events_by_type"], [])
            self.assertEqual(summary["patterns_by_field"], {})
            self.assertEqual(summary["quick_actions"]["total"], 0)
            self.assertEqual(summary["top_templates"]["total"], 0)
            self.assertEqual(summary["rewrite_distribution"], [])
        finally:
            os.unlink(db)

    def test_enabled_default_writes_row(self):
        db = _fresh_db()
        try:
            # Default (env unset) means enabled.
            env = {k: v for k, v in os.environ.items()
                   if k != "STEP1_TELEMETRY_ENABLED"}
            with mock.patch.object(tel, "DB_PATH", db), \
                    mock.patch.dict(os.environ, env, clear=True):
                self.assertTrue(tel.telemetry_enabled())
                tel.record_step1_event(
                    "pattern_triggered", study_id=1, user_id=1,
                    session_id="s", field="business_problem",
                    pattern_id="BP_VAGUE_INTENT", length_bucket_value="s",
                )
            self.assertEqual(_row_count(db), 1)
        finally:
            os.unlink(db)

    def test_disabled_values_are_recognised(self):
        for val in ("0", "false", "FALSE", "no", "Off", " 0 "):
            with mock.patch.dict(os.environ,
                                 {"STEP1_TELEMETRY_ENABLED": val}):
                self.assertFalse(tel.telemetry_enabled(),
                                 f"expected disabled for {val!r}")
        for val in ("1", "true", "yes", "on", "anything-else"):
            with mock.patch.dict(os.environ,
                                 {"STEP1_TELEMETRY_ENABLED": val}):
                self.assertTrue(tel.telemetry_enabled(),
                                f"expected enabled for {val!r}")

    def test_summarize_zero_state_when_db_missing(self):
        # Switch off + bogus DB path: must still return the zero dict
        # (no exception bubbles to the dashboard).
        with mock.patch.object(tel, "DB_PATH", "/nonexistent/path.db"), \
                mock.patch.dict(os.environ,
                                {"STEP1_TELEMETRY_ENABLED": "0"}):
            summary = tel.summarize_recent(days=7)
        self.assertFalse(summary["telemetry_enabled"])
        self.assertEqual(summary["total_events"], 0)


class NeverRaisesTests(unittest.TestCase):
    """Prompt 4 §4 — writer must never raise into the caller."""

    def test_bad_event_type_returns_silently(self):
        db = _fresh_db()
        try:
            with mock.patch.object(tel, "DB_PATH", db):
                # Should print STEP1_TEL_BAD_EVENT_TYPE and return; no row.
                tel.record_step1_event(
                    "definitely_not_a_real_event",
                    study_id=1, user_id=1, session_id="s",
                )
            self.assertEqual(_row_count(db), 0)
        finally:
            os.unlink(db)

    def test_missing_table_returns_silently(self):
        # Telemetry-enabled but the table doesn't exist (e.g. fresh DB
        # before init_step1_telemetry has run). Writer must swallow the
        # OperationalError, not crash the request.
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        try:
            with mock.patch.object(tel, "DB_PATH", tmp.name):
                tel.record_step1_event(
                    "pattern_triggered", study_id=1, user_id=1,
                    session_id="s", field="business_problem",
                    pattern_id="BP_VAGUE_INTENT",
                )
                # File exists but the table doesn't — nothing to count.
                conn = sqlite3.connect(tmp.name)
                try:
                    has = conn.execute(
                        "SELECT name FROM sqlite_master "
                        "WHERE type='table' AND name='step1_telemetry_events'"
                    ).fetchone()
                finally:
                    conn.close()
                self.assertIsNone(has)
        finally:
            os.unlink(tmp.name)

    def test_bad_db_path_returns_silently(self):
        # Pointing at a directory (not a file) makes sqlite3.connect
        # fail; the writer must catch and swallow.
        with tempfile.TemporaryDirectory() as d:
            with mock.patch.object(tel, "DB_PATH", d):
                # No assertion needed beyond "did not raise".
                tel.record_step1_event(
                    "pattern_triggered", study_id=1, user_id=1,
                    session_id="s", field="business_problem",
                )

    def test_count_session_quick_actions_safe_on_bad_db(self):
        with tempfile.TemporaryDirectory() as d:
            with mock.patch.object(tel, "DB_PATH", d):
                self.assertEqual(
                    tel.count_session_quick_actions("s", "REWRITE_PROBLEM"),
                    0,
                )

    def test_locked_db_never_raises(self):
        """Deterministic regression: hold an EXCLUSIVE write lock on the
        DB from connection A and invoke ``record_step1_event`` on a
        separate connection (the writer's own short-lived one) with a
        tight busy_timeout. The INSERT must fail with
        ``OperationalError: database is locked``, and that error must
        be swallowed — no exception bubbles to the caller, no row is
        written, and a STEP1_TEL_WRITE_ERROR marker is logged.
        """
        db = _fresh_db()
        try:
            holder = sqlite3.connect(db, timeout=0.1)
            holder.isolation_level = None  # explicit BEGIN/COMMIT
            holder.execute("BEGIN EXCLUSIVE")
            try:
                # Patch _open to use a short busy_timeout so the test
                # finishes in ~100 ms instead of 30 s.
                def _short_open():
                    c = sqlite3.connect(db, timeout=0.1)
                    c.execute("PRAGMA busy_timeout = 100")
                    return c

                buf = io.StringIO()
                with mock.patch.object(tel, "DB_PATH", db), \
                        mock.patch.object(tel, "_open", _short_open), \
                        redirect_stderr(buf):
                    # Should not raise even though the DB is locked.
                    tel.record_step1_event(
                        "pattern_triggered", study_id=1, user_id=1,
                        session_id="locked", field="business_problem",
                        pattern_id="BP_VAGUE_INTENT",
                    )
                output = buf.getvalue()
                # Lock error must have been logged but not raised.
                self.assertIn("STEP1_TEL_WRITE_ERROR", output)
                self.assertIn("locked", output.lower())
            finally:
                holder.execute("ROLLBACK")
                holder.close()
            # And no row landed because the lock blocked the INSERT.
            self.assertEqual(_row_count(db), 0)
        finally:
            os.unlink(db)


class HooksUnderKillSwitchTests(unittest.TestCase):
    """Prompt 4 §3 acceptance: with STEP1_TELEMETRY_ENABLED=0, the four
    telemetry hook code paths execute and write zero rows. Each test
    here mirrors the *exact* argument shape used at the corresponding
    call site in app.py / mark_reply_worker.py so that any future drift
    in the hook signatures is caught.
    """

    def setUp(self):
        self.db = _fresh_db()
        self._db_patch = mock.patch.object(tel, "DB_PATH", self.db)
        self._env_patch = mock.patch.dict(
            os.environ, {"STEP1_TELEMETRY_ENABLED": "0"}
        )
        self._db_patch.start()
        self._env_patch.start()

    def tearDown(self):
        self._env_patch.stop()
        self._db_patch.stop()
        os.unlink(self.db)

    def test_pattern_triggered_hook_writes_nothing(self):
        # Mirrors app.py /save-discovery emission.
        tel.record_step1_event(
            "pattern_triggered",
            study_id=42, user_id=7, session_id="hook-pt",
            field="business_problem",
            pattern_id="BP_VAGUE_INTENT",
            length_bucket_value="s",
        )
        self.assertEqual(_row_count(self.db), 0)

    def test_quick_action_used_hook_writes_nothing(self):
        # Mirrors app.py send_chat emission for each quick action.
        for qa, fld in (
            ("REWRITE_PROBLEM", "business_problem"),
            ("REWRITE_DECISION", "decision_to_support"),
            ("BIAS_CHECK", None),
        ):
            tel.record_step1_event(
                "quick_action_used",
                study_id=42, user_id=7, session_id="hook-qa",
                field=fld, quick_action=qa,
            )
        self.assertEqual(_row_count(self.db), 0)

    def test_template_applied_hook_writes_nothing(self):
        # Mirrors mark_reply_worker._record_template_applied emission.
        for fld, tid in (
            ("business_problem", "BP_GAP_HYPOTHESIS"),
            ("decision_to_support", "DS_INVESTIGATE_OR_PAUSE"),
        ):
            tel.record_step1_event(
                "template_applied",
                study_id=42, user_id=7, session_id="hook-tpl",
                field=fld, template_id=tid,
            )
        self.assertEqual(_row_count(self.db), 0)

    def test_rewrite_count_at_save_hook_writes_nothing(self):
        # Mirrors app.py /save-discovery checkpoint branch emission.
        for fld in ("business_problem", "decision_to_support"):
            tel.record_step1_event(
                "rewrite_count_at_save",
                study_id=42, user_id=7, session_id="hook-rcs",
                field=fld, length_bucket_value="m", count_value=2,
            )
        self.assertEqual(_row_count(self.db), 0)

    def test_summarize_recent_hook_returns_zero_state(self):
        # Mirrors /admin/step1-telemetry rendering path.
        s = tel.summarize_recent(days=7)
        self.assertFalse(s["telemetry_enabled"])
        self.assertEqual(s["total_events"], 0)
        self.assertEqual(s["events_by_type"], [])


class SchemaDisciplineTests(unittest.TestCase):
    """Prompt 4 §3 — append-only storage. No UPDATE / DELETE allowed."""

    SOURCES = (
        "step1_telemetry.py",
        "app.py",
        "mark_reply_worker.py",
        "step1_pattern_library.py",
    )
    BANNED = (
        re.compile(r"\bUPDATE\s+step1_telemetry_events\b", re.IGNORECASE),
        re.compile(r"\bDELETE\s+FROM\s+step1_telemetry_events\b",
                   re.IGNORECASE),
        re.compile(r"\bTRUNCATE\s+step1_telemetry_events\b", re.IGNORECASE),
    )

    def test_no_mutation_against_telemetry_table(self):
        offenders = []
        for name in self.SOURCES:
            path = os.path.join(HERE, name)
            if not os.path.isfile(path):
                continue
            with open(path, encoding="utf-8") as f:
                src = f.read()
            for pat in self.BANNED:
                for m in pat.finditer(src):
                    line_no = src[: m.start()].count("\n") + 1
                    offenders.append(f"{name}:{line_no}: {m.group(0)}")
        self.assertEqual(
            offenders, [],
            "step1_telemetry_events must be append-only; found:\n"
            + "\n".join(offenders),
        )


class HookCallSitesPresentTests(unittest.TestCase):
    """Smoke: the four telemetry hooks defined in Prompts 1–3 are still
    attached at the expected files. We don't check exact line numbers
    (those drift); we check for the canonical INSERT-side function name
    plus the event-type literal.
    """

    def _read(self, name):
        with open(os.path.join(HERE, name), encoding="utf-8") as f:
            return f.read()

    def test_pattern_triggered_in_app(self):
        src = self._read("app.py")
        self.assertIn("record_step1_event", src)
        self.assertIn('"pattern_triggered"', src)

    def test_quick_action_used_in_app(self):
        self.assertIn('"quick_action_used"', self._read("app.py"))

    def test_rewrite_count_at_save_in_app(self):
        self.assertIn('"rewrite_count_at_save"', self._read("app.py"))

    def test_template_applied_in_worker(self):
        src = self._read("mark_reply_worker.py")
        self.assertIn("record_step1_event", src)
        self.assertIn('"template_applied"', src)


class SoftLatencyLogTests(unittest.TestCase):
    """Prompt 4 §4 — latency log is observational only (no raise, no
    return-value change). We force the budget to 0 ms so any write
    trips it, then assert the marker appears in stdout.
    """

    def test_slow_write_emits_marker(self):
        db = _fresh_db()
        try:
            buf = io.StringIO()
            with mock.patch.object(tel, "DB_PATH", db), \
                    mock.patch.dict(os.environ,
                                    {"STEP1_TELEMETRY_SLOW_MS": "0"}), \
                    redirect_stderr(buf):
                tel.record_step1_event(
                    "pattern_triggered", study_id=1, user_id=1,
                    session_id="s", field="business_problem",
                    pattern_id="BP_VAGUE_INTENT",
                )
            output = buf.getvalue()
            self.assertIn("STEP1_TEL_SLOW_WRITE", output)
            # Row still written — slow log is observational only.
            self.assertEqual(_row_count(db), 1)
        finally:
            os.unlink(db)

    def test_no_marker_under_default_budget(self):
        db = _fresh_db()
        try:
            buf = io.StringIO()
            env = {k: v for k, v in os.environ.items()
                   if k != "STEP1_TELEMETRY_SLOW_MS"}
            with mock.patch.object(tel, "DB_PATH", db), \
                    mock.patch.dict(os.environ, env, clear=True), \
                    redirect_stderr(buf):
                tel.record_step1_event(
                    "pattern_triggered", study_id=1, user_id=1,
                    session_id="s", field="business_problem",
                    pattern_id="BP_VAGUE_INTENT",
                )
            self.assertNotIn("STEP1_TEL_SLOW_WRITE", buf.getvalue())
        finally:
            os.unlink(db)


class AllFourEventTypesRoundTripTests(unittest.TestCase):
    """Regression for Task #36 at the unit-test layer: a single
    study/user/session triple records all four event types when the
    switch is on, and ``count_session_quick_actions`` aligns with
    ``rewrite_count_at_save.count_value`` exactly.
    """

    def test_round_trip(self):
        db = _fresh_db()
        try:
            sid = "round-trip-session"
            with mock.patch.object(tel, "DB_PATH", db):
                tel.record_step1_event(
                    "pattern_triggered", study_id=7, user_id=3,
                    session_id=sid, field="business_problem",
                    pattern_id="BP_VAGUE_INTENT", length_bucket_value="s",
                )
                tel.record_step1_event(
                    "quick_action_used", study_id=7, user_id=3,
                    session_id=sid, field="business_problem",
                    quick_action="REWRITE_PROBLEM",
                )
                tel.record_step1_event(
                    "quick_action_used", study_id=7, user_id=3,
                    session_id=sid, field="business_problem",
                    quick_action="REWRITE_PROBLEM",
                )
                tel.record_step1_event(
                    "template_applied", study_id=7, user_id=3,
                    session_id=sid, field="business_problem",
                    template_id="BP_GAP_HYPOTHESIS",
                )
                bp_count = tel.count_session_quick_actions(
                    sid, "REWRITE_PROBLEM", field="business_problem"
                )
                tel.record_step1_event(
                    "rewrite_count_at_save", study_id=7, user_id=3,
                    session_id=sid, field="business_problem",
                    length_bucket_value="m", count_value=bp_count,
                )

            conn = sqlite3.connect(db)
            try:
                rows = conn.execute(
                    "SELECT event_type, count_value "
                    "FROM step1_telemetry_events WHERE session_id = ?",
                    (sid,),
                ).fetchall()
            finally:
                conn.close()
            types = {r[0] for r in rows}
            self.assertEqual(types, {
                "pattern_triggered", "quick_action_used",
                "template_applied", "rewrite_count_at_save",
            })
            save_row = next(r for r in rows
                            if r[0] == "rewrite_count_at_save")
            self.assertEqual(save_row[1], 2)
        finally:
            os.unlink(db)


if __name__ == "__main__":
    unittest.main()
