"""Edge-case tests for db_compat.

Run from artifacts/brainstorm/ in either backend mode:

    python test_db_compat_edges.py            # uses whatever DATABASE_URL is set to
    DATABASE_URL="" python test_db_compat_edges.py   # forces SQLite path
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import db_compat  # noqa: E402


# ---- Pure translation unit tests (no DB needed) ----------------------------

class TranslationUnitTests(unittest.TestCase):
    def test_placeholder_inside_single_quoted_literal_is_left_alone(self):
        sql = "SELECT * FROM t WHERE name = '?' AND id = ?"
        out = db_compat._translate_placeholders(sql)
        # The literal '?' must stay; only the bound ? becomes %s.
        self.assertEqual(out, "SELECT * FROM t WHERE name = '?' AND id = %s")

    def test_placeholder_inside_double_quoted_identifier_left_alone(self):
        sql = 'SELECT "?" FROM t WHERE id = ?'
        out = db_compat._translate_placeholders(sql)
        self.assertEqual(out, 'SELECT "?" FROM t WHERE id = %s')

    def test_doubled_quote_escape_handled(self):
        # 'O''Brien' is a valid SQL literal containing a single quote.
        sql = "SELECT * FROM t WHERE name = 'O''Brien' AND id = ?"
        out = db_compat._translate_placeholders(sql)
        self.assertEqual(out, "SELECT * FROM t WHERE name = 'O''Brien' AND id = %s")

    def test_percent_outside_string_is_doubled(self):
        # If someone wrote "WHERE col % 2 = 0" we must escape the % so
        # psycopg's parameter parser doesn't choke.
        sql = "SELECT * FROM t WHERE id % 2 = ?"
        out = db_compat._translate_placeholders(sql)
        self.assertEqual(out, "SELECT * FROM t WHERE id %% 2 = %s")

    def test_percent_inside_like_literal_is_doubled_too(self):
        # We're conservative: % inside a string literal also gets doubled.
        # That's safe because psycopg will collapse %% back to % before
        # sending to PG. It would only be wrong if the literal already
        # contained a literal %s placeholder, which Brainstorm never does.
        sql = "SELECT * FROM t WHERE x NOT LIKE '%foo%'"
        out = db_compat._translate_placeholders(sql)
        self.assertIn("%%foo%%", out)

    def test_datetime_now_translated(self):
        sql = "INSERT INTO t (ts) VALUES (datetime('now'))"
        out = db_compat._translate_datetime_now(sql)
        self.assertIn("to_char(now() AT TIME ZONE 'UTC'", out)
        self.assertNotIn("datetime('now')", out)

    def test_datetime_now_offset_translated(self):
        sql = "SELECT * FROM t WHERE ts > datetime('now', '-7 days')"
        out = db_compat._translate_datetime_now(sql)
        self.assertNotIn("datetime(", out)
        self.assertIn("interval", out)
        self.assertIn("days", out)

    def test_autoinc_translated(self):
        sql = "CREATE TABLE x (id INTEGER PRIMARY KEY AUTOINCREMENT, n TEXT)"
        out = db_compat._translate_autoinc(sql)
        self.assertIn("BIGSERIAL PRIMARY KEY", out)
        self.assertNotIn("AUTOINCREMENT", out)

    def test_alter_add_column_gets_if_not_exists(self):
        sql = "ALTER TABLE users ADD COLUMN nickname TEXT"
        out = db_compat._translate_alter_add(sql)
        self.assertEqual(
            out, "ALTER TABLE users ADD COLUMN IF NOT EXISTS nickname TEXT"
        )

    def test_alter_add_column_idempotent(self):
        # Don't double-wrap if IF NOT EXISTS is already there.
        sql = "ALTER TABLE users ADD COLUMN IF NOT EXISTS nickname TEXT"
        out = db_compat._translate_alter_add(sql)
        self.assertEqual(out, sql)

    def test_insert_or_ignore_appends_on_conflict(self):
        sql = "INSERT OR IGNORE INTO t (id, n) VALUES (?, ?)"
        out = db_compat._translate_insert_or_ignore(sql)
        self.assertIn("INSERT INTO t", out)
        self.assertIn("ON CONFLICT DO NOTHING", out)
        self.assertNotIn("OR IGNORE", out)


# ---- Live integration tests (use whichever backend is active) --------------

# Use a separate scratch table so we don't pollute the real schema.
_SCRATCH_TABLE = "compat_scratch"


def _scratch_setup(conn):
    conn.execute(f"DROP TABLE IF EXISTS {_SCRATCH_TABLE}")
    conn.execute(f"""
        CREATE TABLE {_SCRATCH_TABLE} (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            name   TEXT UNIQUE NOT NULL,
            tag    TEXT NOT NULL DEFAULT '',
            ts     TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def _scratch_teardown(conn):
    try:
        conn.execute(f"DROP TABLE IF EXISTS {_SCRATCH_TABLE}")
        conn.commit()
    except Exception:
        pass


class LiveIntegrationTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # SQLite needs a file path; PG ignores it.
        cls._tmpfile = None
        if db_compat.IS_POSTGRES:
            cls.path = "/dev/null"  # ignored on PG path
        else:
            fd, cls._tmpfile = tempfile.mkstemp(suffix=".db")
            os.close(fd)
            cls.path = cls._tmpfile
        cls.conn = db_compat.connect(cls.path)
        _scratch_setup(cls.conn)

    @classmethod
    def tearDownClass(cls):
        _scratch_teardown(cls.conn)
        cls.conn.close()
        if cls._tmpfile and os.path.exists(cls._tmpfile):
            os.remove(cls._tmpfile)

    def setUp(self):
        # Drop+recreate between tests for full isolation (some tests do
        # ALTER TABLE ADD COLUMN that would otherwise leak into siblings).
        try:
            _scratch_teardown(self.conn)
            _scratch_setup(self.conn)
        except Exception:
            self.conn.rollback()
            _scratch_teardown(self.conn)
            _scratch_setup(self.conn)

    def test_insert_and_read_back(self):
        cur = self.conn.execute(
            f"INSERT INTO {_SCRATCH_TABLE} (name, tag) VALUES (?, ?)",
            ("alice", "first"),
        )
        self.conn.commit()
        # cursor.lastrowid must work on both backends.
        self.assertIsNotNone(cur.lastrowid)
        self.assertGreater(cur.lastrowid, 0)

        row = self.conn.execute(
            f"SELECT id, name, tag FROM {_SCRATCH_TABLE} WHERE name = ?",
            ("alice",),
        ).fetchone()
        self.assertIsNotNone(row)
        # Both index and name access must work.
        self.assertEqual(row[1], "alice")
        self.assertEqual(row["name"], "alice")
        self.assertEqual(row["tag"], "first")

    def test_select_last_insert_rowid_emulation(self):
        cur = self.conn.execute(
            f"INSERT INTO {_SCRATCH_TABLE} (name) VALUES (?)", ("bob",)
        )
        self.conn.commit()
        # Now use the SQLite-style fetch.
        row = self.conn.execute("SELECT last_insert_rowid()").fetchone()
        self.assertEqual(row[0], cur.lastrowid)

    def test_insert_or_ignore_does_not_raise_on_duplicate(self):
        self.conn.execute(
            f"INSERT INTO {_SCRATCH_TABLE} (name) VALUES (?)", ("dup",)
        )
        self.conn.commit()
        # Second insert with same UNIQUE name must not raise.
        self.conn.execute(
            f"INSERT OR IGNORE INTO {_SCRATCH_TABLE} (name) VALUES (?)",
            ("dup",),
        )
        self.conn.commit()
        # Still only one row.
        row = self.conn.execute(
            f"SELECT COUNT(*) FROM {_SCRATCH_TABLE} WHERE name = ?", ("dup",)
        ).fetchone()
        self.assertEqual(row[0], 1)

    def test_like_with_literal_percent(self):
        # Mirrors app.py:9067 -- "NOT LIKE '%thinking...%'".
        self.conn.execute(
            f"INSERT INTO {_SCRATCH_TABLE} (name, tag) VALUES (?, ?)",
            ("alpha", "thinking..."),
        )
        self.conn.execute(
            f"INSERT INTO {_SCRATCH_TABLE} (name, tag) VALUES (?, ?)",
            ("beta", "real reply"),
        )
        self.conn.commit()
        rows = self.conn.execute(
            f"SELECT name FROM {_SCRATCH_TABLE} "
            f"WHERE tag NOT LIKE '%thinking...%' ORDER BY name"
        ).fetchall()
        self.assertEqual([r["name"] for r in rows], ["beta"])

    def test_pragma_table_info_shape(self):
        # migrate_db introspects schema with PRAGMA table_info(table).
        # row[1] must be the column name (sqlite shape).
        rows = self.conn.execute(
            f"PRAGMA table_info({_SCRATCH_TABLE})"
        ).fetchall()
        col_names = {r[1] for r in rows}
        self.assertEqual(col_names, {"id", "name", "tag", "ts"})

    def test_sqlite_master_table_existence(self):
        # app.py:3086 uses this pattern.
        row = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            f"AND name='{_SCRATCH_TABLE}'"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], _SCRATCH_TABLE)

        # And a non-existent table returns no row.
        row = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='this_table_does_not_exist_anywhere'"
        ).fetchone()
        self.assertIsNone(row)

    def test_pragma_other_is_silent_noop(self):
        # PRAGMAs the app issues at connection setup time.
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA busy_timeout = 30000")
        # No exceptions, no observable effect.

    def test_alter_table_add_column_idempotent(self):
        # Re-running migrate_db should be a no-op. Both backends must
        # tolerate ALTER TABLE ADD COLUMN on a column that already exists
        # (SQLite via try/except in app.py; PG via IF NOT EXISTS injection).
        try:
            self.conn.execute(
                f"ALTER TABLE {_SCRATCH_TABLE} ADD COLUMN extra TEXT DEFAULT 'x'"
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            # SQLite raises "duplicate column" on second add. That mirrors
            # the app's try/except wrap. PG must NOT raise (it gets
            # IF NOT EXISTS injected).
            if db_compat.IS_POSTGRES:
                raise
        # Second add: PG must succeed silently, SQLite will raise.
        try:
            self.conn.execute(
                f"ALTER TABLE {_SCRATCH_TABLE} ADD COLUMN extra TEXT DEFAULT 'x'"
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            if db_compat.IS_POSTGRES:
                self.fail("PG should accept duplicate ADD COLUMN via IF NOT EXISTS")

    def test_fetchone_returns_none_on_empty(self):
        row = self.conn.execute(
            f"SELECT * FROM {_SCRATCH_TABLE} WHERE name = ?", ("nope",)
        ).fetchone()
        self.assertIsNone(row)

    def test_row_keys_method(self):
        self.conn.execute(
            f"INSERT INTO {_SCRATCH_TABLE} (name) VALUES (?)", ("k",)
        )
        self.conn.commit()
        row = self.conn.execute(
            f"SELECT id, name, tag FROM {_SCRATCH_TABLE} WHERE name = ?", ("k",)
        ).fetchone()
        self.assertEqual(set(row.keys()), {"id", "name", "tag"})

    def test_rollback_then_continue(self):
        # On PG, a failing statement poisons the txn until rollback.
        # The wrapper should let callers roll back and continue.
        self.conn.execute(
            f"INSERT INTO {_SCRATCH_TABLE} (name) VALUES (?)", ("ok1",)
        )
        try:
            self.conn.execute(
                f"INSERT INTO {_SCRATCH_TABLE} (name) VALUES (?)", ("ok1",)
            )
        except Exception:
            self.conn.rollback()
        # Now do new work. Must succeed.
        self.conn.execute(
            f"INSERT INTO {_SCRATCH_TABLE} (name) VALUES (?)", ("ok2",)
        )
        self.conn.commit()
        row = self.conn.execute(
            f"SELECT COUNT(*) FROM {_SCRATCH_TABLE} WHERE name IN (?, ?)",
            ("ok1", "ok2"),
        ).fetchone()
        # ok1 is gone (rolled back), ok2 stayed.
        self.assertEqual(row[0], 1)


if __name__ == "__main__":
    print(f"backend = {db_compat.backend()}")
    unittest.main(verbosity=2)
