"""SQLite <-> Postgres compatibility shim.

If ``DATABASE_URL`` is set to a postgres URL, ``connect(sqlite_path)``
returns a thin wrapper around a psycopg connection that mimics the
:class:`sqlite3.Connection` API used by the Brainstorm app. Otherwise it
returns a plain ``sqlite3.Connection`` configured exactly as the original
``get_db()`` did.

Why a shim instead of a rewrite
-------------------------------
``app.py`` issues ~510 SQL calls through ``conn.execute(sql, params)``.
Rewriting each one would touch every route. The shim translates SQLite
dialect to PG at the wrapper boundary so the call sites stay identical.

What gets translated (PG only)
------------------------------
- ``?`` placeholders        -> ``%s`` (string-literal aware)
- ``datetime('now')``       -> ``to_char(now() AT TIME ZONE 'UTC', ...)``
- ``datetime('now', '-N <unit>')`` -> ``to_char((now() + interval ...) ...)``
- ``INTEGER PRIMARY KEY AUTOINCREMENT`` -> ``BIGSERIAL PRIMARY KEY``
- ``ALTER TABLE x ADD COLUMN``  -> ``... ADD COLUMN IF NOT EXISTS`` (PG 9.6+)
- ``INSERT OR IGNORE INTO x``   -> ``INSERT INTO x ... ON CONFLICT DO NOTHING``
- ``PRAGMA table_info(x)``      -> ``information_schema.columns`` query;
  result rows expose ``row[1] = column_name`` to match sqlite3's shape.
- ``SELECT name FROM sqlite_master WHERE type='table' AND name='x'``
                                 -> ``information_schema.tables`` query
- ``SELECT last_insert_rowid()`` -> returns the id captured from the
  previous INSERT-with-RETURNING on the same connection.
- All other ``PRAGMA ...`` statements           -> silent no-op.
- ``INSERT INTO <whitelisted_table> (...) VALUES (...)`` automatically
  has ``RETURNING id`` appended so ``cursor.lastrowid`` works. The
  whitelist is the set of tables with an ``id`` BIGSERIAL primary key;
  see ``_TABLES_WITH_ID``.

What does NOT get translated
----------------------------
- ``INSERT OR REPLACE`` -- there are only 3 sites in the app and they all
  target ``app_settings``. They've been rewritten in app.py to portable
  ``ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`` syntax that
  works on both backends (SQLite 3.24+ supports UPSERT).
- ``CREATE INDEX IF NOT EXISTS`` -- valid in both backends as-is.
- Anything inside string literals -- the placeholder translator is
  string-literal aware so ``WHERE name = '?'`` is left intact.
"""

from __future__ import annotations

import os
import re
import sqlite3
from typing import Any, Iterable, Sequence

DATABASE_URL = os.environ.get("DATABASE_URL", "")
IS_POSTGRES = DATABASE_URL.startswith(("postgres://", "postgresql://"))

# Tables whose primary key is an auto-incrementing ``id`` column. Used to
# decide when to silently append ``RETURNING id`` to INSERT statements so
# ``cursor.lastrowid`` keeps working on Postgres.
_TABLES_WITH_ID = frozenset({
    "users", "studies", "personas", "admin_web_sources", "grounding_traces",
    "cost_telemetry", "chat_messages", "followups", "user_uploads",
    "study_documents", "admin_uploads", "allowed_models", "persona_model_pool",
    "model_health_checks", "weekly_qa_reports", "blog_posts",
    "step1_telemetry_events", "user_uploads_new",
})


# --------------------------------------------------------------------------
# SQL translation helpers (PG only). These all assume input that is valid
# SQLite SQL; behavior on malformed SQL is undefined (psycopg will raise).
# --------------------------------------------------------------------------

def _translate_placeholders(sql: str) -> str:
    """Translate SQLite-style SQL to a psycopg-safe parameter template.

    Two passes:
      1. Double every literal ``%`` (anywhere, including inside string
         literals like ``'%foo%'``). psycopg parses ``%`` as a parameter
         marker and will raise ``ProgrammingError`` on stray percents
         even inside quoted literals -- so they must be escaped.
      2. Replace ``?`` with ``%s`` *outside* of string literals (so
         that ``WHERE name = '?'`` is left intact).

    The doubled ``%%`` is collapsed back to ``%`` by psycopg before the
    SQL is sent to the server, so ``LIKE '%foo%'`` round-trips correctly.
    """
    # Pass 1: escape every % so psycopg won't parse it.
    sql = sql.replace('%', '%%')

    # Pass 2: replace ? with %s outside string literals.
    out: list[str] = []
    i = 0
    n = len(sql)
    in_str: str | None = None
    while i < n:
        c = sql[i]
        if in_str is not None:
            out.append(c)
            if c == in_str:
                # SQL escapes a quote by doubling it.
                if i + 1 < n and sql[i + 1] == c:
                    out.append(sql[i + 1])
                    i += 2
                    continue
                in_str = None
            i += 1
            continue
        if c in ("'", '"'):
            in_str = c
            out.append(c)
            i += 1
            continue
        if c == '?':
            out.append('%s')
            i += 1
            continue
        out.append(c)
        i += 1
    return ''.join(out)


_DT_NOW_OFFSET_RE = re.compile(
    r"datetime\(\s*'now'\s*,\s*'\s*([+-]?\d+)\s+(\w+)\s*'\s*\)",
    re.IGNORECASE,
)
_DT_NOW_RE = re.compile(r"datetime\(\s*'now'\s*\)", re.IGNORECASE)
_PG_NOW = "to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')"


def _translate_datetime_now(sql: str) -> str:
    def _offset_repl(m: re.Match[str]) -> str:
        n = m.group(1)
        unit = m.group(2)
        sign = '+' if not n.startswith('-') else ''
        return (
            f"to_char((now() {sign}{n[1:] if n.startswith('+') else n} * interval '1 {unit}') "
            f"AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')"
        )
    sql = _DT_NOW_OFFSET_RE.sub(_offset_repl, sql)
    sql = _DT_NOW_RE.sub(_PG_NOW, sql)
    return sql


_AUTOINC_RE = re.compile(
    r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT",
    re.IGNORECASE,
)


def _translate_autoinc(sql: str) -> str:
    return _AUTOINC_RE.sub("BIGSERIAL PRIMARY KEY", sql)


_ALTER_ADD_RE = re.compile(
    r"(ALTER\s+TABLE\s+\w+\s+ADD\s+COLUMN)\s+(?!IF\s+NOT\s+EXISTS\b)",
    re.IGNORECASE,
)


def _translate_alter_add(sql: str) -> str:
    return _ALTER_ADD_RE.sub(r"\1 IF NOT EXISTS ", sql)


_INSERT_OR_IGNORE_RE = re.compile(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", re.IGNORECASE)

# SQLite uses ``BEGIN IMMEDIATE`` (and the variant ``BEGIN IMMEDIATE
# TRANSACTION``) to acquire a write-lock at transaction start. Postgres has
# no such modifier — every transaction can read freely and acquires row
# locks as it writes. Translate to plain ``BEGIN`` so legacy call sites
# don't crash with a SyntaxError; per-row / per-key serialization on PG
# is the caller's responsibility (e.g. ``SELECT pg_advisory_xact_lock(?)``
# or ``SELECT ... FOR UPDATE``).
_BEGIN_IMMEDIATE_RE = re.compile(
    r"^\s*BEGIN\s+IMMEDIATE(\s+TRANSACTION)?\s*;?\s*$", re.IGNORECASE
)


def _translate_begin_immediate(sql: str) -> str:
    if _BEGIN_IMMEDIATE_RE.match(sql):
        return "BEGIN"
    return sql


def _translate_insert_or_ignore(sql: str) -> str:
    if not _INSERT_OR_IGNORE_RE.search(sql):
        return sql
    sql = _INSERT_OR_IGNORE_RE.sub("INSERT INTO", sql)
    if re.search(r"\bON\s+CONFLICT\b", sql, re.IGNORECASE):
        return sql
    return sql.rstrip().rstrip(';') + " ON CONFLICT DO NOTHING"


_INSERT_TABLE_RE = re.compile(
    r"^\s*INSERT\s+INTO\s+(\w+)\b",
    re.IGNORECASE,
)


def _translate(sql: str) -> str:
    sql = _translate_placeholders(sql)
    sql = _translate_datetime_now(sql)
    sql = _translate_autoinc(sql)
    sql = _translate_alter_add(sql)
    sql = _translate_insert_or_ignore(sql)
    sql = _translate_begin_immediate(sql)
    return sql


# --------------------------------------------------------------------------
# Result-row + cursor wrappers. Row supports both row[0] and row['col'] so
# any call site that worked against sqlite3.Row works unchanged.
# --------------------------------------------------------------------------

class Row:
    """Mimics ``sqlite3.Row`` for the subset of behavior the app uses."""

    __slots__ = ("_values", "_columns", "_idx")

    def __init__(self, values: Sequence[Any], columns: Sequence[str]) -> None:
        self._values = tuple(values)
        self._columns = tuple(columns)
        self._idx = {c: i for i, c in enumerate(columns)}

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return self._values[key]
        if isinstance(key, slice):
            return self._values[key]
        try:
            return self._values[self._idx[key]]
        except KeyError as exc:
            raise IndexError(f"no such column: {key}") from exc

    def keys(self) -> list[str]:
        return list(self._columns)

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __contains__(self, key: Any) -> bool:
        return key in self._idx

    def get(self, key: str, default: Any = None) -> Any:
        idx = self._idx.get(key)
        if idx is None:
            return default
        return self._values[idx]

    def __repr__(self) -> str:
        return f"<Row {dict(zip(self._columns, self._values))}>"


class _PgCursor:
    """Wraps a psycopg cursor; fetch* return Row objects."""

    def __init__(self, raw_cursor, lastrowid: int | None = None) -> None:
        self._raw = raw_cursor
        self._cols = (
            [d[0] for d in raw_cursor.description] if raw_cursor.description else []
        )
        self.rowcount = raw_cursor.rowcount
        self.lastrowid = lastrowid

    def fetchone(self):
        r = self._raw.fetchone()
        if r is None:
            return None
        return Row(r, self._cols)

    def fetchall(self):
        return [Row(r, self._cols) for r in self._raw.fetchall()]

    def fetchmany(self, size: int = 1):
        return [Row(r, self._cols) for r in self._raw.fetchmany(size)]

    def __iter__(self):
        for r in self._raw:
            yield Row(r, self._cols)

    def close(self) -> None:
        try:
            self._raw.close()
        except Exception:
            pass


class _SyntheticCursor:
    """Returned for special-cased statements that don't hit Postgres
    (PRAGMA, last_insert_rowid). Mimics the parts of the cursor API the
    app uses."""

    def __init__(self, rows: list[Row]) -> None:
        self._rows = rows
        self.rowcount = len(rows)
        self.lastrowid = None

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)

    def close(self) -> None:
        pass


# --------------------------------------------------------------------------
# Connection wrapper. The public API is the subset of sqlite3.Connection
# that app.py / step1_telemetry.py / mark_reply_worker.py actually use:
# .execute(sql, params), .commit(), .rollback(), .close(),
# .row_factory (no-op on the PG side; rows are always Row objects).
# --------------------------------------------------------------------------

_PRAGMA_TABLE_INFO_RE = re.compile(
    r"^\s*PRAGMA\s+table_info\(\s*([\w_]+)\s*\)\s*;?\s*$",
    re.IGNORECASE,
)
_SQLITE_MASTER_TABLE_RE = re.compile(
    r"^\s*SELECT\s+name\s+FROM\s+sqlite_master\s+"
    r"WHERE\s+type\s*=\s*'table'\s+AND\s+name\s*=\s*'(\w+)'\s*;?\s*$",
    re.IGNORECASE,
)
_LAST_INSERT_ROWID_RE = re.compile(
    r"^\s*SELECT\s+last_insert_rowid\(\)\s*;?\s*$",
    re.IGNORECASE,
)
_PRAGMA_OTHER_RE = re.compile(r"^\s*PRAGMA\b", re.IGNORECASE)


class _PgConnection:
    """Thin wrapper around ``psycopg.Connection`` that mimics sqlite3."""

    # ``row_factory`` is referenced by app.py for completeness; we always
    # return Row objects regardless of what's assigned to it.
    row_factory: Any = None

    def __init__(self, raw) -> None:
        self._raw = raw
        # Captured from the most recent INSERT ... RETURNING id on this
        # connection. Used to emulate ``cursor.lastrowid`` and the
        # ``SELECT last_insert_rowid()`` query.
        self._last_rowid: int | None = None
        # Per-connection cache of which tables have an ``id`` column.
        # Seeded from the static whitelist (fast path, zero round-trips
        # for known tables) and grown lazily for any other table the
        # caller INSERTs into.
        self._has_id: dict[str, bool] = {t: True for t in _TABLES_WITH_ID}

    # -- statement classification ------------------------------------------

    @staticmethod
    def _insert_target_table(sql: str) -> str | None:
        m = _INSERT_TABLE_RE.match(sql)
        return m.group(1).lower() if m else None

    # -- public API --------------------------------------------------------

    def execute(self, sql: str, params: Iterable[Any] | None = None):
        s = sql.strip()

        # PRAGMA table_info(<name>) -> emulate via information_schema.
        m = _PRAGMA_TABLE_INFO_RE.match(s)
        if m:
            table = m.group(1)
            cur = self._raw.cursor()
            try:
                cur.execute(
                    "SELECT ordinal_position - 1 AS cid, column_name AS name, "
                    "data_type AS type, "
                    "CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull, "
                    "column_default AS dflt_value, 0 AS pk "
                    "FROM information_schema.columns "
                    "WHERE table_schema = current_schema() AND table_name = %s "
                    "ORDER BY ordinal_position",
                    (table,),
                )
                rows = [Row(r, [d[0] for d in cur.description]) for r in cur.fetchall()]
            finally:
                cur.close()
            return _SyntheticCursor(rows)

        # sqlite_master existence check -> information_schema.tables.
        m = _SQLITE_MASTER_TABLE_RE.match(s)
        if m:
            table = m.group(1)
            cur = self._raw.cursor()
            try:
                cur.execute(
                    "SELECT table_name AS name FROM information_schema.tables "
                    "WHERE table_schema = current_schema() AND table_name = %s",
                    (table,),
                )
                rows = [Row(r, [d[0] for d in cur.description]) for r in cur.fetchall()]
            finally:
                cur.close()
            return _SyntheticCursor(rows)

        # SELECT last_insert_rowid() -> return the cached id.
        if _LAST_INSERT_ROWID_RE.match(s):
            value = self._last_rowid if self._last_rowid is not None else 0
            return _SyntheticCursor([Row([value], ["last_insert_rowid()"])])

        # Any other PRAGMA -> no-op.
        if _PRAGMA_OTHER_RE.match(s):
            return _SyntheticCursor([])

        translated = _translate(s)

        # Auto-append RETURNING id for INSERTs into id-bearing tables so
        # cursor.lastrowid works without changing call sites. Tables not
        # in the static whitelist are probed lazily via a savepoint (so a
        # missing column doesn't poison the outer transaction) and the
        # result is cached for the rest of the connection's life.
        params_tuple = tuple(params) if params else ()
        target = self._insert_target_table(translated)
        wants_returning = (
            target is not None
            and "RETURNING" not in translated.upper()
        )

        if wants_returning and self._has_id.get(target, None) is None:
            # Unknown table: probe via savepoint so a failure here can be
            # rolled back without poisoning the caller's transaction.
            probe_sql = translated.rstrip().rstrip(';') + " RETURNING id"
            sp = self._raw.cursor()
            sp.execute("SAVEPOINT _compat_probe")
            try:
                sp.execute(probe_sql, params_tuple)
                try:
                    row = sp.fetchone()
                except Exception:
                    row = None
                sp.execute("RELEASE SAVEPOINT _compat_probe")
                self._has_id[target] = True
                if row is not None:
                    self._last_rowid = row[0]
                    return _PgCursor(sp, lastrowid=row[0])
                return _PgCursor(sp, lastrowid=None)
            except Exception:
                self._has_id[target] = False
                sp.execute("ROLLBACK TO SAVEPOINT _compat_probe")
                sp.close()
                # Fall through to the no-RETURNING execute below.

        added_returning = False
        if wants_returning and self._has_id.get(target):
            translated = translated.rstrip().rstrip(';') + " RETURNING id"
            added_returning = True

        cur = self._raw.cursor()
        cur.execute(translated, params_tuple)

        captured_id: int | None = None
        if added_returning:
            try:
                row = cur.fetchone()
                if row is not None:
                    captured_id = row[0]
                    self._last_rowid = captured_id
            except Exception:
                # ON CONFLICT DO NOTHING on a duplicate row produces no
                # RETURNING row, which raises -- safe to ignore. Subsequent
                # last_insert_rowid() lookups will see the previous id.
                pass

        return _PgCursor(cur, lastrowid=captured_id)

    def executemany(self, sql: str, seq_of_params: Iterable[Iterable[Any]]):
        translated = _translate(sql.strip())
        cur = self._raw.cursor()
        cur.executemany(translated, [tuple(p) for p in seq_of_params])
        return _PgCursor(cur)

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        self._raw.rollback()

    def close(self) -> None:
        try:
            self._raw.close()
        except Exception:
            pass

    # Some call sites reference attributes that exist on sqlite3.Connection
    # but are no-ops on the PG path. Provide minimal shims.

    def cursor(self):
        # Returned cursor follows the same wrapping rules; callers that
        # use cursor() get the wrapped result for consistency.
        raw = self._raw.cursor()
        return _PgCursor(raw)

    @property
    def in_transaction(self) -> bool:
        # psycopg's connection.info.transaction_status: 0 = idle.
        try:
            return self._raw.info.transaction_status != 0
        except Exception:
            return False


# --------------------------------------------------------------------------
# Public connect()
# --------------------------------------------------------------------------

def connect(sqlite_path: str):
    """Open a connection. Postgres if ``DATABASE_URL`` is set, else SQLite.

    The SQLite path mirrors the original ``get_db()`` configuration in
    app.py exactly (PRAGMAs, row_factory). The PG path returns the wrapper
    above.
    """
    if IS_POSTGRES:
        import psycopg
        raw = psycopg.connect(DATABASE_URL)
        return _PgConnection(raw)
    conn = sqlite3.connect(sqlite_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def backend() -> str:
    """Return ``'postgres'`` or ``'sqlite'`` for diagnostics/logging."""
    return "postgres" if IS_POSTGRES else "sqlite"
