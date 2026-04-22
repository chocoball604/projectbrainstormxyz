"""One-shot copy of every table from the local SQLite DB into Postgres.

Usage (from artifacts/brainstorm/):

    # 1. Make sure DATABASE_URL is set and points at the *target* PG.
    # 2. Make sure init_db() has already created the schema on PG (just
    #    booting app.py once with DATABASE_URL set is enough).
    # 3. Run:

    python migrate_sqlite_to_pg.py                # safe: refuses if PG has data
    python migrate_sqlite_to_pg.py --force        # truncates PG tables first
    python migrate_sqlite_to_pg.py --dry-run      # show counts only

What it does
------------
For every user table in the SQLite file (skipping sqlite_* internals):
  * read all rows
  * insert them into the same-named Postgres table, column-for-column
  * do nothing if the column doesn't exist on the PG side (the schema
    might have a column the source DB never gained, or vice versa)

After loading, every PG sequence backing a SERIAL ``id`` is bumped to
``MAX(id) + 1`` so subsequent inserts don't collide.

Safety
------
* Refuses to overwrite a non-empty PG database unless ``--force``.
* Wraps the whole thing in a single transaction; rolls back on any error.
* Read-only against the SQLite source.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from typing import Iterable

HERE = os.path.dirname(os.path.abspath(__file__))
SQLITE_PATH = os.path.join(HERE, "brainstorm.db")


def _sqlite_user_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def _pg_columns(pg_cur, table: str) -> set[str]:
    pg_cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = %s",
        (table,),
    )
    return {r[0] for r in pg_cur.fetchall()}


def _pg_table_exists(pg_cur, table: str) -> bool:
    pg_cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = current_schema() AND table_name = %s",
        (table,),
    )
    return pg_cur.fetchone() is not None


def _pg_table_count(pg_cur, table: str) -> int:
    pg_cur.execute(f'SELECT COUNT(*) FROM "{table}"')
    return pg_cur.fetchone()[0]


def _quote_ident(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise ValueError(f"refusing to quote suspicious identifier: {name!r}")
    return f'"{name}"'


def _bump_serial(pg_cur, table: str, columns: Iterable[str]) -> None:
    """If the table has an ``id`` column with a backing sequence, set the
    sequence's next value to MAX(id) + 1. No-op if there's no sequence."""
    if "id" not in columns:
        return
    pg_cur.execute("SELECT pg_get_serial_sequence(%s, 'id')", (table,))
    row = pg_cur.fetchone()
    if not row or not row[0]:
        return
    seq = row[0]
    pg_cur.execute(f'SELECT COALESCE(MAX(id), 0) FROM "{table}"')
    max_id = pg_cur.fetchone()[0] or 0
    # setval(..., n, true) means "next nextval() returns n+1".
    # If the table is empty, set to 1 with is_called=false.
    if max_id > 0:
        pg_cur.execute("SELECT setval(%s, %s, true)", (seq, max_id))
    else:
        pg_cur.execute("SELECT setval(%s, 1, false)", (seq,))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true",
                        help="TRUNCATE every target table first")
    parser.add_argument("--dry-run", action="store_true",
                        help="print row counts but don't write")
    parser.add_argument("--sqlite-path", default=SQLITE_PATH,
                        help=f"source SQLite file (default: {SQLITE_PATH})")
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url.startswith(("postgres://", "postgresql://")):
        print("ERROR: DATABASE_URL is not set to a postgres URL", file=sys.stderr)
        return 2

    if not os.path.exists(args.sqlite_path):
        print(f"ERROR: source SQLite file not found: {args.sqlite_path}",
              file=sys.stderr)
        return 2

    import psycopg

    src = sqlite3.connect(args.sqlite_path)
    src.row_factory = sqlite3.Row
    tables = _sqlite_user_tables(src)
    print(f"Source SQLite tables ({len(tables)}): {', '.join(tables)}")

    pg = psycopg.connect(db_url)
    try:
        pg_cur = pg.cursor()

        # Pre-flight: check every target table exists on PG, and that PG is
        # empty (or --force was passed).
        missing = []
        non_empty = []
        for t in tables:
            if not _pg_table_exists(pg_cur, t):
                missing.append(t)
                continue
            if _pg_table_count(pg_cur, t) > 0:
                non_empty.append(t)
        if missing:
            print(f"ERROR: these tables don't exist on PG yet: "
                  f"{', '.join(missing)}", file=sys.stderr)
            print("Boot the app once against DATABASE_URL to create the schema.",
                  file=sys.stderr)
            return 3
        if non_empty and not args.force:
            print(f"ERROR: target PG already has data in: "
                  f"{', '.join(non_empty)}", file=sys.stderr)
            print("Re-run with --force to TRUNCATE these tables first.",
                  file=sys.stderr)
            return 4

        if args.dry_run:
            for t in tables:
                src_count = src.execute(
                    f"SELECT COUNT(*) FROM {_quote_ident(t)}"
                ).fetchone()[0]
                pg_count = _pg_table_count(pg_cur, t)
                print(f"  {t:30s} sqlite={src_count:6d}  pg={pg_count:6d}")
            return 0

        if args.force and non_empty:
            for t in non_empty:
                # CASCADE so FKs don't block.
                pg_cur.execute(f'TRUNCATE TABLE "{t}" RESTART IDENTITY CASCADE')

        # Disable FK constraint checks for the duration of the bulk load so
        # we can preserve the source DB's row order regardless of dependencies
        # (and tolerate orphaned dev rows). Constraints are re-enabled by
        # commit/rollback because session_replication_role is session-scoped.
        pg_cur.execute("SET session_replication_role = 'replica'")

        total_rows = 0
        for t in tables:
            src_cols = _sqlite_columns(src, t)
            pg_cols = _pg_columns(pg_cur, t)
            cols = [c for c in src_cols if c in pg_cols]
            if not cols:
                print(f"  {t}: no overlapping columns, skipping")
                continue
            rows = src.execute(
                f"SELECT {', '.join(_quote_ident(c) for c in cols)} "
                f"FROM {_quote_ident(t)}"
            ).fetchall()
            if not rows:
                _bump_serial(pg_cur, t, src_cols)
                print(f"  {t}: 0 rows")
                continue
            placeholders = ", ".join(["%s"] * len(cols))
            sql = (
                f'INSERT INTO "{t}" ({", ".join(_quote_ident(c) for c in cols)}) '
                f"VALUES ({placeholders})"
            )
            pg_cur.executemany(sql, [tuple(r) for r in rows])
            _bump_serial(pg_cur, t, src_cols)
            print(f"  {t}: {len(rows)} rows")
            total_rows += len(rows)

        pg.commit()
        print(f"\nDone. Loaded {total_rows} rows across {len(tables)} tables.")
    except Exception:
        pg.rollback()
        raise
    finally:
        pg.close()
        src.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
