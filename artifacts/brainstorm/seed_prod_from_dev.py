"""One-shot copy of dev Postgres -> prod Postgres for the V1A cutover.

Run this AFTER the first publish has created the empty production Neon
database. Both databases must already have the application schema (the
prod schema is created automatically by ``migrate_db()`` on the first
request the deployed app receives, so hit the site once before running
this script).

Usage::

    SOURCE_DATABASE_URL="<dev DATABASE_URL>" \\
    TARGET_DATABASE_URL="<prod DATABASE_URL>" \\
    python3 seed_prod_from_dev.py            # dry run, prints plan

    SOURCE_DATABASE_URL=... TARGET_DATABASE_URL=... \\
    python3 seed_prod_from_dev.py --force    # actually copy

If ``SOURCE_DATABASE_URL`` is unset, falls back to ``DATABASE_URL`` (the
one your local dev shell already has). The target URL has no fallback --
you must pass it explicitly so you can't accidentally seed dev into
itself.

Refuses to run if the target has any rows in any of the user-data
tables, unless ``--force`` is passed.
"""

from __future__ import annotations

import os
import sys
import time

import psycopg

# Tables in dependency-safe insert order. FK checks are bypassed via
# session_replication_role=replica, but we still copy parents first to
# keep the replication-role trick from being load-bearing for ordering.
TABLES_IN_ORDER = [
    # Parents (no FK dependencies)
    "users",
    "sessions",
    "app_settings",
    "allowed_models",
    "model_config",
    "admin_web_sources",
    "admin_uploads",
    "blog_posts",
    "chat_messages",
    "cost_telemetry",
    "followups",
    "grounding_traces",
    "model_health_checks",
    "model_health_status",
    "persona_model_pool",
    "step1_telemetry_events",
    "weekly_qa_reports",
    # Children (depend on the above; ordered parents-first within this layer)
    "personas",        # -> users
    "studies",         # -> users
    "user_uploads",    # -> users
    "study_documents", # -> studies, user_uploads
]


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _table_exists(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name=%s",
            (name,),
        )
        return cur.fetchone() is not None


def _columns(conn, name: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s "
            "ORDER BY ordinal_position",
            (name,),
        )
        return [r[0] for r in cur.fetchall()]


def _row_count(conn, name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {_quote_ident(name)}")
        return cur.fetchone()[0]


def _bump_sequences(conn) -> None:
    """Set every owned sequence to MAX(id)+1 so future inserts don't collide."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              c.table_name,
              c.column_name,
              pg_get_serial_sequence(format('%I', c.table_name), c.column_name) AS seq
            FROM information_schema.columns c
            WHERE c.table_schema='public'
              AND c.column_default LIKE 'nextval(%%'
            """
        )
        rows = cur.fetchall()
    for table, col, seq in rows:
        if not seq:
            continue
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT setval(%s, COALESCE((SELECT MAX({_quote_ident(col)}) "
                f"FROM {_quote_ident(table)}), 0) + 1, false)",
                (seq,),
            )
        print(f"  bumped sequence {seq} for {table}.{col}")


def main() -> int:
    force = "--force" in sys.argv

    src_url = os.environ.get("SOURCE_DATABASE_URL") or os.environ.get("DATABASE_URL")
    tgt_url = os.environ.get("TARGET_DATABASE_URL")

    if not src_url:
        print("ERROR: set SOURCE_DATABASE_URL (or DATABASE_URL) to the dev URL", file=sys.stderr)
        return 2
    if not tgt_url:
        print("ERROR: set TARGET_DATABASE_URL to the prod URL", file=sys.stderr)
        return 2
    if src_url == tgt_url:
        print("ERROR: source and target URLs are identical", file=sys.stderr)
        return 2

    print(f"SOURCE host: {psycopg.conninfo.conninfo_to_dict(src_url).get('host', '?')}")
    print(f"TARGET host: {psycopg.conninfo.conninfo_to_dict(tgt_url).get('host', '?')}")
    print()

    src = psycopg.connect(src_url)
    tgt = psycopg.connect(tgt_url)

    # --- safety: don't clobber a non-empty target ----------------------------
    nonempty = []
    for t in TABLES_IN_ORDER:
        if _table_exists(tgt, t):
            n = _row_count(tgt, t)
            if n > 0:
                nonempty.append((t, n))
    if nonempty and not force:
        print("REFUSING: target has existing rows in:")
        for t, n in nonempty:
            print(f"  {t}: {n}")
        print("\nIf you really want to wipe and re-seed, re-run with --force.")
        return 1

    # --- plan -----------------------------------------------------------------
    plan = []
    total_src = 0
    for t in TABLES_IN_ORDER:
        if not _table_exists(src, t):
            print(f"SKIP {t}: not in source")
            continue
        if not _table_exists(tgt, t):
            print(f"SKIP {t}: not in target (run the deployed app once to create the schema)")
            continue
        src_cols = _columns(src, t)
        tgt_cols = _columns(tgt, t)
        common = [c for c in src_cols if c in tgt_cols]
        n = _row_count(src, t)
        total_src += n
        plan.append((t, common, n))

    print(f"\nPlan: copy {total_src} rows across {len(plan)} tables")
    if not force:
        print("DRY RUN. Re-run with --force to execute.")
        return 0

    # --- copy -----------------------------------------------------------------
    # Neon (and most managed PG) doesn't grant the
    # ``session_replication_role`` privilege, so we can't blanket-disable
    # FK checks. Instead we rely on TABLES_IN_ORDER being topologically
    # sorted: DELETE in reverse order (children first), INSERT in forward
    # order (parents first). That keeps every intermediate state FK-valid.
    started = time.time()

    if force:
        for t in reversed([p[0] for p in plan]):
            with tgt.cursor() as tc:
                tc.execute(f"DELETE FROM {_quote_ident(t)}")
        tgt.commit()

    copied_total = 0
    for t, cols, expected in plan:
        if expected == 0:
            print(f"  {t}: 0 rows (skip)")
            continue

        col_list = ", ".join(_quote_ident(c) for c in cols)
        select_sql = f"SELECT {col_list} FROM {_quote_ident(t)}"
        copy_in_sql = f"COPY {_quote_ident(t)} ({col_list}) FROM STDIN"

        copied = 0
        with src.cursor(name="seed_cur") as sc:
            sc.itersize = 1000
            sc.execute(select_sql)
            with tgt.cursor() as tc, tc.copy(copy_in_sql) as cp:
                for row in sc:
                    cp.write_row(row)
                    copied += 1
        tgt.commit()
        copied_total += copied
        print(f"  {t}: {copied} rows copied")

    print(f"\nBumping sequences on target...")
    _bump_sequences(tgt)
    tgt.commit()

    src.close()
    tgt.close()

    print(f"\nDONE: {copied_total} rows in {time.time()-started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
