"""End-to-end check that Step 1 telemetry is wired up correctly.

This script drives the running brainstorm Flask server through a real
Step 1 conversation as the seeded test user, exercising each of the four
``step1_telemetry_events.event_type`` values exactly the way the browser
does, then queries ``brainstorm.db`` to confirm the rows landed with a
consistent ``session_id`` / ``study_id`` / ``user_id`` triple from a
single page load.

How to re-run
-------------
1. Make sure the ``artifacts/brainstorm: web`` workflow is running.
   The script auto-detects the Flask port from worker logs / env, but
   you can also export ``BRAINSTORM_BASE_URL`` (e.g.
   ``http://localhost:24634``) to point it at a specific instance.
2. Make sure ``AI_INTEGRATIONS_OPENROUTER_BASE_URL`` and
   ``AI_INTEGRATIONS_OPENROUTER_API_KEY`` are set so Mark's worker can
   produce a real reply (template_applied requires Mark's rewrite to
   match a canonical BP/DS template signature).
3. Run from the repo root:

       python artifacts/brainstorm/test_step1_e2e.py

   The script prints ``E2E_OK`` and exits 0 on success, or ``E2E_FAIL``
   with a diagnostic and exits 1 on failure.

What it asserts
---------------
- A single browser-style ``step1_session_id`` (from the rendered index
  page) is reused across ``pattern_triggered``, ``quick_action_used``,
  ``template_applied``, and ``rewrite_count_at_save`` events.
- All four events share the same ``study_id`` and ``user_id``.
- ``rewrite_count_at_save.count_value`` matches the number of
  ``quick_action_used`` rewrites observed for that field in the session.
"""

from __future__ import annotations

import os
import re
import sqlite3
import sys
import time
from urllib.parse import urlparse, parse_qs

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "brainstorm.db")

TEST_EMAIL = "test@admin.local"
TEST_PASSWORD = "test123"

# Weak draft on purpose -> compute_step1_weakness should flag both fields
# and the seeded library has a matching pattern_id for each reason.
WEAK_BP = "We should launch a new pricing feature."
WEAK_DS = "Pick a pricing tier."

# Strong drafts that follow canonical BP/DS template signatures so the
# rewrite_count_at_save row carries a meaningful length bucket.
STRONG_BP = (
    "We don't yet understand how fast trial drop-off is changing or what "
    "is driving that change across new and returning cohorts."
)
STRONG_DS = (
    "Whether to invest in deeper exploration of trial drop-off drivers or "
    "pause until the key assumptions are clearer."
)


def _detect_base_url() -> str:
    env = os.environ.get("BRAINSTORM_BASE_URL", "").rstrip("/")
    if env:
        return env
    # Try to discover the actual Flask port from the brainstorm log files
    # the workflow system writes. Falls back to 5000.
    log_dir = "/tmp/logs"
    candidates = []
    if os.path.isdir(log_dir):
        for name in os.listdir(log_dir):
            if "brainstorm" in name.lower() and name.endswith(".log"):
                candidates.append(os.path.join(log_dir, name))
    candidates.sort(key=os.path.getmtime, reverse=True)
    for path in candidates:
        try:
            with open(path) as f:
                text = f.read()
        except OSError:
            continue
        m = re.search(r"Running on http://127\.0\.0\.1:(\d+)", text)
        if m:
            return f"http://127.0.0.1:{m.group(1)}"
    return "http://127.0.0.1:5000"


def _fail(msg: str) -> "None":
    print(f"E2E_FAIL: {msg}", flush=True)
    sys.exit(1)


def _login(session: requests.Session, base: str) -> str:
    resp = session.post(
        f"{base}/login",
        data={"email": TEST_EMAIL, "password": TEST_PASSWORD},
        allow_redirects=False,
        timeout=20,
    )
    if resp.status_code not in (301, 302, 303):
        _fail(f"login expected redirect, got {resp.status_code}: {resp.text[:200]}")
    qs = parse_qs(urlparse(resp.headers["Location"]).query)
    token = (qs.get("token") or [""])[0]
    if not token:
        _fail(f"login redirect missing token: {resp.headers['Location']}")
    return token


def _create_study(session: requests.Session, base: str, token: str) -> int:
    title = f"E2E Step1 Telemetry {int(time.time())}"
    resp = session.post(
        f"{base}/create-study-tbd",
        data={"title": title, "token": token},
        allow_redirects=False,
        timeout=20,
    )
    if resp.status_code not in (301, 302, 303):
        _fail(f"create-study-tbd expected redirect, got {resp.status_code}: "
              f"{resp.text[:200]}")
    qs = parse_qs(urlparse(resp.headers["Location"]).query)
    sid = (qs.get("configure") or [""])[0]
    if not sid:
        _fail(f"create-study redirect missing configure id: {resp.headers['Location']}")
    return int(sid)


def _harvest_session_id(session: requests.Session, base: str,
                        token: str, study_id: int) -> str:
    resp = session.get(
        f"{base}/?token={token}&configure={study_id}", timeout=20,
    )
    if resp.status_code != 200:
        _fail(f"index page returned {resp.status_code}")
    m = re.search(r"window\.__step1SessionId\s*=\s*\"([0-9a-f]+)\"", resp.text)
    if not m:
        _fail("could not find window.__step1SessionId in rendered index page")
    return m.group(1)


def _autosave(session, base, token, study_id, sid, field, value):
    resp = session.post(
        f"{base}/save-discovery/{study_id}",
        data={"token": token, "field": field, "value": value,
              "mode": "autosave", "step1_session_id": sid},
        headers={"X-Requested-With": "XMLHttpRequest"},
        timeout=20,
    )
    if resp.status_code != 200:
        _fail(f"autosave {field} returned {resp.status_code}: {resp.text[:200]}")


def _checkpoint(session, base, token, study_id, sid, field, value):
    resp = session.post(
        f"{base}/save-discovery/{study_id}",
        data={"token": token, "field": field, "value": value,
              "mode": "checkpoint", "step1_session_id": sid},
        headers={"X-Requested-With": "XMLHttpRequest"},
        timeout=20,
    )
    if resp.status_code != 200:
        _fail(f"checkpoint {field} returned {resp.status_code}: {resp.text[:200]}")


def _send_chat(session, base, token, study_id, sid, message_text):
    resp = session.post(
        f"{base}/send-chat/{study_id}",
        data={"token": token, "message_text": message_text,
              "step1_session_id": sid},
        allow_redirects=False,
        timeout=30,
    )
    if resp.status_code not in (200, 301, 302, 303):
        _fail(f"send-chat returned {resp.status_code}: {resp.text[:200]}")


def _wait_for_worker(study_id: int, expected_extra_mark_msgs: int,
                     timeout_s: int = 90) -> None:
    """Block until the chat thread has at least the expected number of
    non-placeholder mark messages, or until the timeout."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM chat_messages "
                "WHERE study_id = ? AND sender = 'mark' "
                "AND message_text NOT LIKE '%thinking...%'",
                (study_id,),
            ).fetchone()
        finally:
            conn.close()
        if row and row[0] >= expected_extra_mark_msgs:
            return
        time.sleep(2)
    print(f"E2E_WARN: worker did not produce {expected_extra_mark_msgs} "
          f"mark messages within {timeout_s}s", flush=True)


def _query_events(session_id: str):
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        rows = conn.execute(
            "SELECT event_type, study_id, user_id, session_id, field, "
            "       pattern_id, template_id, quick_action, length_bucket, "
            "       count_value, created_at "
            "FROM step1_telemetry_events WHERE session_id = ? "
            "ORDER BY id ASC",
            (session_id,),
        ).fetchall()
    finally:
        conn.close()
    return rows


def main() -> None:
    base = _detect_base_url()
    print(f"E2E_BASE_URL={base}", flush=True)

    s = requests.Session()
    token = _login(s, base)
    print(f"E2E_LOGIN_OK token_prefix={token[:6]}", flush=True)

    study_id = _create_study(s, base, token)
    print(f"E2E_STUDY_CREATED id={study_id}", flush=True)

    sid = _harvest_session_id(s, base, token, study_id)
    print(f"E2E_SESSION_ID={sid}", flush=True)

    # 1. autosave weak drafts -> pattern_triggered for each field
    _autosave(s, base, token, study_id, sid, "business_problem", WEAK_BP)
    _autosave(s, base, token, study_id, sid, "decision_to_support", WEAK_DS)
    print("E2E_AUTOSAVE_WEAK_OK", flush=True)

    # 2. free-form chat -> Mark issues both rewrites; one of them should
    #    match a canonical template signature -> template_applied
    _send_chat(s, base, token, study_id, sid,
               "Please coach me on tightening my framing.")
    _wait_for_worker(study_id, expected_extra_mark_msgs=1)
    print("E2E_CHAT_FREEFORM_OK", flush=True)

    # 3. quick-action: REWRITE_PROBLEM -> quick_action_used + likely template_applied
    _send_chat(s, base, token, study_id, sid,
               "ACTION:REWRITE_PROBLEM\nrewrite my business problem please")
    _wait_for_worker(study_id, expected_extra_mark_msgs=2)
    print("E2E_QA_REWRITE_PROBLEM_OK", flush=True)

    # 4. quick-action: REWRITE_DECISION -> quick_action_used
    _send_chat(s, base, token, study_id, sid,
               "ACTION:REWRITE_DECISION\nrewrite my decision please")
    _wait_for_worker(study_id, expected_extra_mark_msgs=3)
    print("E2E_QA_REWRITE_DECISION_OK", flush=True)

    # 5. checkpoint strong values -> rewrite_count_at_save (one per field)
    _checkpoint(s, base, token, study_id, sid, "business_problem", STRONG_BP)
    _checkpoint(s, base, token, study_id, sid, "decision_to_support", STRONG_DS)
    print("E2E_CHECKPOINT_OK", flush=True)

    # Inspect what the writer recorded.
    rows = _query_events(sid)
    if not rows:
        _fail(f"no telemetry rows found for session_id={sid}")

    # All rows must share study_id and user_id with the seeded triple.
    study_ids = {r[1] for r in rows}
    user_ids = {r[2] for r in rows}
    if study_ids != {study_id}:
        _fail(f"study_id leak: expected {{ {study_id} }}, got {study_ids}")
    if len(user_ids) != 1 or None in user_ids:
        _fail(f"user_id inconsistent: {user_ids}")

    by_type: dict = {}
    for r in rows:
        by_type.setdefault(r[0], []).append(r)

    missing = [t for t in (
        "pattern_triggered", "quick_action_used",
        "template_applied", "rewrite_count_at_save",
    ) if t not in by_type]
    if missing:
        print("E2E_DUMP_ROWS:")
        for r in rows:
            print(f"  {r}", flush=True)
        _fail(f"missing event types: {missing}")

    # rewrite_count_at_save.count_value should equal the count of
    # quick_action_used rows for the same field/session prefix.
    qa_rows = by_type["quick_action_used"]
    bp_rewrites = sum(
        1 for r in qa_rows
        if r[4] == "business_problem" and (r[7] or "").startswith("REWRITE_PROBLEM")
    )
    ds_rewrites = sum(
        1 for r in qa_rows
        if r[4] == "decision_to_support" and (r[7] or "").startswith("REWRITE_DECISION")
    )
    bp_save = next((r for r in by_type["rewrite_count_at_save"]
                    if r[4] == "business_problem"), None)
    ds_save = next((r for r in by_type["rewrite_count_at_save"]
                    if r[4] == "decision_to_support"), None)
    if bp_save is None or ds_save is None:
        _fail("rewrite_count_at_save missing per-field rows")
    if bp_save[9] != bp_rewrites:
        _fail(f"BP rewrite_count_at_save.count_value={bp_save[9]} "
              f"but observed {bp_rewrites} REWRITE_PROBLEM quick actions")
    if ds_save[9] != ds_rewrites:
        _fail(f"DS rewrite_count_at_save.count_value={ds_save[9]} "
              f"but observed {ds_rewrites} REWRITE_DECISION quick actions")

    print(
        "E2E_OK "
        f"events={len(rows)} "
        f"types={sorted(by_type)} "
        f"study_id={study_id} "
        f"session_id={sid} "
        f"bp_count={bp_save[9]} ds_count={ds_save[9]}",
        flush=True,
    )


if __name__ == "__main__":
    main()
