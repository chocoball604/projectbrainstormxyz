"""Prompt 6 — 3-state Mark presence model regression tests.

Drives the running brainstorm Flask server (same pattern as
test_prompt5_continuity.py) to verify the Mark tile renders correctly
across the three explicit phases (STEP_1_FRAMING / STEP_2_ANCHORS /
STEP_3_EXECUTION_READY) and that the new alignment-check route is
correctly bounded.

Also asserts that the canonical Step 1 telemetry event-type set has
not drifted (regression check identical to Task #42's).

How to run
----------
  python artifacts/brainstorm/test_prompt6_mark_presence.py

Requires the brainstorm web workflow to be running.
"""

from __future__ import annotations

import os
import re
import sqlite3
import sys
import time
import unittest
from urllib.parse import parse_qs, urlparse

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "brainstorm.db")

TEST_EMAIL = "test@admin.local"
TEST_PASSWORD = "test123"


def _detect_base_url() -> str:
    env = os.environ.get("BRAINSTORM_BASE_URL", "").rstrip("/")
    if env:
        return env
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
    return "http://127.0.0.1:24634"


class MarkPresenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = _detect_base_url()
        cls.session = requests.Session()
        resp = cls.session.post(
            f"{cls.base}/login",
            data={"email": TEST_EMAIL, "password": TEST_PASSWORD},
            allow_redirects=False, timeout=20,
        )
        if resp.status_code not in (301, 302, 303):
            raise unittest.SkipTest(
                f"login failed ({resp.status_code}); skipping"
            )
        cls.token = parse_qs(urlparse(resp.headers["Location"]).query
                             ).get("token", [""])[0]
        if not cls.token:
            raise unittest.SkipTest("login did not return token")

    def _make_study(self, *, study_type=None, qa_status=None,
                    bp="BP TEXT seed", ds="DS TEXT seed",
                    mg="MG TEXT seed", pc="PC TEXT seed",
                    ta="TA TEXT seed"):
        title = f"P6 {int(time.time() * 1000)}"
        r = self.session.post(
            f"{self.base}/create-study-tbd",
            data={"title": title, "token": self.token},
            allow_redirects=False, timeout=20,
        )
        sid = int(parse_qs(urlparse(r.headers["Location"]).query
                           ).get("configure", [""])[0])
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            conn.execute(
                "UPDATE studies SET business_problem = ?, "
                "decision_to_support = ? WHERE id = ?",
                (bp, ds, sid),
            )
            if study_type:
                conn.execute(
                    "UPDATE studies SET study_type = ?, study_fit = ?, "
                    "known_vs_unknown = ?, target_audience = ? WHERE id = ?",
                    (study_type, mg, pc, ta, sid),
                )
            if qa_status:
                conn.execute(
                    "UPDATE studies SET qa_status = ? WHERE id = ?",
                    (qa_status, sid),
                )
            conn.commit()
        finally:
            conn.close()
        return sid

    def _get_configure(self, sid):
        r = self.session.get(
            f"{self.base}/?token={self.token}&configure={sid}",
            timeout=20,
        )
        self.assertEqual(r.status_code, 200, r.text[:300])
        return r.text

    # ------------------------------------------------------------------
    # State 1 — STEP_1_FRAMING (no study type set)
    # ------------------------------------------------------------------
    def test_state1_renders_problem_framing_helper(self):
        sid = self._make_study(study_type=None)
        body = self._get_configure(sid)
        self.assertIn('data-ui-phase="STEP_1_FRAMING"', body)
        self.assertIn("Mark &ndash; Problem Framing Helper", body)
        # Quick actions present
        self.assertIn('data-action="rewrite_problem"', body)
        self.assertIn('data-action="rewrite_decision"', body)
        self.assertIn('data-action="check_bias"', body)
        # Chat input present
        self.assertIn('id="chat-input"', body)
        self.assertIn('id="chat-form"', body)
        # State 2 markup absent
        self.assertNotIn('data-ui-phase="STEP_2_ANCHORS"', body)
        self.assertNotIn("Brief Consistency Helper", body)

    # ------------------------------------------------------------------
    # State 2 — STEP_2_ANCHORS (study type set, qa not passed)
    # ------------------------------------------------------------------
    def test_state2_renders_brief_consistency_helper(self):
        # Leave Target Audience empty so auto_ben_precheck (which runs
        # during configure render) does NOT mark precheck_passed and we
        # stay in STEP_2_ANCHORS rather than transitioning to STEP_3.
        sid = self._make_study(study_type="synthetic_idi", ta="")
        body = self._get_configure(sid)
        self.assertIn('data-ui-phase="STEP_2_ANCHORS"', body)
        self.assertIn("Brief Consistency Helper", body)
        # Three alignment buttons
        self.assertIn('data-intent="align_problem"', body)
        self.assertIn('data-intent="align_decision"', body)
        self.assertIn('data-intent="remind_question"', body)
        # No chat input / form / quick actions / transcript in State 2
        self.assertNotIn('id="chat-input"', body)
        self.assertNotIn('id="chat-form"', body)
        self.assertNotIn('id="chat-thread"', body)
        self.assertNotIn('id="mark-quick-actions"', body)
        # Core Framing card from Prompt 5 still visible
        self.assertIn('id="core-framing-card"', body)
        # Collapsed by default
        self.assertIn('id="mark-s2-toggle"', body)
        self.assertIn('aria-expanded="false"', body)

    # ------------------------------------------------------------------
    # State 3 — STEP_3_EXECUTION_READY (study started, status != draft)
    # ------------------------------------------------------------------
    def test_state3_passing_precheck_in_draft_stays_in_state2(self):
        # Per the corrected Prompt 6 mapping: a brief that PASSES precheck
        # while still in draft must stay in State 2 (Brief Consistency
        # Helper visible) right up until the user starts the study. State 3
        # is a non-draft study; the configure page redirects away from
        # non-draft studies, so the helper-level mapping is verified in
        # test_ui_phase_derivation_branches and the integration check here
        # focuses on the precheck-passed-in-draft case that previously
        # hid the tile incorrectly.
        sid = self._make_study(
            study_type="synthetic_idi", qa_status="precheck_passed",
        )
        body = self._get_configure(sid)
        self.assertIn('data-ui-phase="STEP_2_ANCHORS"', body)
        self.assertIn("Brief Consistency Helper", body)
        self.assertIn('data-intent="align_problem"', body)

    # ------------------------------------------------------------------
    # Alignment-check route — bounded reply, no telemetry
    # ------------------------------------------------------------------
    def test_alignment_check_route_returns_single_sentence_no_telemetry(self):
        # Under the corrected Prompt 6 mapping, any draft study with a
        # study type set is in STEP_2_ANCHORS regardless of precheck —
        # the previous ``ta=""`` workaround is no longer needed.
        sid = self._make_study(study_type="synthetic_idi")

        # Snapshot telemetry row count BEFORE.
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            before = conn.execute(
                "SELECT COUNT(*) FROM step1_telemetry_events "
                "WHERE study_id = ?", (sid,),
            ).fetchone()[0]
        finally:
            conn.close()

        for intent in ("align_problem", "align_decision", "remind_question"):
            r = self.session.post(
                f"{self.base}/mark-alignment-check/{sid}",
                data={"token": self.token, "intent": intent},
                headers={"X-Requested-With": "XMLHttpRequest"},
                timeout=60,
            )
            # Allow 502 (LLM not connected in this env) but require correct
            # contract shape and no telemetry side-effect either way.
            self.assertIn(r.status_code, (200, 502),
                          f"{intent}: {r.status_code} {r.text[:200]}")
            data = r.json()
            if r.status_code == 200:
                self.assertTrue(data.get("ok"), data)
                self.assertEqual(data.get("intent"), intent)
                self.assertIn("reply", data)
                reply = data["reply"]
                self.assertIsInstance(reply, str)
                # Single-sentence guard: no embedded newline runs
                self.assertNotIn("\n\n", reply)
                self.assertLessEqual(len(reply), 400)
            else:
                self.assertFalse(data.get("ok"))

        # Invalid intent → 400, still no telemetry written.
        r_bad = self.session.post(
            f"{self.base}/mark-alignment-check/{sid}",
            data={"token": self.token, "intent": "rewrite_everything"},
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=20,
        )
        self.assertEqual(r_bad.status_code, 400)

        # Snapshot telemetry row count AFTER. Must equal before.
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            after = conn.execute(
                "SELECT COUNT(*) FROM step1_telemetry_events "
                "WHERE study_id = ?", (sid,),
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(
            before, after,
            "alignment-check route MUST NOT emit Step 1 telemetry",
        )

    # ------------------------------------------------------------------
    # Phase derivation unit test (DB-only, no HTTP) — guards every
    # branch of the ui_phase derivation block in app.py.
    # ------------------------------------------------------------------
    def test_ui_phase_derivation_branches(self):
        """Exercises the shared `derive_ui_phase` helper (app.py)."""
        sys.path.insert(0, HERE)
        from app import derive_ui_phase as derive

        cases = [
            ({"study_type": "", "qa_status": "", "status": "draft"},
             "STEP_1_FRAMING"),
            ({"study_type": None, "qa_status": "precheck_passed",
              "status": "draft"}, "STEP_1_FRAMING"),
            ({"study_type": "synthetic_idi", "qa_status": "",
              "status": "draft"}, "STEP_2_ANCHORS"),
            ({"study_type": "synthetic_idi", "qa_status": "precheck_failed",
              "status": "draft"}, "STEP_2_ANCHORS"),
            # Passing precheck while still in draft now stays in State 2.
            ({"study_type": "synthetic_idi", "qa_status": "precheck_passed",
              "status": "draft"}, "STEP_2_ANCHORS"),
            ({"study_type": "synthetic_idi", "qa_status": "",
              "status": "running"}, "STEP_3_EXECUTION_READY"),
            ({"study_type": "synthetic_idi", "qa_status": "",
              "status": "completed"}, "STEP_3_EXECUTION_READY"),
            (None, None),
        ]
        for study, expected in cases:
            self.assertEqual(derive(study), expected, f"case={study}")

        # Source-of-truth check: the helper exists and is wired into
        # the configure render path.
        with open(os.path.join(HERE, "app.py")) as f:
            src = f.read()
        self.assertIn("def derive_ui_phase(", src)
        self.assertIn("ui_phase = derive_ui_phase(", src)
        self.assertIn('"STEP_1_FRAMING"', src)
        self.assertIn('"STEP_2_ANCHORS"', src)
        self.assertIn('"STEP_3_EXECUTION_READY"', src)

    # ------------------------------------------------------------------
    # Telemetry event-type drift guard (same regex as Task #42)
    # ------------------------------------------------------------------
    def test_no_telemetry_event_type_drift(self):
        with open(os.path.join(HERE, "app.py")) as f:
            src = f.read()
        canonical = {
            "pattern_triggered", "template_applied",
            "quick_action_used", "rewrite_count_at_save",
        }
        types = set(re.findall(
            r"record_step1_event\(\s*['\"]([a-z_]+)['\"]", src
        ))
        drift = types - canonical
        self.assertFalse(drift,
                         f"telemetry event-type drift detected: {drift}")


if __name__ == "__main__":
    unittest.main()
