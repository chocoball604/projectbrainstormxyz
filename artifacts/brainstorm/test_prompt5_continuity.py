"""Prompt 5 — Framing continuity & editable anchors regression tests.

Drives the running brainstorm Flask server (same pattern as
test_step1_e2e.py) to verify the Configure Study screen contract:

1. After a study type is set, BP and DS are STILL visible inside a
   Core Framing card (not stripped from the UI).
2. The Core Framing card includes an "Edit framing" affordance and
   pre-fills its textareas with the current BP/DS values.
3. MG / PC / TA always render as <textarea> forms (never read-only
   green summary), so they remain editable after first save.
4. Optional Context fields always render as <textarea> forms.
5. autoRunStudy() — the Start Study handler — contains a window.confirm
   guard warning that anchors will be locked.
6. No telemetry event-type drift: the four canonical Step 1 event
   types are still the only ones referenced by app.py.

How to run
----------
  python artifacts/brainstorm/test_prompt5_continuity.py

Requires the brainstorm web workflow to be running. Auto-detects the
port the same way test_step1_e2e.py does.
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


class FramingContinuityTests(unittest.TestCase):
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
                f"login failed ({resp.status_code}); skipping (server may "
                "not be running or seed user missing)"
            )
        cls.token = parse_qs(urlparse(resp.headers["Location"]).query
                             ).get("token", [""])[0]
        if not cls.token:
            raise unittest.SkipTest("login did not return token")

    def _make_study_with_anchors(self, *, study_type, with_oc=False):
        title = f"P5 {study_type} {int(time.time() * 1000)}"
        r = self.session.post(
            f"{self.base}/create-study-tbd",
            data={"title": title, "token": self.token},
            allow_redirects=False, timeout=20,
        )
        sid = int(parse_qs(urlparse(r.headers["Location"]).query
                           ).get("configure", [""])[0])
        # Seed BP, DS, study_type, MG, PC, TA, optional OC directly via DB.
        # We never commit brainstorm.db, so this is local-only mutation.
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            conn.execute(
                "UPDATE studies SET study_type = ?, business_problem = ?, "
                "decision_to_support = ?, study_fit = ?, "
                "known_vs_unknown = ?, target_audience = ? WHERE id = ?",
                (
                    study_type, "BP TEXT seed", "DS TEXT seed",
                    "MG TEXT seed", "PC TEXT seed", "TA TEXT seed", sid,
                ),
            )
            if with_oc:
                conn.execute(
                    "UPDATE studies SET definition_useful_insight = ?, "
                    "survey_brief = ? WHERE id = ?",
                    (
                        "DUI TEXT seed",
                        '{"optional_context": '
                        '{"competitive_context": "CC TEXT"}}',
                        sid,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
        return sid

    def _get_configure(self, study_id):
        r = self.session.get(
            f"{self.base}/?token={self.token}&configure={study_id}",
            timeout=20,
        )
        self.assertEqual(r.status_code, 200, r.text[:300])
        return r.text

    def test_core_framing_card_renders_after_study_type_set(self):
        sid = self._make_study_with_anchors(study_type="synthetic_idi")
        body = self._get_configure(sid)
        self.assertIn('id="core-framing-card"', body)
        self.assertIn("Core Framing", body)
        self.assertIn("BP TEXT seed", body)
        self.assertIn("DS TEXT seed", body)
        self.assertIn("Edit framing", body)
        self.assertIn('id="core-framing-input-business_problem"', body)
        self.assertIn('id="core-framing-input-decision_to_support"', body)

    def test_mg_pc_ta_remain_textarea_after_save(self):
        sid = self._make_study_with_anchors(study_type="synthetic_idi")
        body = self._get_configure(sid)
        for key, val in [
            ("market_geography", "MG TEXT seed"),
            ("product_concept", "PC TEXT seed"),
            ("target_audience", "TA TEXT seed"),
        ]:
            self.assertIn(f'data-anchor-key="{key}"', body,
                          f"anchor form for {key} missing")
            self.assertIn(val, body)
        # Old read-only "✓ N. Market / Geography:" pattern must be gone.
        self.assertNotIn("&#10003; 3. Market / Geography:", body)
        self.assertNotIn("&#10003; 4. Product / Concept:", body)
        self.assertNotIn("&#10003; 5. Target Audience", body)
        self.assertIn("anchor-saved-badge", body)

    def test_oc_fields_remain_editable_after_save(self):
        sid = self._make_study_with_anchors(
            study_type="synthetic_idi", with_oc=True,
        )
        body = self._get_configure(sid)
        self.assertIn('class="oc-field-form"', body)
        self.assertIn("DUI TEXT seed", body)
        self.assertIn("CC TEXT", body)
        self.assertIn("oc-saved-badge", body)
        self.assertNotIn('class="oc-edit-link"', body)

    def test_start_study_confirm_dialog_is_present(self):
        sid = self._make_study_with_anchors(study_type="synthetic_idi")
        body = self._get_configure(sid)
        self.assertIn("function autoRunStudy()", body)
        self.assertIn("window.confirm(", body)
        self.assertIn("Anchors will be locked", body)

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
