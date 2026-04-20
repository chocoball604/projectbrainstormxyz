"""Prompt 8 — Survey wizard + 500 respondent cap regression tests.

Locks in the small but easy-to-regress backend contracts that Prompt 8
shipped:

  * STUDY_TYPE_LIMITS["synthetic_survey"]["max_respondents"] == 500
  * /save-survey-config clamps 600 -> 500, accepts 500 as-is, and
    /run-study rejects 24 with the new "between 25 and 500" message.
  * ben_precheck() does NOT fail a survey at respondent_count = 500.
  * Save payload contracts for /save-survey-question/<id>/<idx> (per
    q-type) and /save-survey-questions are unchanged.

How to run
----------
  python artifacts/brainstorm/test_prompt8_survey_setup.py

Requires the brainstorm web workflow to be running.
"""

from __future__ import annotations

import json
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


class Prompt8SurveySetupTests(unittest.TestCase):
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

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _make_survey_study(self, *, respondent_count=100, question_count=2,
                           survey_questions=None, qa_status=None):
        title = f"P8 {int(time.time() * 1000)}"
        r = self.session.post(
            f"{self.base}/create-study-tbd",
            data={"title": title, "token": self.token},
            allow_redirects=False, timeout=20,
        )
        sid = int(parse_qs(urlparse(r.headers["Location"]).query
                           ).get("configure", [""])[0])
        if survey_questions is None:
            survey_questions = [
                {"type": "open", "prompt": f"Q{i}", "max_words": 50}
                for i in range(1, question_count + 1)
            ]
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            conn.execute(
                "UPDATE studies SET study_type = 'synthetic_survey', "
                "business_problem = ?, decision_to_support = ?, "
                "study_fit = ?, known_vs_unknown = ?, target_audience = ?, "
                "respondent_count = ?, question_count = ?, "
                "survey_questions = ? WHERE id = ?",
                (
                    "BP seed", "DS seed", "MG seed", "PC seed", "TA seed",
                    respondent_count, question_count,
                    json.dumps(survey_questions), sid,
                ),
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

    def _read_study(self, sid):
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            return dict(conn.execute(
                "SELECT * FROM studies WHERE id = ?", (sid,)
            ).fetchone())
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 1) STUDY_TYPE_LIMITS source-of-truth pin
    # ------------------------------------------------------------------
    def test_study_type_limits_caps_synthetic_survey_at_500(self):
        sys.path.insert(0, HERE)
        from app import STUDY_TYPE_LIMITS
        self.assertIn("synthetic_survey", STUDY_TYPE_LIMITS)
        self.assertEqual(
            STUDY_TYPE_LIMITS["synthetic_survey"]["max_respondents"], 500,
            "Prompt 8 raised the synthetic_survey respondent cap to 500 — "
            "do not silently lower it.",
        )
        self.assertEqual(
            STUDY_TYPE_LIMITS["synthetic_survey"]["max_questions"], 12,
        )

    # ------------------------------------------------------------------
    # 2) /save-survey-config 500 accepted, 600 clamps to 500
    # ------------------------------------------------------------------
    def test_save_survey_config_accepts_500_and_clamps_600_to_500(self):
        # 500 stored as-is
        sid500 = self._make_survey_study()
        r = self.session.post(
            f"{self.base}/save-survey-config/{sid500}",
            data={"token": self.token,
                  "respondent_count": "500", "question_count": "8"},
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=20,
        )
        self.assertEqual(r.status_code, 200, r.text[:200])
        self.assertEqual(self._read_study(sid500)["respondent_count"], 500)

        # 600 clamps down to 500
        sid600 = self._make_survey_study()
        r = self.session.post(
            f"{self.base}/save-survey-config/{sid600}",
            data={"token": self.token,
                  "respondent_count": "600", "question_count": "8"},
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=20,
        )
        self.assertEqual(r.status_code, 200, r.text[:200])
        self.assertEqual(self._read_study(sid600)["respondent_count"], 500)

        # Below floor clamps up to 25 (clamping symmetry)
        sid_low = self._make_survey_study()
        r = self.session.post(
            f"{self.base}/save-survey-config/{sid_low}",
            data={"token": self.token,
                  "respondent_count": "10", "question_count": "8"},
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=20,
        )
        self.assertEqual(r.status_code, 200, r.text[:200])
        self.assertEqual(self._read_study(sid_low)["respondent_count"], 25)

    # ------------------------------------------------------------------
    # 3) /run-study rejects 24 with the new "between 25 and 500" message
    # ------------------------------------------------------------------
    def test_run_study_rejects_24_with_between_25_and_500_message(self):
        # Bypass save-survey-config clamping by writing 24 directly to DB.
        sid = self._make_survey_study(respondent_count=24, question_count=2,
                                      qa_status="precheck_passed")
        r = self.session.post(
            f"{self.base}/run-study/{sid}",
            data={"token": self.token},
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=30,
        )
        self.assertEqual(r.status_code, 400, r.text[:300])
        msg = r.json().get("error", "")
        self.assertIn("between 25 and 500", msg,
                      f"expected new copy, got: {msg!r}")

    # ------------------------------------------------------------------
    # 4) ben_precheck does NOT fail a fully-anchored survey at 500
    # ------------------------------------------------------------------
    def test_ben_precheck_passes_survey_at_respondent_count_500(self):
        sys.path.insert(0, HERE)
        from app import ben_precheck

        sid = self._make_survey_study(
            respondent_count=500, question_count=2,
        )
        study = self._read_study(sid)
        failures = ben_precheck(study, persona_count=0)
        cap_failures = [
            f for f in failures if "between 25 and 500" in f
        ]
        self.assertEqual(
            cap_failures, [],
            f"ben_precheck must accept respondent_count=500; got {failures}",
        )

    # ------------------------------------------------------------------
    # 5) Save payload contracts for each survey q-type
    # ------------------------------------------------------------------
    def test_save_survey_question_contract_per_qtype(self):
        # Wide enough to host one of each shape.
        qtypes = [
            ("open",   {"type": "open",   "prompt": "Open Q",  "max_words": 50}),
            ("likert", {"type": "likert", "prompt": "Likert Q",
                        "options": ["1", "2", "3", "4", "5"]}),
            ("mc",     {"type": "mc",     "prompt": "MC Q",
                        "options": ["A", "B", "C"]}),
            ("ab",     {"type": "ab",     "prompt": "AB Q",
                        "options": ["Left", "Right"]}),
            ("range",  {"type": "range",  "prompt": "Range Q",
                        "min": 0, "max": 10}),
        ]
        sid = self._make_survey_study(question_count=len(qtypes))
        for idx, (label, payload) in enumerate(qtypes, 1):
            r = self.session.post(
                f"{self.base}/save-survey-question/{sid}/{idx}",
                data={"token": self.token,
                      "question_json": json.dumps(payload)},
                headers={"X-Requested-With": "XMLHttpRequest"},
                timeout=20,
            )
            self.assertEqual(r.status_code, 200,
                             f"{label}: {r.status_code} {r.text[:200]}")
            self.assertTrue(r.json().get("ok"),
                            f"{label}: {r.text[:200]}")

        stored = json.loads(self._read_study(sid)["survey_questions"])
        self.assertEqual(len(stored), len(qtypes))
        self.assertEqual(stored[0]["type"], "open")
        self.assertEqual(stored[1]["type"], "likert")
        self.assertEqual(len(stored[1]["options"]), 5)
        self.assertEqual(stored[2]["type"], "mc")
        self.assertGreaterEqual(len(stored[2]["options"]), 3)
        self.assertEqual(stored[3]["type"], "ab")
        self.assertEqual(len(stored[3]["options"]), 2)
        self.assertEqual(stored[4]["type"], "range")

        # ab_image: positive shape sample (both refs non-empty) is accepted,
        # and the empty-refs negative case is still rejected.
        sid_img = self._make_survey_study(question_count=1)
        r_ok = self.session.post(
            f"{self.base}/save-survey-question/{sid_img}/1",
            data={"token": self.token, "question_json": json.dumps({
                "type": "ab_image", "prompt": "AB image",
                "images": {"A": "concept_a.png", "B": "concept_b.png"},
            })},
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=20,
        )
        self.assertEqual(r_ok.status_code, 200, r_ok.text[:200])
        self.assertTrue(r_ok.json().get("ok"), r_ok.text[:200])
        stored_img = json.loads(self._read_study(sid_img)["survey_questions"])
        self.assertEqual(stored_img[0]["type"], "ab_image")
        self.assertEqual(set(stored_img[0]["images"].keys()), {"A", "B"})
        self.assertEqual(stored_img[0]["images"]["A"], "concept_a.png")
        self.assertEqual(stored_img[0]["images"]["B"], "concept_b.png")

        sid_img_bad = self._make_survey_study(question_count=1)
        r_bad = self.session.post(
            f"{self.base}/save-survey-question/{sid_img_bad}/1",
            data={"token": self.token, "question_json": json.dumps({
                "type": "ab_image", "prompt": "AB image",
                "images": {"A": "", "B": ""},
            })},
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=20,
        )
        self.assertEqual(r_bad.status_code, 400, r_bad.text[:200])

    # ------------------------------------------------------------------
    # 6) /save-survey-questions bulk save still accepts a list payload
    # ------------------------------------------------------------------
    def test_save_survey_questions_bulk_accepts_question_list(self):
        sid = self._make_survey_study(question_count=2)
        payload = [
            {"type": "open", "prompt": "Bulk Q1", "max_words": 50},
            {"type": "open", "prompt": "Bulk Q2", "max_words": 50},
        ]
        r = self.session.post(
            f"{self.base}/save-survey-questions/{sid}",
            data={"token": self.token,
                  "survey_questions_json": json.dumps(payload)},
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=20,
        )
        self.assertEqual(r.status_code, 200,
                         f"{r.status_code}: {r.text[:200]}")
        body = r.json()
        self.assertTrue(body.get("ok"), body)
        self.assertEqual(len(body.get("questions", [])), 2)
        stored = json.loads(self._read_study(sid)["survey_questions"])
        self.assertEqual(len(stored), 2)
        self.assertEqual(stored[0]["prompt"], "Bulk Q1")
        self.assertEqual(stored[1]["prompt"], "Bulk Q2")
        self.assertEqual(stored[0]["type"], "open")
        self.assertEqual(stored[1]["type"], "open")


if __name__ == "__main__":
    unittest.main()
