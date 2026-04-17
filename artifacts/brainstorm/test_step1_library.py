"""Tests for step1_pattern_library + step1_telemetry (Prompt 3)."""

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import step1_pattern_library as lib
import step1_telemetry as tel


class ValidateLibraryTests(unittest.TestCase):
    def setUp(self):
        with open(lib.LIBRARY_PATH, "r") as f:
            self.seed = json.load(f)

    def test_seed_library_is_valid(self):
        ok, err = lib.validate_library(self.seed)
        self.assertTrue(ok, f"seed invalid: {err}")

    def test_seed_uses_canonical_template_ids(self):
        bp_ids = [t["id"] for t in self.seed["business_problem"]["templates"]]
        ds_ids = [t["id"] for t in self.seed["decision_to_support"]["templates"]]
        for required in (
            "BP_1_RATE_AND_DRIVER",
            "BP_2_TRIGGER_AND_CONDITIONS",
            "BP_3_INTERPRETATION_A_VS_B",
        ):
            self.assertIn(required, bp_ids)
        for required in (
            "DS_1_INVEST_OR_PAUSE",
            "DS_2_CONTINUE_OR_REDIRECT",
            "DS_3_NARROW_BEFORE_COMMIT",
        ):
            self.assertIn(required, ds_ids)

    def test_missing_field_block(self):
        bad = json.loads(json.dumps(self.seed))
        del bad["business_problem"]
        ok, err = lib.validate_library(bad)
        self.assertFalse(ok)
        self.assertIn("business_problem", err)

    def test_pattern_must_cover_all_reasons(self):
        bad = json.loads(json.dumps(self.seed))
        bad["business_problem"]["patterns"] = [
            p for p in bad["business_problem"]["patterns"]
            if p["weakness_reason"] != "solution_bias"
        ]
        ok, err = lib.validate_library(bad)
        self.assertFalse(ok)
        self.assertIn("solution_bias", err)

    def test_template_id_uniqueness(self):
        bad = json.loads(json.dumps(self.seed))
        bad["business_problem"]["templates"][1]["id"] = \
            bad["business_problem"]["templates"][0]["id"]
        ok, err = lib.validate_library(bad)
        self.assertFalse(ok)
        self.assertIn("duplicate", err)

    def test_pattern_id_uniqueness(self):
        bad = json.loads(json.dumps(self.seed))
        bad["decision_to_support"]["patterns"][0]["pattern_id"] = "bp_empty"
        ok, err = lib.validate_library(bad)
        self.assertFalse(ok)
        self.assertIn("bp_empty", err)

    def test_pattern_must_have_examples(self):
        bad = json.loads(json.dumps(self.seed))
        bad["business_problem"]["patterns"][0]["good_examples"] = []
        ok, err = lib.validate_library(bad)
        self.assertFalse(ok)
        self.assertIn("good_examples", err)

    def test_pattern_references_unknown_template(self):
        bad = json.loads(json.dumps(self.seed))
        bad["business_problem"]["patterns"][0]["suggested_template_ids"] = ["BP-99"]
        ok, err = lib.validate_library(bad)
        self.assertFalse(ok)
        self.assertIn("BP-99", err)

    def test_invalid_reason_rejected(self):
        bad = json.loads(json.dumps(self.seed))
        bad["business_problem"]["patterns"][0]["weakness_reason"] = "vague"
        ok, err = lib.validate_library(bad)
        self.assertFalse(ok)

    def test_solution_bias_triggers_required(self):
        bad = json.loads(json.dumps(self.seed))
        bad["solution_bias_triggers"] = []
        ok, err = lib.validate_library(bad)
        self.assertFalse(ok)
        self.assertIn("solution_bias_triggers", err)


class HelperTests(unittest.TestCase):
    def test_pattern_id_for_reason(self):
        self.assertEqual(lib.pattern_id_for_reason("business_problem", "empty"), "bp_empty")
        self.assertEqual(lib.pattern_id_for_reason("business_problem", "too_short"), "bp_too_short")
        self.assertEqual(lib.pattern_id_for_reason("business_problem", "solution_bias"), "bp_solution_bias")
        self.assertEqual(lib.pattern_id_for_reason("business_problem", "missing_uncertainty"), "bp_missing_uncertainty")
        self.assertEqual(lib.pattern_id_for_reason("decision_to_support", "empty"), "ds_empty")
        self.assertIsNone(lib.pattern_id_for_reason("business_problem", "ok"))
        self.assertIsNone(lib.pattern_id_for_reason("nonsense", "empty"))

    def test_match_template_id_uses_canonical_ids(self):
        bp1 = "We don't yet understand how fast trial drop-off is changing or what is driving that change."
        self.assertEqual(lib.match_template_id(bp1, "business_problem"), "BP_1_RATE_AND_DRIVER")
        bp3 = "It's unclear whether trial drop-off reflects checkout friction or product-fit issues."
        self.assertEqual(lib.match_template_id(bp3, "business_problem"), "BP_3_INTERPRETATION_A_VS_B")
        ds1 = "Whether to invest deeper or pause until assumptions are clearer."
        self.assertEqual(lib.match_template_id(ds1, "decision_to_support"), "DS_1_INVEST_OR_PAUSE")
        self.assertIsNone(lib.match_template_id("totally unrelated text", "business_problem"))

    def test_length_bucket(self):
        self.assertEqual(tel.length_bucket(""), "s")
        self.assertEqual(tel.length_bucket("x" * 30), "s")
        self.assertEqual(tel.length_bucket("x" * 100), "m")
        self.assertEqual(tel.length_bucket("x" * 400), "l")

    def test_solution_bias_triggers_helper(self):
        triggers = lib.solution_bias_triggers()
        self.assertIsInstance(triggers, list)
        self.assertIn("feature", triggers)


class TelemetryWriterTests(unittest.TestCase):
    def test_init_table_uses_created_at_column(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        try:
            with mock.patch.object(tel, "DB_PATH", tmp.name):
                conn = sqlite3.connect(tmp.name)
                tel.init_step1_telemetry(conn)
                conn.commit()
                cols = {row[1] for row in conn.execute(
                    "PRAGMA table_info(step1_telemetry_events)").fetchall()}
                conn.close()
                self.assertIn("created_at", cols)
                self.assertNotIn("occurred_at", cols)
        finally:
            os.unlink(tmp.name)

    def test_legacy_occurred_at_is_migrated(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        try:
            conn = sqlite3.connect(tmp.name)
            conn.execute("""
                CREATE TABLE step1_telemetry_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at TEXT NOT NULL DEFAULT (datetime('now')),
                    study_id INTEGER, user_id INTEGER, session_id TEXT,
                    event_type TEXT NOT NULL, field TEXT, pattern_id TEXT,
                    template_id TEXT, quick_action TEXT, length_bucket TEXT,
                    count_value INTEGER, extra_json TEXT
                )
            """)
            conn.commit()
            with mock.patch.object(tel, "DB_PATH", tmp.name):
                tel.init_step1_telemetry(conn)
                conn.commit()
            cols = {row[1] for row in conn.execute(
                "PRAGMA table_info(step1_telemetry_events)").fetchall()}
            conn.close()
            self.assertIn("created_at", cols)
            self.assertNotIn("occurred_at", cols)
        finally:
            os.unlink(tmp.name)

    def test_insert_round_trip_and_session_count(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        try:
            with mock.patch.object(tel, "DB_PATH", tmp.name):
                conn = sqlite3.connect(tmp.name)
                tel.init_step1_telemetry(conn)
                conn.commit()
                conn.close()
                tel.record_step1_event(
                    "quick_action_used", study_id=1, user_id=2,
                    session_id="s1", field="business_problem",
                    quick_action="REWRITE_PROBLEM",
                )
                tel.record_step1_event(
                    "quick_action_used", study_id=1, user_id=2,
                    session_id="s1", field="business_problem",
                    quick_action="REWRITE_PROBLEM",
                )
                tel.record_step1_event(
                    "quick_action_used", study_id=1, user_id=2,
                    session_id="s2", field="business_problem",
                    quick_action="REWRITE_PROBLEM",
                )
                self.assertEqual(
                    tel.count_session_quick_actions("s1", "REWRITE_PROBLEM",
                                                   field="business_problem"),
                    2,
                )
                self.assertEqual(
                    tel.count_session_quick_actions("s2", "REWRITE_PROBLEM",
                                                   field="business_problem"),
                    1,
                )
                self.assertEqual(
                    tel.count_session_quick_actions("", "REWRITE_PROBLEM"), 0,
                )
        finally:
            os.unlink(tmp.name)


class SaveLibraryTests(unittest.TestCase):
    def test_save_creates_backup_and_bumps_version(self):
        tmp_dir = tempfile.mkdtemp()
        try:
            tmp_lib_path = os.path.join(tmp_dir, "lib.json")
            tmp_backups = os.path.join(tmp_dir, "backups")
            with open(lib.LIBRARY_PATH, "r") as f:
                seed = json.load(f)
            with open(tmp_lib_path, "w") as f:
                json.dump(seed, f)
            with mock.patch.object(lib, "LIBRARY_PATH", tmp_lib_path), \
                 mock.patch.object(lib, "BACKUPS_DIR", tmp_backups):
                lib.bump_cache()
                modified = json.loads(json.dumps(seed))
                modified["business_problem"]["patterns"][0]["good_examples"].append("New example")
                ok, err = lib.save_library(modified, updated_by="test")
                self.assertTrue(ok, err)
                self.assertTrue(os.path.isdir(tmp_backups))
                self.assertGreaterEqual(len(os.listdir(tmp_backups)), 1)
                with open(tmp_lib_path, "r") as f:
                    saved = json.load(f)
                self.assertEqual(saved["updated_by"], "test")
                self.assertGreater(saved["version"], seed["version"])
        finally:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
            lib.bump_cache()

    def test_save_rejects_oversize_payload(self):
        with open(lib.LIBRARY_PATH, "r") as f:
            seed = json.load(f)
        oversized = json.loads(json.dumps(seed))
        oversized["business_problem"]["patterns"][0]["good_examples"].append(
            "x" * (65 * 1024)
        )
        ok, err = lib.save_library(oversized, updated_by="test")
        self.assertFalse(ok)
        self.assertIn("too large", err)


if __name__ == "__main__":
    unittest.main()
