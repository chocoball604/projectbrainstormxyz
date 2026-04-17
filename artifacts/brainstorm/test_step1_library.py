"""Smoke tests for step1_pattern_library + step1_telemetry (Prompt 3)."""

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
        bad["business_problem"]["templates"][1]["id"] = "BP-1"
        ok, err = lib.validate_library(bad)
        self.assertFalse(ok)
        self.assertIn("duplicate", err)

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


class HelperTests(unittest.TestCase):
    def test_pattern_id_for_reason(self):
        self.assertEqual(lib.pattern_id_for_reason("business_problem", "empty"), "bp_empty")
        self.assertEqual(lib.pattern_id_for_reason("business_problem", "too_short"), "bp_too_short")
        self.assertEqual(lib.pattern_id_for_reason("business_problem", "solution_bias"), "bp_solution_bias")
        self.assertEqual(lib.pattern_id_for_reason("business_problem", "missing_uncertainty"), "bp_missing_uncertainty")
        self.assertEqual(lib.pattern_id_for_reason("decision_to_support", "empty"), "ds_empty")
        self.assertIsNone(lib.pattern_id_for_reason("business_problem", "ok"))
        self.assertIsNone(lib.pattern_id_for_reason("nonsense", "empty"))

    def test_match_template_id_bp(self):
        bp1 = "We don't yet understand how fast trial drop-off is changing or what is driving that change."
        self.assertEqual(lib.match_template_id(bp1, "business_problem"), "BP-1")
        bp3 = "It's unclear whether trial drop-off reflects checkout friction or product-fit issues."
        self.assertEqual(lib.match_template_id(bp3, "business_problem"), "BP-3")
        ds1 = "Whether to invest deeper or pause until assumptions are clearer."
        self.assertEqual(lib.match_template_id(ds1, "decision_to_support"), "DS-1")
        self.assertIsNone(lib.match_template_id("totally unrelated text", "business_problem"))

    def test_length_bucket(self):
        self.assertEqual(tel.length_bucket(""), "s")
        self.assertEqual(tel.length_bucket("x" * 30), "s")
        self.assertEqual(tel.length_bucket("x" * 100), "m")
        self.assertEqual(tel.length_bucket("x" * 400), "l")


class TelemetryWriterTests(unittest.TestCase):
    def test_init_table_and_insert(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        try:
            with mock.patch.object(tel, "DB_PATH", tmp.name):
                conn = sqlite3.connect(tmp.name)
                tel.init_step1_telemetry(conn)
                conn.commit()
                conn.close()
                tel.record_step1_event(
                    "pattern_triggered", study_id=1, user_id=2,
                    session_id="s1", field="business_problem",
                    pattern_id="bp_empty", length_bucket_value="s",
                )
                tel.record_step1_event(
                    "rewrite_count_at_save", study_id=1, user_id=2,
                    session_id="s1", field="business_problem",
                    length_bucket_value="m", count_value=3,
                )
                conn = sqlite3.connect(tmp.name)
                rows = conn.execute(
                    "SELECT event_type, pattern_id, count_value FROM step1_telemetry_events ORDER BY id"
                ).fetchall()
                conn.close()
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0][0], "pattern_triggered")
                self.assertEqual(rows[0][1], "bp_empty")
                self.assertEqual(rows[1][2], 3)
        finally:
            os.unlink(tmp.name)


class SaveLibraryTests(unittest.TestCase):
    def test_save_creates_backup_and_updates_metadata(self):
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
                modified["business_problem"]["good_examples"].append("New example")
                ok, err = lib.save_library(modified, updated_by="test")
                self.assertTrue(ok, err)
                self.assertTrue(os.path.isdir(tmp_backups))
                self.assertGreaterEqual(len(os.listdir(tmp_backups)), 1)
                with open(tmp_lib_path, "r") as f:
                    saved = json.load(f)
                self.assertEqual(saved["updated_by"], "test")
                self.assertGreater(saved["version"], seed["version"])
                self.assertIn("New example", saved["business_problem"]["good_examples"])
        finally:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
            lib.bump_cache()


if __name__ == "__main__":
    unittest.main()
