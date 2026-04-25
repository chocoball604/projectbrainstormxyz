"""Smoke tests for the admin AI-failure DM hook (Task #72).

Run from artifacts/brainstorm/ in development mode (the app's startup
hardening rejects a weak ADMIN_PASSWORD when FLASK_ENV is not 'development'):

    FLASK_ENV=development python test_admin_ai_failure_dm.py

Or, if ADMIN_PASSWORD is set to a strong value in your shell:

    ADMIN_PASSWORD='<strong>' python test_admin_ai_failure_dm.py

The tests:
  1. _is_ai_failure_exception correctly classifies AI vs non-AI exceptions.
  2. _alert_admin_ai_study_failure writes one well-formed DM for an AI failure.
  3. A second alert for the same model id within the throttle window is suppressed.
  4. A different model id bypasses the throttle.
  5. A non-AI exception (ValueError) does NOT trigger a DM.
  6. The throttle timestamp is committed only after a successful write
     (a write failure does not silently suppress the next alert).
  7. The DM file is restored to its pre-test state on exit.
"""

from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import app  # noqa: E402


class IsAiFailureClassifierTests(unittest.TestCase):
    def test_runtime_llm_timeout_is_ai_failure(self):
        self.assertTrue(app._is_ai_failure_exception(
            RuntimeError("LLM timeout for openai/foo: wall-clock exceeded")))

    def test_runtime_llm_error_is_ai_failure(self):
        self.assertTrue(app._is_ai_failure_exception(
            RuntimeError("LLM error for openai/foo: 503")))

    def test_not_implemented_is_ai_failure(self):
        self.assertTrue(app._is_ai_failure_exception(
            NotImplementedError("LLM integration not connected yet")))

    def test_openai_apierror_is_ai_failure(self):
        import openai as _o
        try:
            raise _o.APIError("boom", None, body=None)
        except _o.APIError as e:
            self.assertTrue(app._is_ai_failure_exception(e))

    def test_value_error_is_not_ai_failure(self):
        self.assertFalse(app._is_ai_failure_exception(ValueError("bad json")))

    def test_generic_runtime_error_is_not_ai_failure(self):
        self.assertFalse(app._is_ai_failure_exception(
            RuntimeError("something else entirely")))

    def test_none_is_not_ai_failure(self):
        self.assertFalse(app._is_ai_failure_exception(None))


class AlertAdminHookTests(unittest.TestCase):
    def setUp(self):
        self._original_msgs = app._load_dm_messages()
        app._ADMIN_DM_LAST_SENT.clear()

    def tearDown(self):
        # Always restore the message file even if assertions fail mid-test.
        app._save_dm_messages(self._original_msgs)
        app._ADMIN_DM_LAST_SENT.clear()

    def _new_msgs(self):
        return app._load_dm_messages()[len(self._original_msgs):]

    def test_ai_failure_writes_one_well_formed_dm(self):
        ok = app._alert_admin_ai_study_failure(
            12345, "Test Study", "synthetic_survey", "openai/foo",
            RuntimeError("LLM timeout for openai/foo: probe"))
        self.assertTrue(ok)
        new = self._new_msgs()
        self.assertEqual(len(new), 1)
        m = new[0]
        self.assertEqual(m["recipient_type"], "admin")
        self.assertIsNone(m["recipient_user_id"])
        self.assertEqual(m["sender_type"], "admin")
        self.assertFalse(m["read"])
        self.assertEqual(m["category"], "System Alert")
        self.assertLessEqual(len(m["subject"]), 30)
        self.assertLessEqual(len(m["body"]), 300)
        self.assertIn("AI failure", m["subject"])
        self.assertIn("12345", m["subject"])
        self.assertIn("openai/foo", m["body"])
        self.assertIn("Test Study", m["body"])

    def test_same_model_within_throttle_is_suppressed(self):
        ok1 = app._alert_admin_ai_study_failure(
            1, "S1", "synthetic_survey", "openai/foo",
            RuntimeError("LLM timeout for openai/foo: x"))
        ok2 = app._alert_admin_ai_study_failure(
            2, "S2", "synthetic_survey", "openai/foo",
            NotImplementedError("LLM integration not connected yet"))
        self.assertTrue(ok1)
        self.assertFalse(ok2)
        self.assertEqual(len(self._new_msgs()), 1)

    def test_different_model_bypasses_throttle(self):
        ok1 = app._alert_admin_ai_study_failure(
            1, "S1", "synthetic_survey", "openai/foo",
            RuntimeError("LLM timeout for openai/foo: x"))
        ok2 = app._alert_admin_ai_study_failure(
            2, "S2", "synthetic_idi", "anthropic/bar",
            RuntimeError("LLM error for anthropic/bar: y"))
        self.assertTrue(ok1)
        self.assertTrue(ok2)
        self.assertEqual(len(self._new_msgs()), 2)

    def test_non_ai_exception_does_not_dm(self):
        ok = app._alert_admin_ai_study_failure(
            7, "S7", "synthetic_survey", "openai/foo",
            ValueError("config bug"))
        self.assertFalse(ok)
        self.assertEqual(len(self._new_msgs()), 0)

    def test_per_run_dedupe_contract_at_both_catch_sites(self):
        """Lock in the one-DM-per-study-run contract structurally.

        _run_study_core has two LLM-failure catch blocks (synthetic_survey
        and qualitative paths). Each must:
          1. Initialize a per-run flag `ai_failure_dm_sent = False` once.
          2. Guard each call to `_alert_admin_ai_study_failure(...)` with
             `if not ai_failure_dm_sent and _is_ai_failure_exception(e):`.
          3. Set `ai_failure_dm_sent = True` BEFORE invoking the alert
             helper, so that even a throttled or write-failed first
             attempt prevents a second DM from a future LLM call added
             to the same run.
        A regression that drops the guard or flips the flag after the
        helper would let one run emit multiple DMs.
        """
        src_path = os.path.join(HERE, "app.py")
        with open(src_path, "r", encoding="utf-8") as f:
            src = f.read()

        self.assertEqual(
            src.count("ai_failure_dm_sent = False"), 1,
            "expected exactly one initialization of ai_failure_dm_sent in app.py")

        guard_count = src.count(
            "if not ai_failure_dm_sent and _is_ai_failure_exception(e):")
        self.assertEqual(
            guard_count, 2,
            f"expected 2 per-run guarded catch sites, found {guard_count}")

        alert_call_count = src.count("_alert_admin_ai_study_failure(")
        self.assertGreaterEqual(
            alert_call_count, 3,  # 1 def + 2 call sites
            f"expected at least 3 references to _alert_admin_ai_study_failure, "
            f"found {alert_call_count}")

        guard_idx = 0
        sites_checked = 0
        while True:
            guard_idx = src.find(
                "if not ai_failure_dm_sent and _is_ai_failure_exception(e):",
                guard_idx)
            if guard_idx == -1:
                break
            window = src[guard_idx:guard_idx + 600]
            flag_pos = window.find("ai_failure_dm_sent = True")
            call_pos = window.find("_alert_admin_ai_study_failure(")
            self.assertNotEqual(
                flag_pos, -1,
                "missing 'ai_failure_dm_sent = True' inside guarded block")
            self.assertNotEqual(
                call_pos, -1,
                "missing '_alert_admin_ai_study_failure(' inside guarded block")
            self.assertLess(
                flag_pos, call_pos,
                "ai_failure_dm_sent must be set to True BEFORE calling "
                "_alert_admin_ai_study_failure so a throttled / write-failed "
                "first attempt still prevents a second DM in the same run")
            sites_checked += 1
            guard_idx += 1
        self.assertEqual(sites_checked, 2,
            f"expected to verify 2 catch sites, verified {sites_checked}")

    def test_concurrent_alerts_only_one_succeeds(self):
        """Under multi-threaded contention, only one of N concurrent alerts
        for the same model id writes a DM (atomic check-and-reserve)."""
        import threading
        results = []
        results_lock = threading.Lock()

        def _go():
            r = app._alert_admin_ai_study_failure(
                1, "S", "synthetic_survey", "openai/foo",
                RuntimeError("LLM timeout for openai/foo: x"))
            with results_lock:
                results.append(r)

        threads = [threading.Thread(target=_go) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        successes = sum(1 for r in results if r)
        self.assertEqual(
            successes, 1,
            f"expected exactly one success out of 10 concurrent calls, got {successes}")
        self.assertEqual(
            len(self._new_msgs()), 1,
            "exactly one DM should be in the file")

    def test_throttle_only_committed_after_successful_write(self):
        """If the file write fails, the next call must NOT be suppressed."""
        original_save = app._save_dm_messages
        try:
            def _broken_save(_msgs):
                raise IOError("disk full simulation")
            app._save_dm_messages = _broken_save
            ok_fail = app._alert_admin_ai_study_failure(
                1, "S1", "synthetic_survey", "openai/foo",
                RuntimeError("LLM timeout for openai/foo: x"))
            self.assertFalse(ok_fail, "alert should report failure when write throws")
            # Throttle must NOT have been committed for this key.
            self.assertNotIn(
                "ai_study_failure:openai/foo", app._ADMIN_DM_LAST_SENT,
                "throttle key must not be set after a write failure")
        finally:
            app._save_dm_messages = original_save
        # Now the next call (write works) should succeed.
        ok = app._alert_admin_ai_study_failure(
            2, "S2", "synthetic_survey", "openai/foo",
            RuntimeError("LLM timeout for openai/foo: y"))
        self.assertTrue(ok, "next alert must not be suppressed by a prior write failure")
        self.assertEqual(len(self._new_msgs()), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
