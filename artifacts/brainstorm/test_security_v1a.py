"""Security Hardening V1A regression tests (Tasks #55 + #56).

Locks in the security-critical behaviors shipped under the V1A
hardening batches so they cannot regress silently:

  * Bleach-based ``sanitize_html`` Jinja filter strips ``<script>``,
    ``javascript:`` URIs, and ``onerror=`` event-handler attributes
    while preserving safe markup. (Task #55 P0 step 8)
  * Production fail-fast: starting the app with ``FLASK_ENV=production``
    and an insecure ``ADMIN_PASSWORD`` (or no ``FLASK_SECRET``) refuses
    to boot with a clear ``FATAL`` message on stderr.
    (Task #55 P0 step 1)
  * Brute-force lockout: ``/login`` returns ``429`` + ``Retry-After``
    after 5 failed attempts for the same identity from the same IP;
    ``/admin-login`` returns ``302`` with a "Too many ..." error after
    5 failed attempts. (Task #55 P0 step 5)
  * CSRF protection: POSTs without a valid CSRF cookie + token are
    rejected with ``403``; POSTs that submit the matching token in the
    ``X-CSRF-Token`` header are accepted. (Task #56 P1)
  * Security response headers are set on every response:
    Content-Security-Policy, X-Frame-Options, X-Content-Type-Options,
    Referrer-Policy, Permissions-Policy. (Task #56 P1)

How to run
----------
    python artifacts/brainstorm/test_security_v1a.py

Requires the brainstorm web workflow to be running. The login and
admin-login lockout tests use a fresh random identity per run so they
don't accidentally lock out real users or interfere with re-runs.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
import unittest
import uuid

import requests


HERE = os.path.dirname(os.path.abspath(__file__))


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


def _bootstrap_csrf(session: requests.Session, base: str) -> str:
    """GET /landing to receive a fresh ``pb_csrf`` cookie. Returns the
    cookie value so callers can echo it back as ``X-CSRF-Token``."""
    r = session.get(f"{base}/landing", timeout=15)
    if r.status_code != 200:
        raise unittest.SkipTest(
            f"GET /landing returned {r.status_code}; cannot bootstrap CSRF"
        )
    tok = session.cookies.get("pb_csrf", "")
    if not tok:
        raise unittest.SkipTest("GET /landing did not set pb_csrf cookie")
    return tok


class SanitizeHtmlFilterTests(unittest.TestCase):
    """Direct unit test of the ``sanitize_html`` Jinja filter.

    Runs in a subprocess so importing ``app`` (with its Flask init,
    DB connection, etc.) does not pollute this test process.
    """

    def _run_filter(self, payload: str) -> str:
        script = (
            "import sys; sys.path.insert(0, %r);\n"
            "import app as a;\n"
            "import sys as s;\n"
            "s.stdout.write(a._sanitize_html(%r));\n"
        ) % (HERE, payload)
        env = dict(os.environ)
        # sanitize_html itself is pure; running with dev semantics is fine.
        env["FLASK_ENV"] = "development"
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True, text=True, timeout=30, env=env,
        )
        if result.returncode != 0:
            self.fail(
                f"sanitize_html subprocess failed (exit {result.returncode}):\n"
                f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}"
            )
        return result.stdout

    def test_strips_script_tag(self):
        # bleach with strip=True removes the <script> tag but keeps its
        # text content as a plain string — that's the documented & safe
        # behavior (text content can never execute as JS once the tag is
        # gone). What matters for XSS prevention is that NO script tag
        # survives in the rendered HTML.
        out = self._run_filter("Hello <script>alert(1)</script> world")
        self.assertNotIn("<script", out.lower())
        self.assertNotIn("</script", out.lower())
        self.assertIn("Hello", out)
        self.assertIn("world", out)

    def test_strips_javascript_uri(self):
        out = self._run_filter('<a href="javascript:alert(1)">click</a>')
        self.assertNotIn("javascript:", out.lower())
        # The text content survives even when href is stripped.
        self.assertIn("click", out)

    def test_strips_onerror_handler(self):
        out = self._run_filter('<img src="x" onerror="alert(1)">')
        self.assertNotIn("onerror", out.lower())
        self.assertNotIn("alert(1)", out)

    def test_preserves_safe_markup(self):
        out = self._run_filter(
            '<p>Hello <strong>world</strong> — see '
            '<a href="https://example.com">link</a>.</p>'
        )
        self.assertIn("<p>", out)
        self.assertIn("<strong>", out)
        self.assertIn('href="https://example.com"', out)


class ProductionFailFastTests(unittest.TestCase):
    """Importing ``app`` with FLASK_ENV != 'development' and an
    insecure / missing secret must terminate the process."""

    def _import_app(self, env_overrides):
        env = {
            k: v for k, v in os.environ.items()
            if k not in ("FLASK_ENV", "FLASK_SECRET", "ADMIN_PASSWORD")
        }
        env.update(env_overrides)
        return subprocess.run(
            [sys.executable, "-c",
             f"import sys; sys.path.insert(0, {HERE!r}); import app"],
            capture_output=True, text=True, timeout=30, env=env,
        )

    def test_production_with_insecure_admin_password_exits(self):
        result = self._import_app({
            "FLASK_ENV": "production",
            "FLASK_SECRET": "a-real-strong-secret-value-1234567890",
            "ADMIN_PASSWORD": "admin123",
        })
        # SystemExit(1) surfaces as exit code 1 from the subprocess.
        self.assertNotEqual(
            result.returncode, 0,
            f"App should refuse to start with admin123. "
            f"stdout={result.stdout!r} stderr={result.stderr!r}",
        )
        self.assertIn(
            "FATAL", (result.stderr or "") + (result.stdout or ""),
            "Expected FATAL message about insecure ADMIN_PASSWORD",
        )

    def test_production_with_missing_flask_secret_exits(self):
        result = self._import_app({
            "FLASK_ENV": "production",
            "ADMIN_PASSWORD": "a-real-strong-admin-password-9876543210",
            # FLASK_SECRET intentionally absent
        })
        self.assertNotEqual(
            result.returncode, 0,
            f"App should refuse to start without FLASK_SECRET. "
            f"stdout={result.stdout!r} stderr={result.stderr!r}",
        )
        self.assertIn(
            "FATAL", (result.stderr or "") + (result.stdout or ""),
            "Expected FATAL message about missing FLASK_SECRET",
        )

    def test_development_with_default_secrets_starts(self):
        # Dev mode should NOT fail-fast even with weak / default secrets.
        # We import in a subprocess and immediately exit to avoid actually
        # binding the port.
        env = {
            k: v for k, v in os.environ.items()
            if k not in ("FLASK_ENV", "FLASK_SECRET", "ADMIN_PASSWORD")
        }
        env["FLASK_ENV"] = "development"
        result = subprocess.run(
            [sys.executable, "-c",
             f"import sys; sys.path.insert(0, {HERE!r}); import app; "
             f"print('IMPORT_OK')"],
            capture_output=True, text=True, timeout=30, env=env,
        )
        self.assertEqual(
            result.returncode, 0,
            f"Dev import should succeed. "
            f"stdout={result.stdout!r} stderr={result.stderr!r}",
        )
        self.assertIn("IMPORT_OK", result.stdout)


class HttpSecurityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = _detect_base_url()
        try:
            r = requests.get(f"{cls.base}/__health", timeout=5)
        except requests.RequestException as exc:
            raise unittest.SkipTest(f"brainstorm not reachable at {cls.base}: {exc}")
        if r.status_code != 200:
            raise unittest.SkipTest(
                f"brainstorm health check returned {r.status_code}"
            )

    # ---- security headers ------------------------------------------------

    def test_security_headers_on_landing(self):
        r = requests.get(f"{self.base}/landing", timeout=10)
        self.assertEqual(r.status_code, 200)
        self.assertIn("Content-Security-Policy", r.headers,
                      "CSP header missing")
        self.assertEqual(r.headers.get("X-Frame-Options"), "DENY")
        self.assertEqual(r.headers.get("X-Content-Type-Options"), "nosniff")
        self.assertIn("Referrer-Policy", r.headers)
        self.assertIn("Permissions-Policy", r.headers)

    def test_landing_sets_pb_csrf_cookie(self):
        s = requests.Session()
        r = s.get(f"{self.base}/landing", timeout=10)
        self.assertEqual(r.status_code, 200)
        self.assertTrue(
            s.cookies.get("pb_csrf"),
            "GET /landing must set the pb_csrf cookie",
        )

    # ---- CSRF ------------------------------------------------------------

    def test_logout_without_csrf_is_rejected(self):
        # Fresh session: no pb_csrf cookie at all → 403.
        r = requests.post(f"{self.base}/logout", timeout=10,
                          allow_redirects=False)
        self.assertEqual(
            r.status_code, 403,
            f"Expected 403 on POST /logout without CSRF; got {r.status_code}",
        )

    def test_logout_with_csrf_token_is_accepted(self):
        s = requests.Session()
        tok = _bootstrap_csrf(s, self.base)
        r = s.post(
            f"{self.base}/logout",
            headers={"X-CSRF-Token": tok},
            timeout=10, allow_redirects=False,
        )
        # /logout returns a redirect on success.
        self.assertIn(
            r.status_code, (301, 302, 303),
            f"Expected redirect on POST /logout with CSRF; got {r.status_code}",
        )

    def test_logout_with_wrong_csrf_token_is_rejected(self):
        s = requests.Session()
        _bootstrap_csrf(s, self.base)  # cookie set, but we send wrong header
        r = s.post(
            f"{self.base}/logout",
            headers={"X-CSRF-Token": "definitely-not-the-real-token"},
            timeout=10, allow_redirects=False,
        )
        self.assertEqual(
            r.status_code, 403,
            f"Expected 403 on POST /logout with mismatched CSRF; got {r.status_code}",
        )

    # ---- /login lockout --------------------------------------------------

    def test_login_locks_out_after_5_failures(self):
        # Fresh random identity so we can't collide with a real user or
        # with prior test runs (the failure counter is keyed by IP +
        # identity, so a brand-new identity is always a fresh bucket).
        identity = f"sec-test-{uuid.uuid4().hex}@example.invalid"
        s = requests.Session()
        tok = _bootstrap_csrf(s, self.base)
        headers = {"X-CSRF-Token": tok}
        data_template = {"csrf_token": tok, "password": "wrong-password"}

        for i in range(5):
            r = s.post(
                f"{self.base}/login",
                data={**data_template, "email": identity},
                headers=headers, timeout=10, allow_redirects=False,
            )
            # First 5 attempts: NOT throttled. Real auth still fails
            # (returns the render_error page, status 200), but we should
            # never see 429 yet.
            self.assertNotEqual(
                r.status_code, 429,
                f"attempt #{i+1}: should not be locked out yet, got 429",
            )

        # 6th attempt for the same identity from this IP must be 429.
        r = s.post(
            f"{self.base}/login",
            data={**data_template, "email": identity},
            headers=headers, timeout=10, allow_redirects=False,
        )
        self.assertEqual(
            r.status_code, 429,
            f"attempt #6: expected 429 after 5 failed logins; got {r.status_code}",
        )
        retry_after = r.headers.get("Retry-After", "")
        self.assertTrue(
            retry_after.isdigit() and int(retry_after) > 0,
            f"Expected positive numeric Retry-After header; got {retry_after!r}",
        )

    # ---- /admin-login lockout -------------------------------------------

    def test_admin_login_locks_out_after_5_failures(self):
        # NOTE: admin_login is keyed by identity_key="admin" globally, so
        # this test will lock out the real admin path on this IP for the
        # configured 30-minute window. That's intentional — the test
        # asserts the lockout actually fires. If you re-run the suite
        # within 30 minutes, this test will be skipped because the bucket
        # is still locked. Restart the workflow to clear in-memory state.
        s = requests.Session()
        tok = _bootstrap_csrf(s, self.base)
        headers = {"X-CSRF-Token": tok}
        data = {"csrf_token": tok, "admin_password": "wrong-admin-password"}

        # Pre-flight: if first call already returns the "Too many ..."
        # redirect, we're in a stale lockout from a prior run — skip.
        r0 = s.post(
            f"{self.base}/admin-login",
            data=data, headers=headers, timeout=10, allow_redirects=False,
        )
        if r0.status_code in (301, 302, 303):
            location = r0.headers.get("Location", "")
            if "Too+many" in location or "Too%20many" in location or "Too many" in location:
                self.skipTest(
                    "admin_login is already locked out on this IP from a "
                    "prior run; restart the brainstorm workflow to clear."
                )

        # We've already burned attempt #1 above; need 4 more wrong
        # passwords to reach the 5-fail threshold, then a 6th to verify
        # lockout.
        for i in range(4):
            r = s.post(
                f"{self.base}/admin-login",
                data=data, headers=headers, timeout=10, allow_redirects=False,
            )
            self.assertIn(
                r.status_code, (301, 302, 303),
                f"attempt #{i+2}: expected redirect; got {r.status_code}",
            )

        # 6th attempt should redirect with the "Too many ..." error.
        r = s.post(
            f"{self.base}/admin-login",
            data=data, headers=headers, timeout=10, allow_redirects=False,
        )
        self.assertIn(
            r.status_code, (301, 302, 303),
            f"attempt #6: expected redirect; got {r.status_code}",
        )
        location = r.headers.get("Location", "")
        self.assertTrue(
            "Too+many" in location or "Too%20many" in location or "Too many" in location,
            f"Expected 'Too many ...' error in redirect Location; got {location!r}",
        )


class P2HelpersTest(unittest.TestCase):
    """Task #57 P2 — exercise the central helpers directly via import.

    These don't need the running server; they import ``app`` to prove the
    helpers are present, importable, and behave correctly. Running
    ``app`` requires ``ADMIN_PASSWORD`` and ``FLASK_SECRET`` envs (the
    fail-fast guards), so we set safe values before import.
    """

    @classmethod
    def setUpClass(cls):
        # Force development mode so the prod fail-fast guards (which
        # demand a strong ADMIN_PASSWORD + FLASK_SECRET) don't hard-exit
        # this test process. We're only exercising helpers, not booting
        # the app for real traffic.
        os.environ["FLASK_ENV"] = "development"
        os.environ.setdefault("ADMIN_PASSWORD", "test-helpers-admin-pw-strong")
        os.environ.setdefault("FLASK_SECRET", "test-helpers-flask-secret-strong")
        sys.path.insert(0, HERE)
        import importlib
        cls.app_mod = importlib.import_module("app")

    def test_safe_json_loads_depth_rejected(self):
        # Build a JSON value nested 50 levels deep — must be rejected by
        # the depth cap (default 8).
        s = "[" * 50 + "1" + "]" * 50
        with self.assertRaises(ValueError):
            self.app_mod.safe_json_loads(s, max_depth=8)

    def test_safe_json_loads_keys_rejected(self):
        big = {f"k{i}": i for i in range(1000)}
        import json as _j
        with self.assertRaises(ValueError):
            self.app_mod.safe_json_loads(_j.dumps(big), max_keys=500)

    def test_safe_json_loads_happy_path(self):
        out = self.app_mod.safe_json_loads('{"a": [1, 2, {"b": "c"}]}')
        self.assertEqual(out, {"a": [1, 2, {"b": "c"}]})

    def test_sniff_file_type_pdf_signature(self):
        ok, _ = self.app_mod.sniff_file_type(b"%PDF-1.4\n...", "pdf")
        self.assertTrue(ok)
        ok, reason = self.app_mod.sniff_file_type(b"MZ\x90...exe", "pdf")
        self.assertFalse(ok)
        self.assertIn("does not match", reason)

    def test_sniff_file_type_png_jpg_docx(self):
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16
        jpg = b"\xff\xd8\xff" + b"\x00" * 16
        docx = b"PK\x03\x04" + b"\x00" * 16
        self.assertTrue(self.app_mod.sniff_file_type(png, "png")[0])
        self.assertTrue(self.app_mod.sniff_file_type(jpg, "jpg")[0])
        self.assertTrue(self.app_mod.sniff_file_type(jpg, "jpeg")[0])
        self.assertTrue(self.app_mod.sniff_file_type(docx, "docx")[0])
        # Cross-type spoof: PNG payload claiming to be a PDF must fail.
        self.assertFalse(self.app_mod.sniff_file_type(png, "pdf")[0])

    def test_sniff_file_type_text_rejects_nuls(self):
        self.assertTrue(self.app_mod.sniff_file_type(b"name,age\nA,1\n", "csv")[0])
        self.assertTrue(self.app_mod.sniff_file_type(b"hello world", "txt")[0])
        self.assertFalse(self.app_mod.sniff_file_type(b"a\x00b", "csv")[0])

    def test_cap_llm_output_truncates(self):
        big = "A" * (self.app_mod.LLM_MAX_OUTPUT_BYTES + 5000)
        out = self.app_mod.cap_llm_output(big, purpose="unit-test")
        self.assertLessEqual(len(out.encode("utf-8")), self.app_mod.LLM_MAX_OUTPUT_BYTES)

    def test_cap_llm_output_passthrough(self):
        out = self.app_mod.cap_llm_output("short", purpose="unit-test")
        self.assertEqual(out, "short")

    def test_realpath_within_blocks_traversal(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            self.assertTrue(self.app_mod.realpath_within(os.path.join(td, "a.txt"), td))
            self.assertFalse(
                self.app_mod.realpath_within(os.path.join(td, "..", "evil.txt"), td)
            )

    def test_audit_log_appends(self):
        # Capture the path, write, then read it back.
        path = self.app_mod.AUDIT_LOG_PATH
        before = os.path.getsize(path) if os.path.exists(path) else 0
        marker = f"unittest_marker_{uuid.uuid4().hex}"
        # audit_log uses flask.request internally; call inside a test
        # request context so request.remote_addr / .path don't blow up.
        with self.app_mod.app.test_request_context("/__unittest__"):
            self.app_mod.audit_log("unit_test_event", marker=marker)
        self.assertTrue(os.path.exists(path), "audit.log should be created")
        with open(path, "r", encoding="utf-8") as f:
            f.seek(before)
            tail = f.read()
        self.assertIn(marker, tail)
        self.assertIn('"event": "unit_test_event"', tail)


if __name__ == "__main__":
    unittest.main(verbosity=2)
