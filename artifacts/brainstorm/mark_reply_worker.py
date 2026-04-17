#!/usr/bin/env python3
import sys
import os
import json
import sqlite3
import time
import signal
import secrets

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "brainstorm.db")

HARD_TIMEOUT_SECONDS = 45

TIMEOUT_USER_MESSAGE = (
    "Mark couldn\u2019t respond in time using the currently selected model. "
    "This request was not completed. An admin has been notified. "
    "Please try again or ask your admin to select a faster model."
)

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn

class LLMTimeoutError(Exception):
    pass

def _timeout_handler(signum, frame):
    raise LLMTimeoutError(f"LLM call exceeded {HARD_TIMEOUT_SECONDS}s wall-clock timeout")

def call_llm(model_id, messages):
    import openai as _openai
    base_url = os.environ.get("AI_INTEGRATIONS_OPENROUTER_BASE_URL")
    api_key = os.environ.get("AI_INTEGRATIONS_OPENROUTER_API_KEY")
    if not base_url or not api_key:
        print("WORKER: no API credentials found", flush=True)
        return None
    client = _openai.OpenAI(base_url=base_url, api_key=api_key, timeout=90)
    start = time.time()
    old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(HARD_TIMEOUT_SECONDS)
    try:
        resp = client.chat.completions.create(
            model=model_id,
            messages=messages,
            max_tokens=256,
        )
        signal.alarm(0)
        result = resp.choices[0].message.content or ""
        print(f"WORKER_LLM_OK took={time.time()-start:.1f}s chars={len(result)}", flush=True)
        return result
    except LLMTimeoutError:
        print(f"WORKER_LLM_TIMEOUT took={time.time()-start:.1f}s model={model_id}", flush=True)
        raise
    except Exception as e:
        signal.alarm(0)
        print(f"WORKER_LLM_ERROR took={time.time()-start:.1f}s err={e}", flush=True)
        return None
    finally:
        signal.signal(signal.SIGALRM, old_handler)

def record_timeout_incident(model_id, study_id):
    from datetime import datetime, timezone
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    run_id = secrets.token_hex(8)
    conn = None
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO model_health_status (model_id, status, last_tested_at, last_error) "
            "VALUES (?, 'degraded', ?, ?) "
            "ON CONFLICT(model_id) DO UPDATE SET status='degraded', last_tested_at=?, last_error=?",
            (
                model_id,
                now_str,
                f"mark_chat_timeout study_id={study_id}",
                now_str,
                f"mark_chat_timeout study_id={study_id}",
            ),
        )
        details = {
            "incident": "mark_chat_timeout",
            "model_id": model_id,
            "study_id": study_id,
            "timeout_seconds": HARD_TIMEOUT_SECONDS,
            "context": "send_chat/mark_reply_worker",
        }
        conn.execute(
            "INSERT INTO model_health_checks "
            "(run_id, run_type, started_at, finished_at, integration_mode, summary_status, details_json) "
            "VALUES (?, 'incident_mark_chat_timeout', ?, ?, 'live_calls_enabled', 'fail', ?)",
            (run_id, now_str, now_str, json.dumps(details)),
        )
        conn.commit()
        print(f"WORKER_INCIDENT_RECORDED model={model_id} study={study_id} run_id={run_id}", flush=True)
    except Exception as e:
        print(f"WORKER_INCIDENT_RECORD_ERROR: {e}", flush=True)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

_PREFIX_REWRITE_PROBLEM = "rewrite (problem):"
_PREFIX_REWRITE_DECISION = "rewrite (decision):"
_PREFIX_BIAS_CHECK = "bias check:"
_PREFIX_NEXT_STEP = "next step:"
_PREFIX_TIP = "tip:"


def _line_kind(line):
    low = line.strip().lower()
    if low.startswith(_PREFIX_REWRITE_PROBLEM):
        return "rewrite_problem"
    if low.startswith(_PREFIX_REWRITE_DECISION):
        return "rewrite_decision"
    if low.startswith(_PREFIX_BIAS_CHECK):
        return "bias_check"
    if low.startswith(_PREFIX_NEXT_STEP):
        return "next_step"
    if low.startswith(_PREFIX_TIP):
        return "tip"
    return None


_BIAS_PASS_PHRASES = (
    "no major solution bias",
    "no solution bias",
    "no major bias",
    "no significant bias",
    "no notable bias",
    "no obvious bias",
    "no apparent bias",
    "not solution-leaning",
    "not solution leaning",
    "already framed as an uncertainty",
    "already framed as uncertainty",
    "framed as an uncertainty",
    "passes",
    "no bias detected",
    "no major issues",
)

_BIAS_FAIL_KEYWORDS = (
    "feature",
    "tactic",
    "tactical",
    "pricing",
    "launch",
    "campaign",
    "roadmap",
    "intervention",
    "go-to-market",
    "go to market",
    "implies an intervention",
    "implies a solution",
    "sneaks in",
    "sneaks-in",
    "solution-leaning",
    "solution leaning",
    "biased toward",
    "biased towards",
    "prescribes",
    "names a solution",
)


def classify_bias_verdict(bias_line, any_weak):
    """Classify Mark's `Bias check:` line as 'pass' or 'fail'.

    Default to 'pass' to avoid false alarms; only fall back to 'fail' on a
    genuinely ambiguous line when at least one weakness flag is set.
    """
    if not bias_line:
        return "fail" if any_weak else "pass"
    body = bias_line.split(":", 1)[1].lower() if ":" in bias_line else bias_line.lower()
    body = body.strip()
    if not body:
        return "fail" if any_weak else "pass"
    for phrase in _BIAS_PASS_PHRASES:
        if phrase in body:
            return "pass"
    for kw in _BIAS_FAIL_KEYWORDS:
        if kw in body:
            return "fail"
    return "fail" if any_weak else "pass"


def enforce_step1_format(reply, enforce):
    if not reply or not enforce:
        return reply
    action = enforce.get("action") or "full"
    any_weak = bool(enforce.get("any_weak"))
    if "tip_when_weak" in enforce:
        tip_when_weak = bool(enforce.get("tip_when_weak"))
    else:
        tip_when_weak = any_weak
    next_step_value = enforce.get("next_step") or "Revise again"
    tip_value = enforce.get("tip") or ""
    rewrite_requested = bool(enforce.get("rewrite_requested"))

    raw_lines = [ln.rstrip() for ln in reply.replace("\r\n", "\n").split("\n")]
    parsed = {}
    for ln in raw_lines:
        kind = _line_kind(ln)
        if kind is None:
            continue
        if "(no change)" in ln.lower() or "(n/a)" in ln.lower():
            continue
        if kind not in parsed:
            parsed[kind] = ln.strip()

    bias_verdict = None
    out = []
    if action == "rewrite_problem":
        if "rewrite_problem" in parsed:
            out.append(parsed["rewrite_problem"])
    elif action == "rewrite_decision":
        if "rewrite_decision" in parsed:
            out.append(parsed["rewrite_decision"])
    elif action == "bias_check":
        if "bias_check" in parsed:
            out.append(parsed["bias_check"])
        bias_verdict = classify_bias_verdict(parsed.get("bias_check"), any_weak)
        if bias_verdict == "pass":
            next_step_value = "Save checkpoint"
        else:
            next_step_value = "Revise again"
        out.append(f"Next step: {next_step_value}")
    else:
        if "rewrite_problem" in parsed:
            out.append(parsed["rewrite_problem"])
        if "rewrite_decision" in parsed:
            out.append(parsed["rewrite_decision"])
        if "bias_check" in parsed:
            out.append(parsed["bias_check"])
        out.append(f"Next step: {next_step_value}")

    if action == "bias_check":
        allow_tip = (bias_verdict == "fail") or rewrite_requested
        if allow_tip and tip_value:
            out.append(f"Tip: {tip_value}")
        print(
            f"WORKER_BIAS_VERDICT verdict={bias_verdict} "
            f"rewrite_requested={rewrite_requested} "
            f"tip_emitted={'true' if (allow_tip and tip_value) else 'false'} "
            f"next_step={next_step_value!r}",
            flush=True,
        )
    else:
        if tip_when_weak and tip_value:
            out.append(f"Tip: {tip_value}")

    return "\n".join(out).strip()


def update_placeholder(placeholder_id, message_text):
    retries = 3
    for attempt in range(retries):
        conn = None
        try:
            conn = get_db()
            conn.execute(
                "UPDATE chat_messages SET message_text = ? WHERE id = ?",
                (message_text, placeholder_id),
            )
            conn.commit()
            return True
        except Exception as e:
            print(f"WORKER_DB_ERROR attempt={attempt+1}: {e}", flush=True)
            if attempt < retries - 1:
                time.sleep(1)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    return False

def main():
    if len(sys.argv) < 5:
        print("Usage: mark_reply_worker.py <study_id> <placeholder_id> <model_id> <message> [fallback] [prompt_file]", flush=True)
        sys.exit(1)

    study_id = int(sys.argv[1])
    placeholder_id = int(sys.argv[2])
    model_id = sys.argv[3]
    message_text = sys.argv[4]
    fallback_text = sys.argv[5] if len(sys.argv) > 5 else "I'm here to help! What would you like to work on?"
    prompt_file = sys.argv[6] if len(sys.argv) > 6 else None

    print(f"WORKER_START study={study_id} placeholder={placeholder_id} model={model_id}", flush=True)

    messages = []
    step1_enforce = None
    if prompt_file and os.path.exists(prompt_file):
        try:
            with open(prompt_file, "r") as f:
                prompt_data = json.load(f)
            system_prompt = prompt_data.get("system_prompt", "")
            chat_history = prompt_data.get("chat_history", [])
            step1_enforce = prompt_data.get("step1_enforce")
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            for msg in chat_history[-10:]:
                messages.append({"role": msg["role"], "content": msg["content"]})
            messages.append({"role": "user", "content": message_text})
            os.unlink(prompt_file)
        except Exception as e:
            print(f"WORKER: prompt file error: {e}, falling back to raw message", flush=True)
            messages = [{"role": "user", "content": message_text}]
    else:
        messages = [{"role": "user", "content": message_text}]

    timed_out = False
    try:
        mark_reply = call_llm(model_id, messages)
    except LLMTimeoutError:
        mark_reply = None
        timed_out = True

    if timed_out:
        update_placeholder(placeholder_id, TIMEOUT_USER_MESSAGE)
        record_timeout_incident(model_id, study_id)
        print(f"WORKER_TIMEOUT study={study_id} placeholder={placeholder_id} model={model_id}", flush=True)
    else:
        if not mark_reply:
            mark_reply = fallback_text
            print(f"WORKER: using fallback", flush=True)
        if step1_enforce and mark_reply and mark_reply != fallback_text:
            try:
                cleaned = enforce_step1_format(mark_reply, step1_enforce)
                if cleaned:
                    print(f"WORKER_STEP1_ENFORCED action={step1_enforce.get('action')} "
                          f"any_weak={step1_enforce.get('any_weak')} "
                          f"before_chars={len(mark_reply)} after_chars={len(cleaned)}",
                          flush=True)
                    mark_reply = cleaned
            except Exception as e:
                print(f"WORKER_STEP1_ENFORCE_ERROR: {e}", flush=True)
        if update_placeholder(placeholder_id, mark_reply):
            print(f"WORKER_DONE study={study_id} placeholder={placeholder_id}", flush=True)
        else:
            print(f"WORKER_FAILED_DB study={study_id} placeholder={placeholder_id}", flush=True)
            sys.exit(1)

if __name__ == "__main__":
    main()
