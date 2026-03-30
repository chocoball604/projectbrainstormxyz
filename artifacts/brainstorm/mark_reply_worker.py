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
    client = _openai.OpenAI(base_url=base_url, api_key=api_key, timeout=HARD_TIMEOUT_SECONDS)
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

V1A_FIELD_MAP = {
    "market_geography": "study_fit",
    "product_concept": "known_vs_unknown",
}
V1A_ANCHOR_ORDER = [
    ("business_problem", "Business Problem"),
    ("decision_to_support", "Decision to Support"),
    ("market_geography", "Market / Geography"),
    ("product_concept", "Product / Concept"),
    ("target_audience", "Target Audience"),
    ("definition_useful_insight", "Definition of Useful Insight"),
]

_CONFIRMATION_PREFIXES = ("yes", "ok", "save", "confirmed", "confirm", "sure", "go ahead", "do it", "yep", "yeah")

def _is_confirmation_only(text):
    return text.lower().strip().startswith(_CONFIRMATION_PREFIXES) and len(text.strip()) < 40

def _detect_answer_no_proposal(mark_reply, study_id, user_message, chat_history_msgs):
    if not mark_reply or "Proposed updates:" in mark_reply:
        return False
    if not user_message or _is_confirmation_only(user_message):
        return False
    prev_mark_msgs = [m for m in chat_history_msgs if m.get("role") == "assistant"]
    if not prev_mark_msgs:
        return False
    last_mark = prev_mark_msgs[-1].get("content", "")
    if "?" not in last_mark:
        return False
    return True

def _auto_nudge_mark(model_id, messages, study_id):
    nudge_msg = (
        "It looks like you answered my question, but I didn't propose an update yet. "
        "Let me do that properly now."
    )
    forced_instruction = (
        "SYSTEM: The user has provided a valid answer to your last question. "
        "You must now emit a proposal for the appropriate field using the mandatory proposal format. "
        "Do not acknowledge or ask a question. Emit a proposal only."
    )
    retry_messages = list(messages)
    retry_messages.append({"role": "assistant", "content": nudge_msg})
    retry_messages.append({"role": "system", "content": forced_instruction})

    print(f"WORKER_AUTO_NUDGE study={study_id} retrying once", flush=True)
    try:
        retry_reply = call_llm(model_id, retry_messages)
    except LLMTimeoutError:
        print(f"WORKER_AUTO_NUDGE_TIMEOUT study={study_id}", flush=True)
        return nudge_msg, None
    except Exception as e:
        print(f"WORKER_AUTO_NUDGE_ERROR study={study_id}: {e}", flush=True)
        return nudge_msg, None

    if retry_reply and "Proposed updates:" in retry_reply:
        print(f"WORKER_AUTO_NUDGE_OK study={study_id}", flush=True)
        return nudge_msg, retry_reply
    else:
        print(f"WORKER_AUTO_NUDGE_STILL_NO_PROPOSAL study={study_id}", flush=True)
        return nudge_msg, retry_reply


def _maybe_append_fallback_proposal(mark_reply, study_id, user_message):
    if not user_message or len(user_message.strip()) > 200:
        return mark_reply
    try:
        conn = get_db()
        try:
            row = conn.execute("SELECT * FROM studies WHERE id = ?", (study_id,)).fetchone()
            if not row:
                return mark_reply
            study = dict(row)
            next_field = None
            next_label = None
            for key, label in V1A_ANCHOR_ORDER:
                db_key = V1A_FIELD_MAP.get(key, key)
                val = (study.get(db_key) or "").strip()
                if not val:
                    next_field = key
                    next_label = label
                    break
            if not next_field:
                return mark_reply
            reply_lower = mark_reply.lower()
            label_lower = next_label.lower()
            field_words = next_field.replace("_", " ")
            if label_lower not in reply_lower and field_words not in reply_lower:
                return mark_reply
            value = user_message.strip()
            proposal = (
                f"\n\nProposed updates:\n"
                f"- field: {next_field}\n"
                f"  value: {value}\n\n"
                f"Confirmation question:\n"
                f"Should I save these updates?"
            )
            print(f"WORKER_FALLBACK_PROPOSAL study={study_id} field={next_field}", flush=True)
            return mark_reply + proposal
        finally:
            conn.close()
    except Exception as e:
        print(f"WORKER_FALLBACK_ERROR study={study_id}: {e}", flush=True)
        return mark_reply


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
    if prompt_file and os.path.exists(prompt_file):
        try:
            with open(prompt_file, "r") as f:
                prompt_data = json.load(f)
            system_prompt = prompt_data.get("system_prompt", "")
            chat_history = prompt_data.get("chat_history", [])
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
        if mark_reply and "Proposed updates:" not in mark_reply:
            mark_reply = _maybe_append_fallback_proposal(mark_reply, study_id, message_text)
        if mark_reply and "Proposed updates:" not in mark_reply:
            if _detect_answer_no_proposal(mark_reply, study_id, message_text, messages):
                nudge_msg, retry_reply = _auto_nudge_mark(model_id, messages + [{"role": "assistant", "content": mark_reply}], study_id)
                update_placeholder(placeholder_id, mark_reply)
                conn = get_db()
                try:
                    cursor = conn.execute(
                        "INSERT INTO chat_messages (study_id, sender, message_text) VALUES (?, 'mark', ?)",
                        (study_id, nudge_msg),
                    )
                    nudge_id = cursor.lastrowid
                    if retry_reply:
                        conn.execute(
                            "INSERT INTO chat_messages (study_id, sender, message_text) VALUES (?, 'mark', ?)",
                            (study_id, retry_reply),
                        )
                    conn.commit()
                except Exception as e:
                    print(f"WORKER_NUDGE_DB_ERROR study={study_id}: {e}", flush=True)
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass
                print(f"WORKER_DONE_WITH_NUDGE study={study_id} placeholder={placeholder_id}", flush=True)
                sys.exit(0)
        if update_placeholder(placeholder_id, mark_reply):
            print(f"WORKER_DONE study={study_id} placeholder={placeholder_id}", flush=True)
        else:
            print(f"WORKER_FAILED_DB study={study_id} placeholder={placeholder_id}", flush=True)
            sys.exit(1)

if __name__ == "__main__":
    main()
