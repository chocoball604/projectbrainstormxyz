#!/usr/bin/env python3
import sys
import os
import sqlite3
import time

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "brainstorm.db")

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn

def call_llm(model_id, messages):
    import openai as _openai
    base_url = os.environ.get("AI_INTEGRATIONS_OPENROUTER_BASE_URL")
    api_key = os.environ.get("AI_INTEGRATIONS_OPENROUTER_API_KEY")
    if not base_url or not api_key:
        print("WORKER: no API credentials found", flush=True)
        return None
    client = _openai.OpenAI(base_url=base_url, api_key=api_key, timeout=90)
    start = time.time()
    try:
        resp = client.chat.completions.create(
            model=model_id,
            messages=messages,
            max_tokens=256,
        )
        result = resp.choices[0].message.content or ""
        print(f"WORKER_LLM_OK took={time.time()-start:.1f}s chars={len(result)}", flush=True)
        return result
    except Exception as e:
        print(f"WORKER_LLM_ERROR took={time.time()-start:.1f}s err={e}", flush=True)
        return None

def main():
    if len(sys.argv) < 5:
        print("Usage: mark_reply_worker.py <study_id> <placeholder_id> <model_id> <message>", flush=True)
        sys.exit(1)

    study_id = int(sys.argv[1])
    placeholder_id = int(sys.argv[2])
    model_id = sys.argv[3]
    message_text = sys.argv[4]
    fallback_text = sys.argv[5] if len(sys.argv) > 5 else "I'm here to help! What would you like to work on?"

    print(f"WORKER_START study={study_id} placeholder={placeholder_id} model={model_id}", flush=True)

    mark_reply = call_llm(model_id, [{"role": "user", "content": message_text}])

    if not mark_reply:
        mark_reply = fallback_text
        print(f"WORKER: using fallback", flush=True)

    try:
        conn = get_db()
        conn.execute(
            "UPDATE chat_messages SET message_text = ? WHERE id = ?",
            (mark_reply, placeholder_id),
        )
        conn.commit()
        conn.close()
        print(f"WORKER_DONE study={study_id} placeholder={placeholder_id}", flush=True)
    except Exception as e:
        print(f"WORKER_DB_ERROR: {e}", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
