import json
import os
import secrets
import threading
from datetime import datetime, timezone

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
MESSAGES_FILE = os.path.join(DATA_DIR, "messages.json")
_lock = threading.Lock()


def _load():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(MESSAGES_FILE):
        return []
    try:
        with open(MESSAGES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return []


def _save(messages):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(MESSAGES_FILE, "w", encoding="utf-8") as f:
        json.dump(messages, f, ensure_ascii=False, indent=2)


def create_message(sender_type, recipient_user_id, title, body):
    with _lock:
        messages = _load()
        msg = {
            "id": secrets.token_hex(8),
            "sender_type": sender_type,
            "recipient_user_id": recipient_user_id,
            "title": title,
            "body": body,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "read": False,
        }
        messages.append(msg)
        _save(messages)
        return msg


def list_messages(recipient_user_id):
    with _lock:
        messages = _load()
    return sorted(
        [m for m in messages if m["recipient_user_id"] == recipient_user_id],
        key=lambda m: m["timestamp"],
        reverse=True,
    )


def get_unread_count(recipient_user_id):
    with _lock:
        messages = _load()
    return sum(
        1
        for m in messages
        if m["recipient_user_id"] == recipient_user_id and not m["read"]
    )


def mark_read(message_id, recipient_user_id):
    with _lock:
        messages = _load()
        for m in messages:
            if m["id"] == message_id and m["recipient_user_id"] == recipient_user_id:
                m["read"] = True
                _save(messages)
                return True
    return False


def get_latest_system_admin_message(recipient_user_id):
    with _lock:
        messages = _load()
    inbound = sorted(
        [
            m
            for m in messages
            if m["recipient_user_id"] == recipient_user_id
            and m["sender_type"] in ("system", "admin")
        ],
        key=lambda m: m["timestamp"],
        reverse=True,
    )
    return inbound[0] if inbound else None


def get_unread_count_inbound(recipient_user_id):
    with _lock:
        messages = _load()
    return sum(
        1
        for m in messages
        if m["recipient_user_id"] == recipient_user_id
        and not m["read"]
        and m["sender_type"] in ("system", "admin")
    )


def list_all_messages_admin():
    with _lock:
        messages = _load()
    return sorted(messages, key=lambda m: m["timestamp"], reverse=True)
