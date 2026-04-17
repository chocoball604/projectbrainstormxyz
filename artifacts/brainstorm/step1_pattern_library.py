"""Step 1 Pattern Library loader, validator, and helpers (Prompt 3).

The library lives at ``data/step1_pattern_library.json``. It contains the
canonical BP/DS templates, good/avoid examples, and the per-weakness
"patterns" that Mark uses for Step 1 framing coaching.

Loading is mtime-cached. If the file is missing or invalid, a small in-code
fallback library is returned so the app never crashes.
"""

import json
import os
import shutil
import threading
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
LIBRARY_PATH = os.path.join(_HERE, "data", "step1_pattern_library.json")
BACKUPS_DIR = os.path.join(_HERE, "data", "step1_pattern_library_backups")

_FIELDS = ("business_problem", "decision_to_support")
_VALID_REASONS = ("empty", "too_short", "solution_bias", "missing_uncertainty")

_cache_lock = threading.Lock()
_cache = {"mtime": None, "data": None}


_FALLBACK_LIBRARY = {
    "version": 0,
    "updated_at": "1970-01-01T00:00:00Z",
    "updated_by": "fallback",
    "business_problem": {
        "templates": [
            {"id": "BP-1", "label": "rate + driver",
             "text": "We don't yet understand how fast/where/when [phenomenon] is changing or what is driving that change.",
             "match_signature": ["don't yet understand", "driving"]},
            {"id": "BP-2", "label": "trigger + conditions",
             "text": "We don't yet understand what triggers [phenomenon] and under what conditions it accelerates or slows.",
             "match_signature": ["triggers", "conditions"]},
            {"id": "BP-3", "label": "A vs B interpretation",
             "text": "It's unclear whether [observed signal] reflects [meaning/cause A] or [meaning/cause B].",
             "match_signature": ["unclear whether", "reflects"]},
        ],
        "good_examples": [],
        "avoid_examples": [],
        "patterns": [
            {"id": "bp_empty", "weakness_reason": "empty", "label": "Empty draft",
             "advice": "Describe an uncertainty.", "suggested_template_ids": ["BP-3"]},
            {"id": "bp_too_short", "weakness_reason": "too_short", "label": "Too short",
             "advice": "Add observed change and unknown driver.", "suggested_template_ids": ["BP-1"]},
            {"id": "bp_solution_bias", "weakness_reason": "solution_bias", "label": "Solution bias",
             "advice": "Replace solution language with uncertainty.", "suggested_template_ids": ["BP-3"]},
            {"id": "bp_missing_uncertainty", "weakness_reason": "missing_uncertainty", "label": "Missing uncertainty",
             "advice": "Use uncertainty markers and name a driver.", "suggested_template_ids": ["BP-1"]},
        ],
    },
    "decision_to_support": {
        "templates": [
            {"id": "DS-1", "label": "invest vs pause",
             "text": "Whether to invest in deeper exploration of this opportunity or pause until the key assumptions are clearer.",
             "match_signature": ["invest", "pause"]},
            {"id": "DS-2", "label": "continue vs redirect",
             "text": "Whether our current direction is promising enough to pursue, or whether we should redirect to alternatives.",
             "match_signature": ["redirect"]},
            {"id": "DS-3", "label": "narrow directions",
             "text": "How to narrow the plausible directions before committing resources to one.",
             "match_signature": ["narrow", "directions"]},
        ],
        "good_examples": [],
        "avoid_examples": [],
        "patterns": [
            {"id": "ds_empty", "weakness_reason": "empty", "label": "Empty draft",
             "advice": "Describe a discovery-stage choice.", "suggested_template_ids": ["DS-1"]},
            {"id": "ds_too_short", "weakness_reason": "too_short", "label": "Too short",
             "advice": "Frame as invest/pause or continue/redirect.", "suggested_template_ids": ["DS-1"]},
            {"id": "ds_solution_bias", "weakness_reason": "solution_bias", "label": "Solution bias",
             "advice": "Replace execution decision with the discovery-stage decision.", "suggested_template_ids": ["DS-1"]},
            {"id": "ds_missing_uncertainty", "weakness_reason": "missing_uncertainty", "label": "Missing uncertainty",
             "advice": "Make the choice contingent on what is unknown.", "suggested_template_ids": ["DS-1"]},
        ],
    },
    "solution_bias_triggers": [
        "feature", "tactic", "pricing", "launch", "campaign", "roadmap",
        "intervention", "go-to-market",
    ],
}


def validate_library(data):
    """Return (ok: bool, error: str). Strict structural check."""
    if not isinstance(data, dict):
        return False, "library must be a JSON object"
    if "version" not in data or not isinstance(data["version"], int):
        return False, "missing or non-integer 'version'"
    if not isinstance(data.get("solution_bias_triggers"), list):
        return False, "missing 'solution_bias_triggers' list"
    for fld in _FIELDS:
        if fld not in data or not isinstance(data[fld], dict):
            return False, f"missing field block '{fld}'"
        block = data[fld]
        templates = block.get("templates")
        if not isinstance(templates, list) or len(templates) < 1:
            return False, f"{fld}.templates must be a non-empty list"
        tpl_ids = set()
        for tpl in templates:
            if not isinstance(tpl, dict):
                return False, f"{fld}.templates entries must be objects"
            for k in ("id", "label", "text"):
                if not tpl.get(k) or not isinstance(tpl[k], str):
                    return False, f"{fld}.templates entry missing string '{k}'"
            if tpl["id"] in tpl_ids:
                return False, f"{fld}.templates duplicate id '{tpl['id']}'"
            tpl_ids.add(tpl["id"])
            if "match_signature" in tpl and not isinstance(tpl["match_signature"], list):
                return False, f"{fld}.templates['{tpl['id']}'].match_signature must be a list"
        patterns = block.get("patterns")
        if not isinstance(patterns, list) or len(patterns) < 1:
            return False, f"{fld}.patterns must be a non-empty list"
        seen_reasons = set()
        for pat in patterns:
            if not isinstance(pat, dict):
                return False, f"{fld}.patterns entries must be objects"
            for k in ("id", "weakness_reason", "label", "advice"):
                if not pat.get(k) or not isinstance(pat[k], str):
                    return False, f"{fld}.patterns entry missing string '{k}'"
            if pat["weakness_reason"] not in _VALID_REASONS:
                return False, (f"{fld}.patterns['{pat['id']}'].weakness_reason "
                               f"must be one of {_VALID_REASONS}")
            seen_reasons.add(pat["weakness_reason"])
            sti = pat.get("suggested_template_ids", [])
            if not isinstance(sti, list):
                return False, f"{fld}.patterns['{pat['id']}'].suggested_template_ids must be list"
            for tid in sti:
                if tid not in tpl_ids:
                    return False, (f"{fld}.patterns['{pat['id']}'] references "
                                   f"unknown template id '{tid}'")
        for required in _VALID_REASONS:
            if required not in seen_reasons:
                return False, f"{fld}.patterns missing entry for reason '{required}'"
        for k in ("good_examples", "avoid_examples"):
            if k in block and not isinstance(block[k], list):
                return False, f"{fld}.{k} must be a list"
    return True, ""


def load_library():
    """Return the current library dict. Falls back if missing/invalid."""
    try:
        st = os.stat(LIBRARY_PATH)
        mtime = st.st_mtime
    except OSError:
        return _FALLBACK_LIBRARY
    with _cache_lock:
        if _cache["data"] is not None and _cache["mtime"] == mtime:
            return _cache["data"]
        try:
            with open(LIBRARY_PATH, "r") as f:
                data = json.load(f)
        except Exception as e:
            print(f"STEP1_LIBRARY_LOAD_ERROR: {e}", flush=True)
            return _FALLBACK_LIBRARY
        ok, err = validate_library(data)
        if not ok:
            print(f"STEP1_LIBRARY_INVALID: {err}", flush=True)
            return _FALLBACK_LIBRARY
        _cache["data"] = data
        _cache["mtime"] = mtime
        return data


def bump_cache():
    with _cache_lock:
        _cache["data"] = None
        _cache["mtime"] = None


def save_library(new_data, updated_by="admin"):
    """Validate, back up the existing file, and atomically write the new one.

    Returns (ok, error). On success, also bumps the cache.
    """
    if not isinstance(new_data, dict):
        return False, "payload must be an object"
    new_data.setdefault("solution_bias_triggers",
                        load_library().get("solution_bias_triggers", []))
    new_data["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    new_data["updated_by"] = updated_by
    existing = load_library()
    existing_version = int(existing.get("version", 0))
    supplied_version = new_data.get("version")
    if not isinstance(supplied_version, int) or supplied_version <= existing_version:
        new_data["version"] = existing_version + 1
    ok, err = validate_library(new_data)
    if not ok:
        return False, err
    os.makedirs(os.path.dirname(LIBRARY_PATH), exist_ok=True)
    os.makedirs(BACKUPS_DIR, exist_ok=True)
    if os.path.exists(LIBRARY_PATH):
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_path = os.path.join(BACKUPS_DIR, f"step1_pattern_library.{ts}.json")
        try:
            shutil.copy2(LIBRARY_PATH, backup_path)
        except Exception as e:
            return False, f"backup failed: {e}"
        _prune_backups()
    tmp_path = LIBRARY_PATH + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(new_data, f, indent=2, ensure_ascii=False)
            f.write("\n")
        os.replace(tmp_path, LIBRARY_PATH)
    except Exception as e:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        return False, f"write failed: {e}"
    bump_cache()
    return True, ""


def _prune_backups(keep=20):
    try:
        entries = sorted(
            (os.path.join(BACKUPS_DIR, n) for n in os.listdir(BACKUPS_DIR)
             if n.endswith(".json")),
            key=lambda p: os.path.getmtime(p),
            reverse=True,
        )
        for old in entries[keep:]:
            try:
                os.unlink(old)
            except OSError:
                pass
    except OSError:
        pass


def pattern_id_for_reason(field, reason):
    """Return the matching pattern id ('bp_too_short', etc.) or None."""
    if field not in _FIELDS or not reason:
        return None
    lib = load_library()
    for pat in lib[field].get("patterns", []):
        if pat.get("weakness_reason") == reason:
            return pat.get("id")
    return None


def match_template_id(rewrite_text, applies_to):
    """Best-effort match of a Mark rewrite to a canonical template id.

    `applies_to` is one of 'business_problem' or 'decision_to_support'.
    Returns the template id (e.g. 'BP-2') or None.
    """
    if not rewrite_text or applies_to not in _FIELDS:
        return None
    text = rewrite_text.lower()
    lib = load_library()
    best = None
    best_hits = 0
    for tpl in lib[applies_to].get("templates", []):
        sigs = tpl.get("match_signature") or []
        if not sigs:
            continue
        hits = sum(1 for s in sigs if s.lower() in text)
        if hits > best_hits and hits >= max(1, len(sigs) // 2 + (1 if len(sigs) <= 2 else 0)):
            best_hits = hits
            best = tpl.get("id")
    return best
