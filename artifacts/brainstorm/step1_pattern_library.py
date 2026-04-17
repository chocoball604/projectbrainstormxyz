"""Step 1 Pattern Library loader, validator, and helpers (Prompt 3).

The library lives at ``data/step1_pattern_library.json``. It contains the
canonical BP/DS templates, per-pattern good/avoid examples (one entry per
weakness reason), and the ``solution_bias_triggers`` list Mark uses for
Step 1 framing coaching.

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

MAX_PAYLOAD_BYTES = 64 * 1024

_cache_lock = threading.Lock()
_cache = {"mtime": None, "data": None}


def _fb_pattern(pid, reason, label):
    return {
        "pattern_id": pid,
        "label": label,
        "weakness_reason": reason,
        "description": label,
        "good_examples": ["(fallback example)"],
        "avoid_examples": ["(fallback example)"],
        "advice": "Reframe as an uncertainty.",
        "suggested_template_ids": [],
    }


_FALLBACK_LIBRARY = {
    "version": 0,
    "updated_at": "1970-01-01T00:00:00Z",
    "updated_by": "fallback",
    "business_problem": {
        "templates": [
            {"id": "BP_1_RATE_AND_DRIVER", "label": "rate + driver",
             "text": "We don't yet understand how fast/where/when [phenomenon] is changing or what is driving that change.",
             "match_signature": ["don't yet understand", "driving"]},
            {"id": "BP_2_TRIGGER_AND_CONDITIONS", "label": "trigger + conditions",
             "text": "We don't yet understand what triggers [phenomenon] and under what conditions it accelerates or slows.",
             "match_signature": ["triggers", "conditions"]},
            {"id": "BP_3_INTERPRETATION_A_VS_B", "label": "A vs B interpretation",
             "text": "It's unclear whether [observed signal] reflects [meaning/cause A] or [meaning/cause B].",
             "match_signature": ["unclear whether", "reflects"]},
        ],
        "patterns": [
            _fb_pattern("bp_empty", "empty", "Empty draft"),
            _fb_pattern("bp_too_short", "too_short", "Too short"),
            _fb_pattern("bp_solution_bias", "solution_bias", "Solution bias"),
            _fb_pattern("bp_missing_uncertainty", "missing_uncertainty", "Missing uncertainty"),
        ],
    },
    "decision_to_support": {
        "templates": [
            {"id": "DS_1_INVEST_OR_PAUSE", "label": "invest vs pause",
             "text": "Whether to invest in deeper exploration of this opportunity or pause until the key assumptions are clearer.",
             "match_signature": ["invest", "pause"]},
            {"id": "DS_2_CONTINUE_OR_REDIRECT", "label": "continue vs redirect",
             "text": "Whether our current direction is promising enough to pursue, or whether we should redirect to alternatives.",
             "match_signature": ["redirect"]},
            {"id": "DS_3_NARROW_BEFORE_COMMIT", "label": "narrow directions",
             "text": "How to narrow the plausible directions before committing resources to one.",
             "match_signature": ["narrow", "directions"]},
        ],
        "patterns": [
            _fb_pattern("ds_empty", "empty", "Empty draft"),
            _fb_pattern("ds_too_short", "too_short", "Too short"),
            _fb_pattern("ds_solution_bias", "solution_bias", "Solution bias"),
            _fb_pattern("ds_missing_uncertainty", "missing_uncertainty", "Missing uncertainty"),
        ],
    },
    "solution_bias_triggers": [
        "feature", "tactic", "pricing", "launch", "campaign", "roadmap",
        "intervention", "go-to-market",
    ],
    "uncertainty_markers": [
        "don't know", "do not know", "don't yet", "do not yet",
        "unsure", "uncertain", "unclear",
        "not sure", "haven't yet", "have not yet",
        "don't understand", "do not understand", "don't yet understand",
        "we are uncertain", "it is unclear", "it's unclear",
    ],
    "bias_fail_keywords": [
        "feature", "tactic", "pricing", "launch", "campaign", "roadmap",
        "intervention", "go-to-market",
    ],
}


def _is_str_list(v, min_len=0):
    if not isinstance(v, list) or len(v) < min_len:
        return False
    return all(isinstance(x, str) and x.strip() for x in v)


def validate_library(data):
    """Return (ok: bool, error: str). Strict structural check."""
    if not isinstance(data, dict):
        return False, "library must be a JSON object"
    if "version" not in data or not isinstance(data["version"], int):
        return False, "missing or non-integer 'version'"
    triggers = data.get("solution_bias_triggers")
    if not _is_str_list(triggers, min_len=1):
        return False, "'solution_bias_triggers' must be a non-empty list of strings"
    fails = data.get("bias_fail_keywords")
    if not _is_str_list(fails, min_len=1):
        return False, "'bias_fail_keywords' must be a non-empty list of strings"
    if "uncertainty_markers" in data:
        if not _is_str_list(data["uncertainty_markers"], min_len=1):
            return False, "'uncertainty_markers' must be a non-empty list of strings"

    all_pattern_ids = set()
    all_template_ids = set()
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
            if tpl["id"] in all_template_ids:
                return False, f"duplicate template id '{tpl['id']}' across library"
            tpl_ids.add(tpl["id"])
            all_template_ids.add(tpl["id"])
            if "match_signature" in tpl and not isinstance(tpl["match_signature"], list):
                return False, f"{fld}.templates['{tpl['id']}'].match_signature must be a list"

        patterns = block.get("patterns")
        if not isinstance(patterns, list) or len(patterns) < 1:
            return False, f"{fld}.patterns must be a non-empty list"
        seen_reasons = set()
        for pat in patterns:
            if not isinstance(pat, dict):
                return False, f"{fld}.patterns entries must be objects"
            pid = pat.get("pattern_id")
            if not pid or not isinstance(pid, str):
                return False, f"{fld}.patterns entry missing string 'pattern_id'"
            if pid in all_pattern_ids:
                return False, f"duplicate pattern_id '{pid}' across library"
            all_pattern_ids.add(pid)
            for k in ("label", "weakness_reason"):
                if not pat.get(k) or not isinstance(pat[k], str):
                    return False, f"{fld}.patterns['{pid}'] missing string '{k}'"
            if pat["weakness_reason"] not in _VALID_REASONS:
                return False, (f"{fld}.patterns['{pid}'].weakness_reason "
                               f"must be one of {_VALID_REASONS}")
            seen_reasons.add(pat["weakness_reason"])
            if not _is_str_list(pat.get("good_examples"), min_len=1):
                return False, f"{fld}.patterns['{pid}'].good_examples must be non-empty list of strings"
            if not _is_str_list(pat.get("avoid_examples"), min_len=1):
                return False, f"{fld}.patterns['{pid}'].avoid_examples must be non-empty list of strings"
            if "description" in pat and not isinstance(pat["description"], str):
                return False, f"{fld}.patterns['{pid}'].description must be a string"
            sti = pat.get("suggested_template_ids", [])
            if not isinstance(sti, list):
                return False, f"{fld}.patterns['{pid}'].suggested_template_ids must be list"
            for tid in sti:
                if tid not in tpl_ids:
                    return False, (f"{fld}.patterns['{pid}'] references "
                                   f"unknown template id '{tid}'")
        for required in _VALID_REASONS:
            if required not in seen_reasons:
                return False, f"{fld}.patterns missing entry for reason '{required}'"
    return True, ""


_missing_warned = False


def load_library():
    """Return the current library dict. Falls back if missing/invalid."""
    global _missing_warned
    try:
        st = os.stat(LIBRARY_PATH)
        mtime = st.st_mtime
    except OSError:
        if not _missing_warned:
            print(
                f"STEP1_LIBRARY_MISSING: file not found at {LIBRARY_PATH}; "
                f"using in-code fallback. Restore the seed JSON to enable "
                f"library-driven coaching.",
                flush=True,
            )
            _missing_warned = True
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

    Enforces a 64KB max payload size. Returns (ok, error). On success,
    bumps the cache.
    """
    if not isinstance(new_data, dict):
        return False, "payload must be an object"
    try:
        size = len(json.dumps(new_data, ensure_ascii=False).encode("utf-8"))
    except Exception as e:
        return False, f"payload not serialisable: {e}"
    if size > MAX_PAYLOAD_BYTES:
        return False, f"payload too large ({size} bytes; max {MAX_PAYLOAD_BYTES})"
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
            return pat.get("pattern_id")
    return None


def match_template_id(rewrite_text, applies_to):
    """Best-effort match of a Mark rewrite to a canonical template id.

    `applies_to` is one of 'business_problem' or 'decision_to_support'.
    Returns the template id (e.g. 'BP_2_TRIGGER_AND_CONDITIONS') or None.
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


def library_is_fallback():
    """True when the in-code fallback library is in effect (file missing
    or invalid). Callers should prefer their own hard-coded defaults over
    fallback-library values in that case."""
    return load_library() is _FALLBACK_LIBRARY


def solution_bias_triggers():
    """Return the current bias-trigger list (loader-cached).

    Returns an empty list when the file-backed library is missing or
    invalid so callers can fall back to their own defaults rather than
    the in-code fallback library values.
    """
    if library_is_fallback():
        return []
    return list(load_library().get("solution_bias_triggers") or [])


def uncertainty_markers():
    """Return the current uncertainty-marker list (loader-cached).

    Optional in the library; returns an empty list when missing, empty,
    or when the loader is using the in-code fallback library so callers
    can fall back to their own defaults.
    """
    if library_is_fallback():
        return []
    return list(load_library().get("uncertainty_markers") or [])


def bias_fail_keywords():
    """Return the canonical bias-fail keyword list used by the
    Mark-reply worker to classify the `Bias check:` line as fail.
    Single source of truth across prompt + worker."""
    return list(load_library().get("bias_fail_keywords") or [])
