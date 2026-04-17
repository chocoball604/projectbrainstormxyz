import sys
import os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mark_reply_worker import enforce_step1_format, classify_bias_verdict
import step1_pattern_library
import app as brainstorm_app

print("=== compute_step1_weakness reads triggers from library ===")
draft = ("We don't yet understand whether our zorblax metric reflects "
         "real demand or just measurement noise.")
weak, reason = brainstorm_app.compute_step1_weakness(draft)
print(f"baseline (no zorblax in triggers): weak={weak} reason={reason} (expected weak=False reason=ok)")
assert weak is False and reason == "ok", (weak, reason)

_orig_load = step1_pattern_library.load_library
try:
    custom = {"solution_bias_triggers": ["zorblax"], "uncertainty_markers": []}
    step1_pattern_library.load_library = lambda: custom
    weak2, reason2 = brainstorm_app.compute_step1_weakness(draft)
    print(f"after editing library to add 'zorblax': weak={weak2} reason={reason2} (expected weak=True reason=solution_bias)")
    assert weak2 is True and reason2 == "solution_bias", (weak2, reason2)

    custom2 = {"solution_bias_triggers": ["nothingmatchesthis"],
               "uncertainty_markers": ["zorblax"]}
    step1_pattern_library.load_library = lambda: custom2
    plain = "Our team will launch a new pricing tier and roadmap update next quarter."
    weak3, reason3 = brainstorm_app.compute_step1_weakness(plain)
    print(f"with library overriding triggers/markers: weak={weak3} reason={reason3} (expected weak=True reason=missing_uncertainty)")
    assert weak3 is True and reason3 == "missing_uncertainty", (weak3, reason3)
    step1_pattern_library.load_library = lambda: step1_pattern_library._FALLBACK_LIBRARY
    plain_solution = "We will launch a new pricing campaign next quarter for sure."
    weak4, reason4 = brainstorm_app.compute_step1_weakness(plain_solution)
    print(f"missing/invalid library -> uses in-code constants: weak={weak4} reason={reason4} (expected weak=True reason=solution_bias)")
    assert weak4 is True and reason4 == "solution_bias", (weak4, reason4)

    uncertain_text = "We don't know whether xyzzy demand is real or just measurement noise yet."
    weak5, reason5 = brainstorm_app.compute_step1_weakness(uncertain_text)
    print(f"missing/invalid library -> uncertainty constants: weak={weak5} reason={reason5} (expected weak=False reason=ok)")
    assert weak5 is False and reason5 == "ok", (weak5, reason5)
finally:
    step1_pattern_library.load_library = _orig_load

print("=== validate_library rejects empty solution_bias_triggers ===")
import copy
base = copy.deepcopy(step1_pattern_library._FALLBACK_LIBRARY)
base["version"] = 1
base["solution_bias_triggers"] = []
ok, err = step1_pattern_library.validate_library(base)
print(f"empty triggers: ok={ok} err={err}")
assert not ok and "solution_bias_triggers" in err

base2 = copy.deepcopy(step1_pattern_library._FALLBACK_LIBRARY)
base2["version"] = 1
base2["uncertainty_markers"] = []
ok, err = step1_pattern_library.validate_library(base2)
print(f"empty uncertainty_markers: ok={ok} err={err}")
assert not ok and "uncertainty_markers" in err
print()

print("=== classify_bias_verdict ===")
samples = [
    ("Bias check: No major solution bias; the draft is already framed as an uncertainty.", False, "pass"),
    ("Bias check: No major solution bias detected.", True, "pass"),
    ("Bias check: The draft implies an intervention ('what can we do') rather than an uncertainty; rewrite focuses on drivers/conditions.", True, "fail"),
    ("Bias check: The draft proposes launching a new feature instead of asking what is unknown.", False, "fail"),
    ("Bias check: Some ambiguous response.", True, "fail"),
    ("Bias check: Some ambiguous response.", False, "pass"),
    ("Bias check: ", True, "fail"),
    ("Bias check: No major solution bias, but the draft mentions launching a new feature.", False, "fail"),
    ("Bias check: The draft passes overall, though it names a pricing change as the fix.", True, "fail"),
    ("Bias check: No major solution bias detected; nothing to revise.", True, "pass"),
    ("Bias check: No major solution bias; no feature or tactic prescribed.", True, "pass"),
    ("Bias check: Passes; the draft does not propose a launch or campaign.", False, "pass"),
    ("Bias check: No major solution bias, without any specific intervention named.", True, "pass"),
    ("Bias check: No major bias overall, but the draft does name a launch as the fix.", False, "fail"),
]
for line, weak, expected in samples:
    got = classify_bias_verdict(line, weak)
    status = "OK" if got == expected else "FAIL"
    print(f"[{status}] expected={expected} got={got} | {line[:80]}")

print()
print("=== enforce_step1_format BIAS_CHECK scenarios ===")

cases = [
    ("S1 BIAS_CHECK pass with both weak -> Save checkpoint, NO Tip",
     "Bias check: No major solution bias; the draft is already framed as an uncertainty.",
     {"action": "bias_check", "any_weak": True, "tip_when_weak": True, "rewrite_requested": False, "next_step": "Revise again", "tip": "Some tip text."}),
    ("S2 BIAS_CHECK fail names launch -> Revise again, Tip allowed",
     "Bias check: The draft proposes launching a new pricing tier instead of an uncertainty.",
     {"action": "bias_check", "any_weak": True, "tip_when_weak": True, "rewrite_requested": False, "next_step": "Revise again", "tip": "Sharpen the uncertainty."}),
    ("S3 BIAS_CHECK pass with both strong -> Save checkpoint, NO Tip",
     "Bias check: No major solution bias.",
     {"action": "bias_check", "any_weak": False, "tip_when_weak": False, "rewrite_requested": False, "next_step": "Save checkpoint", "tip": "x"}),
    ("S4 BIAS_CHECK pass + rewrite_requested -> Save checkpoint, Tip ALLOWED",
     "Bias check: No major solution bias; framed as uncertainty.",
     {"action": "bias_check", "any_weak": True, "tip_when_weak": True, "rewrite_requested": True, "next_step": "Revise again", "tip": "Try BP-1."}),
    ("S5 REWRITE_PROBLEM unaffected by bias logic",
     "Rewrite (Problem): We don't yet understand churn drivers.\nNext step: leak\nTip: leak",
     {"action": "rewrite_problem", "any_weak": True, "tip_when_weak": True, "rewrite_requested": False, "next_step": "Revise again", "tip": "Try BP."}),
    ("S6 free-form unaffected by bias logic",
     "Rewrite (Problem): a\nRewrite (Decision): b\nBias check: No major solution bias.\nNext step: WRONG",
     {"action": "full", "any_weak": True, "tip_when_weak": True, "rewrite_requested": False, "next_step": "Revise again", "tip": "T."}),
    ("S7 backward-compat: legacy enforce dict (no rewrite_requested), bias_check pass weak",
     "Bias check: No major solution bias.",
     {"action": "bias_check", "any_weak": True, "tip_when_weak": True, "next_step": "Revise again", "tip": "should be suppressed"}),
]
for name, reply, enforce in cases:
    print(f"--- {name} ---")
    print(enforce_step1_format(reply, enforce))
    print()
