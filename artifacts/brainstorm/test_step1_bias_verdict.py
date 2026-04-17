import sys
import os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mark_reply_worker import enforce_step1_format, classify_bias_verdict

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
