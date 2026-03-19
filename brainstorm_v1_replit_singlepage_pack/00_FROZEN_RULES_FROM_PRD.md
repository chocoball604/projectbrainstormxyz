# Frozen rules (must not change) — PRD v3.07

## Study types
- Synthetic Survey (quant)
- Synthetic IDI (qual)
- Synthetic Focus Group (qual)

## Budget ceilings (tokens per study)
- Survey: 100,000 tokens
- IDI: 150,000 tokens
- Focus Group: 300,000 tokens

## QA retries and follow-ups
- Max QA retries per study: 2
- Max follow-up rounds (IDI / Focus Group): 2
- Survey follow-ups: not supported

## Study states
- Draft
- In Progress
- QA-Blocked
- Terminated (System)
- Terminated (User)
- Completed

## Grounding priority
1) Admin-directed web sources (global)
2) Non-English/local sources
3) General web sources

## Confidence labels
Every insight must be labeled: Strong / Indicative / Exploratory.

## Agent roles
- Mark: talks to user, creates Research Brief, formats final report.
- Lisa: runs the study, creates first-pass memo.
- Ben: checks quality; can PASS / DOWNGRADE / FAIL; does not re-search.
