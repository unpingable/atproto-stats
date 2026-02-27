# Design Notes

## Scope

`bsky-noise` is a local-only analyzer for follows graph activity. It prioritizes:

- auditability (`summary.json` contains raw metrics and derived score fields)
- incremental sync behavior (cursor + event dedupe)
- resilience under flaky ATProto reads (retry, degraded modes)

## Current Model

- Core classes: `post`, `reply`, `repost`
- Score: weighted sum (`posts`, `replies`, `reposts`)
- Action metrics: attention share, prior-window delta, what-if mute reduction

## Tradeoffs

- Favor deterministic and explainable metrics over complex ML.
- Favor small dependency surface over framework-heavy architecture.
- Favor local artifacts (SQLite/HTML/JSON) over hosted dashboards.

## Non-Goals

- Social graph prediction
- Topic modeling / NLP pipelines
- Server-hosted multi-user workflows
