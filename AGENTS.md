# AGENTS

## Project Scope: bsky-noise

### Goal
Local-only instrumentation and discovery for Bluesky feed/account dynamics.

### Constraints
- Local-only storage and outputs (SQLite, JSON, HTML, CSV).
- No account actions (no follow/unfollow/mute/block automation).
- No posting automation.
- CLI remains cron-safe and deterministic.
- UI handles interactive filtering and shortlist exploration.

### CLI Responsibilities
- `sync`: ingest graph/feed data.
- `compute`: materialize derived summary JSON from local data.
- `report`: render static artifacts (`summary.json`, `index.html`, optional CSV).
- `candidates refresh/score/shortlist`: discovery data and shortlist text only.

### UI Responsibilities
- Interactive filtering/sorting/toggles (mode, recency, repost tolerance, thresholds).
- Followers-not-followed exploration.
- Local shortlist management and export.

### Non-goals
- Poll generation/posting.
- Any write action to social graph.
- Recommendation black-boxing without explainability.

### Naming
Use neutral discovery language:
- Orbit shortlist
- Candidate explorer
- Interaction map

Avoid action-implying names like "follow-back pipeline" and "poll picker".
