# bsky-noise

Local-only ATProto/Bsky follow noise analyzer.

## Why

`bsky-noise` is built to answer one practical question:

- which followed accounts dominate your feed volume right now
- how that changed vs the prior period
- what volume reduction you get by muting top-N noisy accounts

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Authenticate

```bash
# prompt (recommended)
bsky_noise auth --handle you.bsky.social

# or via env var (handle from BSKY_HANDLE or BSKY_USERNAME)
BSKY_HANDLE=you.bsky.social bsky_noise auth

# or env var (avoid CLI args)
BSKY_APP_PASSWORD='xxxx-xxxx-xxxx-xxxx' bsky_noise auth --handle you.bsky.social

# or stdin (avoid shell history)
printf '%s' 'xxxx-xxxx-xxxx-xxxx' | bsky_noise auth --handle you.bsky.social --app-password -
```

If you prefer a `.env`, use `.env.example` as a template (keep `.env` uncommitted). The CLI will read `.env` from the current working directory.

## Sync

```bash
bsky_noise sync --window 30 --window 90
```

Dry-run to estimate request budget:

```bash
bsky_noise sync --window 7 --dry-run
```

Verbose sync (log request status + backoff hints):

```bash
bsky_noise sync --window 7 --concurrency 1 --verbose
```

Use the public AppView (read-only) to reduce PDS load:

```bash
bsky_noise sync --window 7 --use-appview
```

Degraded mode (slower, more retries, higher timeouts):

```bash
bsky_noise sync --window 7 --degraded --concurrency 2 --verbose
```

Auto-degraded (switches to degraded behavior after 429/5xx/timeouts):

```bash
bsky_noise sync --window 7 --auto-degraded --verbose
```

## Candidates (Follow-Back Roulette)

Build an orbit candidate pool from followers you do not currently follow:

```bash
bsky_noise candidates refresh
```

Score candidates (interaction-first default):

```bash
bsky_noise candidates score --mode interaction --sample 75 --daily-budget 200
```

Pick `k` shortlist candidates and print text:

```bash
bsky_noise candidates shortlist --mode interaction --k 4
```

Render shortlist in selected text format:

```bash
bsky_noise candidates render-shortlist --mode interaction --k 4 --format pollblue
```

## Compute

Materialize derived summary JSON from local data (cron-safe):

```bash
bsky_noise compute --window 7 --compare-prior --summary-output output/summary.json
```

## Report

```bash
bsky_noise report
```

Action-focused report options:

```bash
bsky_noise report --window 7 --compare-prior --what-if-mute 5 --what-if-mute 10
```

Export CSV and include a watchlist section:

```bash
bsky_noise report --window 7 --compare-prior --export-csv output/report.csv --watchlist watchlist.txt
```

`watchlist.txt` format (one DID or handle per line, `#` comments allowed):

```text
# handles or dids
someaccount.bsky.social
did:plc:xxxxxxxxxxxxxxxxxxxxxxx
```

Outputs:
- `output/summary.json`
- `output/index.html`

## Docker

Build and run via compose:

```bash
docker compose run --rm bsky-noise auth --handle you.bsky.social
docker compose run --rm bsky-noise sync --window 7 --dry-run
docker compose run --rm bsky-noise sync --window 30 --window 90
docker compose run --rm bsky-noise report --output /output
```

Set `BSKY_APP_PASSWORD` in your shell or pass via stdin. For example:

```bash
printf '%s' 'xxxx-xxxx-xxxx-xxxx' | docker compose run --rm bsky-noise auth --handle you.bsky.social --app-password -
```

## Notes

- Classification rules:
  - Repost if feed item includes `reason` of type `app.bsky.feed.reasonRepost`.
  - Reply if post record has a `reply` field.
  - Otherwise post.
- Quote posts are treated as posts (unless they are replies).
- Uses SQLite in `~/.config/bsky_noise/bsky_noise.db` (override with `BSKY_DB_PATH`).
- If you set `XDG_CONFIG_HOME`, config lives at `$XDG_CONFIG_HOME/bsky_noise/`.

## Project Docs

- [CONTRIBUTING.md](CONTRIBUTING.md)
- [DESIGN_NOTES.md](DESIGN_NOTES.md)
- [ROADMAP.md](ROADMAP.md)
- [PROVENANCE.md](PROVENANCE.md)

## License

Licensed under either:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.

## Non-Goals

- No follow/unfollow/mute/block automation.
- No posting automation.
- No account action engine.
