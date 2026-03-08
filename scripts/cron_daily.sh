#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${BSKY_REPO_DIR:-$HOME/git/atproto-stats}"
DB_PATH="${BSKY_DB_PATH:-$REPO_DIR/data/bsky_noise.db}"
LOCK_PATH="${BSKY_LOCK_PATH:-$REPO_DIR/data/run.lock}"
LOG_PATH="${BSKY_LOG_PATH:-$REPO_DIR/output/cron.log}"

cd "$REPO_DIR"
mkdir -p "$REPO_DIR/output" "$REPO_DIR/data"

BSKY_DB_PATH="$DB_PATH" BSKY_LOCK_PATH="$LOCK_PATH" .venv/bin/python -m bsky_noise.cli sync \
  --window 7 --window 30 --use-appview --concurrency 1 --auto-degraded >> "$LOG_PATH" 2>&1

BSKY_DB_PATH="$DB_PATH" .venv/bin/python -m bsky_noise.cli reciprocity snapshot \
  --use-appview --auto-degraded >> "$LOG_PATH" 2>&1

BSKY_DB_PATH="$DB_PATH" .venv/bin/python -m bsky_noise.cli label-audit poll \
  --auto-degraded >> "$LOG_PATH" 2>&1

BSKY_DB_PATH="$DB_PATH" BSKY_LOCK_PATH="$LOCK_PATH" .venv/bin/python -m bsky_noise.cli compute \
  --window 7 --window 30 --compare-prior --summary-output "$REPO_DIR/output/summary.json" >> "$LOG_PATH" 2>&1

BSKY_DB_PATH="$DB_PATH" BSKY_LOCK_PATH="$LOCK_PATH" .venv/bin/python -m bsky_noise.cli report \
  --window 7 --window 30 --compare-prior --what-if-mute 5 --what-if-mute 10 \
  --export-csv "$REPO_DIR/output/report.csv" >> "$LOG_PATH" 2>&1
