from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable

from .db import get_latest_sync_run, init_db, write_samples


@dataclass(frozen=True)
class Metrics:
    posts: int
    replies: int
    reposts: int

    @property
    def total(self) -> int:
        return self.posts + self.replies + self.reposts


def _window_bounds(window_days: int) -> tuple[str, str, str]:
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=window_days)
    prior_start = now - timedelta(days=window_days * 2)
    return now.isoformat(), start.isoformat(), prior_start.isoformat()


def _counts_between(conn, did: str, start: str, end: str) -> Metrics:
    cur = conn.execute(
        """
        SELECT
          SUM(CASE WHEN kind = 'post' THEN 1 ELSE 0 END) AS posts,
          SUM(CASE WHEN kind = 'reply' THEN 1 ELSE 0 END) AS replies,
          SUM(CASE WHEN kind = 'repost' THEN 1 ELSE 0 END) AS reposts
        FROM events
        WHERE did = ? AND created_at >= ? AND created_at < ?
        """,
        (did, start, end),
    )
    row = cur.fetchone()
    return Metrics(
        posts=int(row["posts"] or 0),
        replies=int(row["replies"] or 0),
        reposts=int(row["reposts"] or 0),
    )


def _rates(metrics: Metrics, window_days: int) -> dict[str, float]:
    denom = max(window_days, 1)
    posts_per_day = metrics.posts / denom
    replies_per_day = metrics.replies / denom
    reposts_per_day = metrics.reposts / denom
    reply_ratio = metrics.replies / max(metrics.posts, 1)
    interaction_ratio = (metrics.replies + metrics.reposts) / max(metrics.posts, 1)
    return {
        "posts_per_day": posts_per_day,
        "replies_per_day": replies_per_day,
        "reposts_per_day": reposts_per_day,
        "reply_ratio": reply_ratio,
        "interaction_ratio": interaction_ratio,
    }


def _noise_score(metrics: Metrics, weights: dict[str, float]) -> float:
    return (
        metrics.posts * weights["posts"]
        + metrics.replies * weights["replies"]
        + metrics.reposts * weights["reposts"]
    )


def _materialize_day_counts(conn, *, weights: dict[str, float], lookback_days: int) -> None:
    start = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date().isoformat()
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("DELETE FROM did_day_counts WHERE day >= ?", (start,))
    cur = conn.execute(
        """
        SELECT
          did,
          substr(created_at, 1, 10) AS day,
          SUM(CASE WHEN kind = 'post' THEN 1 ELSE 0 END) AS posts,
          SUM(CASE WHEN kind = 'reply' THEN 1 ELSE 0 END) AS replies,
          SUM(CASE WHEN kind = 'repost' THEN 1 ELSE 0 END) AS reposts
        FROM events
        WHERE created_at >= ?
        GROUP BY did, day
        """,
        (start,),
    )
    rows = []
    for row in cur.fetchall():
        posts = int(row["posts"] or 0)
        replies = int(row["replies"] or 0)
        reposts = int(row["reposts"] or 0)
        noise_score_day = (
            posts * weights["posts"]
            + replies * weights["replies"]
            + reposts * weights["reposts"]
        )
        rows.append((row["did"], row["day"], posts, replies, reposts, noise_score_day))
    if rows:
        conn.executemany(
            """
            INSERT INTO did_day_counts (did, day, posts, replies, reposts, noise_score_day)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(did, day) DO UPDATE SET
              posts = excluded.posts,
              replies = excluded.replies,
              reposts = excluded.reposts,
              noise_score_day = excluded.noise_score_day
            """,
            rows,
        )
    conn.commit()


def _series_for_window(conn, did: str, window_days: int) -> list[float]:
    start = (datetime.now(timezone.utc) - timedelta(days=window_days)).date().isoformat()
    cur = conn.execute(
        """
        SELECT noise_score_day
        FROM did_day_counts
        WHERE did = ? AND day >= ?
        ORDER BY day
        """,
        (did, start),
    )
    return [float(r["noise_score_day"] or 0.0) for r in cur.fetchall()]


def _burst_spike(series: list[float]) -> tuple[float, float]:
    if not series:
        return 0.0, 0.0
    peak = max(series)
    med = median(series)
    burst = peak / med if med > 0 else (peak if peak > 0 else 0.0)
    today = series[-1]
    avg = sum(series) / len(series)
    spike = today / avg if avg > 0 else 0.0
    return burst, spike


def _health_status(run_health: dict[str, Any] | None) -> str:
    if not run_health:
        return "partial"
    if run_health["events_fetched"] == 0 and run_health["follows_count"] > 0:
        return "partial"
    if (
        run_health["auto_degraded_tripped"]
        or run_health["rate_limit_count"] > 0
        or run_health["server_error_count"] > 0
        or run_health["timeout_count"] > 0
        or run_health["request_error_count"] > 0
    ):
        return "degraded"
    return "good"


def _reason_label(window_data: dict[str, Any]) -> str:
    counts = window_data["counts"]
    prior = window_data["prior_counts"]
    post_delta = counts["posts"] - prior["posts"]
    reply_delta = counts["replies"] - prior["replies"]
    repost_delta = counts["reposts"] - prior["reposts"]
    rates = window_data["rates"]

    if reply_delta > post_delta and rates["reply_ratio"] >= 1.2:
        return "reply_guy_mode"
    if (reply_delta + repost_delta) > post_delta and rates["interaction_ratio"] >= 1.5:
        return "interaction_heavy"
    if post_delta > 0 and post_delta >= reply_delta and post_delta >= repost_delta:
        return "post_surge"
    return "mixed"


def _build_rankings(
    accounts: list[dict[str, Any]],
    window_key: str,
    what_if: list[int],
    watchlist: set[str],
) -> dict[str, Any]:
    ranked = sorted(
        (
            {
                "did": a["did"],
                "handle": a["handle"],
                "display_name": a["display_name"],
                "score": a["windows"][window_key]["noise_score"],
                "delta_score": a["windows"][window_key]["delta_score"],
                "share": a["windows"][window_key]["attention_share"],
                "reason_label": _reason_label(a["windows"][window_key]),
            }
            for a in accounts
            if window_key in a["windows"]
        ),
        key=lambda x: x["score"],
        reverse=True,
    )
    by_delta = sorted(ranked, key=lambda x: x["delta_score"], reverse=True)

    what_if_reduction: dict[str, float] = {}
    total_score = sum(x["score"] for x in ranked) or 1.0
    for n in sorted(set(what_if)):
        top_slice = ranked[:n]
        drop = sum(x["score"] for x in top_slice)
        what_if_reduction[str(n)] = drop / total_score

    watch_rows = []
    if watchlist:
        matched = [
            row
            for row in ranked
            if row["did"].lower() in watchlist
            or (row.get("handle") or "").lstrip("@").lower() in watchlist
        ]
        watch_rows = sorted(matched, key=lambda x: x["score"], reverse=True)

    return {
        "top_by_score": ranked[:25],
        "top_by_delta": by_delta[:25],
        "what_if_mute": what_if_reduction,
        "watchlist": watch_rows,
    }


def _streaks(active_days: list[str], *, today: datetime) -> tuple[int, int]:
    if not active_days:
        return 0, 0
    active = sorted({datetime.fromisoformat(d).date() for d in active_days})
    active_set = set(active)

    longest = 0
    run = 0
    prev = None
    for day in active:
        if prev is not None and day == prev + timedelta(days=1):
            run += 1
        else:
            run = 1
        longest = max(longest, run)
        prev = day

    current = 0
    cursor = today.date()
    while cursor in active_set:
        current += 1
        cursor -= timedelta(days=1)
    return current, longest


def _self_daily_series(conn, did: str, window_days: int) -> list[dict[str, Any]]:
    start = (datetime.now(timezone.utc) - timedelta(days=window_days - 1)).date()
    start_iso = start.isoformat()
    cur = conn.execute(
        """
        SELECT day, posts, replies, reposts, noise_score_day
        FROM did_day_counts
        WHERE did = ? AND day >= ?
        ORDER BY day
        """,
        (did, start_iso),
    )
    by_day = {
        row["day"]: {
            "posts": int(row["posts"] or 0),
            "replies": int(row["replies"] or 0),
            "reposts": int(row["reposts"] or 0),
            "noise_score": float(row["noise_score_day"] or 0.0),
        }
        for row in cur.fetchall()
    }
    series: list[dict[str, Any]] = []
    for offset in range(window_days):
        day = (start + timedelta(days=offset)).isoformat()
        counts = by_day.get(day, {"posts": 0, "replies": 0, "reposts": 0, "noise_score": 0.0})
        series.append(
            {
                "day": day,
                "posts": counts["posts"],
                "replies": counts["replies"],
                "reposts": counts["reposts"],
                "noise_score": counts["noise_score"],
            }
        )
    return series


def _self_hourly_and_weekday(conn, did: str, start_iso: str, end_iso: str) -> tuple[list[int], list[int]]:
    hourly = [0] * 24
    weekdays = [0] * 7
    cur = conn.execute(
        """
        SELECT
          CAST(strftime('%H', created_at) AS INTEGER) AS hour_utc,
          CAST(strftime('%w', created_at) AS INTEGER) AS weekday_sun0,
          COUNT(*) AS c
        FROM events
        WHERE did = ? AND created_at >= ? AND created_at < ?
        GROUP BY hour_utc, weekday_sun0
        """,
        (did, start_iso, end_iso),
    )
    for row in cur.fetchall():
        hour = int(row["hour_utc"] or 0)
        weekday = int(row["weekday_sun0"] or 0)
        count = int(row["c"] or 0)
        if 0 <= hour <= 23:
            hourly[hour] += count
        # Convert SQLite Sunday=0..Saturday=6 to Monday=0..Sunday=6.
        weekdays[(weekday - 1) % 7] += count
    return hourly, weekdays


def _build_self_account(
    conn,
    *,
    actor_did: str,
    windows: list[int],
    weights: dict[str, float],
    accounts_by_did: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    base = accounts_by_did.get(actor_did, {})
    entry: dict[str, Any] = {
        "did": actor_did,
        "handle": base.get("handle"),
        "display_name": base.get("display_name"),
        "windows": {},
    }
    for window in windows:
        now_iso, start_iso, _ = _window_bounds(window)
        metrics = _counts_between(conn, actor_did, start_iso, now_iso)
        rates = _rates(metrics, window)
        score = _noise_score(metrics, weights)
        series = _self_daily_series(conn, actor_did, window)
        active_days = [row["day"] for row in series if row["posts"] + row["replies"] + row["reposts"] > 0]
        current_streak, longest_streak = _streaks(active_days, today=now)
        hourly, weekdays = _self_hourly_and_weekday(conn, actor_did, start_iso, now_iso)
        total = max(metrics.total, 1)
        entry["windows"][str(window)] = {
            "counts": metrics.__dict__,
            "rates": rates,
            "noise_score": score,
            "reply_share": metrics.replies / total,
            "repost_share": metrics.reposts / total,
            "active_days": len(active_days),
            "current_streak": current_streak,
            "longest_streak": longest_streak,
            "daily": series,
            "hourly_utc": hourly,
            "weekday_mon0": weekdays,
        }
    return entry


def build_summary(
    conn,
    windows: Iterable[int],
    weights: dict[str, float],
    *,
    compare_prior: bool,
    what_if_mute: list[int],
    watchlist: set[str] | None = None,
) -> dict[str, Any]:
    init_db(conn)
    windows = sorted(set(windows))
    max_window = max(windows) if windows else 30
    _materialize_day_counts(conn, weights=weights, lookback_days=max_window * 2 + 7)
    accounts = conn.execute(
        "SELECT did, handle, display_name FROM accounts ORDER BY handle"
    ).fetchall()

    as_of_date = datetime.now(timezone.utc).date().isoformat()
    summary: dict[str, Any] = {
        "as_of_date": as_of_date,
        "windows": windows,
        "weights": weights,
        "accounts": [],
        "rankings": {},
        "run_health": None,
        "self_account": None,
    }

    actor_did: str | None = None
    latest_run = get_latest_sync_run(conn)
    if latest_run:
        actor_did = latest_run["actor"]
        summary["run_health"] = {
            "finished_at": latest_run["finished_at"],
            "follows_count": latest_run["follows_count"],
            "events_fetched": latest_run["events_fetched"],
            "events_inserted": latest_run["events_inserted"],
            "request_count": latest_run["request_count"],
            "retry_count": latest_run["retry_count"],
            "rate_limit_count": latest_run["rate_limit_count"],
            "server_error_count": latest_run["server_error_count"],
            "timeout_count": latest_run["timeout_count"],
            "request_error_count": latest_run["request_error_count"],
            "auto_degraded_tripped": bool(latest_run["auto_degraded_tripped"]),
        }
    summary["run_health_status"] = _health_status(summary["run_health"])

    # First pass computes per-account per-window raw metrics.
    for row in accounts:
        did = row["did"]
        account_entry = {
            "did": did,
            "handle": row["handle"],
            "display_name": row["display_name"],
            "windows": {},
        }
        for window in windows:
            now_iso, start_iso, prior_start_iso = _window_bounds(window)
            current = _counts_between(conn, did, start_iso, now_iso)
            prior = _counts_between(conn, did, prior_start_iso, start_iso)
            rates = _rates(current, window)
            score = _noise_score(current, weights)
            prior_score = _noise_score(prior, weights)
            delta_score = score - prior_score if compare_prior else 0.0
            series = _series_for_window(conn, did, window)
            burst_score, spike_today = _burst_spike(series)
            account_entry["windows"][str(window)] = {
                "counts": current.__dict__,
                "prior_counts": prior.__dict__,
                "rates": rates,
                "noise_score": score,
                "prior_noise_score": prior_score,
                "delta_score": delta_score,
                "attention_share": 0.0,
                "burst_score": burst_score,
                "spike_today_ratio": spike_today,
            }
            write_samples(
                conn,
                did=did,
                window_days=window,
                as_of_date=as_of_date,
                posts=current.posts,
                replies=current.replies,
                reposts=current.reposts,
                threads=current.total,
            )
        summary["accounts"].append(account_entry)

    # Second pass computes attention share and window-level rankings.
    for window in windows:
        key = str(window)
        total_score = sum(
            a["windows"][key]["noise_score"] for a in summary["accounts"] if key in a["windows"]
        )
        denom = total_score or 1.0
        for account in summary["accounts"]:
            if key not in account["windows"]:
                continue
            account["windows"][key]["attention_share"] = account["windows"][key]["noise_score"] / denom
        summary["rankings"][key] = _build_rankings(
            summary["accounts"], key, what_if_mute, watchlist or set()
        )

    if actor_did:
        accounts_by_did = {a["did"]: a for a in summary["accounts"]}
        summary["self_account"] = _build_self_account(
            conn,
            actor_did=actor_did,
            windows=windows,
            weights=weights,
            accounts_by_did=accounts_by_did,
        )

    return summary


def _write_csv_export(summary: dict[str, Any], csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "window_days",
        "did",
        "handle",
        "display_name",
        "noise_score",
        "attention_share",
        "delta_score",
        "reason_label",
        "burst_score",
        "spike_today_ratio",
        "posts",
        "replies",
        "reposts",
        "posts_per_day",
        "replies_per_day",
        "reposts_per_day",
        "reply_ratio",
        "interaction_ratio",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for account in summary["accounts"]:
            for window_key, window in account["windows"].items():
                writer.writerow(
                    {
                        "window_days": window_key,
                        "did": account["did"],
                        "handle": account["handle"] or "",
                        "display_name": account["display_name"] or "",
                        "noise_score": f"{window['noise_score']:.4f}",
                        "attention_share": f"{window['attention_share']:.6f}",
                        "delta_score": f"{window['delta_score']:.4f}",
                        "reason_label": _reason_label(window),
                        "burst_score": f"{window['burst_score']:.4f}",
                        "spike_today_ratio": f"{window['spike_today_ratio']:.4f}",
                        "posts": window["counts"]["posts"],
                        "replies": window["counts"]["replies"],
                        "reposts": window["counts"]["reposts"],
                        "posts_per_day": f"{window['rates']['posts_per_day']:.6f}",
                        "replies_per_day": f"{window['rates']['replies_per_day']:.6f}",
                        "reposts_per_day": f"{window['rates']['reposts_per_day']:.6f}",
                        "reply_ratio": f"{window['rates']['reply_ratio']:.6f}",
                        "interaction_ratio": f"{window['rates']['interaction_ratio']:.6f}",
                    }
                )


def _render_html(summary: dict[str, Any]) -> str:
    data_json = json.dumps(summary)
    html = """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>bsky-noise report</title>
  <style>
    :root {
      --bg: #f6f1e8;
      --ink: #2f2a24;
      --muted: #6e6257;
      --table-bg: #fffdf8;
      --accent: #c04e2a;
      --panel-bg: #fffaf2;
      --panel-border: #e8d9c7;
      --input-bg: #fff;
      --input-border: #d7c9b8;
      --th-bg: #f2e7d8;
      --row-border: #efe5d7;
      --badge-bg: #f9d7c3;
      --badge-ink: #7a3b28;
      --shadow: 0 8px 22px rgba(0,0,0,0.06);
    }
    :root[data-theme="dark"] {
      --bg: #0f1318;
      --ink: #e5edf7;
      --muted: #a3b4c8;
      --table-bg: #1a232e;
      --accent: #ff9a5c;
      --panel-bg: #16202a;
      --panel-border: #2a3a4a;
      --input-bg: #17212b;
      --input-border: #344759;
      --th-bg: #223142;
      --row-border: #2a3949;
      --badge-bg: #2d445a;
      --badge-ink: #d8e8f9;
      --shadow: 0 10px 24px rgba(0,0,0,0.35);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background: radial-gradient(circle at top right, #fde9d2 0%, #f6f1e8 45%, #efe7db 100%);
      color: var(--ink);
    }
    :root[data-theme="dark"] body {
      background: radial-gradient(circle at top right, #22374c 0%, #0f1318 48%, #0b1015 100%);
    }
    body.compact th, body.compact td { padding: 6px 8px; font-size: 12px; }
    main { max-width: 1280px; margin: 0 auto; padding-bottom: 20px; }
    header { padding: 24px; }
    h1 { margin: 0 0 6px 0; font-size: 30px; letter-spacing: .2px; }
    h2 { margin: 0 0 8px 0; font-size: 17px; }
    .subtitle { color: var(--muted); font-size: 14px; }
    .controls {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px;
      padding: 0 24px 12px 24px;
      align-items: end;
    }
    .control label { display: block; font-size: 12px; color: var(--muted); margin-bottom: 4px; }
    input, select, button {
      width: 100%;
      border: 1px solid var(--input-border);
      border-radius: 8px;
      padding: 8px 10px;
      font-size: 14px;
      background: var(--input-bg);
      color: var(--ink);
    }
    button { cursor: pointer; }
    .panel {
      margin: 0 24px 12px 24px;
      background: var(--panel-bg);
      border: 1px solid var(--panel-border);
      border-radius: 12px;
      padding: 12px;
      box-shadow: var(--shadow);
    }
    .status-pill {
      display: inline-block;
      margin-left: 8px;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: .03em;
      text-transform: uppercase;
    }
    .status-good { background: #2f7d32; color: #fff; }
    .status-degraded { background: #b35a00; color: #fff; }
    .status-partial { background: #7a7a7a; color: #fff; }
    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 10px;
    }
    .card {
      border: 1px solid var(--panel-border);
      border-radius: 10px;
      padding: 10px;
      background: color-mix(in oklab, var(--panel-bg) 92%, var(--accent) 8%);
    }
    .card .k { color: var(--muted); font-size: 12px; }
    .card .v { font-size: 22px; font-weight: 700; margin-top: 4px; }
    .grid-2 {
      margin: 0 24px 12px 24px;
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
    }
    @media (max-width: 900px) { .grid-2 { grid-template-columns: 1fr; } }
    .hidden { display: none !important; }
    table {
      width: 100%;
      border-collapse: collapse;
      background: var(--table-bg);
      border-radius: 12px;
      overflow: hidden;
      box-shadow: var(--shadow);
    }
    th, td {
      padding: 8px 10px;
      border-bottom: 1px solid var(--row-border);
      text-align: left;
      font-size: 13px;
    }
    tbody tr:nth-child(odd) td { background: color-mix(in oklab, var(--table-bg) 96%, var(--panel-border) 4%); }
    th {
      background: var(--th-bg);
      position: sticky;
      top: 0;
      z-index: 2;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .04em;
    }
    .num { text-align: right; font-variant-numeric: tabular-nums; }
    .muted { color: var(--muted); }
    details { margin: 0 24px 12px 24px; }
    summary { cursor: pointer; font-weight: 600; }
    .note { font-size: 12px; color: var(--muted); margin-top: 8px; }
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Follow Noise Report</h1>
      <div class=\"subtitle\">As of __AS_OF_DATE__. Weights: posts=__W_POSTS__, replies=__W_REPLIES__, reposts=__W_REPOSTS__.</div>
    </header>

    <div class=\"controls\">
      <div class=\"control\"><label>Window</label><select id=\"windowSelect\"></select></div>
      <div class=\"control\"><label>Search</label><input id=\"searchBox\" placeholder=\"handle or display name\" /></div>
      <div class=\"control\"><label>Show</label><select id=\"rowLimit\"><option value=\"25\">Top 25</option><option value=\"50\">Top 50</option><option value=\"100\" selected>Top 100</option><option value=\"999999\">All</option></select></div>
      <div class=\"control\"><label>Density</label><select id=\"densitySelect\"><option value=\"comfortable\" selected>Comfortable</option><option value=\"compact\">Compact</option></select></div>
      <div class=\"control\"><label>Rates Columns</label><select id=\"ratesSelect\"><option value=\"hide\" selected>Hide</option><option value=\"show\">Show</option></select></div>
      <div class=\"control\"><label>Focus Mode</label><select id=\"focusSelect\"><option value=\"off\" selected>Off</option><option value=\"on\">On</option></select></div>
      <div class=\"control\"><label>Theme</label><button id=\"themeToggle\" type=\"button\">Dark mode</button></div>
    </div>

    <div id=\"overview\" class=\"panel\"></div>
    <div id=\"selfPanel\" class=\"panel\"></div>
    <div id=\"health\" class=\"panel\"></div>
    <div id=\"watchlistPanel\" class=\"panel hidden\"></div>

    <div id=\"actionGrid\" class=\"grid-2\">
      <section>
        <h2>Top by Score</h2>
        <table id=\"topByScore\"></table>
      </section>
      <section id=\"deltaSection\">
        <h2>Newly Noisy</h2>
        <table id=\"topByDelta\"></table>
      </section>
    </div>

    <details id=\"detailsSection\" open>
      <summary>Detailed Table</summary>
      <div class=\"note\">Tip: keep rates hidden for quick triage, show rates when comparing posting style.</div>
      <table id=\"fullTable\"></table>
    </details>
  </main>

  <script>
    const summary = __DATA_JSON__;
    const windowSelect = document.getElementById('windowSelect');
    const searchBox = document.getElementById('searchBox');
    const themeToggle = document.getElementById('themeToggle');
    const rowLimit = document.getElementById('rowLimit');
    const densitySelect = document.getElementById('densitySelect');
    const ratesSelect = document.getElementById('ratesSelect');
    const focusSelect = document.getElementById('focusSelect');
    const selfPanel = document.getElementById('selfPanel');
    const healthPanel = document.getElementById('health');
    const watchlistPanel = document.getElementById('watchlistPanel');
    const deltaSection = document.getElementById('deltaSection');
    const detailsSection = document.getElementById('detailsSection');

    function fmtPct(v) { return (v * 100).toFixed(2) + '%'; }
    function fmtNum(v) { return Number(v || 0).toFixed(1); }

    function applyTheme(theme) {
      document.documentElement.setAttribute('data-theme', theme);
      localStorage.setItem('bsky-noise-theme', theme);
      themeToggle.textContent = theme === 'dark' ? 'Light mode' : 'Dark mode';
    }

    function initTheme() {
      const stored = localStorage.getItem('bsky-noise-theme');
      if (stored === 'dark' || stored === 'light') return applyTheme(stored);
      const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
      applyTheme(prefersDark ? 'dark' : 'light');
    }

    function applyFocusMode(mode) {
      const on = mode === 'on';
      healthPanel.classList.toggle('hidden', on);
      watchlistPanel.classList.toggle('hidden', on || watchlistPanel.innerHTML === '');
      deltaSection.classList.toggle('hidden', on);
      detailsSection.classList.toggle('hidden', on);
      localStorage.setItem('bsky-noise-focus', mode);
    }

    function initFocusMode() {
      const saved = localStorage.getItem('bsky-noise-focus');
      const mode = saved === 'on' ? 'on' : 'off';
      focusSelect.value = mode;
      applyFocusMode(mode);
    }

    function renderHealth() {
      const health = summary.run_health;
      const el = document.getElementById('health');
      const status = summary.run_health_status || 'partial';
      const statusClass = `status-pill status-${status}`;
      if (!health) {
        el.innerHTML = `Run quality: no sync receipt found yet. <span class=\"${statusClass}\">${status}</span>`;
        return;
      }
      el.innerHTML = `
        <div>
          Run quality
          <span class=\"${statusClass}\">${status}</span>
        </div>
        <details>
          <summary>Show receipt details</summary>
          <div class=\"note\">
            fetched=${health.events_fetched} inserted=${health.events_inserted}
            · requests=${health.request_count} retries=${health.retry_count}
            · 429=${health.rate_limit_count} 5xx=${health.server_error_count}
            · timeouts=${health.timeout_count}
            · request_errors=${health.request_error_count}
            · degraded_tripped=${health.auto_degraded_tripped ? 'yes' : 'no'}
          </div>
        </details>
      `;
    }

    function renderTable(el, rows, cols) {
      const head = '<thead><tr>' + cols.map(c => `<th class=\"${c.num ? 'num' : ''}\">${c.label}</th>`).join('') + '</tr></thead>';
      const body = rows.map(r => '<tr>' + cols.map(c => {
        const value = r[c.key];
        const out = c.fmt ? c.fmt(value) : (value ?? '');
        return `<td class=\"${c.num ? 'num' : ''}\">${out}</td>`;
      }).join('') + '</tr>').join('');
      el.innerHTML = head + '<tbody>' + body + '</tbody>';
    }

    function windowRows(windowKey) {
      const term = searchBox.value.trim().toLowerCase();
      const rows = summary.accounts
        .filter(a => a.windows[windowKey])
        .map(a => {
          const w = a.windows[windowKey];
          return {
            did: a.did,
            handle: a.handle || a.did,
            display_name: a.display_name || '',
            score: w.noise_score,
            share: w.attention_share,
            delta_score: w.delta_score,
            burst_score: w.burst_score,
            spike_today_ratio: w.spike_today_ratio,
            posts: w.counts.posts,
            replies: w.counts.replies,
            reposts: w.counts.reposts,
            posts_per_day: w.rates.posts_per_day,
            replies_per_day: w.rates.replies_per_day,
            reposts_per_day: w.rates.reposts_per_day,
          };
        })
        .filter(r => !term || r.handle.toLowerCase().includes(term) || r.display_name.toLowerCase().includes(term))
        .sort((a, b) => b.score - a.score);
      return rows;
    }

    function renderOverview(windowKey, rows) {
      const ranking = summary.rankings[windowKey];
      const top = ranking.top_by_score[0];
      const whatIf = Object.entries(ranking.what_if_mute)
        .map(([n, pct]) => `Mute top ${n}: <b>${fmtPct(pct)}</b> less volume`)
        .join(' · ');
      const totalScore = rows.reduce((acc, r) => acc + r.score, 0);
      const totalEvents = rows.reduce((acc, r) => acc + r.posts + r.replies + r.reposts, 0);
      const el = document.getElementById('overview');
      el.innerHTML = `
        <div class=\"cards\">
          <div class=\"card\"><div class=\"k\">Accounts in view</div><div class=\"v\">${rows.length}</div></div>
          <div class=\"card\"><div class=\"k\">Total noise score</div><div class=\"v\">${fmtNum(totalScore)}</div></div>
          <div class=\"card\"><div class=\"k\">Total events</div><div class=\"v\">${totalEvents}</div></div>
          <div class=\"card\"><div class=\"k\">Top account share</div><div class=\"v\">${top ? fmtPct(top.share) : '0%'}</div></div>
        </div>
        <div class=\"note\">${whatIf}</div>
      `;
    }

    function renderSelf(windowKey) {
      const selfData = summary.self_account;
      if (!selfData || !selfData.windows || !selfData.windows[windowKey]) {
        selfPanel.innerHTML = '<b>My Account</b><div class=\"note\">No self-account data yet. Run a fresh sync to include your account feed.</div>';
        return;
      }
      const w = selfData.windows[windowKey];
      const handle = selfData.handle || selfData.did;
      const days = w.daily || [];
      const preview = days.slice(-14).map((d) => {
        const total = d.posts + d.replies + d.reposts;
        return `${d.day.slice(5)}:${total}`;
      }).join(' · ');
      const weekdayLabels = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
      const topDayIdx = w.weekday_mon0.indexOf(Math.max(...w.weekday_mon0));
      const topHourIdx = w.hourly_utc.indexOf(Math.max(...w.hourly_utc));
      selfPanel.innerHTML = `
        <div><b>My Account</b> · ${handle}</div>
        <div class=\"cards\" style=\"margin-top:8px;\">
          <div class=\"card\"><div class=\"k\">Posts/day</div><div class=\"v\">${w.rates.posts_per_day.toFixed(2)}</div></div>
          <div class=\"card\"><div class=\"k\">Replies/day</div><div class=\"v\">${w.rates.replies_per_day.toFixed(2)}</div></div>
          <div class=\"card\"><div class=\"k\">Repost share</div><div class=\"v\">${fmtPct(w.repost_share)}</div></div>
          <div class=\"card\"><div class=\"k\">Active days</div><div class=\"v\">${w.active_days}/${windowKey}</div></div>
          <div class=\"card\"><div class=\"k\">Current streak</div><div class=\"v\">${w.current_streak}d</div></div>
          <div class=\"card\"><div class=\"k\">Longest streak</div><div class=\"v\">${w.longest_streak}d</div></div>
        </div>
        <div class=\"note\">Recent daily totals: ${preview || 'no activity'}</div>
        <div class=\"note\">Most active weekday: ${weekdayLabels[topDayIdx] || 'n/a'} · Most active UTC hour: ${topHourIdx}:00</div>
      `;
    }

    function renderWindow(windowKey) {
      const limit = Number(rowLimit.value);
      const showRates = ratesSelect.value === 'show';
      const rows = windowRows(windowKey);
      const ranking = summary.rankings[windowKey];
      const watchRows = ranking.watchlist || [];

      renderOverview(windowKey, rows);
      renderSelf(windowKey);

      if (watchRows.length > 0) {
        const summaryLine = watchRows
          .slice(0, 8)
          .map(r => `${r.handle}: ${fmtNum(r.score)} (${r.reason_label})`)
          .join(' · ');
        watchlistPanel.classList.remove('hidden');
        watchlistPanel.innerHTML = `<b>Watchlist</b>: ${summaryLine}`;
      } else {
        watchlistPanel.classList.add('hidden');
        watchlistPanel.innerHTML = '';
      }

      renderTable(document.getElementById('topByScore'), ranking.top_by_score.slice(0, 15), [
        { key: 'handle', label: 'Handle' },
        { key: 'score', label: 'Score', num: true, fmt: fmtNum },
        { key: 'share', label: 'Share', num: true, fmt: fmtPct },
        { key: 'delta_score', label: 'Delta', num: true, fmt: fmtNum },
        { key: 'burst_score', label: 'Burst', num: true, fmt: fmtNum },
      ]);

      renderTable(document.getElementById('topByDelta'), ranking.top_by_delta.slice(0, 15), [
        { key: 'handle', label: 'Handle' },
        { key: 'delta_score', label: 'Delta', num: true, fmt: fmtNum },
        { key: 'reason_label', label: 'Reason' },
        { key: 'score', label: 'Score', num: true, fmt: fmtNum },
        { key: 'share', label: 'Share', num: true, fmt: fmtPct },
      ]);

      const detailCols = [
        { key: 'handle', label: 'Handle' },
        { key: 'display_name', label: 'Name' },
        { key: 'score', label: 'Score', num: true, fmt: fmtNum },
        { key: 'share', label: 'Share', num: true, fmt: fmtPct },
        { key: 'delta_score', label: 'Delta', num: true, fmt: fmtNum },
        { key: 'burst_score', label: 'Burst', num: true, fmt: fmtNum },
        { key: 'spike_today_ratio', label: 'Spike', num: true, fmt: fmtNum },
        { key: 'posts', label: 'Posts', num: true },
        { key: 'replies', label: 'Replies', num: true },
        { key: 'reposts', label: 'Reposts', num: true },
      ];
      if (showRates) {
        detailCols.push(
          { key: 'posts_per_day', label: 'Posts/day', num: true, fmt: v => Number(v).toFixed(2) },
          { key: 'replies_per_day', label: 'Replies/day', num: true, fmt: v => Number(v).toFixed(2) },
          { key: 'reposts_per_day', label: 'Reposts/day', num: true, fmt: v => Number(v).toFixed(2) },
        );
      }
      renderTable(document.getElementById('fullTable'), rows.slice(0, limit), detailCols);
      applyFocusMode(focusSelect.value);
    }

    function render() {
      renderHealth();
      const windows = summary.windows.map(String);
      windowSelect.innerHTML = windows.map(w => `<option value=\"${w}\">${w} days</option>`).join('');
      const current = windowSelect.value || windows[0];
      renderWindow(current);

      windowSelect.addEventListener('change', e => renderWindow(e.target.value));
      searchBox.addEventListener('input', () => renderWindow(windowSelect.value));
      rowLimit.addEventListener('change', () => renderWindow(windowSelect.value));
      ratesSelect.addEventListener('change', () => renderWindow(windowSelect.value));
      densitySelect.addEventListener('change', () => {
        document.body.classList.toggle('compact', densitySelect.value === 'compact');
      });
      focusSelect.addEventListener('change', () => applyFocusMode(focusSelect.value));
      themeToggle.addEventListener('click', () => {
        const currentTheme = document.documentElement.getAttribute('data-theme') || 'light';
        applyTheme(currentTheme === 'dark' ? 'light' : 'dark');
      });
    }

    initTheme();
    initFocusMode();
    render();
  </script>
</body>
</html>
"""
    return (
        html.replace("__DATA_JSON__", data_json)
        .replace("__AS_OF_DATE__", summary["as_of_date"])
        .replace("__W_POSTS__", str(summary["weights"]["posts"]))
        .replace("__W_REPLIES__", str(summary["weights"]["replies"]))
        .replace("__W_REPOSTS__", str(summary["weights"]["reposts"]))
    )


def write_report(
    conn,
    windows: Iterable[int],
    output_dir: Path,
    weights: dict[str, float],
    *,
    compare_prior: bool,
    what_if_mute: list[int],
    watchlist: set[str] | None = None,
    export_csv: Path | None = None,
) -> dict[str, Any]:
    summary = build_summary(
        conn,
        windows=windows,
        weights=weights,
        compare_prior=compare_prior,
        what_if_mute=what_if_mute,
        watchlist=watchlist,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    (output_dir / "index.html").write_text(_render_html(summary), encoding="utf-8")
    if export_csv is not None:
        _write_csv_export(summary, export_csv)
    return summary
