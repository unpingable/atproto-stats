from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import median
from typing import Any

from rich.console import Console

from .atproto import XrpcClient
from .db import init_db

console = Console()

_POST_URI_RE = re.compile(r"^at://([^/]+)/app\.bsky\.feed\.post/(.+)")
_BSKY_APP_BASE = "https://bsky.app"

# Lag buckets in seconds
_RETRO_BUCKETS = [
    ("realtime", 600),       # < 10 minutes
    ("same_day", 86400),     # < 24 hours
    ("late", 604800),        # < 7 days
    ("necromancy", None),    # >= 7 days
]


def _lag_bucket(lag_s: float) -> str:
    for name, threshold in _RETRO_BUCKETS:
        if threshold is None or lag_s < threshold:
            return name
    return "necromancy"


def _parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def _ts_to_epoch(value: str) -> float:
    return _parse_ts(value).timestamp()


@dataclass(frozen=True)
class LabelRecord:
    src: str
    uri: str
    val: str
    cts: str
    neg: bool
    cid: str | None
    exp: str | None


async def poll_labels(
    conn, client: XrpcClient, actor_did: str, *, max_pages: int = 20
) -> int:
    """Poll com.atproto.label.queryLabels for labels on our content + account."""
    init_db(conn)
    now = datetime.now(timezone.utc).isoformat()

    # Get stored cursor
    row = conn.execute(
        "SELECT value FROM label_poll_meta WHERE key = 'cursor'",
    ).fetchone()
    cursor = row["value"] if row else None

    uri_patterns = [f"at://{actor_did}/*", actor_did]
    total_inserted = 0

    try:
        for _ in range(max_pages):
            data = await client.query_labels(
                uri_patterns=uri_patterns, cursor=cursor
            )
            labels = data.get("labels", [])
            if not labels:
                break

            for raw in labels:
                src = raw.get("src", "")
                uri = raw.get("uri", "")
                val = raw.get("val", "")
                cts = raw.get("cts", "")
                if not (src and uri and val and cts):
                    continue
                neg = 1 if raw.get("neg") else 0
                cid = raw.get("cid")
                exp = raw.get("exp")

                result = conn.execute(
                    """
                    INSERT INTO my_labels (src, uri, val, cts, neg, cid, exp, ingested_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(src, uri, val, cts) DO NOTHING
                    """,
                    (src, uri, val, cts, neg, cid, exp, now),
                )
                total_inserted += result.rowcount

            cursor = data.get("cursor")
            if cursor:
                conn.execute(
                    """
                    INSERT INTO label_poll_meta (key, value) VALUES ('cursor', ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    (cursor,),
                )
            conn.commit()
            if not cursor:
                break

        # Record success
        conn.execute(
            """
            INSERT INTO label_poll_meta (key, value) VALUES ('last_ok_at', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO label_poll_meta (key, value) VALUES ('last_err', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            ("",),
        )
        conn.commit()

    except Exception as exc:
        conn.execute(
            """
            INSERT INTO label_poll_meta (key, value) VALUES ('last_err', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (str(exc)[:500],),
        )
        conn.commit()
        raise

    console.print(f"Label poll: {total_inserted} new label events ingested")
    return total_inserted


def _is_post_uri(uri: str) -> bool:
    return bool(_POST_URI_RE.match(uri))


def _bsky_app_url(at_uri: str) -> str | None:
    """Convert at://did/app.bsky.feed.post/rkey to a bsky.app link."""
    m = _POST_URI_RE.match(at_uri)
    if not m:
        return None
    did, rkey = m.group(1), m.group(2)
    return f"{_BSKY_APP_BASE}/profile/{did}/post/{rkey}"


def build_audit(
    conn,
    actor_did: str,
    *,
    retro_threshold_s: float = 86400,
    window_days: int | None = None,
) -> dict[str, Any]:
    """Build the label audit report."""
    init_db(conn)

    # Load all labels
    if window_days:
        cutoff = datetime.now(timezone.utc)
        from datetime import timedelta
        cutoff = cutoff - timedelta(days=window_days)
        cutoff_iso = cutoff.isoformat()
        label_rows = conn.execute(
            "SELECT * FROM my_labels WHERE cts >= ? ORDER BY cts", (cutoff_iso,)
        ).fetchall()
    else:
        label_rows = conn.execute(
            "SELECT * FROM my_labels ORDER BY cts"
        ).fetchall()

    # Separate account labels vs post labels
    account_labels: list[dict[str, Any]] = []
    post_labels: list[dict[str, Any]] = []

    for row in label_rows:
        entry = {
            "src": row["src"],
            "uri": row["uri"],
            "val": row["val"],
            "cts": row["cts"],
            "neg": bool(row["neg"]),
        }
        if _is_post_uri(row["uri"]):
            post_labels.append(entry)
        else:
            account_labels.append(entry)

    # For post labels, compute lag against events table
    post_audit: list[dict[str, Any]] = []
    lags: list[float] = []
    lags_by_labeler: dict[str, list[float]] = {}
    lags_by_val: dict[str, list[float]] = {}
    bucket_counts: dict[str, int] = {"realtime": 0, "same_day": 0, "late": 0, "necromancy": 0}

    for label in post_labels:
        lag_s = None
        bucket = None
        post_created_at = None

        if not label["neg"]:
            # Look up post creation time from events table (skip for negations)
            post_row = conn.execute(
                "SELECT MIN(created_at) as created_at FROM events WHERE uri = ?",
                (label["uri"],),
            ).fetchone()

            if post_row and post_row["created_at"]:
                post_created_at = post_row["created_at"]
                try:
                    label_epoch = _ts_to_epoch(label["cts"])
                    post_epoch = _ts_to_epoch(post_created_at)
                    lag_s = label_epoch - post_epoch
                    if lag_s >= 0:
                        bucket = _lag_bucket(lag_s)
                        lags.append(lag_s)
                        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1

                        lags_by_labeler.setdefault(label["src"], []).append(lag_s)
                        lags_by_val.setdefault(label["val"], []).append(lag_s)
                except (ValueError, OSError):
                    pass

        post_audit.append({
            **label,
            "post_created_at": post_created_at,
            "lag_s": lag_s,
            "lag_bucket": bucket,
        })

    # Resolve labeler handles and bsky.app URLs for post_audit detail
    _handle_cache: dict[str, str | None] = {}
    for entry in post_audit:
        src = entry["src"]
        if src not in _handle_cache:
            row = conn.execute("SELECT handle FROM accounts WHERE did = ?", (src,)).fetchone()
            _handle_cache[src] = row["handle"] if row else None
        entry["src_handle"] = _handle_cache[src]
        entry["bsky_url"] = _bsky_app_url(entry["uri"])

    # Also enrich account_labels
    for entry in account_labels:
        src = entry["src"]
        if src not in _handle_cache:
            row = conn.execute("SELECT handle FROM accounts WHERE did = ?", (src,)).fetchone()
            _handle_cache[src] = row["handle"] if row else None
        entry["src_handle"] = _handle_cache[src]

    # Per-labeler stats
    labeler_stats: list[dict[str, Any]] = []
    for src, src_lags in sorted(lags_by_labeler.items()):
        sorted_lags = sorted(src_lags)
        n = len(sorted_lags)
        retro = sum(1 for l in sorted_lags if l >= retro_threshold_s)
        # Resolve labeler handle
        acct_row = conn.execute(
            "SELECT handle FROM accounts WHERE did = ?", (src,)
        ).fetchone()
        labeler_stats.append({
            "src": src,
            "handle": acct_row["handle"] if acct_row else None,
            "label_count": n,
            "median_lag_s": median(sorted_lags),
            "p90_lag_s": sorted_lags[min(n * 9 // 10, n - 1)],
            "retroactive_count": retro,
            "retroactive_rate": retro / max(n, 1),
        })

    # Per-value stats
    val_stats: list[dict[str, Any]] = []
    for val, val_lags in sorted(lags_by_val.items()):
        sorted_lags = sorted(val_lags)
        n = len(sorted_lags)
        retro = sum(1 for l in sorted_lags if l >= retro_threshold_s)
        val_stats.append({
            "val": val,
            "label_count": n,
            "median_lag_s": median(sorted_lags),
            "retroactive_count": retro,
            "retroactive_rate": retro / max(n, 1),
        })

    # Poll health
    last_ok = conn.execute(
        "SELECT value FROM label_poll_meta WHERE key = 'last_ok_at'"
    ).fetchone()
    last_err = conn.execute(
        "SELECT value FROM label_poll_meta WHERE key = 'last_err'"
    ).fetchone()

    return {
        "actor_did": actor_did,
        "retro_threshold_s": retro_threshold_s,
        "total_post_labels": len(post_labels),
        "total_account_labels": len(account_labels),
        "account_labels": account_labels,
        "bucket_counts": bucket_counts,
        "lag_median_s": median(lags) if lags else None,
        "retroactive_count": sum(1 for l in lags if l >= retro_threshold_s),
        "labeler_stats": sorted(labeler_stats, key=lambda x: x["median_lag_s"], reverse=True),
        "val_stats": sorted(val_stats, key=lambda x: x["label_count"], reverse=True),
        "top_late_labels": sorted(
            [p for p in post_audit if p["lag_s"] is not None and p["lag_s"] >= retro_threshold_s],
            key=lambda x: x["lag_s"],
            reverse=True,
        )[:20],
        "post_labels_detail": sorted(
            post_audit,
            key=lambda x: x["cts"],
            reverse=True,
        ),
        "poll_health": {
            "last_ok_at": last_ok["value"] if last_ok else None,
            "last_err": last_err["value"] if last_err else None,
            "stale": _is_stale(last_ok["value"] if last_ok else None),
        },
    }


def _is_stale(last_ok: str | None) -> bool:
    if not last_ok:
        return True
    try:
        last = _parse_ts(last_ok)
        age = (datetime.now(timezone.utc) - last).total_seconds()
        return age > 86400 * 2  # stale after 2 days
    except (ValueError, OSError):
        return True


def _fmt_lag(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.0f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def render_audit(audit: dict[str, Any], *, show_late: bool = False, show_labels: bool = False) -> str:
    lines: list[str] = []
    lines.append("Label Audit")
    lines.append("")

    # Poll health
    health = audit["poll_health"]
    stale_marker = " (STALE)" if health["stale"] else ""
    lines.append(f"Poll: last_ok={health['last_ok_at'] or 'never'}{stale_marker}")
    if health["last_err"]:
        lines.append(f"  last_err: {health['last_err']}")
    lines.append("")

    # Overview
    lines.append(f"Post labels: {audit['total_post_labels']}  Account labels: {audit['total_account_labels']}")
    if audit["lag_median_s"] is not None:
        lines.append(f"Median lag: {_fmt_lag(audit['lag_median_s'])}  Retroactive (>{_fmt_lag(audit['retro_threshold_s'])}): {audit['retroactive_count']}")

    bc = audit["bucket_counts"]
    lines.append(f"Buckets: realtime={bc['realtime']} same_day={bc['same_day']} late={bc['late']} necromancy={bc['necromancy']}")

    # Account labels
    if audit["account_labels"]:
        lines.append("")
        lines.append(f"Account labels ({len(audit['account_labels'])}):")
        for al in audit["account_labels"]:
            neg = " (negated)" if al["neg"] else ""
            src_name = al.get("src_handle") or al["src"][:24]
            lines.append(f"  [{al['val']}] from {src_name} at {al['cts'][:10]}{neg}")

    # Per-labeler stats
    if audit["labeler_stats"]:
        lines.append("")
        lines.append("Per labeler:")
        for ls in audit["labeler_stats"]:
            name = ls["handle"] or ls["src"][:24]
            lines.append(
                f"  {name}: {ls['label_count']} labels, "
                f"median_lag={_fmt_lag(ls['median_lag_s'])}, "
                f"retro={ls['retroactive_count']} ({ls['retroactive_rate']:.0%})"
            )

    # Per-value stats
    if audit["val_stats"]:
        lines.append("")
        lines.append("Per label value:")
        for vs in audit["val_stats"]:
            lines.append(
                f"  {vs['val']}: {vs['label_count']}x, "
                f"median_lag={_fmt_lag(vs['median_lag_s'])}, "
                f"retro={vs['retroactive_count']} ({vs['retroactive_rate']:.0%})"
            )

    # Top late labels
    if show_late and audit["top_late_labels"]:
        lines.append("")
        lines.append(f"Top retroactive labels ({len(audit['top_late_labels'])}):")
        for tl in audit["top_late_labels"]:
            src_name = tl.get("src_handle") or tl["src"][:24]
            lines.append(
                f"  [{tl['val']}] lag={_fmt_lag(tl['lag_s'])}  from {src_name}"
            )
            url = tl.get("bsky_url") or _bsky_app_url(tl["uri"])
            if url:
                lines.append(f"    {url}")

    # Individual labeled posts
    if show_labels and audit["post_labels_detail"]:
        lines.append("")
        lines.append(f"Labeled posts ({len(audit['post_labels_detail'])}):")
        for pl in audit["post_labels_detail"]:
            neg = " (negated)" if pl["neg"] else ""
            src_name = pl.get("src_handle") or pl["src"][:24]
            lag_str = f"  lag={_fmt_lag(pl['lag_s'])}" if pl.get("lag_s") is not None else ""
            bucket_str = f"  [{pl['lag_bucket']}]" if pl.get("lag_bucket") else ""
            lines.append(
                f"  [{pl['val']}]{neg} from {src_name}{lag_str}{bucket_str}"
            )
            url = pl.get("bsky_url")
            if url:
                lines.append(f"    {url}")

    return "\n".join(lines)
