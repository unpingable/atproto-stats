from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from rich.console import Console

from . import classify
from .atproto import XrpcClient
from .db import (
    Event,
    get_cursor,
    init_db,
    insert_events,
    update_cursor,
    upsert_account,
    write_sync_run,
)


console = Console()


def _parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)


def _event_created_at(item: dict[str, Any], kind: str) -> str | None:
    post = item.get("post", {})
    record = post.get("record", {})
    reason = item.get("reason") or {}

    if kind == "repost":
        return reason.get("indexedAt") or reason.get("createdAt") or post.get("indexedAt")

    return record.get("createdAt") or post.get("indexedAt")


def _event_uri(item: dict[str, Any], kind: str, created_at: str) -> str | None:
    post = item.get("post", {})
    post_uri = post.get("uri")
    if not post_uri:
        return None
    if kind == "repost":
        return f"{post_uri}#repost:{created_at}"
    return post_uri


@dataclass(frozen=True)
class Follow:
    did: str
    handle: str | None
    display_name: str | None


async def _collect_follows(client: XrpcClient, actor: str) -> list[Follow]:
    follows: list[Follow] = []
    async for item in client.get_follows(actor):
        follows.append(
            Follow(
                did=item.get("did"),
                handle=item.get("handle"),
                display_name=item.get("displayName"),
            )
        )
    return follows


async def _sync_one(
    client: XrpcClient,
    conn,
    follow: Follow,
    window_start: datetime,
    last_seen_at: datetime | None,
) -> tuple[str, int, int, str | None]:
    cursor: str | None = None
    newest: datetime | None = None
    inserted = 0
    fetched = 0

    while True:
        data = await client.get_author_feed(actor=follow.did, cursor=cursor)
        feed = data.get("feed", [])
        fetched += len(feed)
        if not feed:
            break

        events: list[Event] = []
        stop = False
        for item in feed:
            kind = classify.classify_feed_item(item)
            created_at = _event_created_at(item, kind)
            if not created_at:
                continue
            created_dt = _parse_ts(created_at)
            created_iso = created_dt.astimezone(timezone.utc).isoformat()
            if newest is None or created_dt > newest:
                newest = created_dt

            if last_seen_at and last_seen_at > window_start and created_dt <= last_seen_at:
                stop = True
                break
            if created_dt < window_start:
                stop = True
                break

            uri = _event_uri(item, kind, created_iso)
            if not uri:
                continue
            events.append(Event(did=follow.did, uri=uri, created_at=created_iso, kind=kind))

        if events:
            inserted += insert_events(conn, events)
        if stop:
            cursor = None
            break

        cursor = data.get("cursor")
        if not cursor:
            break

    newest_iso = newest.isoformat() if newest else None
    return follow.did, inserted, fetched, newest_iso


async def sync_follows(
    conn,
    client: XrpcClient,
    actor: str,
    windows: Iterable[int],
    actor_handle: str | None = None,
    max_accounts: int = 6,
    dry_run: bool = False,
) -> None:
    init_db(conn)
    started_at = datetime.now(timezone.utc)

    windows = list(windows)
    max_window = max(windows) if windows else 30
    window_start = datetime.now(timezone.utc) - timedelta(days=max_window)

    console.print(f"Fetching follows for {actor}...")
    follows = await _collect_follows(client, actor)
    if all(f.did != actor for f in follows):
        follows.append(Follow(did=actor, handle=actor_handle, display_name="(you)"))
    console.print(f"Found {len(follows)} accounts (including your own)")
    if dry_run:
        follow_pages = max(1, (len(follows) + 99) // 100)
        min_requests = follow_pages + len(follows)
        console.print(
            "Dry run: estimated minimum requests "
            f"~{min_requests} (follows pages {follow_pages} + 1 feed page per follow)."
        )
        console.print(
            "No data was fetched beyond follows list. Use smaller windows to reduce paging."
        )
        return

    for follow in follows:
        upsert_account(
            conn,
            did=follow.did,
            handle=follow.handle,
            display_name=follow.display_name,
            seen_at=datetime.now(timezone.utc).isoformat(),
        )
    conn.commit()

    sem = asyncio.Semaphore(max_accounts)

    totals = {"inserted": 0, "fetched": 0}

    async def run_one(follow: Follow) -> None:
        async with sem:
            row = get_cursor(conn, follow.did)
            last_seen_at = _parse_ts(row["last_seen_at"]) if row and row["last_seen_at"] else None
            did, inserted, fetched, newest = await _sync_one(
                client, conn, follow, window_start=window_start, last_seen_at=last_seen_at
            )
            update_cursor(
                conn,
                did=did,
                last_seen_at=newest or (last_seen_at.isoformat() if last_seen_at else None),
                last_cursor=None,
                last_sync=datetime.now(timezone.utc).isoformat(),
            )
            conn.commit()
            totals["inserted"] += inserted
            totals["fetched"] += fetched
            if inserted:
                console.print(f"{follow.handle or follow.did}: +{inserted}")

    await asyncio.gather(*(run_one(f) for f in follows))
    write_sync_run(
        conn,
        run_id=str(uuid.uuid4()),
        started_at=started_at.isoformat(),
        finished_at=datetime.now(timezone.utc).isoformat(),
        actor=actor,
        windows=json.dumps(windows),
        follows_count=len(follows),
        events_fetched=totals["fetched"],
        events_inserted=totals["inserted"],
        request_count=client.stats.request_count,
        retry_count=client.stats.retry_count,
        rate_limit_count=client.stats.rate_limit_count,
        server_error_count=client.stats.server_error_count,
        timeout_count=client.stats.timeout_count,
        request_error_count=client.stats.request_error_count,
        auto_degraded_tripped=client.stats.auto_degraded_tripped,
    )
