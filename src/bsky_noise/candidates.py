from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import median
from typing import Any

from rich.console import Console

from . import classify
from .atproto import XrpcClient
from .db import (
    init_db,
    list_candidate_scores,
    list_candidates,
    upsert_candidate_pool,
    upsert_account,
    write_candidate_score,
)


console = Console()


def _parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def _parent_did_from_reply(item: dict[str, Any]) -> str | None:
    post = item.get("post", {})
    record = post.get("record", {})
    reply = record.get("reply") or {}
    parent = reply.get("parent") or {}
    uri = parent.get("uri")
    if not uri or not uri.startswith("at://"):
        return None
    parts = uri.split("/")
    if len(parts) < 3:
        return None
    return parts[2]


@dataclass
class FeatureVector:
    sample_size: int
    posts: int
    replies: int
    reposts: int
    repost_ratio: float
    replies_to_others_rate: float
    conversation_breadth: float
    engagement: float
    recency_days: float


def _extract_features(items: list[dict[str, Any]], candidate_did: str) -> FeatureVector:
    posts = 0
    replies = 0
    reposts = 0
    reply_to_others = 0
    parent_dids: set[str] = set()
    reply_engagement: list[float] = []
    latest_ts: datetime | None = None

    for item in items:
        kind = classify.classify_feed_item(item)
        post = item.get("post", {})
        record = post.get("record", {})
        created = record.get("createdAt") or post.get("indexedAt")
        if created:
            dt = _parse_ts(created)
            if latest_ts is None or dt > latest_ts:
                latest_ts = dt

        if kind == "repost":
            reposts += 1
            continue
        if kind == "reply":
            replies += 1
            parent_did = _parent_did_from_reply(item)
            if parent_did and parent_did != candidate_did:
                reply_to_others += 1
                parent_dids.add(parent_did)
            engagement = float(post.get("likeCount", 0) + post.get("replyCount", 0) + post.get("repostCount", 0))
            reply_engagement.append(engagement)
            continue
        posts += 1

    total = max(len(items), 1)
    repost_ratio = reposts / total
    replies_to_others_rate = reply_to_others / max(replies, 1)
    conversation_breadth = min(len(parent_dids) / 10.0, 1.0)
    engagement = min((median(reply_engagement) if reply_engagement else 0.0) / 10.0, 1.0)

    if latest_ts is None:
        recency_days = 365.0
    else:
        recency_days = max((datetime.now(timezone.utc) - latest_ts).total_seconds() / 86400.0, 0.0)

    return FeatureVector(
        sample_size=len(items),
        posts=posts,
        replies=replies,
        reposts=reposts,
        repost_ratio=repost_ratio,
        replies_to_others_rate=replies_to_others_rate,
        conversation_breadth=conversation_breadth,
        engagement=engagement,
        recency_days=recency_days,
    )


def _score_interaction(f: FeatureVector) -> float:
    recency_score = max(0.0, 1.0 - min(f.recency_days, 30.0) / 30.0)
    base = (
        0.38 * f.replies_to_others_rate
        + 0.22 * f.conversation_breadth
        + 0.25 * f.engagement
        + 0.15 * recency_score
    )
    penalty = max(0.0, 1.0 - min(f.repost_ratio * 1.5, 1.0))
    return base * penalty


def _score_poster(f: FeatureVector) -> float:
    total_non_repost = max(f.posts + f.replies, 1)
    original_rate = f.posts / total_non_repost
    recency_score = max(0.0, 1.0 - min(f.recency_days, 30.0) / 30.0)
    base = 0.55 * original_rate + 0.25 * f.engagement + 0.20 * recency_score
    penalty = max(0.0, 1.0 - min(f.repost_ratio * 1.6, 1.0))
    return base * penalty


async def refresh_candidates(conn, client: XrpcClient, actor_did: str) -> int:
    init_db(conn)
    seen_at = datetime.now(timezone.utc).isoformat()

    followers: dict[str, dict[str, Any]] = {}
    follows: dict[str, dict[str, Any]] = {}

    async for item in client.get_followers(actor_did):
        did = item.get("did")
        if did:
            followers[did] = item

    async for item in client.get_follows(actor_did):
        did = item.get("did")
        if did:
            follows[did] = item

    all_dids = set(followers.keys()) | set(follows.keys())
    for did in all_dids:
        f_item = followers.get(did)
        fo_item = follows.get(did)
        handle = (f_item or fo_item or {}).get("handle")
        display_name = (f_item or fo_item or {}).get("displayName")

        upsert_account(
            conn,
            did=did,
            handle=handle,
            display_name=display_name,
            seen_at=seen_at,
        )
        upsert_candidate_pool(
            conn,
            actor_did=actor_did,
            did=did,
            handle=handle,
            display_name=display_name,
            in_followers=did in followers,
            in_follows=did in follows,
            seen_at=seen_at,
        )

    conn.commit()
    candidates = list_candidates(conn, actor_did)
    console.print(f"Followers={len(followers)} Follows={len(follows)} Candidates={len(candidates)}")
    return len(candidates)


async def score_candidates(
    conn,
    client: XrpcClient,
    actor_did: str,
    *,
    mode: str,
    sample_items: int,
    daily_budget: int,
) -> int:
    init_db(conn)
    candidates = list_candidates(conn, actor_did)
    if not candidates:
        return 0

    scored = 0
    sem = asyncio.Semaphore(4)

    async def score_one(row) -> None:
        nonlocal scored
        did = row["did"]
        items: list[dict[str, Any]] = []
        cursor: str | None = None
        while len(items) < sample_items:
            data = await client.get_author_feed(did, limit=100, cursor=cursor)
            feed = data.get("feed", [])
            if not feed:
                break
            items.extend(feed)
            cursor = data.get("cursor")
            if not cursor:
                break

        items = items[:sample_items]
        features = _extract_features(items, did)
        if mode == "poster":
            score = _score_poster(features)
        else:
            score = _score_interaction(features)

        write_candidate_score(
            conn,
            actor_did=actor_did,
            did=did,
            mode=mode,
            scored_at=datetime.now(timezone.utc).isoformat(),
            sample_size=features.sample_size,
            score=score,
            repost_ratio=features.repost_ratio,
            replies_to_others_rate=features.replies_to_others_rate,
            conversation_breadth=features.conversation_breadth,
            engagement=features.engagement,
            recency_days=features.recency_days,
            posts=features.posts,
            replies=features.replies,
            reposts=features.reposts,
        )
        scored += 1

    async def wrapped(row) -> None:
        async with sem:
            await score_one(row)

    sample = candidates[: max(0, daily_budget)]
    await asyncio.gather(*(wrapped(r) for r in sample))
    conn.commit()
    console.print(f"Scored {scored} candidates in mode={mode}")
    return scored


def pick_candidates(
    conn,
    actor_did: str,
    *,
    mode: str,
    k: int,
) -> list[dict[str, Any]]:
    rows = list_candidate_scores(conn, actor_did, mode)
    if not rows:
        return []

    scored = [dict(r) for r in rows if float(r["repost_ratio"]) <= 0.8 and float(r["recency_days"]) <= 30.0]
    if not scored:
        return []

    scored.sort(key=lambda x: float(x["score"]), reverse=True)
    picks: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_if(row: dict[str, Any]) -> None:
        did = row["did"]
        if did not in seen and len(picks) < k:
            picks.append(row)
            seen.add(did)

    for row in scored[:2]:
        add_if(row)

    breadth_sorted = sorted(scored, key=lambda x: float(x["conversation_breadth"]), reverse=True)
    if breadth_sorted:
        add_if(breadth_sorted[0])

    pool = [r for r in scored if r["did"] not in seen]
    if pool:
        add_if(random.choice(pool[: max(10, min(len(pool), 100))]))

    for row in scored:
        if len(picks) >= k:
            break
        add_if(row)

    return picks[:k]


def render_poll_text(picks: list[dict[str, Any]], mode: str) -> str:
    if not picks:
        return "No picks available. Run candidates refresh + score first."

    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    lines = [f"Orbit shortlist ({mode} mode): pick one."]
    detail = ["\nWhy these picks:"]
    for idx, row in enumerate(picks):
        label = letters[idx]
        handle = row.get("handle") or row.get("did")
        lines.append(f"{label}) @{str(handle).lstrip('@')}")
        detail.append(
            f"{label}: @{str(handle).lstrip('@')} score={float(row['score']):.2f} "
            f"repost_ratio={float(row['repost_ratio']):.2f} "
            f"breadth={float(row['conversation_breadth']):.2f}"
        )
    return "\n".join(lines + detail)


def render_pollblue_text(picks: list[dict[str, Any]], mode: str) -> str:
    if not picks:
        return "No picks available. Run candidates refresh + score first."

    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    prompt = f"Orbit shortlist ({mode}): pick one."
    options = []
    details = ["\nReply text (paste as follow-up):"]
    for idx, row in enumerate(picks):
        handle = str(row.get("handle") or row.get("did")).lstrip("@")
        options.append(f"{letters[idx]}) @{handle}")
        details.append(
            f"{letters[idx]}: @{handle} score={float(row['score']):.2f} "
            f"repost_ratio={float(row['repost_ratio']):.2f} "
            f"breadth={float(row['conversation_breadth']):.2f}"
        )
    return "\n".join([prompt] + options + details)
