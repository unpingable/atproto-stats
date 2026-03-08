from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import median
from typing import Any

from rich.console import Console

from .atproto import XrpcClient
from .db import (
    get_latest_reciprocity_snapshots,
    init_db,
    upsert_account,
    write_reciprocity_snapshot,
)

console = Console()


@dataclass(frozen=True)
class Transition:
    did: str
    handle: str | None
    display_name: str | None
    prev_state: str
    curr_state: str
    label: str


def _rel_state(following: set[str], followers: set[str], did: str) -> str:
    i_follow = did in following
    they_follow = did in followers
    if i_follow and they_follow:
        return "mutual"
    if i_follow:
        return "i_follow_only"
    if they_follow:
        return "they_follow_only"
    return "none"


_TRANSITION_LABELS = {
    ("mutual", "i_follow_only"): "they_unfollowed",
    ("mutual", "none"): "mutual_dissolved",
    ("mutual", "they_follow_only"): "you_unfollowed",
    ("i_follow_only", "mutual"): "they_followed_back",
    ("i_follow_only", "none"): "you_unfollowed",
    ("i_follow_only", "they_follow_only"): "you_swapped",
    ("they_follow_only", "mutual"): "you_followed_back",
    ("they_follow_only", "none"): "they_unfollowed",
    ("they_follow_only", "i_follow_only"): "they_swapped",
    ("none", "mutual"): "new_mutual",
    ("none", "i_follow_only"): "you_followed",
    ("none", "they_follow_only"): "they_followed",
}

# Transitions where the person disappeared from our follower list —
# could be a true unfollow or could be block/deactivation/nuke.
_VISIBILITY_AMBIGUOUS = {"they_unfollowed", "mutual_dissolved"}


async def _check_visibility(
    client: XrpcClient, dids: list[str], max_concurrency: int = 4
) -> dict[str, bool]:
    """Resolve profiles to distinguish true unfollows from blocks/deactivations.

    Returns {did: True} if the profile is resolvable (true unfollow),
    {did: False} if it's not (blocked, deactivated, nuked).
    """
    results: dict[str, bool] = {}
    sem = asyncio.Semaphore(max_concurrency)

    async def check_one(did: str) -> None:
        async with sem:
            try:
                await client._request(
                    "GET", "app.bsky.actor.getProfile", {"actor": did}
                )
                results[did] = True
            except Exception:
                results[did] = False

    await asyncio.gather(*(check_one(d) for d in dids))
    return results


def _update_edges(conn, actor_did: str, following: set[str], followers: set[str], taken_at: str) -> None:
    """Update mutual_edges and asymmetry_edges tables based on current snapshot."""
    curr_mutuals = following & followers

    # Load existing mutual edges for this actor
    existing = {}
    for row in conn.execute(
        "SELECT did, currently_mutual FROM mutual_edges WHERE actor_did = ?",
        (actor_did,),
    ).fetchall():
        existing[row["did"]] = bool(row["currently_mutual"])

    for did in curr_mutuals:
        if did in existing:
            if not existing[did]:
                # Was broken, now mutual again — rejoin
                conn.execute(
                    """
                    UPDATE mutual_edges
                    SET currently_mutual = 1, last_mutual_at = ?, num_rejoins = num_rejoins + 1
                    WHERE actor_did = ? AND did = ?
                    """,
                    (taken_at, actor_did, did),
                )
            else:
                # Still mutual — update last_mutual_at
                conn.execute(
                    "UPDATE mutual_edges SET last_mutual_at = ? WHERE actor_did = ? AND did = ?",
                    (taken_at, actor_did, did),
                )
        else:
            # New mutual
            conn.execute(
                """
                INSERT INTO mutual_edges (actor_did, did, first_mutual_at, last_mutual_at, currently_mutual, num_breaks, num_rejoins)
                VALUES (?, ?, ?, ?, 1, 0, 0)
                """,
                (actor_did, did, taken_at, taken_at),
            )

    # Mark edges that are no longer mutual
    for did, was_mutual in existing.items():
        if was_mutual and did not in curr_mutuals:
            conn.execute(
                """
                UPDATE mutual_edges
                SET currently_mutual = 0, num_breaks = num_breaks + 1
                WHERE actor_did = ? AND did = ?
                """,
                (actor_did, did),
            )

    # Update asymmetry edges
    conn.execute("DELETE FROM asymmetry_edges WHERE actor_did = ?", (actor_did,))
    i_follow_only = following - followers
    they_follow_only = followers - following
    if i_follow_only:
        conn.executemany(
            """
            INSERT INTO asymmetry_edges (actor_did, did, state, since)
            VALUES (?, ?, 'i_follow_only', ?)
            """,
            [(actor_did, d, taken_at) for d in i_follow_only],
        )
    if they_follow_only:
        conn.executemany(
            """
            INSERT INTO asymmetry_edges (actor_did, did, state, since)
            VALUES (?, ?, 'they_follow_only', ?)
            """,
            [(actor_did, d, taken_at) for d in they_follow_only],
        )
    conn.commit()


async def take_snapshot(
    conn, client: XrpcClient, actor_did: str
) -> int:
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
        upsert_account(conn, did=did, handle=handle, display_name=display_name, seen_at=seen_at)
    conn.commit()

    run_id = write_reciprocity_snapshot(
        conn,
        taken_at=seen_at,
        actor_did=actor_did,
        following_dids=follows.keys(),
        follower_dids=followers.keys(),
    )

    _update_edges(conn, actor_did, set(follows.keys()), set(followers.keys()), seen_at)

    console.print(
        f"Snapshot #{run_id}: following={len(follows)} followers={len(followers)}"
    )
    return run_id


def _load_snapshot_sets(conn, run_id: int) -> tuple[set[str], set[str]]:
    following = {
        r["did"]
        for r in conn.execute(
            "SELECT did FROM snapshot_following WHERE run_id = ?", (run_id,)
        ).fetchall()
    }
    followers = {
        r["did"]
        for r in conn.execute(
            "SELECT did FROM snapshot_followers WHERE run_id = ?", (run_id,)
        ).fetchall()
    }
    return following, followers


def _resolve_handles(conn, dids: set[str]) -> dict[str, tuple[str | None, str | None]]:
    if not dids:
        return {}
    placeholders = ",".join("?" for _ in dids)
    rows = conn.execute(
        f"SELECT did, handle, display_name FROM accounts WHERE did IN ({placeholders})",
        list(dids),
    ).fetchall()
    return {r["did"]: (r["handle"], r["display_name"]) for r in rows}


def diff_snapshots(
    conn,
    actor_did: str,
    *,
    prev_id: int | None = None,
    curr_id: int | None = None,
    visibility: dict[str, bool] | None = None,
) -> dict[str, Any]:
    init_db(conn)
    snapshots = get_latest_reciprocity_snapshots(conn, actor_did, n=2)
    if not snapshots:
        return {"error": "no snapshots found", "transitions": [], "summary": {}}

    if curr_id is None:
        curr_id = snapshots[0]["id"]
    if prev_id is None:
        if len(snapshots) < 2:
            return {"error": "need at least 2 snapshots to diff", "transitions": [], "summary": {}}
        prev_id = snapshots[1]["id"]

    prev_following, prev_followers = _load_snapshot_sets(conn, prev_id)
    curr_following, curr_followers = _load_snapshot_sets(conn, curr_id)

    all_dids = (prev_following | prev_followers | curr_following | curr_followers)
    handles = _resolve_handles(conn, all_dids)

    transitions: list[Transition] = []
    for did in all_dids:
        prev_state = _rel_state(prev_following, prev_followers, did)
        curr_state = _rel_state(curr_following, curr_followers, did)
        if prev_state == curr_state:
            continue
        label = _TRANSITION_LABELS.get((prev_state, curr_state), "unknown")
        # Reclassify ambiguous disappearances if visibility data is available
        if visibility is not None and label in _VISIBILITY_AMBIGUOUS:
            if not visibility.get(did, True):
                label = "visibility_lost"
        handle, display_name = handles.get(did, (None, None))
        transitions.append(
            Transition(
                did=did,
                handle=handle,
                display_name=display_name,
                prev_state=prev_state,
                curr_state=curr_state,
                label=label,
            )
        )

    # Build counts per label
    label_counts: dict[str, int] = {}
    for t in transitions:
        label_counts[t.label] = label_counts.get(t.label, 0) + 1

    # Mutuals counts
    prev_mutuals = prev_following & prev_followers
    curr_mutuals = curr_following & curr_followers

    # Rates (safe division)
    prev_mutual_n = len(prev_mutuals) or 1
    prev_i_follow_only = prev_following - prev_followers
    prev_ifo_n = len(prev_i_follow_only) or 1

    return {
        "prev_run_id": prev_id,
        "curr_run_id": curr_id,
        "prev_following": len(prev_following),
        "prev_followers": len(prev_followers),
        "prev_mutuals": len(prev_mutuals),
        "curr_following": len(curr_following),
        "curr_followers": len(curr_followers),
        "curr_mutuals": len(curr_mutuals),
        "transitions": transitions,
        "label_counts": label_counts,
        "rates": {
            "breakage_rate": label_counts.get("they_unfollowed", 0) / prev_mutual_n,
            "prune_rate": (
                label_counts.get("you_unfollowed", 0)
                + label_counts.get("mutual_dissolved", 0)
            ) / prev_mutual_n,
            "accretion_rate": label_counts.get("new_mutual", 0) / prev_mutual_n,
            "rejoin_rate": label_counts.get("they_followed_back", 0) / prev_ifo_n,
        },
        "summary": {
            "reciprocity_breakages": label_counts.get("they_unfollowed", 0),
            "your_prunes": (
                label_counts.get("you_unfollowed", 0)
                + label_counts.get("mutual_dissolved", 0)
            ),
            "new_mutuals": label_counts.get("new_mutual", 0),
            "new_followers": label_counts.get("they_followed", 0),
            "lost_followers": (
                label_counts.get("they_unfollowed", 0)
                + label_counts.get("mutual_dissolved", 0)
            ),
            "visibility_lost": label_counts.get("visibility_lost", 0),
        },
    }


def build_status(conn, actor_did: str) -> dict[str, Any]:
    """Build the self-audit dashboard: half-life, rates, asymmetry inventory."""
    init_db(conn)
    now = datetime.now(timezone.utc)

    # --- Mutual half-life ---
    edges = conn.execute(
        "SELECT * FROM mutual_edges WHERE actor_did = ?", (actor_did,)
    ).fetchall()

    currently_mutual = [e for e in edges if e["currently_mutual"]]
    ever_broken = [e for e in edges if e["num_breaks"] > 0]

    lifespans_days: list[float] = []
    for e in edges:
        first = datetime.fromisoformat(e["first_mutual_at"])
        last = datetime.fromisoformat(e["last_mutual_at"])
        days = max((last - first).total_seconds() / 86400.0, 0.0)
        lifespans_days.append(days)

    active_lifespans: list[float] = []
    for e in currently_mutual:
        first = datetime.fromisoformat(e["first_mutual_at"])
        days = max((now - first).total_seconds() / 86400.0, 0.0)
        active_lifespans.append(days)

    total_rejoins = sum(e["num_rejoins"] for e in edges)
    total_breaks = sum(e["num_breaks"] for e in edges)

    half_life: dict[str, Any] = {
        "total_ever_mutual": len(edges),
        "currently_mutual": len(currently_mutual),
        "ever_broken": len(ever_broken),
        "total_breaks": total_breaks,
        "total_rejoins": total_rejoins,
        "rejoin_rate": total_rejoins / max(total_breaks, 1),
    }

    if lifespans_days:
        sorted_ls = sorted(lifespans_days)
        half_life["median_lifespan_days"] = median(sorted_ls)
        n = len(sorted_ls)
        half_life["p10_lifespan_days"] = sorted_ls[max(n // 10, 0)]
        half_life["p90_lifespan_days"] = sorted_ls[min(n * 9 // 10, n - 1)]
    else:
        half_life["median_lifespan_days"] = 0.0
        half_life["p10_lifespan_days"] = 0.0
        half_life["p90_lifespan_days"] = 0.0

    if active_lifespans:
        half_life["median_active_mutual_age_days"] = median(active_lifespans)
    else:
        half_life["median_active_mutual_age_days"] = 0.0

    # --- Asymmetry inventory ---
    asym_rows = conn.execute(
        "SELECT state, COUNT(*) as n FROM asymmetry_edges WHERE actor_did = ? GROUP BY state",
        (actor_did,),
    ).fetchall()
    asymmetry = {r["state"]: r["n"] for r in asym_rows}

    # --- Latest diff rates ---
    snapshots = get_latest_reciprocity_snapshots(conn, actor_did, n=2)
    rates = None
    if len(snapshots) >= 2:
        diff = diff_snapshots(conn, actor_did)
        rates = diff.get("rates")

    return {
        "actor_did": actor_did,
        "as_of": now.isoformat(),
        "half_life": half_life,
        "asymmetry": {
            "i_follow_only": asymmetry.get("i_follow_only", 0),
            "they_follow_only": asymmetry.get("they_follow_only", 0),
        },
        "latest_rates": rates,
    }


def render_status(status: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"Reciprocity Status — {status['as_of'][:10]}")
    lines.append("")

    hl = status["half_life"]
    lines.append(f"Mutuals: {hl['currently_mutual']} current / {hl['total_ever_mutual']} ever")
    lines.append(f"Median mutual lifespan: {hl['median_lifespan_days']:.0f} days")
    lines.append(f"  p10: {hl['p10_lifespan_days']:.0f}d  p90: {hl['p90_lifespan_days']:.0f}d")
    if hl["currently_mutual"]:
        lines.append(f"Median active mutual age: {hl['median_active_mutual_age_days']:.0f} days")
    lines.append(f"Breaks: {hl['total_breaks']}  Rejoins: {hl['total_rejoins']}  Rejoin rate: {hl['rejoin_rate']:.1%}")

    lines.append("")
    asym = status["asymmetry"]
    lines.append(f"Asymmetry: {asym['i_follow_only']} i_follow_only / {asym['they_follow_only']} they_follow_only")

    rates = status.get("latest_rates")
    if rates:
        lines.append("")
        lines.append("Latest diff rates:")
        lines.append(f"  Breakage:  {rates['breakage_rate']:.2%}")
        lines.append(f"  Prune:     {rates['prune_rate']:.2%}")
        lines.append(f"  Accretion: {rates['accretion_rate']:.2%}")
        lines.append(f"  Rejoin:    {rates['rejoin_rate']:.2%}")

    return "\n".join(lines)


def render_diff(
    result: dict[str, Any], *, show_names: bool = False, limit: int = 10
) -> str:
    if result.get("error"):
        return result["error"]

    lines: list[str] = []
    lines.append(
        f"Snapshot #{result['prev_run_id']} -> #{result['curr_run_id']}"
    )
    lines.append(
        f"Following: {result['prev_following']} -> {result['curr_following']}  "
        f"Followers: {result['prev_followers']} -> {result['curr_followers']}  "
        f"Mutuals: {result['prev_mutuals']} -> {result['curr_mutuals']}"
    )

    summary = result["summary"]
    lines.append("")
    lines.append(f"Reciprocity breakages: {summary['reciprocity_breakages']}")
    lines.append(f"Your prunes: {summary['your_prunes']}")
    lines.append(f"New mutuals: {summary['new_mutuals']}")
    lines.append(f"New followers: {summary['new_followers']}")
    if summary.get("visibility_lost"):
        lines.append(f"Visibility lost (block/deactivation): {summary['visibility_lost']}")

    rates = result.get("rates")
    if rates:
        lines.append("")
        lines.append(
            f"Rates: breakage={rates['breakage_rate']:.2%}  "
            f"prune={rates['prune_rate']:.2%}  "
            f"accretion={rates['accretion_rate']:.2%}  "
            f"rejoin={rates['rejoin_rate']:.2%}"
        )

    transitions = result["transitions"]
    if not transitions:
        lines.append("\nNo transitions detected.")
        return "\n".join(lines)

    if not show_names:
        return "\n".join(lines)

    # Group by label for display
    by_label: dict[str, list[Transition]] = {}
    for t in transitions:
        by_label.setdefault(t.label, []).append(t)

    display_order = [
        "they_unfollowed",
        "visibility_lost",
        "you_unfollowed",
        "mutual_dissolved",
        "they_followed_back",
        "you_followed_back",
        "new_mutual",
        "you_followed",
        "they_followed",
        "you_swapped",
        "they_swapped",
    ]
    for label in display_order:
        group = by_label.pop(label, [])
        if not group:
            continue
        lines.append(f"\n{label} ({len(group)}):")
        shown = sorted(group, key=lambda x: x.handle or x.did)[:limit]
        for t in shown:
            name = t.handle or t.did
            lines.append(f"  {name}")
        remaining = len(group) - len(shown)
        if remaining > 0:
            lines.append(f"  ... and {remaining} more")

    for label, group in sorted(by_label.items()):
        lines.append(f"\n{label} ({len(group)}):")
        shown = sorted(group, key=lambda x: x.handle or x.did)[:limit]
        for t in shown:
            name = t.handle or t.did
            lines.append(f"  {name}")
        remaining = len(group) - len(shown)
        if remaining > 0:
            lines.append(f"  ... and {remaining} more")

    return "\n".join(lines)
