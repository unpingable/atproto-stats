from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .config import DB_PATH, ensure_app_dir


@dataclass(frozen=True)
class Event:
    did: str
    uri: str
    created_at: str
    kind: str  # post | reply | repost


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    if db_path is None:
        ensure_app_dir()
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            did TEXT PRIMARY KEY,
            handle TEXT,
            display_name TEXT,
            first_seen TEXT,
            last_seen TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            did TEXT NOT NULL,
            uri TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            kind TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_did_created ON events(did, created_at)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cursors (
            did TEXT PRIMARY KEY,
            last_seen_at TEXT,
            last_cursor TEXT,
            last_sync TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS samples (
            did TEXT NOT NULL,
            window_days INTEGER NOT NULL,
            as_of_date TEXT NOT NULL,
            posts INTEGER NOT NULL,
            replies INTEGER NOT NULL,
            reposts INTEGER NOT NULL,
            threads INTEGER NOT NULL,
            PRIMARY KEY(did, window_days, as_of_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            actor TEXT NOT NULL,
            windows TEXT NOT NULL,
            follows_count INTEGER NOT NULL,
            events_fetched INTEGER NOT NULL,
            events_inserted INTEGER NOT NULL,
            request_count INTEGER NOT NULL,
            retry_count INTEGER NOT NULL,
            rate_limit_count INTEGER NOT NULL,
            server_error_count INTEGER NOT NULL,
            timeout_count INTEGER NOT NULL,
            request_error_count INTEGER NOT NULL,
            auto_degraded_tripped INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_pool (
            actor_did TEXT NOT NULL,
            did TEXT NOT NULL,
            handle TEXT,
            display_name TEXT,
            in_followers INTEGER NOT NULL,
            in_follows INTEGER NOT NULL,
            candidate INTEGER NOT NULL,
            last_seen TEXT NOT NULL,
            PRIMARY KEY(actor_did, did)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_scores (
            actor_did TEXT NOT NULL,
            did TEXT NOT NULL,
            mode TEXT NOT NULL,
            scored_at TEXT NOT NULL,
            sample_size INTEGER NOT NULL,
            score REAL NOT NULL,
            repost_ratio REAL NOT NULL,
            replies_to_others_rate REAL NOT NULL,
            conversation_breadth REAL NOT NULL,
            engagement REAL NOT NULL,
            recency_days REAL NOT NULL,
            posts INTEGER NOT NULL,
            replies INTEGER NOT NULL,
            reposts INTEGER NOT NULL,
            PRIMARY KEY(actor_did, did, mode)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS did_day_counts (
            did TEXT NOT NULL,
            day TEXT NOT NULL,
            posts INTEGER NOT NULL,
            replies INTEGER NOT NULL,
            reposts INTEGER NOT NULL,
            noise_score_day REAL NOT NULL,
            PRIMARY KEY(did, day)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reciprocity_snapshot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            taken_at TEXT NOT NULL,
            actor_did TEXT NOT NULL,
            following_count INTEGER NOT NULL,
            follower_count INTEGER NOT NULL,
            complete INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshot_following (
            run_id INTEGER NOT NULL REFERENCES reciprocity_snapshot(id),
            did TEXT NOT NULL,
            PRIMARY KEY(run_id, did)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_snapshot_following_did ON snapshot_following(did)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshot_followers (
            run_id INTEGER NOT NULL REFERENCES reciprocity_snapshot(id),
            did TEXT NOT NULL,
            PRIMARY KEY(run_id, did)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_snapshot_followers_did ON snapshot_followers(did)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mutual_edges (
            actor_did TEXT NOT NULL,
            did TEXT NOT NULL,
            first_mutual_at TEXT NOT NULL,
            last_mutual_at TEXT NOT NULL,
            currently_mutual INTEGER NOT NULL DEFAULT 1,
            num_breaks INTEGER NOT NULL DEFAULT 0,
            num_rejoins INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(actor_did, did)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS asymmetry_edges (
            actor_did TEXT NOT NULL,
            did TEXT NOT NULL,
            state TEXT NOT NULL,
            since TEXT NOT NULL,
            PRIMARY KEY(actor_did, did)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS my_labels (
            src TEXT NOT NULL,
            uri TEXT NOT NULL,
            val TEXT NOT NULL,
            cts TEXT NOT NULL,
            neg INTEGER NOT NULL DEFAULT 0,
            cid TEXT,
            exp TEXT,
            ingested_at TEXT NOT NULL,
            PRIMARY KEY(src, uri, val, cts)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_my_labels_uri_cts ON my_labels(uri, cts)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_my_labels_val_cts ON my_labels(val, cts)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_my_labels_src_cts ON my_labels(src, cts)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS label_poll_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.commit()


def upsert_account(
    conn: sqlite3.Connection,
    did: str,
    handle: str | None,
    display_name: str | None,
    seen_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO accounts (did, handle, display_name, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(did) DO UPDATE SET
          handle = COALESCE(excluded.handle, accounts.handle),
          display_name = COALESCE(excluded.display_name, accounts.display_name),
          last_seen = excluded.last_seen
        """,
        (did, handle, display_name, seen_at, seen_at),
    )


def insert_events(conn: sqlite3.Connection, events: Iterable[Event]) -> int:
    rows = [(e.did, e.uri, e.created_at, e.kind) for e in events]
    if not rows:
        return 0
    before = conn.total_changes
    conn.executemany(
        "INSERT OR IGNORE INTO events (did, uri, created_at, kind) VALUES (?, ?, ?, ?)",
        rows,
    )
    return conn.total_changes - before


def update_cursor(
    conn: sqlite3.Connection,
    did: str,
    last_seen_at: str | None,
    last_cursor: str | None,
    last_sync: str,
) -> None:
    conn.execute(
        """
        INSERT INTO cursors (did, last_seen_at, last_cursor, last_sync)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(did) DO UPDATE SET
          last_seen_at = excluded.last_seen_at,
          last_cursor = excluded.last_cursor,
          last_sync = excluded.last_sync
        """,
        (did, last_seen_at, last_cursor, last_sync),
    )


def get_cursor(conn: sqlite3.Connection, did: str) -> sqlite3.Row | None:
    cur = conn.execute("SELECT * FROM cursors WHERE did = ?", (did,))
    return cur.fetchone()


def write_samples(
    conn: sqlite3.Connection,
    did: str,
    window_days: int,
    as_of_date: str,
    posts: int,
    replies: int,
    reposts: int,
    threads: int,
) -> None:
    conn.execute(
        """
        INSERT INTO samples (did, window_days, as_of_date, posts, replies, reposts, threads)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(did, window_days, as_of_date) DO UPDATE SET
          posts = excluded.posts,
          replies = excluded.replies,
          reposts = excluded.reposts,
          threads = excluded.threads
        """,
        (did, window_days, as_of_date, posts, replies, reposts, threads),
    )
    conn.commit()


def write_sync_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    started_at: str,
    finished_at: str,
    actor: str,
    windows: str,
    follows_count: int,
    events_fetched: int,
    events_inserted: int,
    request_count: int,
    retry_count: int,
    rate_limit_count: int,
    server_error_count: int,
    timeout_count: int,
    request_error_count: int,
    auto_degraded_tripped: bool,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO sync_runs (
            run_id, started_at, finished_at, actor, windows, follows_count,
            events_fetched, events_inserted, request_count, retry_count,
            rate_limit_count, server_error_count, timeout_count, request_error_count,
            auto_degraded_tripped
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            started_at,
            finished_at,
            actor,
            windows,
            follows_count,
            events_fetched,
            events_inserted,
            request_count,
            retry_count,
            rate_limit_count,
            server_error_count,
            timeout_count,
            request_error_count,
            1 if auto_degraded_tripped else 0,
        ),
    )
    conn.commit()


def get_latest_sync_run(conn: sqlite3.Connection) -> sqlite3.Row | None:
    cur = conn.execute(
        "SELECT * FROM sync_runs ORDER BY finished_at DESC LIMIT 1"
    )
    return cur.fetchone()


def upsert_candidate_pool(
    conn: sqlite3.Connection,
    *,
    actor_did: str,
    did: str,
    handle: str | None,
    display_name: str | None,
    in_followers: bool,
    in_follows: bool,
    seen_at: str,
) -> None:
    candidate = in_followers and (not in_follows)
    conn.execute(
        """
        INSERT INTO candidate_pool (
            actor_did, did, handle, display_name, in_followers, in_follows, candidate, last_seen
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(actor_did, did) DO UPDATE SET
            handle = COALESCE(excluded.handle, candidate_pool.handle),
            display_name = COALESCE(excluded.display_name, candidate_pool.display_name),
            in_followers = excluded.in_followers,
            in_follows = excluded.in_follows,
            candidate = excluded.candidate,
            last_seen = excluded.last_seen
        """,
        (
            actor_did,
            did,
            handle,
            display_name,
            1 if in_followers else 0,
            1 if in_follows else 0,
            1 if candidate else 0,
            seen_at,
        ),
    )


def list_candidates(conn: sqlite3.Connection, actor_did: str) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT actor_did, did, handle, display_name, last_seen
        FROM candidate_pool
        WHERE actor_did = ? AND candidate = 1
        ORDER BY COALESCE(handle, did)
        """,
        (actor_did,),
    )
    return list(cur.fetchall())


def write_candidate_score(
    conn: sqlite3.Connection,
    *,
    actor_did: str,
    did: str,
    mode: str,
    scored_at: str,
    sample_size: int,
    score: float,
    repost_ratio: float,
    replies_to_others_rate: float,
    conversation_breadth: float,
    engagement: float,
    recency_days: float,
    posts: int,
    replies: int,
    reposts: int,
) -> None:
    conn.execute(
        """
        INSERT INTO candidate_scores (
            actor_did, did, mode, scored_at, sample_size, score, repost_ratio,
            replies_to_others_rate, conversation_breadth, engagement, recency_days,
            posts, replies, reposts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(actor_did, did, mode) DO UPDATE SET
            scored_at = excluded.scored_at,
            sample_size = excluded.sample_size,
            score = excluded.score,
            repost_ratio = excluded.repost_ratio,
            replies_to_others_rate = excluded.replies_to_others_rate,
            conversation_breadth = excluded.conversation_breadth,
            engagement = excluded.engagement,
            recency_days = excluded.recency_days,
            posts = excluded.posts,
            replies = excluded.replies,
            reposts = excluded.reposts
        """,
        (
            actor_did,
            did,
            mode,
            scored_at,
            sample_size,
            score,
            repost_ratio,
            replies_to_others_rate,
            conversation_breadth,
            engagement,
            recency_days,
            posts,
            replies,
            reposts,
        ),
    )


def write_reciprocity_snapshot(
    conn: sqlite3.Connection,
    *,
    taken_at: str,
    actor_did: str,
    following_dids: Iterable[str],
    follower_dids: Iterable[str],
) -> int:
    following_list = list(following_dids)
    follower_list = list(follower_dids)
    try:
        conn.execute("BEGIN")
        cur = conn.execute(
            """
            INSERT INTO reciprocity_snapshot
                (taken_at, actor_did, following_count, follower_count, complete)
            VALUES (?, ?, ?, ?, 0)
            """,
            (taken_at, actor_did, len(following_list), len(follower_list)),
        )
        run_id = cur.lastrowid
        if following_list:
            conn.executemany(
                "INSERT INTO snapshot_following (run_id, did) VALUES (?, ?)",
                [(run_id, d) for d in following_list],
            )
        if follower_list:
            conn.executemany(
                "INSERT INTO snapshot_followers (run_id, did) VALUES (?, ?)",
                [(run_id, d) for d in follower_list],
            )
        conn.execute(
            "UPDATE reciprocity_snapshot SET complete = 1 WHERE id = ?", (run_id,)
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return run_id


def get_latest_reciprocity_snapshots(
    conn: sqlite3.Connection, actor_did: str, n: int = 2
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT * FROM reciprocity_snapshot
        WHERE actor_did = ? AND complete = 1
        ORDER BY id DESC
        LIMIT ?
        """,
        (actor_did, n),
    )
    return list(cur.fetchall())


def list_candidate_scores(conn: sqlite3.Connection, actor_did: str, mode: str) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT p.did, p.handle, p.display_name, s.mode, s.scored_at, s.sample_size,
               s.score, s.repost_ratio, s.replies_to_others_rate, s.conversation_breadth,
               s.engagement, s.recency_days, s.posts, s.replies, s.reposts
        FROM candidate_scores s
        JOIN candidate_pool p ON p.actor_did = s.actor_did AND p.did = s.did
        WHERE s.actor_did = ? AND s.mode = ? AND p.candidate = 1
        ORDER BY s.score DESC
        """,
        (actor_did, mode),
    )
    return list(cur.fetchall())
