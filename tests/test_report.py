import json
from datetime import datetime, timezone
from pathlib import Path

import uuid

from bsky_noise.db import (
    Event,
    connect,
    init_db,
    insert_events,
    upsert_account,
    write_sync_run,
)
from bsky_noise.report import write_report


def test_report_generation(tmp_path: Path):
    db_path = tmp_path / "test.db"
    conn = connect(db_path)
    init_db(conn)

    now = datetime.now(timezone.utc)
    upsert_account(conn, "did:1", "alice", "Alice", now.isoformat())
    events = [
        Event("did:1", "at://post/1", now.isoformat(), "post"),
        Event("did:1", "at://post/2", now.isoformat(), "reply"),
    ]
    insert_events(conn, events)

    out_dir = tmp_path / "out"
    summary = write_report(
        conn,
        windows=[30],
        output_dir=out_dir,
        weights={"posts": 1.0, "replies": 1.5, "reposts": 0.75},
        compare_prior=True,
        what_if_mute=[5, 10],
        watchlist={"alice"},
        export_csv=out_dir / "report.csv",
    )

    assert (out_dir / "summary.json").exists()
    assert (out_dir / "index.html").exists()
    assert (out_dir / "report.csv").exists()

    data = json.loads((out_dir / "summary.json").read_text())
    assert data["accounts"][0]["windows"]["30"]["counts"]["posts"] == 1
    assert summary["accounts"][0]["windows"]["30"]["counts"]["replies"] == 1
    assert "reason_label" in data["rankings"]["30"]["top_by_delta"][0]
    assert "burst_score" in data["accounts"][0]["windows"]["30"]
    assert "spike_today_ratio" in data["accounts"][0]["windows"]["30"]
    assert data["run_health_status"] in {"good", "degraded", "partial"}


def test_report_includes_self_account_metrics(tmp_path: Path):
    db_path = tmp_path / "test-self.db"
    conn = connect(db_path)
    init_db(conn)

    now = datetime.now(timezone.utc)
    actor_did = "did:self"
    upsert_account(conn, actor_did, "me.test", "Me", now.isoformat())
    insert_events(
        conn,
        [
            Event(actor_did, "at://post/a", now.isoformat(), "post"),
            Event(actor_did, "at://post/b", now.isoformat(), "reply"),
            Event(actor_did, "at://post/c", now.isoformat(), "repost"),
        ],
    )
    write_sync_run(
        conn,
        run_id=str(uuid.uuid4()),
        started_at=now.isoformat(),
        finished_at=now.isoformat(),
        actor=actor_did,
        windows="[7]",
        follows_count=1,
        events_fetched=3,
        events_inserted=3,
        request_count=1,
        retry_count=0,
        rate_limit_count=0,
        server_error_count=0,
        timeout_count=0,
        request_error_count=0,
        auto_degraded_tripped=False,
    )

    summary = write_report(
        conn,
        windows=[7],
        output_dir=tmp_path / "out-self",
        weights={"posts": 1.0, "replies": 1.5, "reposts": 0.75},
        compare_prior=True,
        what_if_mute=[5],
    )
    assert summary["self_account"] is not None
    self_window = summary["self_account"]["windows"]["7"]
    assert self_window["counts"] == {"posts": 1, "replies": 1, "reposts": 1}
    assert self_window["active_days"] >= 1
    assert len(self_window["hourly_utc"]) == 24
    assert len(self_window["weekday_mon0"]) == 7
