import json
from datetime import datetime, timezone
from pathlib import Path

from bsky_noise.db import Event, connect, init_db, insert_events, upsert_account
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
