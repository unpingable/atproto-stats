from bsky_noise.db import Event, connect, init_db, insert_events, upsert_account


def test_upsert_and_idempotent_events(tmp_path):
    db_path = tmp_path / "test.db"
    conn = connect(db_path)
    init_db(conn)

    upsert_account(conn, "did:1", "alice", "Alice", "2024-01-01T00:00:00+00:00")
    upsert_account(conn, "did:1", "alice", "Alice A", "2024-01-02T00:00:00+00:00")

    events = [
        Event("did:1", "at://post/1", "2024-01-01T00:00:00+00:00", "post"),
        Event("did:1", "at://post/1", "2024-01-01T00:00:00+00:00", "post"),
    ]
    inserted = insert_events(conn, events)
    assert inserted == 1

    row = conn.execute("SELECT * FROM accounts WHERE did = ?", ("did:1",)).fetchone()
    assert row["display_name"] == "Alice A"
