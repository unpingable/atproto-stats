from bsky_noise.db import connect, init_db, upsert_account, write_reciprocity_snapshot
from bsky_noise.reciprocity import (
    _rel_state,
    _update_edges,
    build_status,
    diff_snapshots,
    render_diff,
    render_status,
)


def _setup(tmp_path):
    conn = connect(tmp_path / "test.db")
    init_db(conn)
    return conn


def _seed_accounts(conn, dids):
    for did in dids:
        upsert_account(conn, did, f"handle-{did}", f"Name {did}", "2024-01-01T00:00:00+00:00")
    conn.commit()


def test_rel_state():
    following = {"a", "b"}
    followers = {"b", "c"}
    assert _rel_state(following, followers, "a") == "i_follow_only"
    assert _rel_state(following, followers, "b") == "mutual"
    assert _rel_state(following, followers, "c") == "they_follow_only"
    assert _rel_state(following, followers, "d") == "none"


def test_snapshot_roundtrip(tmp_path):
    conn = _setup(tmp_path)
    run_id = write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b"],
        follower_dids=["b", "c"],
    )
    assert run_id is not None
    row = conn.execute("SELECT * FROM reciprocity_snapshot WHERE id = ?", (run_id,)).fetchone()
    assert row["following_count"] == 2
    assert row["follower_count"] == 2
    assert row["complete"] == 1


def test_snapshot_incomplete_not_used(tmp_path):
    conn = _setup(tmp_path)
    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=["a"],
        follower_dids=["a"],
    )
    conn.execute(
        "INSERT INTO reciprocity_snapshot (taken_at, actor_did, following_count, follower_count, complete) "
        "VALUES ('2024-01-02T00:00:00+00:00', 'me', 0, 0, 0)"
    )
    conn.commit()
    result = diff_snapshots(conn, "me")
    assert result.get("error")


def test_diff_they_unfollowed(tmp_path):
    conn = _setup(tmp_path)
    _seed_accounts(conn, ["a", "b", "c"])

    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b"],
        follower_dids=["a", "c"],
    )
    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-02T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b"],
        follower_dids=["c"],
    )

    result = diff_snapshots(conn, "me")
    assert result["summary"]["reciprocity_breakages"] == 1
    labels = {t.did: t.label for t in result["transitions"]}
    assert labels["a"] == "they_unfollowed"


def test_diff_you_unfollowed(tmp_path):
    conn = _setup(tmp_path)
    _seed_accounts(conn, ["a"])

    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=["a"],
        follower_dids=["a"],
    )
    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-02T00:00:00+00:00",
        actor_did="me",
        following_dids=[],
        follower_dids=["a"],
    )

    result = diff_snapshots(conn, "me")
    labels = {t.did: t.label for t in result["transitions"]}
    assert labels["a"] == "you_unfollowed"
    assert result["summary"]["your_prunes"] == 1


def test_diff_new_mutual(tmp_path):
    conn = _setup(tmp_path)
    _seed_accounts(conn, ["a"])

    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=[],
        follower_dids=[],
    )
    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-02T00:00:00+00:00",
        actor_did="me",
        following_dids=["a"],
        follower_dids=["a"],
    )

    result = diff_snapshots(conn, "me")
    labels = {t.did: t.label for t in result["transitions"]}
    assert labels["a"] == "new_mutual"
    assert result["summary"]["new_mutuals"] == 1


def test_diff_no_changes(tmp_path):
    conn = _setup(tmp_path)

    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=["a"],
        follower_dids=["a"],
    )
    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-02T00:00:00+00:00",
        actor_did="me",
        following_dids=["a"],
        follower_dids=["a"],
    )

    result = diff_snapshots(conn, "me")
    assert result["transitions"] == []
    assert result["summary"]["reciprocity_breakages"] == 0


def test_diff_not_enough_snapshots(tmp_path):
    conn = _setup(tmp_path)
    result = diff_snapshots(conn, "me")
    assert result.get("error")


def test_diff_rates(tmp_path):
    conn = _setup(tmp_path)
    _seed_accounts(conn, ["a", "b", "c", "d"])

    # prev: a,b,c are mutual, d is i_follow_only
    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b", "c", "d"],
        follower_dids=["a", "b", "c"],
    )
    # curr: a unfollowed (breakage), d followed back (rejoin)
    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-02T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b", "c", "d"],
        follower_dids=["b", "c", "d"],
    )

    result = diff_snapshots(conn, "me")
    rates = result["rates"]
    # 1 breakage out of 3 prev mutuals
    assert abs(rates["breakage_rate"] - 1 / 3) < 0.01
    # d followed back out of 1 prev i_follow_only
    assert abs(rates["rejoin_rate"] - 1.0) < 0.01


def test_render_diff_counts_only_by_default(tmp_path):
    conn = _setup(tmp_path)
    _seed_accounts(conn, ["a", "b"])

    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b"],
        follower_dids=["a", "b"],
    )
    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-02T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b"],
        follower_dids=["b"],
    )

    result = diff_snapshots(conn, "me")
    text = render_diff(result)
    assert "Reciprocity breakages: 1" in text
    assert "handle-a" not in text


def test_render_diff_show_names(tmp_path):
    conn = _setup(tmp_path)
    _seed_accounts(conn, ["a", "b"])

    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b"],
        follower_dids=["a", "b"],
    )
    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-02T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b"],
        follower_dids=["b"],
    )

    result = diff_snapshots(conn, "me")
    text = render_diff(result, show_names=True)
    assert "they_unfollowed" in text
    assert "handle-a" in text


def test_render_diff_limit(tmp_path):
    conn = _setup(tmp_path)
    dids = [f"d{i}" for i in range(20)]
    _seed_accounts(conn, dids)

    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=dids,
        follower_dids=dids,
    )
    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-02T00:00:00+00:00",
        actor_did="me",
        following_dids=dids,
        follower_dids=[],
    )

    result = diff_snapshots(conn, "me")
    text = render_diff(result, show_names=True, limit=5)
    assert "... and 15 more" in text


def test_mutuals_count(tmp_path):
    conn = _setup(tmp_path)

    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b", "c"],
        follower_dids=["a", "b", "d"],
    )
    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-02T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b", "c"],
        follower_dids=["a", "d"],
    )

    result = diff_snapshots(conn, "me")
    assert result["prev_mutuals"] == 2
    assert result["curr_mutuals"] == 1


def test_visibility_lost_reclassification(tmp_path):
    conn = _setup(tmp_path)
    _seed_accounts(conn, ["a", "b"])

    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b"],
        follower_dids=["a", "b"],
    )
    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-02T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b"],
        follower_dids=[],
    )

    visibility = {"a": True, "b": False}
    result = diff_snapshots(conn, "me", visibility=visibility)
    labels = {t.did: t.label for t in result["transitions"]}
    assert labels["a"] == "they_unfollowed"
    assert labels["b"] == "visibility_lost"
    assert result["summary"]["reciprocity_breakages"] == 1
    assert result["summary"]["visibility_lost"] == 1


# --- mutual_edges + asymmetry_edges tests ---


def test_update_edges_new_mutuals(tmp_path):
    conn = _setup(tmp_path)
    _update_edges(conn, "me", {"a", "b"}, {"a", "b", "c"}, "2024-01-01T00:00:00+00:00")

    edges = conn.execute("SELECT * FROM mutual_edges WHERE actor_did = 'me'").fetchall()
    assert len(edges) == 2
    dids = {e["did"] for e in edges}
    assert dids == {"a", "b"}
    for e in edges:
        assert e["currently_mutual"] == 1
        assert e["num_breaks"] == 0

    # Asymmetry: c is they_follow_only
    asym = conn.execute("SELECT * FROM asymmetry_edges WHERE actor_did = 'me'").fetchall()
    assert len(asym) == 1
    assert asym[0]["did"] == "c"
    assert asym[0]["state"] == "they_follow_only"


def test_update_edges_break_and_rejoin(tmp_path):
    conn = _setup(tmp_path)

    # Snapshot 1: a is mutual
    _update_edges(conn, "me", {"a"}, {"a"}, "2024-01-01T00:00:00+00:00")
    edge = conn.execute("SELECT * FROM mutual_edges WHERE actor_did = 'me' AND did = 'a'").fetchone()
    assert edge["currently_mutual"] == 1
    assert edge["num_breaks"] == 0

    # Snapshot 2: a is no longer mutual (they unfollowed)
    _update_edges(conn, "me", {"a"}, set(), "2024-01-02T00:00:00+00:00")
    edge = conn.execute("SELECT * FROM mutual_edges WHERE actor_did = 'me' AND did = 'a'").fetchone()
    assert edge["currently_mutual"] == 0
    assert edge["num_breaks"] == 1

    # Snapshot 3: a came back
    _update_edges(conn, "me", {"a"}, {"a"}, "2024-01-03T00:00:00+00:00")
    edge = conn.execute("SELECT * FROM mutual_edges WHERE actor_did = 'me' AND did = 'a'").fetchone()
    assert edge["currently_mutual"] == 1
    assert edge["num_breaks"] == 1
    assert edge["num_rejoins"] == 1


def test_update_edges_asymmetry_refreshed(tmp_path):
    conn = _setup(tmp_path)

    _update_edges(conn, "me", {"a", "b"}, {"b"}, "2024-01-01T00:00:00+00:00")
    asym = conn.execute(
        "SELECT * FROM asymmetry_edges WHERE actor_did = 'me' ORDER BY did"
    ).fetchall()
    assert len(asym) == 1
    assert asym[0]["did"] == "a"
    assert asym[0]["state"] == "i_follow_only"

    # New snapshot: a is now mutual, c is i_follow_only
    _update_edges(conn, "me", {"a", "b", "c"}, {"a", "b"}, "2024-01-02T00:00:00+00:00")
    asym = conn.execute(
        "SELECT * FROM asymmetry_edges WHERE actor_did = 'me' ORDER BY did"
    ).fetchall()
    assert len(asym) == 1
    assert asym[0]["did"] == "c"
    assert asym[0]["state"] == "i_follow_only"


def test_build_status_basic(tmp_path):
    conn = _setup(tmp_path)
    _seed_accounts(conn, ["a", "b", "c"])

    # Simulate two snapshots with edge tracking
    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b", "c"],
        follower_dids=["a", "b"],
    )
    _update_edges(conn, "me", {"a", "b", "c"}, {"a", "b"}, "2024-01-01T00:00:00+00:00")

    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-02T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b", "c"],
        follower_dids=["a"],
    )
    _update_edges(conn, "me", {"a", "b", "c"}, {"a"}, "2024-01-02T00:00:00+00:00")

    status = build_status(conn, "me")
    assert status["half_life"]["total_ever_mutual"] == 2
    assert status["half_life"]["currently_mutual"] == 1
    assert status["half_life"]["total_breaks"] == 1
    assert status["asymmetry"]["i_follow_only"] == 2  # b and c
    assert status["latest_rates"] is not None
    assert status["latest_rates"]["breakage_rate"] > 0


def test_build_status_no_data(tmp_path):
    conn = _setup(tmp_path)
    status = build_status(conn, "me")
    assert status["half_life"]["total_ever_mutual"] == 0
    assert status["half_life"]["currently_mutual"] == 0
    assert status["latest_rates"] is None


def test_render_status_output(tmp_path):
    conn = _setup(tmp_path)
    _seed_accounts(conn, ["a"])

    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=["a"],
        follower_dids=["a"],
    )
    _update_edges(conn, "me", {"a"}, {"a"}, "2024-01-01T00:00:00+00:00")

    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-02T00:00:00+00:00",
        actor_did="me",
        following_dids=["a"],
        follower_dids=["a"],
    )
    _update_edges(conn, "me", {"a"}, {"a"}, "2024-01-02T00:00:00+00:00")

    status = build_status(conn, "me")
    text = render_status(status)
    assert "Mutuals: 1 current / 1 ever" in text
    assert "Median mutual lifespan:" in text
    assert "Asymmetry:" in text
    assert "Latest diff rates:" in text


def test_render_diff_includes_rates(tmp_path):
    conn = _setup(tmp_path)
    _seed_accounts(conn, ["a", "b"])

    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-01T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b"],
        follower_dids=["a", "b"],
    )
    write_reciprocity_snapshot(
        conn,
        taken_at="2024-01-02T00:00:00+00:00",
        actor_did="me",
        following_dids=["a", "b"],
        follower_dids=["b"],
    )

    result = diff_snapshots(conn, "me")
    text = render_diff(result)
    assert "Rates:" in text
    assert "breakage=" in text
