from bsky_noise.db import connect, init_db, insert_events, Event
from bsky_noise.label_audit import (
    _bsky_app_url,
    _lag_bucket,
    _parse_ts,
    _fmt_lag,
    _is_stale,
    build_audit,
    render_audit,
)


def _setup(tmp_path):
    conn = connect(tmp_path / "test.db")
    init_db(conn)
    return conn


def _insert_label(conn, *, src="labeler1", uri="at://me/app.bsky.feed.post/abc",
                  val="porn", cts="2024-01-01T01:00:00+00:00", neg=0):
    conn.execute(
        """
        INSERT OR IGNORE INTO my_labels (src, uri, val, cts, neg, cid, exp, ingested_at)
        VALUES (?, ?, ?, ?, ?, NULL, NULL, '2024-01-01T02:00:00+00:00')
        """,
        (src, uri, val, cts, neg),
    )
    conn.commit()


def _insert_post_event(conn, *, did="me", uri="at://me/app.bsky.feed.post/abc",
                       created_at="2024-01-01T00:00:00+00:00"):
    insert_events(conn, [Event(did=did, uri=uri, created_at=created_at, kind="post")])
    conn.commit()


# --- _lag_bucket ---

def test_lag_bucket_realtime():
    assert _lag_bucket(300) == "realtime"  # 5 min


def test_lag_bucket_same_day():
    assert _lag_bucket(3600) == "same_day"  # 1 hour


def test_lag_bucket_late():
    assert _lag_bucket(86400 * 3) == "late"  # 3 days


def test_lag_bucket_necromancy():
    assert _lag_bucket(86400 * 10) == "necromancy"  # 10 days


# --- _parse_ts ---

def test_parse_ts_zulu():
    dt = _parse_ts("2024-01-01T00:00:00Z")
    assert dt.year == 2024
    assert dt.tzinfo is not None


def test_parse_ts_offset():
    dt = _parse_ts("2024-01-01T00:00:00+00:00")
    assert dt.year == 2024


# --- _fmt_lag ---

def test_fmt_lag_seconds():
    assert _fmt_lag(30) == "30s"


def test_fmt_lag_minutes():
    assert _fmt_lag(300) == "5m"


def test_fmt_lag_hours():
    assert _fmt_lag(7200) == "2.0h"


def test_fmt_lag_days():
    assert _fmt_lag(172800) == "2.0d"


def test_fmt_lag_none():
    assert _fmt_lag(None) == "n/a"


# --- _is_stale ---

def test_is_stale_none():
    assert _is_stale(None) is True


def test_is_stale_recent():
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    assert _is_stale(now) is False


def test_is_stale_old():
    assert _is_stale("2020-01-01T00:00:00+00:00") is True


# --- build_audit ---

def test_build_audit_empty(tmp_path):
    conn = _setup(tmp_path)
    audit = build_audit(conn, "me")
    assert audit["total_post_labels"] == 0
    assert audit["total_account_labels"] == 0
    assert audit["lag_median_s"] is None
    assert audit["labeler_stats"] == []
    assert audit["val_stats"] == []


def test_build_audit_post_label_with_event(tmp_path):
    conn = _setup(tmp_path)
    uri = "at://me/app.bsky.feed.post/abc"

    _insert_post_event(conn, uri=uri, created_at="2024-01-01T00:00:00+00:00")
    _insert_label(conn, uri=uri, cts="2024-01-01T00:05:00+00:00", val="porn")

    audit = build_audit(conn, "me")
    assert audit["total_post_labels"] == 1
    assert audit["total_account_labels"] == 0
    assert audit["lag_median_s"] == 300.0  # 5 minutes
    assert audit["bucket_counts"]["realtime"] == 1


def test_build_audit_post_label_no_event(tmp_path):
    conn = _setup(tmp_path)
    uri = "at://me/app.bsky.feed.post/xyz"

    _insert_label(conn, uri=uri, cts="2024-01-01T00:05:00+00:00")

    audit = build_audit(conn, "me")
    assert audit["total_post_labels"] == 1
    assert audit["lag_median_s"] is None  # no event to compute lag


def test_build_audit_account_label(tmp_path):
    conn = _setup(tmp_path)
    # Account labels have a DID as the URI, not a post URI
    _insert_label(conn, uri="did:plc:abc123", val="!takedown", cts="2024-01-01T00:00:00+00:00")

    audit = build_audit(conn, "me")
    assert audit["total_account_labels"] == 1
    assert audit["total_post_labels"] == 0
    assert len(audit["account_labels"]) == 1
    assert audit["account_labels"][0]["val"] == "!takedown"


def test_build_audit_negation_skipped_for_lag(tmp_path):
    conn = _setup(tmp_path)
    uri = "at://me/app.bsky.feed.post/abc"

    _insert_post_event(conn, uri=uri, created_at="2024-01-01T00:00:00+00:00")
    _insert_label(conn, uri=uri, cts="2024-01-01T00:05:00+00:00", val="porn", neg=1)

    audit = build_audit(conn, "me")
    assert audit["total_post_labels"] == 1
    # Negations are skipped for lag calculation
    assert audit["lag_median_s"] is None
    assert len(audit["labeler_stats"]) == 0


def test_build_audit_per_labeler_stats(tmp_path):
    conn = _setup(tmp_path)
    uri1 = "at://me/app.bsky.feed.post/a"
    uri2 = "at://me/app.bsky.feed.post/b"

    _insert_post_event(conn, uri=uri1, created_at="2024-01-01T00:00:00+00:00")
    _insert_post_event(conn, uri=uri2, created_at="2024-01-01T00:00:00+00:00")

    _insert_label(conn, src="labeler1", uri=uri1, cts="2024-01-01T00:10:00+00:00", val="porn")
    _insert_label(conn, src="labeler2", uri=uri2, cts="2024-01-01T12:00:00+00:00", val="spam")

    # Seed account so handle resolves
    conn.execute(
        "INSERT INTO accounts (did, handle, display_name, first_seen, last_seen) VALUES (?, ?, ?, ?, ?)",
        ("labeler1", "mod.bsky.social", "Mod", "2024-01-01", "2024-01-01"),
    )
    conn.commit()

    audit = build_audit(conn, "me")
    assert len(audit["labeler_stats"]) == 2

    # labeler2 has higher median lag (12h vs 10m), should be sorted first (descending by median)
    assert audit["labeler_stats"][0]["src"] == "labeler2"
    assert audit["labeler_stats"][1]["src"] == "labeler1"
    assert audit["labeler_stats"][1]["handle"] == "mod.bsky.social"


def test_build_audit_per_val_stats(tmp_path):
    conn = _setup(tmp_path)
    uri1 = "at://me/app.bsky.feed.post/a"
    uri2 = "at://me/app.bsky.feed.post/b"
    uri3 = "at://me/app.bsky.feed.post/c"

    for uri in [uri1, uri2, uri3]:
        _insert_post_event(conn, uri=uri, created_at="2024-01-01T00:00:00+00:00")

    _insert_label(conn, uri=uri1, cts="2024-01-01T00:05:00+00:00", val="porn")
    _insert_label(conn, uri=uri2, cts="2024-01-01T00:05:00+00:00", val="porn")
    _insert_label(conn, uri=uri3, cts="2024-01-01T00:05:00+00:00", val="spam")

    audit = build_audit(conn, "me")
    assert len(audit["val_stats"]) == 2
    # Sorted by label_count desc: porn=2, spam=1
    assert audit["val_stats"][0]["val"] == "porn"
    assert audit["val_stats"][0]["label_count"] == 2
    assert audit["val_stats"][1]["val"] == "spam"


def test_build_audit_bucket_counts(tmp_path):
    conn = _setup(tmp_path)
    uri1 = "at://me/app.bsky.feed.post/a"
    uri2 = "at://me/app.bsky.feed.post/b"
    uri3 = "at://me/app.bsky.feed.post/c"

    for uri in [uri1, uri2, uri3]:
        _insert_post_event(conn, uri=uri, created_at="2024-01-01T00:00:00+00:00")

    # realtime: 5 min lag
    _insert_label(conn, uri=uri1, cts="2024-01-01T00:05:00+00:00", val="a")
    # same_day: 6 hour lag
    _insert_label(conn, uri=uri2, cts="2024-01-01T06:00:00+00:00", val="b")
    # necromancy: 10 day lag
    _insert_label(conn, uri=uri3, cts="2024-01-11T00:00:00+00:00", val="c")

    audit = build_audit(conn, "me")
    assert audit["bucket_counts"]["realtime"] == 1
    assert audit["bucket_counts"]["same_day"] == 1
    assert audit["bucket_counts"]["necromancy"] == 1
    assert audit["bucket_counts"]["late"] == 0


def test_build_audit_top_late_labels(tmp_path):
    conn = _setup(tmp_path)

    # Create 3 post labels all with > 24h lag (default retro_threshold_s)
    for i in range(3):
        uri = f"at://me/app.bsky.feed.post/{i}"
        _insert_post_event(conn, uri=uri, created_at="2024-01-01T00:00:00+00:00")
        # 2 days + i hours lag
        _insert_label(conn, uri=uri, cts=f"2024-01-03T{i:02d}:00:00+00:00", val="spam")

    audit = build_audit(conn, "me")
    assert audit["retroactive_count"] == 3
    assert len(audit["top_late_labels"]) == 3
    # Sorted by lag descending
    assert audit["top_late_labels"][0]["lag_s"] > audit["top_late_labels"][1]["lag_s"]


def test_build_audit_window_days_filter(tmp_path):
    conn = _setup(tmp_path)
    uri_old = "at://me/app.bsky.feed.post/old"
    uri_new = "at://me/app.bsky.feed.post/new"

    _insert_label(conn, uri=uri_old, cts="2020-01-01T00:00:00+00:00", val="old")
    _insert_label(conn, uri=uri_new, cts="2026-03-07T00:00:00+00:00", val="new")

    audit = build_audit(conn, "me", window_days=30)
    assert audit["total_post_labels"] == 1


def test_build_audit_retroactive_threshold(tmp_path):
    conn = _setup(tmp_path)
    uri = "at://me/app.bsky.feed.post/abc"
    _insert_post_event(conn, uri=uri, created_at="2024-01-01T00:00:00+00:00")
    # 2 hour lag
    _insert_label(conn, uri=uri, cts="2024-01-01T02:00:00+00:00", val="test")

    # Default threshold is 24h, so 2h lag should NOT be retroactive
    audit = build_audit(conn, "me")
    assert audit["retroactive_count"] == 0

    # But with a 1h threshold, it should be retroactive
    audit = build_audit(conn, "me", retro_threshold_s=3600)
    assert audit["retroactive_count"] == 1


# --- poll health ---

def test_build_audit_poll_health(tmp_path):
    conn = _setup(tmp_path)
    conn.execute(
        "INSERT INTO label_poll_meta (key, value) VALUES ('last_ok_at', '2026-03-08T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO label_poll_meta (key, value) VALUES ('last_err', '')"
    )
    conn.commit()

    audit = build_audit(conn, "me")
    assert audit["poll_health"]["last_ok_at"] == "2026-03-08T00:00:00+00:00"
    assert audit["poll_health"]["last_err"] == ""
    assert audit["poll_health"]["stale"] is False


def test_build_audit_poll_health_stale(tmp_path):
    conn = _setup(tmp_path)
    conn.execute(
        "INSERT INTO label_poll_meta (key, value) VALUES ('last_ok_at', '2020-01-01T00:00:00+00:00')"
    )
    conn.commit()

    audit = build_audit(conn, "me")
    assert audit["poll_health"]["stale"] is True


# --- render_audit ---

def test_render_audit_empty(tmp_path):
    conn = _setup(tmp_path)
    audit = build_audit(conn, "me")
    text = render_audit(audit)
    assert "Label Audit" in text
    assert "Post labels: 0" in text
    assert "Account labels: 0" in text


def test_render_audit_with_data(tmp_path):
    conn = _setup(tmp_path)
    uri = "at://me/app.bsky.feed.post/abc"
    _insert_post_event(conn, uri=uri, created_at="2024-01-01T00:00:00+00:00")
    _insert_label(conn, uri=uri, cts="2024-01-01T00:05:00+00:00", val="porn")

    audit = build_audit(conn, "me")
    text = render_audit(audit)
    assert "Post labels: 1" in text
    assert "Median lag: 5m" in text
    assert "realtime=1" in text


def test_render_audit_account_labels(tmp_path):
    conn = _setup(tmp_path)
    _insert_label(conn, uri="did:plc:abc123", val="!takedown", cts="2024-01-01T00:00:00+00:00")

    audit = build_audit(conn, "me")
    text = render_audit(audit)
    assert "Account labels (1):" in text
    assert "!takedown" in text


def test_render_audit_show_late(tmp_path):
    conn = _setup(tmp_path)
    uri = "at://me/app.bsky.feed.post/abc"
    _insert_post_event(conn, uri=uri, created_at="2024-01-01T00:00:00+00:00")
    # 3 day lag -> retroactive
    _insert_label(conn, uri=uri, cts="2024-01-04T00:00:00+00:00", val="spam")

    audit = build_audit(conn, "me")

    # Without show_late
    text = render_audit(audit)
    assert "Top retroactive" not in text

    # With show_late
    text = render_audit(audit, show_late=True)
    assert "Top retroactive labels" in text


def test_render_audit_labeler_stats(tmp_path):
    conn = _setup(tmp_path)
    uri = "at://me/app.bsky.feed.post/abc"
    _insert_post_event(conn, uri=uri, created_at="2024-01-01T00:00:00+00:00")
    _insert_label(conn, src="labeler1", uri=uri, cts="2024-01-01T00:10:00+00:00", val="porn")

    conn.execute(
        "INSERT INTO accounts (did, handle, display_name, first_seen, last_seen) VALUES (?, ?, ?, ?, ?)",
        ("labeler1", "mod.bsky.social", "Mod", "2024-01-01", "2024-01-01"),
    )
    conn.commit()

    audit = build_audit(conn, "me")
    text = render_audit(audit)
    assert "Per labeler:" in text
    assert "mod.bsky.social" in text


def test_render_audit_val_stats(tmp_path):
    conn = _setup(tmp_path)
    uri = "at://me/app.bsky.feed.post/abc"
    _insert_post_event(conn, uri=uri, created_at="2024-01-01T00:00:00+00:00")
    _insert_label(conn, uri=uri, cts="2024-01-01T00:10:00+00:00", val="porn")

    audit = build_audit(conn, "me")
    text = render_audit(audit)
    assert "Per label value:" in text
    assert "porn:" in text


# --- _bsky_app_url ---

def test_bsky_app_url_post():
    url = _bsky_app_url("at://did:plc:abc123/app.bsky.feed.post/xyz789")
    assert url == "https://bsky.app/profile/did:plc:abc123/post/xyz789"


def test_bsky_app_url_non_post():
    assert _bsky_app_url("did:plc:abc123") is None
    assert _bsky_app_url("at://did:plc:abc/app.bsky.graph.follow/xyz") is None


# --- show_labels rendering ---

def test_render_audit_show_labels(tmp_path):
    conn = _setup(tmp_path)
    uri = "at://did:plc:me/app.bsky.feed.post/abc123"
    _insert_post_event(conn, did="did:plc:me", uri=uri, created_at="2024-01-01T00:00:00+00:00")
    _insert_label(conn, src="labeler1", uri=uri, cts="2024-01-01T00:05:00+00:00", val="porn")

    conn.execute(
        "INSERT INTO accounts (did, handle, display_name, first_seen, last_seen) VALUES (?, ?, ?, ?, ?)",
        ("labeler1", "mod.bsky.social", "Mod", "2024-01-01", "2024-01-01"),
    )
    conn.commit()

    audit = build_audit(conn, "did:plc:me")

    # Without show_labels
    text = render_audit(audit)
    assert "Labeled posts" not in text
    assert "bsky.app" not in text

    # With show_labels
    text = render_audit(audit, show_labels=True)
    assert "Labeled posts (1):" in text
    assert "[porn]" in text
    assert "mod.bsky.social" in text
    assert "https://bsky.app/profile/did:plc:me/post/abc123" in text
    assert "lag=5m" in text
    assert "[realtime]" in text


def test_render_audit_show_labels_negated(tmp_path):
    conn = _setup(tmp_path)
    uri = "at://did:plc:me/app.bsky.feed.post/abc123"
    _insert_label(conn, uri=uri, cts="2024-01-01T00:05:00+00:00", val="porn", neg=1)

    audit = build_audit(conn, "did:plc:me")
    text = render_audit(audit, show_labels=True)
    assert "(negated)" in text


def test_render_audit_show_late_includes_links(tmp_path):
    conn = _setup(tmp_path)
    uri = "at://did:plc:me/app.bsky.feed.post/abc123"
    _insert_post_event(conn, did="did:plc:me", uri=uri, created_at="2024-01-01T00:00:00+00:00")
    # 3 day lag
    _insert_label(conn, uri=uri, cts="2024-01-04T00:00:00+00:00", val="spam")

    audit = build_audit(conn, "did:plc:me")
    text = render_audit(audit, show_late=True)
    assert "https://bsky.app/profile/did:plc:me/post/abc123" in text


def test_build_audit_post_labels_detail_has_bsky_url(tmp_path):
    conn = _setup(tmp_path)
    uri = "at://did:plc:me/app.bsky.feed.post/abc123"
    _insert_post_event(conn, did="did:plc:me", uri=uri, created_at="2024-01-01T00:00:00+00:00")
    _insert_label(conn, uri=uri, cts="2024-01-01T00:05:00+00:00", val="porn")

    audit = build_audit(conn, "did:plc:me")
    assert len(audit["post_labels_detail"]) == 1
    detail = audit["post_labels_detail"][0]
    assert detail["bsky_url"] == "https://bsky.app/profile/did:plc:me/post/abc123"
    assert detail["val"] == "porn"


def test_build_audit_detail_sorted_newest_first(tmp_path):
    conn = _setup(tmp_path)
    uri1 = "at://me/app.bsky.feed.post/old"
    uri2 = "at://me/app.bsky.feed.post/new"

    _insert_label(conn, uri=uri1, cts="2024-01-01T00:00:00+00:00", val="a")
    _insert_label(conn, uri=uri2, cts="2024-01-02T00:00:00+00:00", val="b")

    audit = build_audit(conn, "me")
    assert audit["post_labels_detail"][0]["val"] == "b"  # newer first
    assert audit["post_labels_detail"][1]["val"] == "a"
