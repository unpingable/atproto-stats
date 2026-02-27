from datetime import datetime, timezone

from bsky_noise.candidates import (
    _extract_features,
    _score_interaction,
    pick_candidates,
    render_pollblue_text,
)
from bsky_noise.db import connect, init_db, upsert_candidate_pool, write_candidate_score


def test_extract_features_and_interaction_score():
    items = [
        {
            "post": {
                "record": {
                    "createdAt": "2026-02-27T00:00:00Z",
                    "reply": {"parent": {"uri": "at://did:plc:other/app.bsky.feed.post/1"}},
                },
                "likeCount": 5,
                "replyCount": 2,
                "repostCount": 1,
            }
        },
        {
            "post": {
                "record": {"createdAt": "2026-02-26T00:00:00Z", "text": "hi"},
                "likeCount": 1,
                "replyCount": 0,
                "repostCount": 0,
            }
        },
        {
            "reason": {"$type": "app.bsky.feed.reasonRepost"},
            "post": {"record": {"createdAt": "2026-02-25T00:00:00Z"}},
        },
    ]
    f = _extract_features(items, "did:plc:candidate")
    assert f.sample_size == 3
    assert f.posts == 1
    assert f.replies == 1
    assert f.reposts == 1
    assert f.replies_to_others_rate == 1.0
    assert 0.0 <= _score_interaction(f) <= 1.0


def test_pick_candidates_from_scores(tmp_path):
    conn = connect(tmp_path / "candidates.db")
    init_db(conn)
    actor = "did:plc:actor"
    now = datetime.now(timezone.utc).isoformat()

    for i in range(8):
        did = f"did:plc:{i}"
        upsert_candidate_pool(
            conn,
            actor_did=actor,
            did=did,
            handle=f"h{i}.bsky.social",
            display_name=f"H {i}",
            in_followers=True,
            in_follows=False,
            seen_at=now,
        )
        write_candidate_score(
            conn,
            actor_did=actor,
            did=did,
            mode="interaction",
            scored_at=now,
            sample_size=50,
            score=1.0 - (i * 0.05),
            repost_ratio=0.1,
            replies_to_others_rate=0.7,
            conversation_breadth=0.4 + i * 0.01,
            engagement=0.3,
            recency_days=2.0,
            posts=10,
            replies=10,
            reposts=1,
        )

    conn.commit()
    picks = pick_candidates(conn, actor, mode="interaction", k=4)
    assert len(picks) == 4
    assert len({p["did"] for p in picks}) == 4

    rendered = render_pollblue_text(picks, "interaction")
    assert "Orbit shortlist (interaction): pick one." in rendered
