from __future__ import annotations

from typing import Any


def classify_feed_item(item: dict[str, Any]) -> str:
    reason = item.get("reason")
    if reason and reason.get("$type") == "app.bsky.feed.reasonRepost":
        return "repost"

    post = item.get("post", {})
    record = post.get("record", {})
    if record.get("reply") is not None:
        return "reply"

    return "post"
