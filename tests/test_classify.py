from bsky_noise.classify import classify_feed_item


def test_classify_repost():
    item = {"reason": {"$type": "app.bsky.feed.reasonRepost"}, "post": {}}
    assert classify_feed_item(item) == "repost"


def test_classify_reply():
    item = {"post": {"record": {"reply": {"root": {}, "parent": {}}}}}
    assert classify_feed_item(item) == "reply"


def test_classify_post():
    item = {"post": {"record": {"text": "hello"}}}
    assert classify_feed_item(item) == "post"
