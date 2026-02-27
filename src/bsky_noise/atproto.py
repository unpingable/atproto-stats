from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any, AsyncIterator

import httpx


DEFAULT_PDS = "https://bsky.social"


@dataclass(frozen=True)
class SessionData:
    handle: str
    did: str
    accessJwt: str
    refreshJwt: str
    pds: str


@dataclass
class RequestStats:
    request_count: int = 0
    retry_count: int = 0
    rate_limit_count: int = 0
    server_error_count: int = 0
    timeout_count: int = 0
    request_error_count: int = 0
    auto_degraded_tripped: bool = False


def _pds_base(pds: str | None) -> str:
    base = (pds or DEFAULT_PDS).rstrip("/")
    return base


async def create_session(
    handle: str, app_password: str, pds: str | None = None
) -> SessionData:
    base = _pds_base(pds)
    url = f"{base}/xrpc/com.atproto.server.createSession"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, json={"identifier": handle, "password": app_password})
        resp.raise_for_status()
        data = resp.json()
        data["pds"] = base
        filtered = {
            "handle": data.get("handle"),
            "did": data.get("did"),
            "accessJwt": data.get("accessJwt"),
            "refreshJwt": data.get("refreshJwt"),
            "pds": data.get("pds"),
        }
        return SessionData(**filtered)


class XrpcClient:
    def __init__(
        self,
        access_jwt: str,
        pds: str | None = None,
        appview_base: str | None = None,
        use_appview: bool = False,
        max_concurrency: int = 4,
        verbose: bool = False,
        max_retries: int = 6,
        timeout: float = 30.0,
        degraded: bool = False,
        auto_degraded: bool = False,
    ):
        self.base = _pds_base(pds)
        self.appview_base = _pds_base(appview_base) if appview_base else None
        self.use_appview = use_appview
        self.access_jwt = access_jwt
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.verbose = verbose
        self.max_retries = max_retries
        self.timeout = timeout
        self.degraded = degraded
        self.auto_degraded = auto_degraded
        self.stats = RequestStats()

    async def _request(self, method: str, nsid: str, params: dict[str, Any] | None = None) -> Any:
        base = self.base
        if self.use_appview and self.appview_base and nsid.startswith("app.bsky."):
            base = self.appview_base
        url = f"{base}/xrpc/{nsid}"
        headers = {"Authorization": f"Bearer {self.access_jwt}"}
        params = params or {}

        backoff = 2.0 if self.degraded else 1.0
        jitter_max = 1.0 if self.degraded else 0.5
        for attempt in range(self.max_retries):
            async with self.semaphore:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    if self.verbose:
                        print(f"[xrpc] {method} {nsid} attempt={attempt+1}")
                    try:
                        self.stats.request_count += 1
                        resp = await client.request(method, url, params=params, headers=headers)
                    except httpx.RequestError as exc:
                        self.stats.retry_count += 1
                        self.stats.request_error_count += 1
                        if isinstance(exc, httpx.TimeoutException):
                            self.stats.timeout_count += 1
                        if self.verbose:
                            print(f"[xrpc] {nsid} error={exc}")
                        if self.auto_degraded:
                            self.degraded = True
                            self.stats.auto_degraded_tripped = True
                            backoff = max(backoff, 2.0)
                            jitter_max = max(jitter_max, 1.0)
                        jitter = random.uniform(0, jitter_max)
                        await asyncio.sleep(backoff + jitter)
                        backoff *= 2
                        continue
            if self.verbose:
                print(f"[xrpc] {nsid} status={resp.status_code}")
            rate_limited = resp.status_code == 429
            if resp.status_code == 403:
                body = (resp.text or "").lower()
                if "rate" in body and "limit" in body:
                    rate_limited = True
            if rate_limited or resp.status_code in (500, 502, 503, 504):
                self.stats.retry_count += 1
                if rate_limited:
                    self.stats.rate_limit_count += 1
                else:
                    self.stats.server_error_count += 1
                if self.auto_degraded:
                    self.degraded = True
                    self.stats.auto_degraded_tripped = True
                    backoff = max(backoff, 2.0)
                    jitter_max = max(jitter_max, 1.0)
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    backoff = max(backoff, float(retry_after))
                jitter = random.uniform(0, jitter_max)
                await asyncio.sleep(backoff + jitter)
                backoff *= 2
                continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()

    async def get_follows(self, actor: str, limit: int = 100) -> AsyncIterator[dict[str, Any]]:
        cursor: str | None = None
        while True:
            params = {"actor": actor, "limit": limit}
            if cursor:
                params["cursor"] = cursor
            data = await self._request("GET", "app.bsky.graph.getFollows", params=params)
            for item in data.get("follows", []):
                yield item
            cursor = data.get("cursor")
            if not cursor:
                break

    async def get_followers(self, actor: str, limit: int = 100) -> AsyncIterator[dict[str, Any]]:
        cursor: str | None = None
        while True:
            params = {"actor": actor, "limit": limit}
            if cursor:
                params["cursor"] = cursor
            data = await self._request("GET", "app.bsky.graph.getFollowers", params=params)
            for item in data.get("followers", []):
                yield item
            cursor = data.get("cursor")
            if not cursor:
                break

    async def get_author_feed(
        self, actor: str, limit: int = 100, cursor: str | None = None
    ) -> dict[str, Any]:
        params = {"actor": actor, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        return await self._request("GET", "app.bsky.feed.getAuthorFeed", params=params)
