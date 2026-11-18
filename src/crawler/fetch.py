"""Fetch cheaply first, render only when the page proves it needs a browser.

Plain HTTP is ~50x cheaper than a headless browser and leaves a smaller
fingerprint, so the browser is an escalation path, not the default.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from typing import Any, ClassVar
from urllib.parse import urljoin, urlsplit

import httpx

from crawler.identity import Identity


@dataclass
class FetchResult:
    url: str
    final_url: str
    status: int
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""
    render_mode: str = "http"
    elapsed: float = 0.0
    error: str | None = None  # transport-level failure: timeout, redirect loop, DNS...
    redirect_to: str | None = None  # off-domain redirect, handed back to the frontier
    hops: int = 0


class HttpFetcher:
    def __init__(self, timeout: float = 10.0, max_redirects: int = 5, max_bytes: int = 5_000_000) -> None:
        self.timeout = timeout
        self.max_redirects = max_redirects
        self.max_bytes = max_bytes
        self._clients: dict[str, httpx.AsyncClient] = {}

    def _client(self, identity: Identity) -> httpx.AsyncClient:
        if identity.domain not in self._clients:
            self._clients[identity.domain] = httpx.AsyncClient(
                headers=identity.headers(),
                cookies=identity.cookies,
                proxy=identity.proxy,
                # Redirects are followed by hand: each hop is a real request to the
                # site, so it must be paced, and loops must stop at the first repeat.
                follow_redirects=False,
                timeout=self.timeout,
                trust_env=False,  # proxies come from the identity only, never from ambient env vars
            )
        client = self._clients[identity.domain]
        client.cookies = identity.cookies  # pick up cookies the browser may have added
        return client

    async def fetch(self, url: str, identity: Identity, hop_delay: float = 0.0) -> FetchResult:
        start = time.monotonic()
        current, visited, hops = url, {url}, 0
        try:
            client = self._client(identity)
            while True:
                status, headers, body = await self._get(client, current, identity)
                location = headers.get("location")
                if status not in (301, 302, 303, 307, 308) or not location:
                    return FetchResult(
                        url, current, status, headers, body, "http", time.monotonic() - start, hops=hops
                    )
                target = urljoin(current, location)
                if urlsplit(target).netloc.lower() != identity.domain:
                    return FetchResult(
                        url,
                        current,
                        status,
                        headers,
                        "",
                        "http",
                        time.monotonic() - start,
                        redirect_to=target,
                        hops=hops,
                    )
                if target in visited or hops >= self.max_redirects:
                    return FetchResult(
                        url,
                        current,
                        status,
                        headers,
                        "",
                        "http",
                        time.monotonic() - start,
                        error="redirect_loop",
                        hops=hops,
                    )
                visited.add(target)
                hops += 1
                current = target
                await asyncio.sleep(hop_delay)
        except httpx.TimeoutException:
            return FetchResult(url, current, 0, error="timeout", elapsed=time.monotonic() - start)
        except httpx.TransportError as exc:
            return FetchResult(
                url, current, 0, error=f"transport:{type(exc).__name__}", elapsed=time.monotonic() - start
            )

    async def _get(
        self, client: httpx.AsyncClient, url: str, identity: Identity
    ) -> tuple[int, dict[str, str], str]:
        async with client.stream("GET", url) as resp:
            chunks: list[bytes] = []
            size = 0
            async for chunk in resp.aiter_bytes():
                size += len(chunk)
                if size > self.max_bytes:
                    break
                chunks.append(chunk)
            identity.cookies.update(resp.cookies)
            body = b"".join(chunks).decode(resp.encoding or "utf-8", errors="replace")
            return resp.status_code, {k.lower(): v for k, v in resp.headers.items()}, body

    async def close(self) -> None:
        await asyncio.gather(*(c.aclose() for c in self._clients.values()))
        self._clients.clear()


class BrowserFetcher:
    """Headless Chromium via Playwright, one browser context per identity."""

    BLOCKED_RESOURCES: ClassVar[frozenset[str]] = frozenset({"image", "media", "font"})

    def __init__(self, timeout: float = 15.0, executable_path: str | None = None) -> None:
        self.timeout = timeout
        self.executable_path = executable_path or os.environ.get("CRAWLER_CHROME_PATH") or None
        self._pw: Any = None
        self._browser: Any = None
        self._contexts: dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def _ensure(self) -> None:
        async with self._lock:
            if self._browser is None:
                from playwright.async_api import async_playwright

                self._pw = await async_playwright().start()
                self._browser = await self._pw.chromium.launch(
                    headless=True,
                    executable_path=self.executable_path,
                    args=["--no-sandbox", "--disable-dev-shm-usage"],
                )

    async def _context(self, identity: Identity) -> Any:
        if identity.domain not in self._contexts:
            ctx = await self._browser.new_context(
                user_agent=identity.user_agent,
                locale=identity.accept_language.split(",")[0],
                proxy={"server": identity.proxy} if identity.proxy else None,
            )

            async def _route(route: Any) -> None:
                if route.request.resource_type in self.BLOCKED_RESOURCES:
                    await route.abort()
                else:
                    await route.continue_()

            await ctx.route("**/*", _route)
            self._contexts[identity.domain] = ctx
        ctx = self._contexts[identity.domain]
        cookies = identity.to_playwright_cookies()
        if cookies:
            await ctx.add_cookies(cookies)
        return ctx

    async def fetch(self, url: str, identity: Identity) -> FetchResult:
        start = time.monotonic()
        await self._ensure()
        ctx = await self._context(identity)
        page = await ctx.new_page()
        try:
            resp = await page.goto(url, wait_until="networkidle", timeout=self.timeout * 1000)
            body = await page.content()
            identity.absorb_playwright_cookies(await ctx.cookies())
            status = resp.status if resp else 0
            headers = await resp.all_headers() if resp else {}
            return FetchResult(
                url, page.url, status, dict(headers), body, "browser", time.monotonic() - start
            )
        except Exception as exc:  # noqa: BLE001 - Playwright raises its own error tree
            name = type(exc).__name__
            return FetchResult(
                url,
                url,
                0,
                render_mode="browser",
                elapsed=time.monotonic() - start,
                error="timeout" if "Timeout" in name else f"browser:{name}",
            )
        finally:
            await page.close()

    async def close(self) -> None:
        for ctx in self._contexts.values():
            await ctx.close()
        self._contexts.clear()
        if self._browser is not None:
            await self._browser.close()
            await self._pw.stop()
            self._browser = self._pw = None
