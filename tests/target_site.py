"""A deliberately difficult website, served locally for the tests.

It throttles, blocks, challenges, traps and hides content behind JavaScript,
and it records every request it receives, so the tests can assert on what
the crawler actually did to the site (including what it did NOT fetch).
"""

from __future__ import annotations

import asyncio
import hashlib
import socket
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse


@dataclass
class Hit:
    host: str
    path: str
    at: float
    user_agent: str


@dataclass
class SiteLog:
    hits: list[Hit] = field(default_factory=list)
    counters: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def count(self, path: str, host: str | None = None) -> int:
        return sum(1 for h in self.hits if h.path == path and (host is None or h.host == host))

    def for_host(self, host: str) -> list[Hit]:
        return [h for h in self.hits if h.host == host]


def page(title: str, body: str, extra_head: str = "") -> HTMLResponse:
    return HTMLResponse(
        f"<!doctype html><html><head><title>{title}</title>{extra_head}</head>"
        f"<body><h1>{title}</h1>{body}</body></html>"
    )


FILLER = "<p>" + ("Open-source information about the subject of this page. " * 6) + "</p>"


def build_app(log: SiteLog, other_host: str) -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def record(request: Request, call_next):  # type: ignore[no-untyped-def]
        host = request.headers.get("host", "")
        path = request.url.path + (f"?{request.url.query}" if request.url.query else "")
        log.hits.append(Hit(host, path, time.monotonic(), request.headers.get("user-agent", "")))
        return await call_next(request)

    @app.get("/robots.txt")
    async def robots() -> PlainTextResponse:
        return PlainTextResponse("User-agent: *\nDisallow: /private/\n")

    @app.get("/")
    async def home(request: Request) -> HTMLResponse:
        links = [
            "/static/1",
            "/static/1#section",
            "/static/1?utm_source=newsletter",
            "/static/2",
            "/js-only",
            "/flaky",
            "/guard/start",
            "/soft-block",
            "/dup-a",
            "/dup-b",
            "/private/secret",
            "/calendar?month=1",
            "/loop/",
            "/gone",
            "/redirect-loop",
            "/error-once",
            "/slow",
            f"http://{other_host}/captcha",
            "https://example.org/elsewhere",
            "mailto:press@example.org",
            "/nofollow-me",
        ]
        anchors = "".join(
            f'<a href="{u}" rel="nofollow">x</a>' if u == "/nofollow-me" else f'<a href="{u}">{u}</a>'
            for u in links
        )
        return page("Home", FILLER + anchors)

    @app.get("/static/{n}")
    async def static(n: int) -> HTMLResponse:
        return page(f"Static {n}", FILLER + '<a href="/">home</a>')

    @app.get("/js-only")
    async def js_only() -> HTMLResponse:
        # Plain HTTP sees an empty shell. Only a real browser finds the link to /static/3.
        return HTMLResponse(
            "<!doctype html><html><head><title>App</title></head><body><div id=root></div>"
            "<noscript>Please enable JavaScript to use this app.</noscript>"
            "<script>document.getElementById('root').innerHTML ="
            "'<h1>Rendered by JavaScript</h1><p>"
            + "Dynamic profile data loaded client side. " * 8
            + '</p><a href="/static/3">hidden link</a>\';</script></body></html>'
        )

    @app.get("/flaky")
    async def flaky(request: Request) -> Response:
        log.counters["flaky"] += 1
        if log.counters["flaky"] <= 2:
            return PlainTextResponse("slow down", status_code=429, headers={"Retry-After": "1"})
        return page("Flaky but fine", FILLER)

    @app.get("/guard/start")
    async def guard_start(request: Request) -> HTMLResponse:
        ua = request.headers.get("user-agent", "")
        token = hashlib.sha256(ua.encode()).hexdigest()[:16]
        resp = page("Guarded entry", FILLER + '<a href="/guard/area">enter</a>')
        resp.set_cookie("visitor", token)
        return resp

    @app.get("/guard/area")
    async def guard_area(request: Request) -> Response:
        # Only a visitor that keeps the same cookie AND the same user agent gets in.
        ua = request.headers.get("user-agent", "")
        expected = hashlib.sha256(ua.encode()).hexdigest()[:16]
        if request.cookies.get("visitor") != expected:
            return PlainTextResponse("forbidden", status_code=403)
        return page("Guarded area", FILLER)

    @app.get("/soft-block")
    async def soft_block() -> HTMLResponse:
        return page("Access denied", "<p>Your request has been blocked due to unusual traffic.</p>")

    @app.get("/dup-a")
    @app.get("/dup-b")
    async def duplicate() -> HTMLResponse:
        return page("Mirrored article", FILLER, '<link rel="canonical" href="/dup-a">')

    @app.get("/private/secret")
    async def secret() -> HTMLResponse:
        return page("Should never be fetched", FILLER)

    @app.get("/calendar")
    async def calendar(month: int = 1) -> HTMLResponse:
        return page(f"Calendar {month}", FILLER + f'<a href="/calendar?month={month + 1}">next</a>')

    @app.get("/loop/{rest:path}")
    async def loop(rest: str) -> HTMLResponse:
        return page("Loop", FILLER + '<a href="loop/">deeper</a>')

    @app.get("/gone")
    async def gone() -> Response:
        return PlainTextResponse("not here", status_code=404)

    @app.get("/redirect-loop")
    async def redirect_loop() -> RedirectResponse:
        return RedirectResponse("/redirect-loop", status_code=302)

    @app.get("/error-once")
    async def error_once() -> Response:
        log.counters["error-once"] += 1
        if log.counters["error-once"] == 1:
            return PlainTextResponse("boom", status_code=500)
        return page("Recovered", FILLER)

    @app.get("/slow")
    async def slow() -> HTMLResponse:
        await asyncio.sleep(3)
        return page("Too slow", FILLER)

    @app.get("/captcha")
    async def captcha() -> HTMLResponse:
        return page("Check", '<div class="g-recaptcha" data-sitekey="x"></div><p>Verify you are human</p>')

    @app.get("/nofollow-me")
    async def nofollow() -> HTMLResponse:
        return page("Should not be followed", FILLER)

    return app


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class TargetSite:
    def __init__(self) -> None:
        self.port = free_port()
        self.main_host = f"127.0.0.1:{self.port}"
        self.other_host = f"localhost:{self.port}"
        self.log = SiteLog()
        config = uvicorn.Config(
            build_app(self.log, self.other_host), host="127.0.0.1", port=self.port, log_level="warning"
        )
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    @property
    def base(self) -> str:
        return f"http://{self.main_host}"

    def start(self) -> None:
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started:
            if time.monotonic() > deadline:
                raise RuntimeError("target site did not start")
            time.sleep(0.05)

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)
