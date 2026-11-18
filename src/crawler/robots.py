"""robots.txt, read the way RFC 9309 says to read it.

4xx means no rules (crawl allowed). 5xx or an unreachable file means the
site is unhealthy, so treat everything as disallowed and check again
later: crawling a struggling site is how you get blocked.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from urllib.robotparser import RobotFileParser

from crawler.fetch import HttpFetcher
from crawler.identity import Identity


@dataclass
class RobotsRules:
    parser: RobotFileParser | None  # None = everything allowed
    disallow_all: bool
    crawl_delay: float | None
    fetched_at: float

    def allows(self, url: str, agent: str) -> bool:
        if self.disallow_all:
            return False
        return True if self.parser is None else self.parser.can_fetch(agent, url)


class RobotsCache:
    def __init__(
        self, fetcher: HttpFetcher, agent_token: str, ttl: float = 3600.0, unhealthy_ttl: float = 300.0
    ) -> None:
        self.fetcher = fetcher
        self.agent_token = agent_token
        self.ttl = ttl
        self.unhealthy_ttl = unhealthy_ttl
        self._cache: dict[str, RobotsRules] = {}

    async def rules(self, scheme: str, identity: Identity) -> tuple[RobotsRules, bool]:
        """Return (rules, fetched_now). fetched_now lets the caller count the request."""
        cached = self._cache.get(identity.domain)
        now = time.monotonic()
        if cached and now - cached.fetched_at < (self.unhealthy_ttl if cached.disallow_all else self.ttl):
            return cached, False
        result = await self.fetcher.fetch(f"{scheme}://{identity.domain}/robots.txt", identity)
        if result.error or result.status >= 500:
            rules = RobotsRules(None, True, None, now)
        elif result.status >= 400:
            rules = RobotsRules(None, False, None, now)
        else:
            parser = RobotFileParser()
            parser.parse(result.body.splitlines())
            delay = parser.crawl_delay(self.agent_token)
            rules = RobotsRules(parser, False, float(delay) if delay else None, now)
        self._cache[identity.domain] = rules
        return rules, True

    async def allowed(self, url: str, scheme: str, identity: Identity) -> tuple[bool, float | None, bool]:
        rules, fetched = await self.rules(scheme, identity)
        return rules.allows(url, self.agent_token), rules.crawl_delay, fetched
