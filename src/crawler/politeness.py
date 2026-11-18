"""Per-domain pacing: minimum gap, Retry-After, backoff, and parking.

The goal is not to out-run a site's defences; it is to crawl at a pace the
site tolerates so the crawl keeps working tomorrow. Every decision here
slows down, none of them speed up.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime

from crawler.detect import Verdict


def parse_retry_after(value: str | None) -> float | None:
    """Retry-After is either delay-seconds or an HTTP date."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, when.timestamp() - time.time())


@dataclass
class DomainState:
    next_allowed: float = 0.0
    strikes: int = 0  # consecutive throttling/blocking responses
    parked_until: float = 0.0
    park_reason: str = ""
    crawl_delay: float = 0.0  # from robots.txt Crawl-delay; never go faster than this


@dataclass
class Politeness:
    min_delay: float = 1.0
    max_backoff: float = 300.0
    park_seconds: float = 3600.0
    max_strikes: int = 5
    jitter: float = 0.25
    clock: object = field(default=time.monotonic, repr=False)
    _state: dict[str, DomainState] = field(default_factory=dict, repr=False)

    def _now(self) -> float:
        return float(self.clock())  # type: ignore[operator]

    def state(self, domain: str) -> DomainState:
        return self._state.setdefault(domain, DomainState())

    def wait_time(self, domain: str) -> float:
        """Seconds until this domain may be fetched again (0 = now)."""
        s = self.state(domain)
        now = self._now()
        return max(0.0, max(s.next_allowed, s.parked_until) - now)

    def is_parked(self, domain: str) -> bool:
        return self.state(domain).parked_until > self._now()

    def record(self, domain: str, verdict: Verdict, retry_after: float | None = None) -> None:
        s = self.state(domain)
        now = self._now()
        base = max(self.min_delay, s.crawl_delay)
        gap = base * (1 + random.uniform(0, self.jitter))  # noqa: S311
        if verdict in (Verdict.RATE_LIMITED, Verdict.FORBIDDEN, Verdict.SOFT_BLOCK, Verdict.SERVER_ERROR):
            s.strikes += 1
            backoff = min(self.max_backoff, base * (2**s.strikes))
            gap = max(gap, backoff, retry_after or 0.0)
            if s.strikes >= self.max_strikes:
                self.park(domain, f"{s.strikes}_consecutive_{verdict.value}")
        elif verdict == Verdict.CAPTCHA:
            # A challenge means the site has decided we look automated. Solving
            # or rotating around it escalates; stopping is what keeps access.
            self.park(domain, "captcha_challenge")
        else:
            s.strikes = 0
        s.next_allowed = now + gap

    def park(self, domain: str, reason: str) -> None:
        s = self.state(domain)
        s.parked_until = self._now() + self.park_seconds
        s.park_reason = reason
