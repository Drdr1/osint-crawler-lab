"""One consistent identity per domain.

Sites score consistency: a session whose cookies, user agent, and exit IP
all stay stable looks like one visitor; one that switches any of them
between requests looks like a bot farm. So each domain gets exactly one
identity for the life of the worker, and the plain HTTP client and the
headless browser share it, including cookies set by either one.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from http.cookiejar import Cookie

import httpx


@dataclass
class Identity:
    domain: str
    user_agent: str
    proxy: str | None = None
    accept_language: str = "en-GB,en;q=0.9"
    cookies: httpx.Cookies = field(default_factory=httpx.Cookies)

    def headers(self) -> dict[str, str]:
        return {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": self.accept_language,
        }

    # -- cookie hand-off between httpx and Playwright -----------------------

    def to_playwright_cookies(self) -> list[dict[str, object]]:
        out: list[dict[str, object]] = []
        for c in self.cookies.jar:
            out.append(
                {
                    "name": c.name,
                    "value": c.value or "",
                    "domain": c.domain or self.domain.split(":")[0],
                    "path": c.path or "/",
                    "secure": bool(c.secure),
                    "httpOnly": False,
                }
            )
        return out

    def absorb_playwright_cookies(self, cookies: list[dict[str, object]]) -> None:
        for pc in cookies:
            self.cookies.jar.set_cookie(
                Cookie(
                    version=0,
                    name=str(pc["name"]),
                    value=str(pc["value"]),
                    port=None,
                    port_specified=False,
                    domain=str(pc["domain"]),
                    domain_specified=True,
                    domain_initial_dot=False,
                    path=str(pc.get("path", "/")),
                    path_specified=True,
                    secure=bool(pc.get("secure")),
                    expires=None,
                    discard=True,
                    comment=None,
                    comment_url=None,
                    rest={},
                )
            )


class IdentityPool:
    """Assigns identities deterministically, so restarts keep the same mapping."""

    def __init__(self, user_agents: list[str], proxies: list[str] | None = None) -> None:
        if not user_agents:
            raise ValueError("at least one user agent is required")
        self.user_agents = user_agents
        self.proxies = proxies or []
        self._by_domain: dict[str, Identity] = {}

    def _pick(self, domain: str, options: list[str]) -> str:
        index = int(hashlib.sha256(domain.encode()).hexdigest(), 16) % len(options)
        return options[index]

    def for_domain(self, domain: str) -> Identity:
        if domain not in self._by_domain:
            self._by_domain[domain] = Identity(
                domain=domain,
                user_agent=self._pick(domain, self.user_agents),
                proxy=self._pick(domain, self.proxies) if self.proxies else None,
            )
        return self._by_domain[domain]
