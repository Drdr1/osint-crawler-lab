"""FastAPI control plane on top of the same pipeline."""

from __future__ import annotations

import httpx

from conftest import DATABASE_URL, Rig
from crawler.api import create_app


async def test_start_crawl_and_read_results(rig: Rig) -> None:
    app = create_app(DATABASE_URL, rig.producer, max_per_pattern=5)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://api") as client:
            assert (await client.get("/healthz")).json() == {"status": "ok"}
            bad = await client.post("/crawls", json={"seeds": ["javascript:alert(1)"]})
            assert bad.status_code == 422

            r = await client.post(
                "/crawls",
                json={
                    "seeds": [rig.site.base + "/#top", rig.site.base + "/"],  # same page twice
                    "allowed_domains": [rig.site.main_host],
                    "max_depth": 1,
                    "max_pages": 50,
                },
            )
            assert r.status_code == 202 and r.json()["accepted"] == 1
            crawl_id = r.json()["crawl_id"]

            await rig.worker.run_until_idle(deadline=90)

            summary = (await client.get(f"/crawls/{crawl_id}")).json()
            assert summary["outcomes"]["ok"] >= 5 and summary["deferred"] == 0
            blocked = (
                await client.get(f"/crawls/{crawl_id}/pages", params={"outcome": "robots_disallowed"})
            ).json()
            assert [p["url"] for p in blocked] == [rig.site.base + "/private/secret"]
            js = [
                p
                for p in (await client.get(f"/crawls/{crawl_id}/pages")).json()
                if p["url"].endswith("/js-only")
            ]
            assert js[0]["render_mode"] == "browser"
            # localhost was out of scope for this crawl, so the captcha host was never touched.
            assert rig.site.log.for_host(rig.site.other_host) == []
            assert (await client.get("/crawls/00000000-0000-0000-0000-000000000000")).status_code == 404
