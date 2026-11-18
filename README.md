# OSINT crawler lab

A small, tested version of the core of an OSINT crawling platform. It
crawls polite, block-aware and restart-safe, on FastAPI, asyncio,
Playwright, BeautifulSoup, Kafka, PostgreSQL and MinIO.

Its tests run against a local **hostile test site** that throttles, blocks,
challenges, traps and hides content behind JavaScript. The site logs every
request it receives, so the tests check both what the crawler stored and
what it actually did to the site: what it fetched, how often, how fast,
and what it never touched.

This is a public work sample showing how I approach this kind of system.
It is not a client system.

## Layout

| Path | What it does |
| --- | --- |
| `src/crawler/urls.py` | URL normalisation (fragments, tracking parameters, ports, parameter order), scope rules, URL shapes for trap budgets. |
| `src/crawler/detect.py` | Labels every response before trusting it: ok, JS shell, CAPTCHA, soft block, rate limited, forbidden, not found, server error. |
| `src/crawler/politeness.py` | Per-domain pacing: minimum gap, robots `Crawl-delay`, `Retry-After`, exponential backoff, parking. |
| `src/crawler/identity.py` | One consistent identity per domain (user agent, proxy, cookies), shared by the HTTP client and the browser. |
| `src/crawler/fetch.py` | httpx first, Playwright only when the page proves it needs JavaScript. Redirects are followed by hand. |
| `src/crawler/robots.py` | robots.txt per RFC 9309: a 4xx means no rules, a 5xx or no answer means stop and check again later. |
| `src/crawler/extract.py` | BeautifulSoup parsing, canonical link, `nofollow`, trap detection. |
| `src/crawler/queue.py` | Kafka producer and consumer (manual commits), plus an in-memory version with the same behaviour for fast tests. |
| `src/crawler/store.py`, `models.py` | PostgreSQL: frontier de-duplication, page budget, transactional outbox, deferred work, domain parking. |
| `src/crawler/blobs.py` | Raw HTML in MinIO (S3 API), keyed by content hash. |
| `src/crawler/worker.py` | The consumer loop. |
| `src/crawler/api.py` | FastAPI: start crawls, read results, inspect parked domains. |
| `tests/target_site.py` | The hostile test site. |

## How one URL flows

```
Kafka crawl.frontier (key = domain)
  -> already final in Postgres?          yes: acknowledge, no fetch
  -> domain parked?                      yes: defer until the park ends
  -> robots.txt allows?                  no: record robots_disallowed, never request it
  -> politeness gate                     short wait in-line, long wait deferred (partition keeps moving)
  -> httpx GET, redirects followed by hand with a pause between hops
  -> verdict  --JS shell-->  Chromium render with the same identity, verdict again
  -> ok: HTML to MinIO by hash; page row + new links + outbox rows in ONE transaction
  -> commit the Kafka offset; outbox publisher sends new links to Kafka
```

The frontier is keyed by domain, so each domain stays on one partition and
its pacing is decided in one place. Delivery is at least once, so the
database makes processing idempotent: a redelivered URL whose outcome is
final is acknowledged without being fetched again.

New links go into an outbox table in the same transaction as the page and
are published afterwards. A crash between the two can only cause a
re-publish, which the idempotency above absorbs. It can never lose a link.

## Casebook

Each case is asserted in `tests/test_crawl.py` against the hostile site.

| Case | What the crawler does | How the test proves it |
| --- | --- | --- |
| `/page`, `/page#x`, `/page?utm_source=y` | Normalised to one URL | The site saw exactly one request |
| JavaScript-only page | Plain HTTP sees an empty shell, so it is rendered in Chromium with the same identity | The page is stored with `render_mode=browser`, and a link that exists only after rendering was found and crawled |
| 429 + `Retry-After: 1` | Deferred, retried after the server's stated delay, then succeeds | Gaps between attempts were at least 1s |
| Cookie-gated area (cookie tied to the user agent) | Passes, because cookies and user agent stay consistent per domain | A control test drops the cookie and gets a 403 |
| 200 "access denied" page | Labelled a soft block, retried with backoff, never stored as content | `content_sha256` is null and attempts are capped |
| robots.txt `Disallow` | Recorded as `robots_disallowed` | The site never received the request |
| Mirrored pages | One stored object | Both URLs point at the same content hash, so storage keeps one copy |
| Endless calendar `?month=n+1` | Capped by a per-URL-shape budget | At most 5 calendar requests |
| `/loop/loop/loop/...` | Stopped by its URL shape | No request with more than 3 repeated segments |
| 404 | Recorded once | One request |
| Redirect loop | Stopped at the first repeated URL | Recorded as `redirect_loop` |
| Transient 500 | Retried, recovers | Two requests, outcome `ok` |
| Slow page beyond the timeout | Limited retries, then `failed_timeout` | Three requests, then stops |
| CAPTCHA | Domain parked; retried only after the park, at most `max_attempts` times. The crawler never solves CAPTCHAs or rotates identities to get past them | Gaps between challenges were at least the park time; the park is stored in Postgres |
| Off-site and `nofollow` links | Never fetched | Absent from the site log |
| Overall pacing | At least `min_delay` between requests to a host, including robots.txt and redirect hops | Checked across every request in the site log |
| Crash before the offset commit | Redelivered URLs are acknowledged without a second fetch | No duplicate rows, and the request count is unchanged |
| Page budget | Hard cap, enforced in the database | Exactly `max_pages` URLs enqueued |
| Worker restart | A parked domain stays parked | A fresh worker defers instead of fetching |
| Malformed message in the frontier | Sent to `crawl.dlq` with the reason; the partition keeps moving | The real message behind it is still crawled |

`tests/test_kafka.py` runs on a real broker in CI. It shows that same-key
messages stay in order, that uncommitted messages are redelivered to a new
consumer, and that a full crawl works through Kafka.

### Two bugs the tests caught

First, the initial version let httpx follow redirects automatically. The pacing
assertion failed: a redirect loop hit the site six times in about 10 ms,
because each hop is a real request that skips the politeness gate.
Redirects are now followed by hand, with a pause between hops. Loops stop
at the first repeated URL, and redirects to another domain go back to the
frontier, where scope rules and that domain's pacing apply.

Second, on the real Kafka broker in CI, the crawl test read leftover
messages from an earlier test that had no `crawl_id`. The worker crashed
on them, so one bad message could have stopped its partition for good.
Malformed messages now go to a dead-letter topic with the reason, and
processing continues.

## Anti-bot stance

The approach is to be a visitor the site tolerates, not to break through
its defences: one consistent identity per domain, pacing that only ever
slows down, robots.txt honoured, and CAPTCHAs treated as "stop" signals.
Proxies are supported per identity and are sticky per domain (rotating
mid-session looks more like a bot, not less). They are only ever set
explicitly; ambient proxy environment variables are ignored.

## Run it

```bash
pip install -r requirements-dev.txt
python -m playwright install --with-deps chromium
docker compose up -d
export CRAWLER_TEST_DATABASE_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/crawl
export CRAWLER_TEST_S3_ENDPOINT=http://127.0.0.1:9000 KAFKA_BOOTSTRAP=127.0.0.1:9092
ruff check src tests migrations && mypy && pytest -v
```

Without `KAFKA_BOOTSTRAP` the Kafka tests are skipped and everything else
runs on the in-memory queue. Without `CRAWLER_TEST_S3_ENDPOINT` the tests
use moto in place of MinIO.

To run the services: `alembic upgrade head`, then `python -m crawler api`
and `python -m crawler worker`. Settings are listed in `src/crawler/__main__.py`.

## What a production version would add

- Multiple workers per partition group, with metrics on lag, outcomes per
  domain, and blocked or parked rates.
- Per-domain rules from config (custom delays, render-always lists, login flows).
- Change detection on re-crawls (ETag / Last-Modified, content-hash diff).
- Browser pool limits and crash recovery for Chromium under memory pressure.
- Structured extraction per site type, feeding search or entity resolution.
