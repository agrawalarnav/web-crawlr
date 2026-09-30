# Web Crawler

A production-quality web crawler with a two-path fetch strategy, async job queue,
and session persistence. Built as a three-part engineering assignment:

- **Part 1 (this repo)** — working demo with full crawl pipeline
- **[Part 2](docs/part2_design.md)** — scale architecture for 1 billion URLs/month
- **[Part 3](docs/part3_poc.md)** — PoC plan, blocker analysis, and release strategy

---

## Demo Architecture

```
                         Client
                            │
                    POST /crawl {url}
                            │
                            ▼
               ┌────────────────────────┐
               │  FastAPI  (Cloud Run)  │
               │                        │
               │  1. Normalize URL      │
               │     strip tracking     │
               │     params, lowercase  │
               │     domain             │
               │                        │
               │  2. Load session       │
               │     cookies from Redis │
               │     (session:{domain}) │
               │                        │
               │  3. fetch_fast         │◄── curl_cffi
               │     Chrome124 TLS      │    Chrome124
               │     fingerprint spoof  │    TLS spoof
               │                        │
               └────────────┬───────────┘
                            │
              ┌─────────────┴──────────────┐
              │                            │
      body ≥ 200 chars               thin / JS wall
      (fast path, ~70%)              (slow path, ~30%)
              │                            │
              ▼                            ▼
       Return result          Publish to RabbitMQ
       immediately            (crawl_jobs queue)
       HTTP 200                            │
                               Return 202 + job_id
                               + poll_url
                                           │
                            ┌──────────────▼──────────────┐
                            │         Worker  (VM)        │
                            │                             │
                            │  1. Load session:{domain}   │
                            │     from Redis              │
                            │     (storage_state cookies) │
                            │                             │
                            │  2. Playwright (Chromium)   │
                            │     · playwright-stealth    │
                            │     · block images / fonts  │
                            │     · networkidle wait      │
                            │                             │
                            │  3. Save session:{domain}   │
                            │     to Redis  (TTL 1h)      │
                            │                             │
                            │  4. Write result to Redis   │
                            │     job:{job_id} (TTL 1h)   │
                            │                             │
                            │  Retry up to 3× on failure  │
                            │  → DLQ (crawl_jobs.dlq)     │
                            └─────────────────────────────┘
                                           │
                            GET /result/{job_id}
                                           │
                                    Redis lookup
                                    job:{job_id}
                                           │
                               pending → 202  /  done → 200
```

### Services

| Service | Role | Where |
|---|---|---|
| **FastAPI** | REST API — normalize, fast-fetch, queue slow jobs | Cloud Run (auto-scales to zero) |
| **curl_cffi** | Fast path — Chrome124 TLS fingerprint, bypasses Akamai | In-process (API) |
| **Playwright** | Slow path — full browser render, stealth plugin | Worker (VM) |
| **RabbitMQ** | Async job queue with DLQ, 3-attempt retry | VM (Docker) |
| **Redis** | Session cookies per domain + job result store + Caching| VM (Docker) |

### Two-Path Fetch Strategy

Every URL is first attempted with `curl_cffi` impersonating Chrome 124 — it spoofs the
TLS fingerprint and HTTP/2 ALPN that browser-challenge systems use to detect bots. This
handles ~70% of URLs in ~2s each with no browser overhead.

If the returned HTML has fewer than 200 characters of body text (a JS-rendered shell or
redirect wall), the job is handed off to a Playwright worker. Playwright renders the page
in a headless Chromium instance with `playwright-stealth` applied to mask automation
signals. Session cookies and `localStorage` are persisted to Redis per domain —
subsequent visits to the same domain reuse the established session, bypassing login
walls and re-verification challenges.

### Redis Key Map

```
session:{domain}   Playwright storage_state (cookies + localStorage) — TTL 1h
job:{job_id}       Crawl result or pending status — TTL 1h
```

---

## API Reference

### POST /crawl

Submit a URL for crawling.

```bash
curl -X POST https://<API_URL>/crawl \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.example.com"}'
```

**Fast path response (HTTP 200)** — returned immediately when curl_cffi succeeds:

```json
{
  "url": "https://www.example.com",
  "normalized_url": "https://www.example.com/",
  "status_code": 200,
  "title": "Example Domain",
  "meta_description": "...",
  "topics": ["example", "domain", "internet"],
  "word_count": 42,
  "fetch_method": "httpx",
  "crawl_duration_ms": 310
}
```

**Slow path response (HTTP 202)** — returned when Playwright is needed:

```json
{
  "job_id": "3f7a9c12-...",
  "status": "pending",
  "poll_url": "/result/3f7a9c12-..."
}
```

### GET /result/{job_id}

Poll for a queued job's result.

```bash
curl https://<API_URL>/result/3f7a9c12-...
```

Returns `202` while pending, `200` with the full result when complete.

### GET /health

```bash
curl https://<API_URL>/health
# {"status": "ok", "service": "api"}
```

---

## Running Locally

**Prerequisites**: Docker, Python 3.11+

```bash
# Start Redis + RabbitMQ + API + Worker
docker compose up --build

# API available at http://localhost:8080
# RabbitMQ management UI at http://localhost:15672  (guest / guest)
```

Test a crawl:

```bash
# Fast path
curl -X POST http://localhost:8080/crawl \
  -H "Content-Type: application/json" \
  -d '{"url": "https://en.wikipedia.org/wiki/Web_crawler"}'

# Slow path (JS-rendered)
curl -X POST http://localhost:8080/crawl \
  -H "Content-Type: application/json" \
  -d '{"url": "https://old.reddit.com/r/programming/"}'

# Poll result for slow path
curl http://localhost:8080/result/<job_id>
```

**Environment variables** (`.env` file or shell):

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection |
| `HTTPX_TIMEOUT` | `10.0` | curl_cffi request timeout (seconds) |
| `PLAYWRIGHT_TIMEOUT_MS` | `30000` | Playwright navigation timeout |
| `MAX_RETRIES` | `3` | Attempts before a job moves to DLQ |
| `PROXY_URL` | *(none)* | Optional proxy (`http://user:pass@host:port`) |

---

## Deployment

The production deployment runs on GCP:

- **API** → Cloud Run (auto-scales to zero, `${PORT}` env var for port binding)
- **Worker + Redis + RabbitMQ** → single `e2-small` VM via `docker-compose.worker.yml`
- **CI/CD** → GitHub Actions (`.github/workflows/deploy.yml`)
  - Push to `main` → build API + worker images → push to Artifact Registry → deploy API to Cloud Run → SSH into VM and `docker compose up -d --build`

To provision the full GCP environment from scratch, fill in the variables at the top of `deploy.sh` (gitignored) and run it. It installs prerequisites, creates the VM, sets GitHub secrets, and triggers the first deploy.

---

## Project Structure

```
api/
  main.py          FastAPI app — normalize, fast-fetch, queue slow jobs
  models.py        Pydantic request/response models

crawler/
  fetcher.py       fetch_fast (curl_cffi) + fetch_with_browser (Playwright)
  extractor.py     Build result dict from raw HTML
  parser.py        BeautifulSoup metadata extraction
  classifier.py    YAKE keyword/topic extraction

worker/
  main.py          RabbitMQ consumer — Playwright slow path + retry logic

config.py          Settings (pydantic-settings, reads from .env)
docker-compose.yml Local dev stack (Redis + RabbitMQ + API + Worker)
docker-compose.worker.yml  Production VM stack (Redis + RabbitMQ + Worker)
Dockerfile.api     Cloud Run image
Dockerfile.worker  VM worker image
docs/
  part2_design.md  Scale architecture — 1B URLs/month
  part3_poc.md     PoC plan, blockers, release strategy
```
