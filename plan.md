# BrightEdge Engineering Assignment — Plan

## Overview

Design and implement a web crawler service that:
- Crawls any URL, extracts metadata, and classifies topics (Part 1)
- Scales to billions of URLs with reliability and cost efficiency (Part 2)
- Has a clear PoC breakdown, blockers, and release plan (Part 3)

---

## Technology Decisions

| Component | Decision | Rationale |
|-----------|----------|-----------|
| Language | Python | Best crawling ecosystem |
| Browser automation | Playwright | Async-native, network interception, multi-page concurrency per browser |
| HTML parsing | BeautifulSoup4 + lxml | Battle-tested, flexible |
| Body extraction | Trafilatura | Strips nav/ads/footers better than raw BS4 |
| Topic classification | YAKE | Offline, statistical, no model download, deterministic |
| Language detection | langdetect | Lightweight, fallback to `<html lang>` |
| API framework | FastAPI + uvicorn | Async-native, matches Playwright's async model |
| PoC job queue | RabbitMQ (CloudAMQP free tier) | Simpler than Kafka, sufficient for PoC async job handling |
| Production queue | Kafka (Confluent Cloud / MSK) | Replay on worker crash, domain partitioning at billion-URL scale |
| Rate limiting | Redis token bucket per domain | Centralized, tunable, O(1) per request |
| Hot storage | PostgreSQL + PgBouncer + read replicas | SQL flexibility, full-text search, sufficient write throughput at our scale |
| Cold storage | S3 Parquet + Athena | Cost-effective analytics over billions of records |
| Raw HTML | gzip compressed + S3 lifecycle (30d → Glacier) | Enables reprocessing if parser logic improves |
| Dedup (URL) | Redis bloom filter | O(1), ~1GB RAM for 1B URLs at 1% false positive |
| Dedup (content) | SHA256(body_text) compared against PostgreSQL | Avoids re-writing unchanged pages |
| Deployment | Google Cloud Run | Docker-native, scale-to-zero, 2M requests/month free |
| Monitoring | Grafana + CloudWatch | Dashboards, alerting, structured JSON logs |

---

## Part 1 — Core Crawler

### Project Structure

```
brightedge/
├── crawler/
│   ├── fetcher.py       # httpx fast path + Playwright slow path
│   ├── parser.py        # BeautifulSoup4: metadata, tags, structure
│   ├── extractor.py     # Trafilatura: clean body text
│   └── classifier.py    # YAKE: topic extraction, langdetect
├── api/
│   ├── main.py          # FastAPI app: POST /crawl, GET /result/{id}, GET /health
│   └── models.py        # Pydantic request/response schemas
├── worker/
│   └── main.py          # RabbitMQ consumer: Playwright crawl jobs
├── tests/
│   ├── test_fetcher.py
│   ├── test_parser.py
│   └── test_classifier.py
├── docs/
│   ├── part2_design.md
│   └── part3_poc.md
├── Dockerfile.api        # api-service image (512MB target)
├── Dockerfile.worker     # worker-service image (Playwright + Chromium, 2GB)
├── docker-compose.yml    # local dev: api + worker + RabbitMQ + Redis
├── requirements.txt
└── plan.md
```

### Fetch Strategy

```
Request received
      │
      ▼
httpx GET (fast, no JS, 10s timeout)
      │
      ├── Success + body has content → proceed to parse
      │
      └── Empty body / SPA detected / blocked → escalate to Playwright
                │
                ▼
           Playwright (Chromium)
           - Block images/CSS/fonts (route interception, ~50% faster)
           - Wait: networkidle, max 30s
           - playwright-stealth patches applied
           - Realistic user-agent, viewport, timezone
```

### Job Handling Strategy

```
POST /crawl
      │
      ├── httpx fast path (10s timeout)
      │     → Success + body has content: return 200 directly (no queue)
      │
      └── Slow / JS-heavy / empty body detected:
            → Generate job_id (UUID)
            → Publish to RabbitMQ: { job_id, url }
            → Store in Redis: job:{id} = { status: "pending" }
            → Return 202 + { job_id, poll_url: "/result/{job_id}" }

RabbitMQ: crawl_jobs queue
      └── Playwright Worker (worker-service on Cloud Run)
            → Consume message
            → Run Playwright crawl
            → Parse + classify result
            → Write to Redis: job:{id} = { status: "complete", result: {...} } (TTL: 1h)
            → ack on success / nack + requeue on failure (max 3 retries via DLQ)

GET /result/{job_id}
      → Redis lookup
      → "pending"  → 202
      → "complete" → 200 + result
      → "failed"   → 200 + { status: "failed", reason: "..." }
      → not found  → 404
```

### RabbitMQ Setup (CloudAMQP free tier)

```
Exchange:  crawl_exchange (direct)
Queue:     crawl_jobs       → Playwright worker consumes
Queue:     crawl_jobs.dlq   → messages that failed 3 times land here

Message payload:
{
  "job_id": "uuid",
  "url": "https://...",
  "attempt": 1
}

Per-message TTL: 3600s (matches Redis job TTL)
```

### Deployment Shape (Two Cloud Run Services)

```
api-service     (512MB RAM)  — FastAPI, httpx, publishes to RabbitMQ
worker-service  (2GB RAM)    — RabbitMQ consumer + Playwright
```

Clean separation: API stays fast and lightweight; worker handles memory-heavy Playwright independently.

### API Contract

```
POST /crawl
Body: { "url": "https://..." }

Response 200 (fast path — static page):
{
  "url": "https://...",
  "normalized_url": "https://...",
  "title": "...",
  "meta_description": "...",
  "meta_keywords": [...],
  "og_tags": { "og:title": "...", "og:image": "..." },
  "canonical_url": "...",
  "h1_tags": [...],
  "language": "en",
  "topics": ["kitchen appliances", "toasters", "cuisinart"],
  "word_count": 1240,
  "http_status": 200,
  "crawl_duration_ms": 3200,
  "crawled_at": "2026-05-08T12:00:00Z"
}

Response 202 (slow path — JS-heavy page routed via RabbitMQ):
{
  "job_id": "uuid",
  "status": "pending",
  "poll_url": "/result/uuid"
}

GET /result/{job_id}    → poll for Playwright job result
GET /health             → liveness probe for both services
```

### Metadata Extracted

| Field | Source |
|-------|--------|
| title | `<title>` tag, fallback `og:title` |
| meta_description | `<meta name="description">`, fallback `og:description` |
| meta_keywords | `<meta name="keywords">` |
| og_tags | All `og:*` and `twitter:*` meta tags |
| canonical_url | `<link rel="canonical">` |
| h1_tags | All `<h1>` elements |
| language | `<html lang>` attribute, fallback langdetect on body |
| structured_data | JSON-LD `<script type="application/ld+json">` |
| body_text | Trafilatura extraction |
| topics | YAKE top 10 phrases from title + meta + body (truncated 2000 chars) |

### Deployment (Google Cloud Run — Two Services)

**api-service** (Dockerfile.api):
```dockerfile
FROM python:3.11-slim-bookworm
COPY requirements.txt .
RUN pip install -r requirements.txt   # no Playwright here
COPY crawler/ crawler/
COPY api/ api/
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
```
Cloud Run config: 512MB RAM, 1 vCPU, concurrency 80, min 0, max 3

**worker-service** (Dockerfile.worker):
```dockerfile
FROM python:3.11-slim-bookworm
RUN apt-get install -y <Chromium system deps>
RUN pip install playwright && playwright install chromium
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY crawler/ crawler/
COPY worker/ worker/
CMD ["python", "-m", "worker.main"]
```
Cloud Run config: 2GB RAM, 2 vCPU, concurrency 4, min 1 (keep Playwright warm), max 3

**Local dev** (docker-compose.yml):
```
services: api, worker, rabbitmq (management UI on :15672), redis
```

---

## Part 2 — Scale Architecture (Billions of URLs)

### The Numbers

```
1 billion URLs/month = 385 URLs/second sustained

httpx workers (70% static pages, 1s avg):  ~270M URLs → ~5 worker instances
Playwright workers (30% JS pages, 5s avg): ~130M URLs → ~217 worker instances
```

### Full System Diagram

```
INGESTION
  Text file → S3 → Lambda trigger ─┐
  MySQL/Aurora ─────────────────────┴→ URL Normalizer → Dedup Filter (Redis Bloom) → Kafka

KAFKA (Confluent Cloud / MSK)
  Topic: urls-to-crawl
  Partitioned by hash(domain) → per-domain ordering, natural rate limiting
  Topic: crawl-results
  Topic: failed-urls (DLQ)

WORKERS (ECS Fargate, auto-scale on queue depth)
  httpx workers  (512MB, 50 concurrent)
  Playwright workers (2GB, 4 concurrent pages per instance)
  Both: check Redis token bucket before each domain request

PROCESSING PIPELINE (per crawled URL)
  Raw HTML → BeautifulSoup4 (metadata) + Trafilatura (body) + YAKE (topics)
           → SHA256(body_text) → content dedup check
           → Write PostgreSQL + S3

STORAGE
  Hot:  PostgreSQL (metadata, topics, structured fields)
        + PgBouncer (connection pooling)
        + Read replicas (for API query scaling)
  Cold: S3 Parquet partitioned by domain/year/month + Athena
  Raw:  S3 gzip (30d Standard → Glacier Deep Archive)
  Cache: Redis LRU (URL → result, TTL 1h)

API LAYER (Google Cloud Run)
  GET /metadata?url=... → Redis cache → PostgreSQL → return
```

### Redis Key Map

```
bloom:urls          → Bloom filter (1B capacity, 1% false positive rate)
rate:{domain}       → Token bucket { tokens, last_refill }
cache:{url_hash}    → LRU result cache (TTL: 1h)
circuit:{domain}    → Circuit breaker { fail_count, tripped_at }
robots:{domain}     → Parsed robots.txt rules (TTL: 24h)
```

### PostgreSQL Schema

```sql
CREATE TABLE crawl_metadata (
    url_hash          CHAR(64)     NOT NULL,  -- SHA256 of normalized URL
    url               TEXT         NOT NULL,
    domain            TEXT         NOT NULL,
    year_month        CHAR(7)      NOT NULL,  -- '2026-07'
    crawled_at        TIMESTAMPTZ  NOT NULL,
    last_seen         TIMESTAMPTZ  NOT NULL,
    content_hash      CHAR(64),              -- SHA256 of body text
    http_status       SMALLINT,
    title             TEXT,
    meta_desc         TEXT,
    meta_keywords     TEXT[],
    og_tags           JSONB,
    canonical_url     TEXT,
    language          CHAR(10),
    h1_tags           TEXT[],
    topics            TEXT[],
    word_count        INT,
    crawl_duration_ms INT,
    raw_html_s3_path  TEXT,
    crawl_status      TEXT,       -- SUCCESS / BLOCKED / PERMANENT_FAIL
    PRIMARY KEY (url_hash, year_month)
) PARTITION BY LIST (year_month);

CREATE INDEX idx_domain_month  ON crawl_metadata (domain, year_month);
CREATE INDEX idx_topics        ON crawl_metadata USING GIN (topics);
CREATE INDEX idx_crawl_status  ON crawl_metadata (crawl_status);
```

### Failure Handling

```
Transient failure (timeout, network):
  Retry 1 → 5s delay
  Retry 2 → 30s delay
  Retry 3 → 120s delay
  Still failing → failed-urls Kafka topic (DLQ)

Soft block (Cloudflare, CAPTCHA):
  Escalate to Playwright + stealth
  Still blocked → mark BLOCKED, send to DLQ
  [Production]: proxy rotation hook

Hard fail (404, 410, DNS):
  Mark PERMANENT_FAIL in PostgreSQL
  No retry

Circuit breaker per domain:
  Window: 5 minutes sliding
  Threshold: >50% failure rate
  Action: pause domain 15 minutes, alert on-call
```

### SLOs / SLAs

| Metric | SLO |
|--------|-----|
| Crawl throughput | 95% of queued URLs crawled within 24h |
| API read latency | p95 < 200ms (PostgreSQL hot path) |
| Monthly batch completion | 100% of input URLs attempted within calendar month |
| Data freshness | Metadata available within 5min of crawl completion |
| Crawl error rate | < 2% permanently failed (excl. 404/blocked) |
| API availability | 99.9% uptime |

### Monitoring (Grafana Dashboards)

```
Row 1 - Throughput:   URLs crawled/min | httpx vs Playwright ratio | Queue depth by domain
Row 2 - Health:       Error rate by type | Circuit breakers tripped | Worker crash rate
Row 3 - Performance:  p50/p95/p99 crawl duration | Playwright render time | Redis cache hit rate
Row 4 - Storage:      PostgreSQL write rate | S3 bytes/day | Bloom filter fill %
Row 5 - Cost:         Fargate vCPU hours | S3 storage GB | Kafka message volume
```

---

## Part 3 — PoC Breakdown & Release Plan

### PoC Go/No-Go Criteria

| Metric | Go | No-Go |
|--------|----|-------|
| Crawl success rate | > 90% across test URLs | < 80% |
| Playwright accuracy | Correct metadata on 5/5 test URLs | Any major field missing |
| YAKE topic relevance | Topics make semantic sense (human review) | Topics are noise |
| API p95 latency | < 5s for live crawl | > 15s |
| Dedup (URL) | 0 false negatives | Any duplicate writes |
| Dedup (content) | Correctly skips unchanged pages | Overwrites unchanged content |

### Known Blockers

**Critical:**

1. **Anti-bot on Amazon/Walmart**
   - Both use Cloudflare + proprietary fingerprinting
   - `playwright-stealth` mitigates partially
   - Production fix: proxy rotation (Bright Data / Oxylabs)
   - Estimate: 2 days to tune, document as production blocker

2. **Playwright Docker image size + cold start**
   - Chromium + deps = ~1.3GB image, ~10-15s cold start
   - Mitigation: min-instances=1 on Cloud Run
   - Estimate: 1 day Dockerfile optimization

3. **Playwright memory under concurrent load**
   - 4 concurrent pages can spike to 2GB+ on heavy sites
   - Mitigation: memory limits, worker restart on OOM
   - Estimate: 1 day load testing

**Medium:**

4. **robots.txt compliance at ingestion speed** — lazy parse + Redis cache (TTL 24h), half day
5. **YAKE quality on short pages** — fallback to meta keywords if body < 100 words, 1 day
6. **RabbitMQ message loss on worker crash** — nack + requeue prevents loss; DLQ catches repeat failures, half day

**Trivial (solved problems):**

| Item | Solution | Time |
|------|----------|------|
| URL normalization | `urllib.parse` + tracking param strip list | 2h |
| Bloom filter sizing | `rbloom` library, pre-calculated for 1B | 1h |
| PostgreSQL partitioning | `pg_partman`, automated monthly partitions | 2h |
| gzip HTML compression | Python `gzip` stdlib + S3 `ContentEncoding` | 1h |
| langdetect short text | Fallback to `<html lang>` if body < 50 chars | 1h |
| S3 lifecycle policy | AWS console, 30d Standard → Glacier | 30min |

### Implementation Schedule

```
Week 1 — Core Crawler
  Day 1-2: Fetcher (httpx + Playwright + stealth + robots.txt)
  Day 3:   Parser + Extractor (BS4 + Trafilatura + YAKE + langdetect)
  Day 4:   Content dedup (SHA256 + PostgreSQL lookup)
  Day 5:   FastAPI wrapper + integration tests on all 5 test URLs

Week 2 — Infrastructure
  Day 1:   PostgreSQL (schema, partitioning, indexes, PgBouncer)
  Day 2:   Redis (bloom filter, token bucket, LRU cache, circuit breaker)
  Day 3:   RabbitMQ (exchange, queues, DLQ, CloudAMQP setup) + worker-service
  Day 4:   S3 + lifecycle + Athena table definition
  Day 5:   Dockerfile.api + Dockerfile.worker + docker-compose + Cloud Run deploy + CI/CD (GitHub Actions)

Week 3 — Scale Testing + PoC Validation
  Day 1-2: Load testing (Locust/k6), find Playwright concurrency ceiling
  Day 3:   Failure scenario testing (Kafka replay, circuit breaker, dedup)
  Day 4:   Monitoring setup (Grafana dashboards, CloudWatch alarms)
  Day 5:   PoC review + Go/No-Go decision

Week 4+ — Production Hardening (post Go)
  Proxy rotation, ECS Fargate auto-scaling, Aurora Multi-AZ,
  Terraform IaC, runbooks, security (VPC, IAM, Secrets Manager),
  batch PostgreSQL writes (buffer 100 rows, flush every 5s)
```

### Release Stages

| Stage | When | Criteria |
|-------|------|----------|
| Internal Alpha | End Week 2 | 5 test URLs crawl correctly, manual validation |
| PoC Demo | End Week 3 | Go/No-Go review, live demo, Grafana under light load |
| Limited Beta | Week 5-6 | 1M URL batch, 24h monitoring, real failure modes |
| Production | Week 8 | Full pipeline, runbooks live, on-call rotation set |

### What Makes This Release High Quality

1. Go/No-Go criteria defined before building — no "works on my machine" releases
2. Failure scenarios tested explicitly — Kafka replay, circuit breaker, dedup all have dedicated test days
3. Proxy rotation documented as a blocker — not hidden, stakeholders know the risk upfront
4. Gradual scale ramp — alpha → PoC → 1M → 1B, never 0 to billion overnight
5. Runbooks before production — on-call team knows what to do when queue backs up or a domain blocks

---

## Test URLs

```
https://www.amazon.com/Cuisinart-CPT-122-Compact-2-Slice-Toaster/dp/B009GQ034C/
http://blog.rei.com/camp/how-to-introduce-your-indoorsy-friend-to-the-outdoors/
https://www.cnn.com/2025/09/23/tech/google-study-90-percent-tech-jobs-ai
```

## AI Tools Used

- **Claude Code (claude-sonnet-4-6)**: Architecture design, implementation planning, code generation across all three parts of the assignment.
