# Part 2 — Scale Architecture: Billions of URLs

---

## Back-of-the-Envelope Estimation

### Throughput

```
Target: 1 billion URLs / month
      = 385 URLs / second sustained
      = 33M URLs / day

Split by fetch path:
  Fast path  (curl_cffi, ~2s avg):   70% → 270M URLs/month
  Slow path  (Playwright, ~10s avg): 30% → 130M URLs/month

Fast workers needed:
  270M ÷ (30d × 86,400s) = 104 URLs/s
  Each instance: 50 concurrent × (1/2s) = 25 URLs/s
  → 104 / 25 = ~5 instances (baseline); scale to 50 at peak

Playwright workers needed:
  130M ÷ (30d × 86,400s) = 50 URLs/s
  Each instance: 4 concurrent × (1/10s) = 0.4 URLs/s
  → 50 / 0.4 = ~125 instances (auto-scale with queue depth)
```

### Storage

```
Average page size:
  Raw HTML (gzip):          ~25 KB
  Metadata row (PostgreSQL): ~2 KB
  Metadata row (Parquet):   ~500 bytes (columnar + Snappy)

Per month (1B URLs):
  Raw HTML  → 1B × 25 KB  = 25 TB   (S3 Standard → Glacier after 30d)
  PostgreSQL → 1B × 2 KB   = 2 TB    (3 months hot = 6 TB total)
  Parquet   → 1B × 500 B  = 500 GB  (S3, partitioned by domain/month)

Annual costs:
  Parquet cold storage:  500 GB × 12 = 6 TB  @ $0.023/GB  = ~$140/year
  Raw HTML (Glacier):    25 TB  × 12         @ $0.004/GB  = ~$1,200/year
```

### Network

```
Inbound (fetching pages):  385 URLs/s × 100 KB avg = 38.5 MB/s = 3.3 TB/day

Redis Bloom filter:        1.2 GB RAM at 1% false-positive rate for 1B URLs

Kafka throughput:          385 msg/s × 1 KB/msg = 385 KB/s input ≈ 33 GB/day
```

---

## System Architecture

```
                      ┌────────────────────────────────────────────┐
                      │                 INGESTION                  │
                      │                                            │
  Text file (S3) ─────► File Reader (stream line-by-line, 10k/batch)
                      │        │                                   │
  MySQL (year/month) ─► MySQL Reader                              │
                      │  SELECT url, domain FROM urls              │
                      │  WHERE year_month = '2026-07'              │
                      │  ORDER BY domain  (10k rows/batch)         │
                      │        │                                   │
                      │        ▼                                   │
                      │   URL Normalizer                           │
                      │   · lowercase domain                       │
                      │   · strip tracking params (?utm_*, &ref=)  │
                      │   · resolve http → https                  │
                      │        │                                   │
                      │        ▼                                   │
                      │   Bloom Filter (Redis bloom:urls)          │
                      │   · seen this month? → skip               │
                      │        │                                   │
                      │        ▼                                   │
                      │   Priority Router                          │
                      │   · bulk input   → P3 urls-batch           │
                      │   · real-time    → P0 urls-realtime        │
                      │        │                                   │
                      │        ▼                                   │
                      │   Kafka  (partition key = hash(domain))    │
                      └──────────────────┬─────────────────────────┘
                                         │
                      ┌──────────────────▼─────────────────────────┐
                      │                 WORKERS                    │
                      │          (ECS Fargate, auto-scale)         │
                      │                                            │
                      │  ┌────────────────────────────────────┐   │
                      │  │  Fast Workers  (512 MB, 50 threads) │   │
                      │  │  curl_cffi · Chrome124 TLS spoof    │   │
                      │  │  → handles 70% of URLs              │   │
                      │  └────────────────┬───────────────────┘   │
                      │                   │ JS wall detected       │
                      │  ┌────────────────▼───────────────────┐   │
                      │  │  Playwright Workers (2 GB, 4 tabs)  │   │
                      │  │  stealth plugin · session cookies   │   │
                      │  │  → handles remaining 30%            │   │
                      │  └────────────────────────────────────┘   │
                      │                                            │
                      │  Both workers check before each request:   │
                      │  · Redis token bucket  (rate limit/domain) │
                      │  · Redis circuit breaker (per domain)      │
                      │  · robots.txt cache    (TTL 24h)           │
                      └──────────────────┬─────────────────────────┘
                                         │
                      ┌──────────────────▼─────────────────────────┐
                      │            PROCESSING PIPELINE              │
                      │                                            │
                      │  Raw HTML                                  │
                      │    ├── BeautifulSoup4  → metadata fields   │
                      │    ├── Trafilatura     → clean body text   │
                      │    └── YAKE            → topics (top 10)   │
                      │                                            │
                      │  SHA256(body_text) → content dedup         │
                      │    └── unchanged? → update last_seen only  │
                      │                                            │
                      │  → publish to Kafka: crawl-results         │
                      └──────────────────┬─────────────────────────┘
                                         │
                      ┌──────────────────▼─────────────────────────┐
                      │                 STORAGE                    │
                      │                                            │
                      │  Hot   → PostgreSQL (partitioned by month) │
                      │          PgBouncer + read replicas         │
                      │                                            │
                      │  Cold  → S3 Parquet + Athena               │
                      │          partitioned by domain/year/month  │
                      │                                            │
                      │  Raw   → S3 gzip                           │
                      │          30d Standard → Glacier            │
                      │                                            │
                      │  Cache → Redis LRU (url_hash → result, 1h) │
                      └────────────────────────────────────────────┘
```

---

## Key Design Decisions

### Kafka over RabbitMQ

RabbitMQ is sufficient for thousands of jobs/day. At 385 URLs/second it becomes the bottleneck:

| | RabbitMQ | Kafka |
|---|---|---|
| Throughput | ~50k msg/s | ~1M msg/s |
| Message replay | No | Yes (offset replay) |
| Domain partitioning | Manual routing keys | Native partition key |
| Worker crash recovery | nack + requeue | Consumer group offset |
| Ops cost at scale | High (clustering) | Low (managed MSK/Confluent) |

Partition key = `hash(domain)` ensures all URLs for a domain land on the same partition, giving natural per-domain ordering and making rate limiting stateless in the worker.

### PostgreSQL over NoSQL

- `topics TEXT[]` with a GIN index is the primary product query: `WHERE 'python' = ANY(topics)`. DynamoDB has no equivalent.
- The core access pattern is SEO metadata lookup with SQL filtering and JOINs — relational model is the right fit.
- Read scaling via replicas; write pressure solved by partitioning + 100-row batch inserts.

### Two-Tier Storage (Hot + Cold)

- PostgreSQL holds the last 3 months — nearly all product queries hit recent data.
- S3 Parquet handles historical analytics (Athena for trend reports, domain statistics).
- Raw HTML stored in S3 decoupled from metadata — if parser logic improves, reprocess from S3 without re-crawling.

---

## Crawl Politeness

Aggressive crawlers get blocked, throttled, or banned. Every domain gets its own rate limit enforced before a URL is dispatched.

### robots.txt

```
Fetched once per domain, cached in Redis (key: robots:{domain}, TTL 24h)
Checked before every URL dispatch.

Rules enforced:
  Disallow: paths       → skip URL entirely, mark PERMANENT_FAIL
  Crawl-delay: N        → override token bucket to 1/N req/s
  User-agent matching   → crawler agent checked first, then *
```

### Rate Limiting — Token Bucket per Domain

```
Default:          1 req / 2s  (0.5 tokens/s)
Large CDN sites:  up to 5 req/s  (amazon.com, walmart.com)
Unknown domains:  1 req / 5s  (conservative until behaviour observed)

Redis key:   rate:{domain}
Fields:      tokens (float), last_refill (unix timestamp)
Atomicity:   Lua script — check + decrement in one round-trip (no race condition)
```

```lua
-- Atomic token bucket: KEYS[1]=rate:{domain}, ARGV[1]=rate, ARGV[2]=now
local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
local tokens      = tonumber(bucket[1]) or tonumber(ARGV[1])
local last_refill = tonumber(bucket[2]) or tonumber(ARGV[2])

local elapsed = tonumber(ARGV[2]) - last_refill
tokens = math.min(tonumber(ARGV[1]), tokens + elapsed * tonumber(ARGV[1]))

if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HMSET', KEYS[1], 'tokens', tokens, 'last_refill', ARGV[2])
    redis.call('EXPIRE', KEYS[1], 3600)
    return 1
end
return 0
```

### User-Agent

```
User-Agent: Crawler/1.0 (+https://example.com/crawler; crawler@example.com)
```

Identifies the crawler and provides a contact address. Domains can whitelist or rate-limit by agent string.

### Backoff on Server Stress Signals

```
429 Too Many Requests   → respect Retry-After header, minimum 60s backoff
503 Service Unavailable → exponential backoff: 30s → 2min → 10min
5xx streak (>5 in 10min)→ circuit breaker trips, domain paused 15min
```

---

## URL Priority

Four priority tiers, each a separate Kafka topic. Workers always drain higher-priority topics before lower ones.

```
P0 — Real-time   /crawl API requests from users
                 SLA: result delivered within 45s end-to-end
                 Topic: urls-realtime

P1 — Fresh       New URLs from sitemaps, RSS feeds, link extraction
                 SLA: crawled within 1 hour of discovery
                 Topic: urls-fresh

P2 — Recrawl     Known URLs due for freshness check
                 SLA: crawled within 24 hours of schedule
                 Topic: urls-recrawl

P3 — Bulk        Monthly text file / MySQL input (the billion-URL batch)
                 SLA: 100% attempted within the calendar month
                 Topic: urls-batch
```

### Recrawl Scheduling

```
Change frequency is estimated from three signals, applied in priority order:

  1. sitemap <changefreq> + <lastmod>  — explicit publisher signal
  2. Content-hash diff history         — did the last 3 crawls change the content?
  3. Domain-type heuristic:
       News sites       → every 6h
       E-commerce PDPs  → every 24h
       Static blogs     → every 7d
       Docs / wikis     → every 30d
```

### Deduplication at Ingestion

```
Before any URL enters Kafka:
  1. Normalise  — lowercase domain, strip tracking params, resolve http → https
  2. Bloom filter check (Redis bloom:urls) — seen this month? → drop
  3. Content-hash check (PostgreSQL)       — content unchanged? → update last_seen
                                             only, no full re-write

Bloom filter: 1.2 GB Redis, 1B capacity, 1% false-positive rate
  Worst case: 10M URLs needlessly re-crawled per month — acceptable cost.
```

---

## Failure Handling

```
Transient (timeout, network blip)
  → Exponential backoff: 5s → 30s → 120s
  → After 3 retries → Kafka failed-urls topic (DLQ)

Soft block (Cloudflare JS challenge, CAPTCHA)
  → Escalate: curl_cffi → Playwright + stealth plugin
  → Still blocked → mark BLOCKED, send to DLQ
  → [Production] proxy rotation hook

Hard fail (404, 410, DNS NXDOMAIN)
  → Mark PERMANENT_FAIL immediately
  → No retry, no DLQ

Circuit breaker (per domain, stored in Redis)
  → Window:    5-minute sliding
  → Threshold: > 50% failure rate over 20+ requests
  → Action:    pause domain 15 minutes, fire PagerDuty P2 alert
  → Reset:     automatic after 15 minutes
```

---

## Worker Scaling

```
Trigger metric → ECS Fargate target tracking

Fast workers:
  Scale-out:   Kafka consumer lag (urls-batch + urls-recrawl) > 10,000 messages
  Max:         50 instances  (50 × 50 concurrent × 0.5 req/s = 1,250 URL/s headroom)

Playwright workers:
  Scale-out:   Kafka consumer lag > 2,000 messages  (lower threshold — each task is slower)
  Max:         300 instances  (300 × 4 concurrent × 0.1 req/s = 120 URL/s)

Scale-down cooldown: 15 minutes for both types — Playwright browser launch takes ~10s;
  thrashing between scale-down and immediate scale-out wastes more time than holding capacity.
```

---

## Redis Key Map

```
bloom:urls           Bloom filter         1B URLs, 1% false-positive, ~1.2 GB RAM
rate:{domain}        Token bucket         { tokens: float, last_refill: timestamp }
circuit:{domain}     Circuit breaker      { fail_count: int, tripped_at: timestamp }
robots:{domain}      robots.txt rules     allow/disallow paths, TTL 24h
session:{domain}     Playwright state     storage_state cookies/localStorage, TTL 1h
cache:{url_hash}     API result cache     subset of crawl_metadata row, TTL 1h
```

---

## Schema

The data layer has three tiers. All tiers use identical field names so application code never transforms field names when moving data between them.

### Field Taxonomy

| Group | Fields | Purpose |
|---|---|---|
| Identity | `url_hash`, `url`, `domain`, `year_month` | Unique record identification and partitioning |
| Crawl provenance | `crawled_at`, `last_seen`, `content_hash`, `http_status`, `fetch_method`, `crawl_duration_ms`, `raw_html_s3_path`, `crawl_status` | How and when the page was fetched |
| Content signals | `title`, `meta_description`, `meta_keywords`, `og_tags`, `canonical_url`, `language`, `h1_tags`, `structured_data`, `topics`, `word_count` | What the page contains (SEO metadata queries) |

### PostgreSQL — Hot Tier (last 3 months)

```sql
CREATE TABLE crawl_metadata (

    -- Identity
    -- url_hash: SHA256(normalized_url). CHAR(64) fixed-width for fast equality vs TEXT.
    --   Never use raw URL as PK — URLs can exceed 4KB.
    -- year_month: partition key. Composite PK means a URL recrawled next month gets
    --   a new row (full history preserved), not an upsert into the same row.
    url_hash          CHAR(64)     NOT NULL,
    url               TEXT         NOT NULL,
    domain            TEXT         NOT NULL,
    year_month        CHAR(7)      NOT NULL,   -- '2026-05'

    -- Crawl provenance
    -- crawled_at: set once at crawl time, immutable.
    -- last_seen:  updated on every successful recrawl, even when content is unchanged.
    --   The content-hash dedup path only updates this field to avoid a full row write.
    -- content_hash: SHA256(cleaned body text). NULL on any fetch failure.
    --   If this matches the prior crawl → update last_seen only. Drives the >15% dedup SLO.
    -- http_status: SMALLINT (2 bytes) rather than INT (4 bytes) — small saving across 1B rows.
    -- fetch_method: audit trail for which domains consistently route to the slow path.
    -- crawl_duration_ms: feeds p95 crawl latency dashboards and $/1000 URL cost tracking.
    -- raw_html_s3_path: pointer to gzip HTML in S3. NULL for failed crawls.
    --   Enables parser reprocessing without re-crawling.
    -- crawl_status: SUCCESS | BLOCKED | TIMEOUT | PERMANENT_FAIL
    crawled_at        TIMESTAMPTZ  NOT NULL,
    last_seen         TIMESTAMPTZ  NOT NULL,
    content_hash      CHAR(64),
    http_status       SMALLINT,
    fetch_method      TEXT,
    crawl_duration_ms INT,
    raw_html_s3_path  TEXT,
    crawl_status      TEXT,

    -- Content signals
    -- title / meta_description / canonical_url / language: scalar SEO fields.
    --   TEXT not VARCHAR — PostgreSQL stores short strings inline; VARCHAR limits add
    --   validation overhead with no storage benefit.
    -- meta_keywords / h1_tags: TEXT[] because both fields are multi-value.
    --   GIN index enables WHERE 'seo' = ANY(meta_keywords).
    -- og_tags / structured_data: JSONB. Schema varies per site — JSONB beats 20 nullable
    --   columns. GIN index supports og_tags ? 'og:image' and structured_data @> checks.
    -- topics: TEXT[]. Top-10 YAKE-extracted keywords from clean body text.
    --   Primary product query target: WHERE 'machine learning' = ANY(topics).
    --   GIN index is mandatory — B-tree cannot index array membership.
    -- word_count: proxy for content depth; used in content quality scoring.
    title             TEXT,
    meta_description  TEXT,
    meta_keywords     TEXT[],
    og_tags           JSONB,
    canonical_url     TEXT,
    language          CHAR(10),
    h1_tags           TEXT[],
    structured_data   JSONB,
    topics            TEXT[],
    word_count        INT,

    PRIMARY KEY (url_hash, year_month)
) PARTITION BY LIST (year_month);
```

### Index Strategy

Every index carries a write cost at 385 inserts/second. Each one below is justified by a named query pattern.

```sql
-- "fetch metadata for a specific URL" — the API hot path.
-- Covered by PRIMARY KEY (url_hash, year_month). No extra index needed.

-- "list all URLs crawled for domain X in month Y"
-- WHERE domain = 'example.com' AND year_month = '2026-05'
CREATE INDEX idx_domain_month ON crawl_metadata (domain, year_month);

-- "find all pages about 'python'"
-- WHERE 'python' = ANY(topics)  — GIN required; B-tree cannot index array membership.
-- GIN stores one entry per array element; lookup is O(log n) over distinct topic values.
CREATE INDEX idx_topics ON crawl_metadata USING GIN (topics);

-- "how many URLs succeeded vs failed this month?" (dashboard aggregates)
-- Low cardinality (4 values) but avoids heap fetches on large partition scans.
CREATE INDEX idx_crawl_status ON crawl_metadata (crawl_status);

-- "has this content been seen before?" (dedup check)
-- Partial index excludes NULL content_hash (all failed crawls), cutting index size ~20%.
CREATE INDEX idx_content_hash ON crawl_metadata (content_hash)
    WHERE content_hash IS NOT NULL;

-- "find pages with Product structured data"
-- structured_data @> '{"@type": "Product"}' — requires GIN on JSONB.
CREATE INDEX idx_structured_data ON crawl_metadata USING GIN (structured_data);

-- "which URLs are oldest and due for recrawl?" (recrawl scheduler)
-- Partial: only SUCCESS rows — failed URLs are not scheduled for recrawl.
CREATE INDEX idx_last_seen_success ON crawl_metadata (domain, last_seen)
    WHERE crawl_status = 'SUCCESS';
```

### Partitioning

```
Strategy:  LIST on year_month ('2026-05', '2026-06', ...)
Manager:   pg_partman — auto-creates next month's partition on the 1st of each month
Size:      ~3–4 GB per partition (1B URLs × 2 KB/row ÷ 12 months)
Hot data:  3 partitions live at any time = 9–12 GB total

Partition pruning:
  Any query with WHERE year_month = 'YYYY-MM' touches exactly one partition.
  The planner skips all others at planning time — zero runtime overhead.

Detach and archive (monthly cron, 1st of month):
  1. pg_partman detaches the 4th-oldest partition (now read-only)
  2. COPY ... TO STDOUT exports it to Parquet, uploaded to S3
  3. Partition dropped — hot tier remains at exactly 3 months

Vacuum:
  Active partitions: autovacuum runs normally.
  Detached partitions: autovacuum disabled after export — no new writes, no dead tuples.
```

### Batch Write Pattern

```python
BUFFER_SIZE    = 100   # rows
FLUSH_INTERVAL = 5     # seconds — flush whichever limit is hit first

if len(buffer) >= BUFFER_SIZE or time.time() - last_flush > FLUSH_INTERVAL:
    await conn.executemany("""
        INSERT INTO crawl_metadata (url_hash, url, domain, year_month, ...)
        VALUES ($1, $2, $3, $4, ...)
        ON CONFLICT (url_hash, year_month) DO UPDATE SET
            last_seen         = EXCLUDED.last_seen,
            content_hash      = EXCLUDED.content_hash,
            http_status       = EXCLUDED.http_status,
            crawl_status      = EXCLUDED.crawl_status,
            crawl_duration_ms = EXCLUDED.crawl_duration_ms
    """, buffer)
    buffer.clear()
    last_flush = time.time()

# Impact: 3.85 round-trips/s instead of 385 connections/s.
# PostgreSQL connection overhead ~1ms each → saves 381ms/s of pure overhead per worker.
```

### Cold Tier — S3 Parquet

Column names are identical to PostgreSQL. No field mapping required when moving data between tiers.

```
Path layout:  s3://webcrawler-data/crawls/domain={domain}/year_month={YYYY-MM}/part-*.parquet
Compression:  Snappy — faster decode than gzip at ~60% of gzip's ratio (right tradeoff for Athena)
Row group:    128 MB — Athena reads one row group per split; smaller groups = more parallelism
Encoding:     DELTA_BINARY_PACKED for timestamps; PLAIN_DICTIONARY for domain, crawl_status

Schema:
  url_hash, url, domain, year_month    STRING
  crawled_at, last_seen                INT64  (epoch ms)
  content_hash                         STRING
  http_status                          INT32
  fetch_method, crawl_status           STRING (dictionary encoded)
  topics                               LIST<STRING>
  og_tags, structured_data             STRING (JSON; Athena uses json_extract)
  word_count, crawl_duration_ms        INT32
  raw_html_s3_path                     STRING

Athena example — pages about 'python' by domain in April 2026:
  SELECT domain, COUNT(*) AS pages, AVG(word_count) AS avg_words
  FROM crawls
  WHERE year_month = '2026-04'
    AND cardinality(filter(topics, t -> t = 'python')) > 0
  GROUP BY domain
  ORDER BY pages DESC
  LIMIT 100;
```

### Cold Tier: S3 Parquet vs ClickHouse

| Dimension | S3 + Parquet + Athena | ClickHouse |
|---|---|---|
| Query latency | 5–60s (scan-based, Athena cold start) | 50ms–2s (NVMe, vectorized execution) |
| Throughput | Unlimited horizontal scale per query | Bound by node; ClickHouse Cloud scales out |
| Write pattern | Batch export (nightly Parquet dumps) | Real-time bulk insert or Kafka consumer |
| Storage cost | ~$0.023/GB/month → ~$140/year for 6 TB | ~$0.10–0.25/GB/month → 4–10× more |
| Ops overhead | Zero (Athena is serverless) | Managed (ClickHouse Cloud) or self-hosted |
| Array queries | `cardinality(filter(topics, t -> t='python'))` — full scan | `has(topics, 'python')` — bloom filter skip index |
| Data freshness | 6–12h lag (nightly export) | Seconds (streaming insert) |
| Reprocessing | Trivial — Parquet is the source | Requires re-insert into ClickHouse |
| Schema evolution | Add column; old files read as NULL | `ALTER TABLE ADD COLUMN` online |
| Best for | Monthly/quarterly batch reports, cost-sensitive analytics | Interactive dashboards, sub-second aggregations |

**Default: Parquet.** The primary use case is quarterly trend reports where 10–60s latency is acceptable. S3 + Athena costs $140/year vs $600–1,400/year for ClickHouse. The raw HTML and Parquet share one storage system (S3). Parquet files are the canonical reprocessing source — re-run YAKE on existing Parquet if extraction logic improves, no re-crawl needed.

**Add ClickHouse when** a customer-facing interactive analytics feature is required (e.g., "show all competitor pages about 'python' updated this week"). At that point ClickHouse becomes a serving layer alongside PostgreSQL, not a replacement for Parquet:

```
Kafka crawl-results
    ├── PostgreSQL consumer   → hot tier      (API lookups, p95 < 150ms)
    ├── ClickHouse consumer   → analytics tier (interactive aggregates, p95 < 2s)
    └── Parquet exporter      → cold tier      (S3 archive, unlimited history)

Query routing:
    /api/crawl/{url}           → PostgreSQL
    /api/analytics/domain/...  → ClickHouse
    Historical batch reports   → Athena on S3 Parquet
```

### Redis Cache

```
Key:    cache:{SHA256(normalized_url)}
Value:  JSON subset of crawl_metadata
TTL:    3600s (1 hour)

Cached (kept small — Redis is RAM-bound):
  url, domain, crawled_at, title, meta_description, topics, crawl_status, http_status

Not cached:
  og_tags, structured_data, h1_tags, meta_keywords  — too large, queried infrequently together
  raw_html_s3_path                                  — internal field, never surfaced by API
```

---

## SLAs and SLOs

**SLA (Service Level Agreement)** — external, customer-facing commitment written into contracts. Breach triggers a service credit. Intentionally conservative: the buffer below the SLO gives the engineering team room to absorb incidents without a contract breach.

**SLO (Service Level Objective)** — internal engineering target, tighter than the SLA. Consistently missing an SLO is a warning that the SLA is at risk. The gap between them is the error budget.

### SLA — Customer-Facing Commitments

| Service | Commitment | Window | Remedy on Breach |
|---|---|---|---|
| API availability | 99.9% uptime | Rolling 30 days | 10% credit per 0.1% below target |
| API read latency | p95 < 500ms | Rolling 7 days | 5% credit per breach week |
| Batch crawl completion | 99% of submitted URLs attempted within 72h | Per batch | Re-crawl at no charge + 5% credit |
| Data freshness | Results queryable within 15min of crawl completion | Per event, 1% sampled | Credit on > 3 events/month |
| Crawl coverage | < 5% permanently failed (excl. 404, 410, robots.txt blocks) | Per batch | Credit proportional to coverage gap |

SLAs exclude: maintenance windows (≤4h/month with 48h notice), force majeure, 404/410 URLs, robots.txt-blocked URLs, customer-submitted malformed URLs.

### SLOs — Internal Engineering Targets

#### Crawl Pipeline

| Metric | SLO | SLA | Error Budget |
|---|---|---|---|
| Batch URL throughput | 99.5% attempted within 48h | 99% within 72h | 0.5% before SLA breach |
| Transient crawl error rate | < 1% fail after 3 retries | < 5% | 4% headroom |
| Playwright escalation rate | < 35% of URLs hit slow path | — | Internal efficiency |
| Kafka consumer lag | p99 < 50k messages | — | Alert at 500k |
| Simultaneous circuit breakers | < 20 domains tripped | — | Incident at > 20 |
| DLQ drain time | Processed within 4h | — | Alert at 2h unprocessed |

#### API

| Metric | SLO | SLA | Error Budget |
|---|---|---|---|
| Availability | 99.95% | 99.9% | 0.05% burn before SLA breach |
| Read latency — cache hit | p95 < 20ms | p95 < 500ms | Large buffer |
| Read latency — PostgreSQL | p95 < 150ms | p95 < 500ms | 350ms buffer |
| P0 end-to-end latency | p95 < 45s | — | P0 queue always drained first |
| 5xx error rate | < 0.1% of requests | — | Alert at 0.5% |

#### Data Quality

| Metric | SLO | Rationale |
|---|---|---|
| Content dedup savings | > 15% of URLs skipped | Validates content-hash pipeline |
| Metadata completeness | > 95% of SUCCESS crawls have title + meta_description | Blank fields break SEO product |
| Topics extraction | > 90% of SUCCESS crawls have ≥ 1 topic | YAKE running on all content |
| Parquet export lag | < 6h after month rollover | Analytics freshness |

### Error Budget Policy

```
Error budget = (1 − SLO) measured over a rolling 30-day window.

Example — API availability SLO 99.95%:
  Budget = 0.05% × 30d × 24h × 60min = 21.6 minutes/month

Consumption bands:
  < 50%  — normal velocity; experiments and non-critical deploys allowed
  50–75% — freeze non-essential deploys; reliability work prioritized
  > 75%  — reliability work only; no feature releases
  100%   — postmortem required; feature freeze until root cause resolved

Budget resets on the 1st of each calendar month.
```

---

## Monitoring

### Tool Stack

| Tool | Role |
|---|---|
| Prometheus | Metrics collection — scrapes every 15s from all services |
| Thanos | Long-term metrics storage — 12 months, S3-backed |
| Grafana | Dashboards — defined as code (Jsonnet + Grafonnet) |
| Grafana Alerting → PagerDuty | P1/P2 alerts page on-call; P3 goes to Slack |
| Filebeat → Logstash → Elasticsearch | Log shipping and storage |
| Kibana | Log querying — KQL, saved searches, Watcher alerts |
| Confluent Control Center | Kafka-specific visibility: consumer group lag, rebalances, leader elections |
| AWS X-Ray | Distributed tracing — API → Kafka → Worker → PostgreSQL per crawl job |
| Grafana Synthetic Monitoring | External uptime probes every 60s from 5 regions |

Each tool has a narrow, non-overlapping role. Prometheus owns time-series metrics. ELK owns raw structured logs — Kibana's cross-field KQL is far more powerful than any generic log search for incident investigation at millions of lines/hour. Confluent Control Center owns Kafka internals that Prometheus metrics alone cannot replicate (partition leader elections, consumer group rebalances). X-Ray connects the dots across service boundaries for a single crawl job.

---

### Log Aggregation — ELK Stack

#### Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│  ECS Task  (API / Fast Worker / Playwright Worker)          │
│                                                             │
│  Application → stdout (structured JSON)                     │
│       │                                                     │
│  Filebeat sidecar                                           │
│  · reads container stdout via Docker socket                 │
│  · tags: ecs_task_id, service, environment, region          │
│  · batches 512 events or flushes every 1s                   │
└──────────────────────┬──────────────────────────────────────┘
                       │ TLS, compressed
                       ▼
              ┌─────────────────┐
              │    Logstash     │  (2 nodes, active-active)
              │                 │
              │  · parse JSON   │
              │  · enrich with  │
              │    geo_ip on    │
              │    domain IP    │
              │  · drop DEBUG   │
              │    in prod      │
              └────────┬────────┘
                       │
                       ▼
         ┌─────────────────────────┐
         │      Elasticsearch      │  (3-node, hot-warm-cold)
         │                         │
         │  hot  (NVMe SSD)        │  0–7d, all log levels
         │  warm (HDD)             │  8–30d, WARN + ERROR only
         │  cold (S3 snapshots)    │  31–90d, ERROR only
         └─────────────────────────┘
                       │
                       ▼
                  ┌─────────┐
                  │  Kibana │
                  └─────────┘
```

#### Log Schema

Every service emits the same JSON envelope to stdout:

```json
{
  "timestamp":    "2026-05-11T14:23:01.412Z",
  "level":        "ERROR",
  "service":      "playwright-worker",
  "instance_id":  "arn:aws:ecs:us-east-1:...:task/abc123",
  "job_id":       "job-7f3a9c",
  "url_hash":     "a3f2...",
  "domain":       "reddit.com",
  "error_type":   "blocked",
  "retry_count":  2,
  "fetch_method": "playwright",
  "duration_ms":  8420,
  "message":      "CAPTCHA wall detected after navigation"
}
```

`domain` is already present in every log line — Logstash passes the JSON through cleanly, only adding `geo_ip` enrichment and dropping DEBUG lines before they reach Elasticsearch.

#### Index Lifecycle Management (ILM)

| Phase | Duration | Nodes | Content | Action |
|---|---|---|---|---|
| hot | 0–7d | NVMe SSD | All levels | Rollover at 50 GB or 7 days. 1 primary + 1 replica. |
| warm | 8–30d | HDD | WARN + ERROR | INFO/DEBUG deleted at rollover. Force-merge to 1 segment. Read-only. |
| cold | 31–90d | S3 (frozen) | ERROR only | Searchable snapshots — data in S3, ES fetches on demand. Zero node storage cost. |
| delete | > 90d | — | — | Index deleted. |

INFO/DEBUG logs are dropped after 7 days deliberately. At 385 URLs/s, INFO volume alone exceeds 20k lines/minute. Keeping them for 90 days at hot-tier SSD rates would cost more than the entire Parquet cold store.

#### Kibana Saved Searches

```
"Blocked domain spike"  — used when the circuit breaker P2 alert fires
  KQL:  level: "ERROR" AND error_type: "blocked" AND @timestamp > now-1h
  View: count by domain, sorted desc
  Why:  Returns in < 2s from the hot node. Tells you immediately which domain
        deployed new bot protection and at what rate — no SSH, no log tailing.

"Crawl job trace"  — used when a specific job_id is flagged by a user
  KQL:  job_id: "job-7f3a9c"
  View: timestamp, service, duration_ms, fetch_method, error_type, message
  Why:  Full end-to-end trace of one job across API → Worker → result without
        needing X-Ray if the issue is at the application layer.

"OOM pattern"  — used after a Playwright worker OOM alert
  KQL:  service: "playwright-worker" AND level: "WARN"
        AND message: *memory* AND @timestamp > now-24h
  Why:  Distinguishes gradual memory leak (warnings building over hours) from
        a single large page that caused a sudden OOM.

"Retry storm"  — used when DLQ depth climbs
  KQL:  retry_count >= 2 AND @timestamp > now-30m
  View: count by domain, error_type
  Why:  DLQ filling means retries are exhausting. Shows exactly which domain
        and error type to intervene on first.
```

#### Kibana Watcher Alerts

Watcher runs on Elasticsearch and catches log-pattern signals before they escalate to Prometheus metric alerts:

```
Watcher 1 — Novel error type
  Trigger:  A distinct error_type value appears that has never been seen before
  Action:   Slack #crawl-alerts
  Why:      Sites occasionally deploy novel bot-detection (new header, new redirect pattern).
            A new error_type is the earliest possible signal — before error rates accumulate.

Watcher 2 — Per-domain error rate spike
  Trigger:  (error count for domain X in 5m) / (total requests for domain X in 5m) > 0.5
  Action:   PagerDuty P3
  Why:      Fires before the Redis circuit breaker trips (threshold: 50% over 20+ requests).
            Log-level detection is faster because log shipping latency is ~1s vs 15s Prometheus scrape.

Watcher 3 — Logstash pipeline lag
  Trigger:  Filebeat acknowledged events < 90% of emitted events for > 60s
  Action:   Slack #infra
  Why:      Logstash backpressure means logs are delayed or dropped — the monitoring
            system itself is degraded.
```

---

### Dashboard 1 — Crawl Throughput (NOC view)

**Purpose**: is the crawler keeping up?

```
Panel 1 — URLs/minute
  Metric:  rate(crawl_results_total[1m]) × 60
  Target:  23,100 URLs/min. Red below 18,000.

Panel 2 — Kafka consumer lag by topic
  Metric:  kafka_consumergroup_lag{topic=~"urls-.*"}
  Why:     Lag on urls-realtime means the P0 45s SLO is at risk.
           Lag on urls-batch during batch ingest is expected — alert only if > 6h sustained.

Panel 3 — Fast vs Playwright split
  Metric:  rate(crawl_results_total{fetch_method="playwright"}[5m])
           / rate(crawl_results_total[5m])
  Target:  < 35%. Spike above 40% = sites pushing back or new JS protection deployed.

Panel 4 — Kafka input vs output rate (overlay)
  Why:     If input consistently exceeds output the queue is growing — earliest warning
           before the consumer lag alert fires.

Panel 5 — Worker instance count (fast + Playwright)
  Metric:  ecs_service_running_count{service=~".*worker.*"}
  Why:     Confirms auto-scaling is responding. High lag + max instances = fetch speed
           issue, not a scaling issue.
```

### Dashboard 2 — Crawl Health (on-call reference)

**Purpose**: why is the crawler failing?

```
Panel 1 — Error rate by type (stacked)
  Metrics: rate(crawl_errors_total{type=~"timeout|blocked|permanent_fail|dns_fail"}[5m])
  Why:     Timeout spike → network or target-site slowdown.
           Blocked spike → new anti-bot measure at a major domain.
           DNS fail spike → resolver issue or upstream CDN routing change.

Panel 2 — Circuit breakers active
  Metric:  sum(redis_circuit_breaker_tripped) by (domain)
  Display: tripped domain count + top-10 table
  Why:     > 20 simultaneous trips = systemic issue (proxy, IP block, infra fault).

Panel 3 — DLQ depth
  Metric:  kafka_topic_messages_in_total{topic="failed-urls"}
           - kafka_topic_messages_out_total{topic="failed-urls"}
  Alert:   > 50,000 unprocessed → P2

Panel 4 — HTTP status distribution (pie + time series)
  Buckets: 2xx | 3xx | 4xx | 5xx | 0 (connection fail)
  Why:     Sudden 403 spike at scale = IP or user-agent blocked.
           429 spike = token bucket rate misconfigured for that domain.

Panel 5 — OOM restarts
  Metric:  ecs_task_stopped_total{stop_reason="OutOfMemory"}
  Why:     Playwright is the only worker type that OOMs in practice — single large
           pages or memory leaks over long-running instances.
```

### Dashboard 3 — API Performance (SLO tracking)

**Purpose**: are we meeting customer commitments?

```
Panel 1 — Read latency percentiles
  Metrics: histogram_quantile(0.50/0.95/0.99, http_request_duration_seconds_bucket{handler="/crawl"})
  Lines:   p95 < 150ms (SLO) and p95 < 500ms (SLA) drawn as reference lines.

Panel 2 — Cache hit rate
  Metric:  hits / (hits + misses)  from Redis cache counters
  Target:  > 60%. Low hit rate during batch ingest is expected; sustained low rate
           outside ingest windows means TTL is too short or query patterns changed.

Panel 3 — API error rate
  Metric:  rate(http_requests_total{status=~"5.."}[5m]) / rate(http_requests_total[5m])
  Lines:   0.1% (SLO alert), 0.5% (P1 page)

Panel 4 — P0 end-to-end latency
  Metric:  histogram_quantile(0.95, realtime_crawl_end_to_end_seconds_bucket)
  Line:    45s SLO. Alert at 30s — 2/3 of budget consumed, leaving no margin.

Panel 5 — API availability (30-day window)
  Metric:  1 − (5xx count / total requests) over 30d
  Lines:   99.95% (SLO), 99.9% (SLA)
```

### Dashboard 4 — Storage and Infrastructure

**Purpose**: prevent capacity surprises before they become incidents.

```
Panel 1 — PostgreSQL write rate
  Metric:  rate(pg_stat_user_tables_n_tup_ins{relname="crawl_metadata"}[5m])
  Why:     Sudden drop = workers stopped writing (Kafka disconnect, batch flush timeout).

Panel 2 — PostgreSQL replication lag
  Metric:  pg_replication_lag_seconds
  Alert:   > 5s → P3 Slack. > 10s → P2 PagerDuty (read replicas serving stale data).

Panel 3 — Partition sizes (table)
  Metric:  pg_table_size_bytes{relname=~"crawl_metadata_.*"}
  Why:     A partition growing to 8 GB+ means dedup is broken or a batch was re-ingested.

Panel 4 — Bloom filter fill %
  Metric:  redis_bloom_filter_items{key="bloom:urls"} / 1_000_000_000
  Alert:   > 80% → P2. At 80% fill, false-positive rate rises to ~2%. Provision larger
           filter with a 24h dual-write transition period.

Panel 5 — Redis memory usage
  Metric:  redis_memory_used_bytes by (key_pattern)
  Breakdown: bloom:urls (~1.2 GB) | session:* | rate:* | cache:* | circuit:*
  Alert:   > 85% of instance RAM → scale Redis. Eviction would corrupt the bloom filter.
```

### Dashboard 5 — Cost per URL

**Purpose**: keep crawl economics visible so engineering decisions are informed by unit cost.

```
Panel 1 — $/1000 URLs (daily)
  Formula: (Fargate + MSK + RDS + S3 cost) / (URLs crawled / 1000)
  Target:  < $0.05 / 1000 URLs

Panel 2 — Fargate vCPU-hours by worker type
  Why:     Playwright consumes ~5× CPU of fast workers. Playwright fraction creeping up
           raises cost per URL disproportionately.

Panel 3 — Spot interruption rate (Playwright workers)
  Target:  < 5%. Higher means the instance family is too contested — switch types.

Panel 4 — Kafka message volume by topic
  Why:     MSK billed by throughput. Unexpected DLQ volume = retry storm (cost signal
           and indicator of a deeper problem).
```

---

### Alerting Rules

```
P1 — Page immediately (any hour)
  · API availability drops below 99.9% in any 5-minute window
  · API p95 latency > 500ms for 5 consecutive minutes
  · Kafka lag on urls-realtime > 100 messages for > 2 minutes  (P0 SLO imminent breach)
  · PostgreSQL primary unreachable for > 30s

P2 — Page within 15 minutes
  · Kafka consumer lag (any topic) > 500,000 messages for > 10 minutes
  · Circuit breakers tripped on > 20 domains simultaneously
  · DLQ depth > 50,000 messages
  · PostgreSQL replication lag > 10s
  · Bloom filter > 80% full
  · Redis memory > 85% of instance RAM

P3 — Slack notification only
  · Crawl error rate > 5% for 15 minutes
  · Playwright worker OOM restart
  · Spot interruption rate > 5%
  · S3 daily write volume deviates > 50% from 7-day average
  · Any single circuit breaker tripped  (watch for escalation)

Info — Dashboard annotation only
  · Auto-scaling event (scale-up or scale-down)
  · Monthly partition created by pg_partman
  · Bloom filter hit rate changes > 5% from prior day
```

### Incident Response Runbook

```
P1 — API latency
  1. Dashboard 3 Panel 1: all percentiles or just p99?
  2. Dashboard 3 Panel 2: cache hit rate dropped? → PostgreSQL absorbing all traffic
  3. Dashboard 4 Panel 2: replication lag high? → reads hitting stale replica
  4. Dashboard 1 Panel 5: all workers running? → if down, no new results = cache misses
  Actions:
    Replication lag  → failover reads to primary
    Cache miss storm → scale Redis

P1 — Kafka realtime lag
  1. Dashboard 1 Panel 5: fast worker count at max (50)?
  2. If at max:  Dashboard 2 Panel 1 — timeout storm? → proxy issue
  3. If not max: ECS console — is auto-scaling stuck? (task failing health check)
  Actions:
    Scaling stuck → manual scale via ECS CLI
    Proxy issue   → failover to backup proxy

P2 — DLQ depth
  1. Dashboard 2 Panel 1: which error type is filling the DLQ?
  2. If "blocked": Dashboard 2 Panel 2 circuit breaker table — which domains?
  3. Single domain > 10k messages → new bot protection deployed at that site
  Actions:
    Mark domain PAUSED, investigate residential proxy escalation for that domain
```
