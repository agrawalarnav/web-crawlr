# Part 3 — PoC Plan, Blockers & Release Strategy

---

## Objective

Validate the Part 2 scale architecture at **1% load (10M URLs / month)** before
committing to full production infrastructure. The PoC uses a real domain batch
(amazon.com + walmart.com URLs) as input and exercises every architectural
component end-to-end.

10M URLs costs ~$50 in compute. It surfaces every failure mode — Kafka
rebalancing, PostgreSQL write contention, Bloom filter behaviour, ECS
auto-scaling — at 1/100th the production cost. Failures caught here are fixed in
days; failures caught after full ramp take weeks.

---

## PoC Architecture

```
INPUT
  MySQL:     SELECT url, domain FROM urls WHERE year_month = '2026-07'
             (10M rows, cursor-paginated in 10k batches)
  Text file: s3://webcrawler-poc/urls-2026-07.txt
        │
        ▼
  URL Normalizer
  · lowercase domain, strip tracking params, resolve http → https
        │
        ▼
  Bloom Filter  (Redis)
  · skip already-seen URLs (1% false positive acceptable at 10M scale)
        │
        ▼
  Kafka  (Confluent Cloud, single broker — free tier)
  · urls-realtime   P0 — real-time API requests
  · urls-batch      P3 — MySQL / text file input
  · partitioned by hash(domain)
        │
        ├──► Fast Workers  (2 × ECS Fargate, 0.5 vCPU / 512 MB)
        │    curl_cffi · Chrome124 TLS fingerprint · 50 concurrent threads
        │
        └──► Playwright Workers  (4 × ECS Fargate, 1 vCPU / 2 GB)
             Playwright + stealth + session persistence
             4 concurrent contexts per instance
        │
        ▼
  PostgreSQL  (RDS db.t3.medium)
  · crawl_metadata table, partitioned by year_month
  · batch insert: 100 rows buffered, flushed every 5s
        │
        ├──► S3  raw HTML (gzip)
        └──► S3  Parquet  (Athena queryable)
        │
        ▼
  Grafana  (3 dashboards: URLs/min · Kafka lag · error rate by domain)
```

**Intentional simplifications** at 10M scale — not needed until production hardening:

- Single Kafka broker, no multi-AZ
- No PgBouncer, no read replicas
- Fixed worker count, no auto-scaling
- Manual provisioning, no Terraform

---

## Validation Plan

### Step 1 — Ingestion correctness (2 hours)

```
Feed 100k URLs from MySQL into Kafka.
Assert: all 100k appear in topic (no silent drops)
Assert: 100 deliberately injected duplicates are skipped by Bloom filter
Assert: all amazon.com URLs land on the same Kafka partition
```

### Step 2 — Pipeline throughput (24-hour run)

```
Run full 10M URL batch.
Measure: sustained crawl rate        target ≥ 3.8 URLs/s
Measure: fast vs Playwright split    expect ~70 / 30
Measure: PostgreSQL write success    target > 99%
Measure: Kafka consumer lag          must not grow unbounded
```

### Step 3 — Data quality review (4 hours)

```
Sample 100 random crawled URLs from PostgreSQL.
Human review (two independent reviewers):
  · title populated?            pass: > 95% of sample
  · topics semantically valid?  pass: avg relevance score ≥ 2.5 / 3.0
  · structured_data extracted?  pass: > 90% on product pages

Run Athena query on Parquet export — verify schema matches PostgreSQL exactly.
```

### Step 4 — Failure recovery (4 hours)

```
Kill one Playwright worker mid-batch.
  Assert: Kafka offset replay re-queues in-flight jobs, no data loss.

Restart PostgreSQL.
  Assert: workers reconnect, writes resume without crashes.

Inject sustained 429 responses from a test domain.
  Assert: circuit breaker trips, domain paused 15 min, Grafana alert fires.

Replay failed-urls DLQ topic.
  Assert: failed jobs reprocessed and written to PostgreSQL correctly.
```

### Step 5 — Go / No-Go review

```
Present: throughput metrics, data quality scores, failure recovery results.
Attendees: engineering lead + product stakeholder.
Decision documented and signed off in writing before any further spend.
```

---

## Go / No-Go Criteria

| Metric | Go | No-Go |
|---|---|---|
| Ingestion completeness | 100% of MySQL rows reach Kafka | Any silent drop |
| Bloom filter dedup | 0 false negatives on injected duplicates | Any duplicate write |
| Sustained crawl rate | ≥ 3.8 URLs/s over 24h | < 3 URLs/s |
| PostgreSQL write success | > 99% of crawled URLs written | > 1% write failures |
| Data quality — title | > 95% of sample populated | < 90% |
| Topic relevance (human) | avg ≥ 2.5 / 3.0 | avg < 2.0 |
| Failure recovery | Zero unrecovered job losses | Any silent drop |
| Kafka consumer lag | Bounded (< 50k messages) throughout | Grows unbounded |

---

## Blocker Analysis

### Known Blockers — must be resolved before the 24h PoC run

**B1 — Major retail bot protection (Amazon, Walmart, Best Buy)**

The primary input domains. Cloudflare + proprietary fingerprinting blocks datacenter
IPs at batch volume. This is the item most likely to require unplanned spend and is
tested on Week 2 Day 1 — not the day of the Go/No-Go meeting.

```
Escalation path:
  1. curl_cffi Chrome124 TLS spoofing (free, first line of defence)
  2. If still blocked at batch volume: residential proxy rotation via PROXY_URL
     Proxy cost: ~$15–50/GB (Bright Data / Oxylabs)
```

- ETA: Week 2, Day 1–2 — 2 engineer-days
- Risk: **Critical** — these are the primary assignment domains

**B2 — Playwright memory ceiling under concurrent load**

Each Playwright context peaks at ~400 MB. With 4 concurrent contexts on a 2 GB
instance, burst load can trigger OOM kills and lose in-flight jobs.

```
Fix: prefetch_count=1 (one Kafka message in-flight per worker)
     ECS memory limit alarm → auto-restart on OOM
     Load test confirms stable at 4 concurrent tabs before PoC run
```

- ETA: Week 2, Day 3 — 1 engineer-day of load testing
- Risk: **High** — OOM kills cause job loss, directly fails the reliability criterion

**B3 — Kafka consumer rebalancing during scaling**

When new ECS tasks join or leave a consumer group, Kafka triggers a group rebalance
that pauses all consumption for 5–30s, causing lag spikes.

```
Fix: partition.assignment.strategy = cooperative-sticky
     (incremental rebalancing — only reassigns the partitions that need moving)
```

- ETA: Week 1, Day 4 — 4 engineer-hours (Kafka config only)
- Risk: **Medium** — causes lag spikes but not data loss

**B4 — PostgreSQL write throughput at sustained load**

Single-row inserts at 3.8 URLs/s create connection churn and lock contention over
the 24h run.

```
Fix: buffer 100 rows in memory, flush every 5s with executemany()
     Reduces round-trips 100× without meaningfully increasing write latency.
     PoC scale is survivable without this fix; production is not.
```

- ETA: Week 1, Day 3 — 1 engineer-day
- Risk: **Medium**

### Medium Blockers — must be in place before the 24h run

**B5 — MySQL ingestion without full-table scans**

Reading 10M+ rows requires cursor-based pagination to avoid locking the source table.

```sql
SELECT url, domain FROM urls
WHERE year_month = '2026-07'
  AND (domain, url) > (:last_domain, :last_url)
ORDER BY domain, url
LIMIT 10000
```

- ETA: Week 1, Day 2 — 1 engineer-day

**B6 — robots.txt compliance**

Batch-crawling at 3.8 URLs/s without robots.txt checking violates crawl etiquette
and risks IP bans on the PoC's primary domains.

```
Fix: robotparser (Python stdlib) + Redis cache (key: robots:{domain}, TTL 24h)
     Checked before every URL dispatch — must be live before the 24h run.
```

- ETA: Week 1, Day 3 — 1 engineer-day

**B7 — Playwright cold start (15–20s)**

New Playwright ECS tasks take 15–20s before they begin processing messages, causing
lag spikes when scaling out.

```
Fix: keep minimum 1 Playwright task warm at all times
     pre-pull Docker image on ECS host at launch
```

- ETA: Week 2, Day 2 — 4 engineer-hours

### Trivial — under 2 engineer-hours each

| Blocker | Solution |
|---|---|
| Bloom filter sizing for 1B URLs | `rbloom` lib, allocate 1.2 GB Redis |
| S3 lifecycle Standard → Glacier after 30d | AWS console lifecycle policy |
| PostgreSQL monthly partitioning | `pg_partman` auto-partition config |
| Parquet writer + Athena DDL | `pyarrow` write + Athena `CREATE TABLE` |
| gzip HTML before S3 upload | Python `gzip` stdlib |
| Content-hash dedup check | SHA256(body_text) vs PostgreSQL `content_hash` |
| Circuit breaker per domain | Redis sliding window + trip threshold |

---

## Implementation Schedule

```
─────────────────────────────────────────────────────────────
Week 1   INFRASTRUCTURE BUILD
─────────────────────────────────────────────────────────────
  Day 1   Kafka setup (Confluent Cloud free tier)
          · Topics: urls-realtime, urls-batch, failed-urls
          · Partition key: hash(domain)
          · Fast workers + Playwright workers wired as consumers

  Day 2   Ingestion pipeline (B5)
          · Cursor-paginated MySQL batch reader
          · Text file line-streamer
          · Both publish to urls-batch topic
          · Validate: 1k row batch reaches Kafka, domain partitioning confirmed

  Day 3   PostgreSQL schema + batch writer (B4)
          · crawl_metadata table, year_month LIST partition
          · Batch insert: 100 rows buffered, flush every 5s
          · Unit tests: insert, upsert on conflict, content-hash dedup path

  Day 4   robots.txt compliance (B6)
          · robotparser + Redis cache (key: robots:{domain}, TTL 24h)
          · Kafka cooperative rebalancing config (B3)
          · Bloom filter Redis setup

  Day 5   Storage layer
          · S3 raw HTML gzip upload
          · Parquet writer + Athena DDL
          · S3 lifecycle policy (Standard → Glacier after 30d)

─────────────────────────────────────────────────────────────
Week 2   INTEGRATION + BOT PROTECTION
─────────────────────────────────────────────────────────────
  Day 1   Grafana: 3 dashboards wired
          · URLs/min · Kafka consumer lag · error rate by domain
          End-to-end integration test: 10k URLs
          MySQL → Kafka → workers → PostgreSQL → S3 → verified in Grafana

  Day 2   Fix integration issues surfaced in Day 1 test
          (There will always be wiring bugs on the first full-stack run.
           This buffer day prevents them from spilling into the bot test.)

  Day 3   Bot protection stress test — Amazon / Walmart / Best Buy (B1)
          · Run 50k URLs against primary domains at full rate
          · If blocked: begin residential proxy account setup (billing takes time)

  Day 4   B1 continued — proxy integration if needed
          · Wire PROXY_URL config, retest blocked domains
          · Confirm crawl success rate recovers to > 95% on primary domains
          Note: proxy account approval can take 24–48h; Day 3 kickoff ensures
          credentials arrive by Day 4 even with delays.

  Day 5   Buffer / contingency
          · Address any remaining B1 issues
          · If B1 is resolved early: begin Playwright load testing (B2 preview)

─────────────────────────────────────────────────────────────
Week 3   PLAYWRIGHT HARDENING + 24H RUN
─────────────────────────────────────────────────────────────
  Day 1   Playwright OOM load test (B2)
          · Run 4 concurrent contexts for 4h, watch RSS memory in Grafana
          · Confirm prefetch_count=1 holds stable; no OOM kills
          · If leak found: profile heap, patch, re-test same day

  Day 2   Playwright cold-start fix (B7)
          · Keep minimum 1 task warm; pre-pull image at ECS launch
          · Confirm < 5s effective cold-start after fix
          Final smoke test: 10k URLs across all domain types, all blockers green

  Day 3   Start full 10M URL batch (Step 2 — 24h run begins)
          · Monitor: crawl rate, Kafka lag, PostgreSQL write rate, error rate
          · No other work scheduled — engineers available to respond to incidents

  Day 4   (24h run in progress)
          · Data quality human review — 100 URL sample (Step 3)
            Two independent reviewers score title, topics, structured_data
          · Failure recovery tests run in isolation after the batch completes:
            worker kill, DB restart, 429 flood, DLQ replay (Step 4)

  Day 5   Post-run analysis and documentation
          · Compile throughput metrics, error breakdown, data quality scores
          · Document all failure recovery results
          · Prepare Go/No-Go summary for stakeholder review

─────────────────────────────────────────────────────────────
Week 4   GO / NO-GO + CONTINGENCY
─────────────────────────────────────────────────────────────
  Day 1   Go / No-Go review meeting (Step 5)
          · Present: throughput metrics, data quality scores, failure recovery
          · Attendees: engineering lead + product stakeholder
          · Decision documented and signed off in writing

  Days 2–5  Contingency buffer
          · If Go: begin production hardening setup in parallel
          · If No-Go on one criterion: targeted fix + re-test that criterion only
            (a single failed criterion does not require re-running the full 24h batch)
          · If multiple No-Go criteria: root cause analysis, revised plan presented
            to stakeholders before further spend

─────────────────────────────────────────────────────────────
Weeks 5–8  PRODUCTION HARDENING  (post Go decision)
─────────────────────────────────────────────────────────────
  Week 5   Infrastructure promotion
           · Multi-AZ Kafka cluster (MSK or Confluent Standard)
           · Aurora Multi-AZ + PgBouncer + read replicas
           · ECS auto-scaling on Kafka consumer lag metric
           · Terraform IaC: codify all PoC infrastructure

  Week 6   Observability stack
           · ELK stack: Filebeat → Logstash → Elasticsearch → Kibana
           · Full Grafana dashboards (5 dashboards from Part 2)
           · PagerDuty alert routing + Kibana Watcher rules
           · X-Ray distributed tracing

  Week 7   Security + operations
           · VPC, security groups, IAM least-privilege
           · Secrets Manager (graduate from GitHub Secrets)
           · Runbooks: queue backup, OOM, domain block, DB replication lag
           · On-call rotation defined, engineers trained on runbooks

  Week 8   Load validation
           · Run 50M URL batch as pre-production stress test
           · Confirm auto-scaling triggers and stabilises correctly
           · Confirm all alerts fire and route correctly under load

─────────────────────────────────────────────────────────────
Weeks 9–12  PRODUCTION RAMP
─────────────────────────────────────────────────────────────
  Week 9:  100M URL beta batch — 1-week soak at 10% of production load
  Week 10: Monitor, triage tail issues, tune per-domain rate limits
  Week 11: 500M URL ramp — confirm scaling headroom, proxy cost tracking
  Week 12: Production sign-off, 1B URL/month capacity confirmed
           SLAs activated, on-call live
```

---

## CI/CD Pipeline

### Pipeline Overview

```
Developer pushes
        │
        ▼
┌───────────────────┐
│  Stage 1          │  on: pull_request
│  PR Quality Gate  │  blocks merge on failure
└────────┬──────────┘
         │ merge to main
         ▼
┌───────────────────┐
│  Stage 2          │  on: push to main
│  Build & Push     │  API + Worker images → Artifact Registry
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│  Stage 3          │  automatic
│  Deploy Staging   │
│  + Smoke Test     │
└────────┬──────────┘
         │ manual approval + git tag vX.Y.Z
         ▼
┌───────────────────┐
│  Stage 4          │
│  Deploy Prod      │
│  Canary (10%→100%)│
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│  Stage 5          │  5 min post-deploy
│  Verification     │
│  Auto-rollback    │
└───────────────────┘
```

### Stage 1 — PR Quality Gate

Runs on every pull request. Blocks merge on any failure.

```yaml
on: pull_request

jobs:
  quality:
    steps:
      - Lint         ruff check .
      - Type check   mypy crawler/ api/ worker/
      - Unit tests   pytest tests/ -x --timeout=30
      - Build check  docker build -f Dockerfile.api .
                     docker build -f Dockerfile.worker .   (build only, no push)
```

Unit tests cover:
- `crawler/parser.py` — metadata extraction against fixture HTML files
- `crawler/classifier.py` — YAKE topics on known text inputs with expected keywords
- `crawler/fetcher.py` — URL candidate logic, cookie passthrough
- `api/main.py` — URL normalisation, job state transitions

### Stage 2 — Build & Push Images

Runs on every merge to `main`. Both jobs run in parallel — the worker image
(~8 min, includes Chromium) does not block the API image (~2 min).

```yaml
on:
  push:
    branches: [main]

jobs:
  build-api:
    steps:
      - Authenticate    google-github-actions/auth
      - Configure Docker gcloud auth configure-docker REGION-docker.pkg.dev
      - Build & push    docker build -f Dockerfile.api
                        tags: api:SHA, api:latest

  build-worker:           # parallel with build-api
    steps:
      - (same steps with Dockerfile.worker)
```

### Stage 3 — Deploy to Staging

Automatic after images are pushed. Staging: Cloud Run API + single Playwright ECS
task + shared RDS.

```yaml
  deploy-staging:
    needs: [build-api, build-worker]
    steps:
      - Deploy API    gcloud run deploy webcrawler-api-staging
                      --image api:SHA
                      --set-env-vars REDIS_URL=...,KAFKA_BOOTSTRAP=...

      - Deploy Worker aws ecs update-service
                      --cluster webcrawler-staging --service worker
                      --force-new-deployment

      - Smoke test    crawl 5 URLs against staging endpoint
                      assert: HTTP 200, word_count > 0, topics non-empty
                      fail pipeline on any assertion failure
```

Smoke test URLs (one per path type):

```
https://en.wikipedia.org/wiki/Python_(programming_language)   → fast path
https://old.reddit.com/r/funny/comments/3hahrw/...           → Playwright path
https://www.amazon.com/dp/B009GQ034C/                        → e-commerce / bot protection
https://help.shopify.com/en/manual/products/details          → static docs / CDN
https://news.ycombinator.com/                                → lightweight HTML
```

### Stage 4 — Deploy to Production

Triggered by a **git tag** (`v1.0.0`, `v1.1.0`) — not every merge to main.
Requires **manual approval** in GitHub Actions before any traffic shift.

```yaml
  deploy-production:
    needs: deploy-staging
    environment: production       # manual approval gate in GitHub UI
    if: startsWith(github.ref, 'refs/tags/v')
    steps:
      - Deploy API (canary)   gcloud run deploy webcrawler-api
                              --image api:SHA --tag canary --no-traffic

      - Shift 10% traffic     gcloud run services update-traffic
                              --to-tags canary=10

      - Wait 5 min            check /health, check Grafana error rate unchanged

      - Shift 100% traffic    gcloud run services update-traffic
                              --to-tags canary=100

      - Deploy Worker         aws ecs update-service
                              --cluster webcrawler-prod --service worker
                              --force-new-deployment

      - Tag stable            docker tag api:SHA api:stable
```

### Stage 5 — Post-Deploy Verification and Auto-Rollback

Runs 5 minutes after every production deploy.

```yaml
  verify-production:
    steps:
      - Health check    GET /health → assert 200

      - Crawl probe     POST /crawl with a known Wikipedia URL
                        assert result.http_status == 200
                        assert result.word_count > 1000
                        assert len(result.topics) >= 5

      - Grafana check   query Grafana API:
                        error_rate_5min < 5%
                        p95_latency_fast_path < 3000ms

      - Auto-rollback   if any assertion fails:
                          gcloud run services update-traffic --to-tags stable=100
                          aws ecs update-service --task-def PREVIOUS_VERSION
                          PagerDuty alert: "Production rollback triggered"
```

### Secrets Management

All credentials stored in **GitHub Actions Secrets**, never in code or config files.

| Secret | Used by | Value |
|---|---|---|
| `GCP_SA_KEY` | Build + deploy | GCP service account JSON |
| `GCP_PROJECT` | Build + deploy | GCP project ID |
| `GCP_REGION` | Build + deploy | `us-central1` |
| `WORKER_VM_IP` | Worker deploy | VM external IP |
| `WORKER_SSH_KEY` | Worker deploy | ED25519 private key |
| `REDIS_PASSWORD` | API + Worker | Redis auth password |
| `KAFKA_BOOTSTRAP` | API + Worker | Confluent Cloud bootstrap servers |
| `KAFKA_API_KEY` | API + Worker | Confluent Cloud API key |
| `KAFKA_API_SECRET` | API + Worker | Confluent Cloud API secret |

In production, secrets graduate from GitHub Secrets → **AWS Secrets Manager / GCP Secret Manager**, injected at runtime via IAM role binding. No secret ever touches an environment variable at rest.

### Infrastructure Changes

Code deploys via GitHub Actions. Infrastructure changes (new Kafka topics, schema
migrations, scaling config) deploy via a separate Terraform pipeline:

```
terraform plan   → runs on PR, posts plan diff as PR comment
terraform apply  → runs on merge to main, requires approval
                   state in S3 + DynamoDB lock table
```

Schema migrations run as a pre-deploy step before the new worker image is live:

```
alembic upgrade head   → applies pending migrations
                         rolls back automatically on migration failure
```

---

## Cost Analysis

### PoC — 10M URLs over 2 weeks (~$50)

| Component | Config | Cost |
|---|---|---|
| Confluent Cloud Kafka | Free tier (10 GB storage) | $0 |
| ECS Fargate — fast workers | 2 × 0.5 vCPU / 1 GB, 40h active | $2 |
| ECS Fargate — Playwright workers | 4 × 1 vCPU / 2 GB, 40h active | $8 |
| RDS PostgreSQL db.t3.medium | 14 days | $23 |
| ElastiCache Redis t3.micro | 14 days | $6 |
| S3 raw HTML gzip (250 GB) | Standard storage | $6 |
| S3 Parquet (5 GB) | Standard storage | $0 |
| **Total** | | **~$50** |

Workers run ~40 hours during the 24h validation run plus setup. The dominant PoC cost
is always-on RDS + ElastiCache for 2 weeks, not compute.

### Beta — 100M URLs over 1 month (~$900)

| Component | Config | Cost |
|---|---|---|
| Confluent Cloud Kafka | Basic cluster, 10 MB/s | $120 |
| ECS Fargate — fast workers | 2 × 0.5 vCPU / 1 GB, 730h | $36 |
| ECS on EC2 Spot — Playwright workers | 13 × 1 vCPU / 2 GB, Spot, 730h | $190 |
| RDS PostgreSQL db.t3.large | 1 month | $110 |
| ElastiCache Redis r6g.large | 1 month | $120 |
| S3 raw HTML gzip (2.5 TB) | Standard → Glacier after 30d | $58 |
| S3 Parquet (50 GB) | Standard storage | $1 |
| ELK stack (self-hosted, single node) | t3.medium, 30d | $30 |
| Data transfer out | ~100 GB API responses | $9 |
| **Total** | | **~$675** |

Playwright workers move to EC2 Spot at Beta — the PoC used Fargate On-Demand for
simplicity, but Spot saves ~60% and Kafka offset replay handles interruptions safely.

### Production — 1B URLs / month (~$2,300)

| Component | Config | Cost |
|---|---|---|
| Confluent Cloud Kafka (or MSK) | Standard cluster, 100 MB/s | $500 |
| ECS Fargate — fast workers | 5 × 0.5 vCPU / 1 GB, 730h | $90 |
| EC2 Spot — Playwright workers | 125 × 1 vCPU / 2 GB, Spot | $950 |
| Aurora Serverless v2 PostgreSQL | 2–8 ACU, 500 GB, Multi-AZ | $400 |
| ElastiCache Redis r6g.large | 13 GB, Multi-AZ | $240 |
| S3 raw HTML gzip (25 TB/month) | Standard 30d → Glacier Deep Archive | $90 |
| S3 Parquet (500 GB) | Standard storage | $12 |
| Athena queries | ~1 TB scanned/month | $5 |
| ELK stack (Elastic Cloud) | 3-node, hot-warm | $300 |
| Cloud Run API | ~50M requests/month | $20 |
| Residential proxies (optional) | ~5% of URLs, 10 GB/month | $150 |
| Misc (data transfer, CloudWatch, X-Ray) | | $50 |
| **Total** | | **~$2,600/month** |

### Cost per URL

```
Stage       Volume    Total     Per URL       Per 1M URLs
─────────   ───────   ───────   ───────────   ───────────
PoC         10M       ~$50      $0.0000050    $5.00
Beta        100M      ~$675     $0.0000068    $6.75
Production  1B        ~$2,600   $0.0000026    $2.60

Scale economics: fixed costs (Kafka, Redis, Aurora) amortise over more URLs.
Production cost per URL is 48% lower than PoC despite more robust infrastructure.
```

### Infrastructure Trade-offs

#### Kafka: Confluent Cloud vs Self-Hosted MSK

| | Confluent Cloud | Self-Hosted MSK |
|---|---|---|
| Ops burden | None | Broker tuning, replication, upgrades |
| Cost at PoC | $0 (free tier) | ~$150/month (3-broker minimum) |
| Cost at production | ~$500/month | ~$350/month |
| Recommendation | PoC + Beta | Production, if a platform team exists |

Confluent Cloud is cheaper for PoC and Beta. At production with a dedicated platform
team, MSK saves ~$150/month but adds full broker operational responsibility.

#### Playwright Workers: Fargate On-Demand vs EC2 Spot

| | Fargate On-Demand | EC2 Spot |
|---|---|---|
| Cost (125 workers, 730h) | ~$4,500/month | ~$950/month |
| Interruption risk | None | ~5% of instances reclaimed/month |
| Interruption handling | N/A | Kafka offset replay — job re-queued automatically |
| Recommendation | PoC | Beta + Production |

Spot saves 79% on the largest cost line. Interruptions are safe because Kafka
offsets are only committed after a successful PostgreSQL write — a reclaimed
instance re-processes from its last committed offset with no data loss.

#### Storage: PostgreSQL vs Cassandra vs DynamoDB

| | PostgreSQL (Aurora) | Cassandra | DynamoDB |
|---|---|---|---|
| Query flexibility | High (SQL, GIN indexes) | Low (partition key only) | Medium |
| Write throughput at 385/s | Good with batch inserts | Excellent | Excellent |
| Cost at 1B rows | ~$400/month | ~$300/month | ~$800/month |
| Topics array indexing | GIN index — native | Not supported | No array type |
| Recommendation | **Yes** | No | No |

PostgreSQL wins on query flexibility. The core product query —
`WHERE 'python' = ANY(topics)` — requires GIN indexing on an array column.
Neither Cassandra nor DynamoDB supports this natively.

#### Raw HTML Storage: S3 Standard vs Glacier vs Discard

| | S3 Standard | S3 → Glacier (30d) | Discard |
|---|---|---|---|
| Cost (25 TB/month) | $575/month | $90/month | $0 |
| Reprocessing if parser improves | Yes | Yes (3–5h retrieval) | No |
| Recommendation | No | **Yes** | Only if storage is a hard constraint |

Lifecycle to Glacier after 30 days saves 84%. The value of retaining raw HTML is
parser reprocessing — if YAKE thresholds or Trafilatura logic improves, re-extract
from existing HTML without re-crawling any domain.

### Key Cost Levers

| Lever | Saving at Production | Notes |
|---|---|---|
| EC2 Spot for Playwright workers | ~$3,550/month | Fully recoverable via Kafka offset replay |
| S3 Glacier lifecycle after 30d | ~$485/month | 3–5h retrieval latency for historical HTML |
| Bloom filter + content-hash dedup | ~$390/month | 15% of 1B URLs skipped = 150M avoided crawls |
| curl_cffi fast path maximisation | Variable — largest per-URL lever | Every URL kept on fast path avoids a Playwright worker slot |

---

## Release Strategy

### Stages

| Stage | Weeks | Volume | Purpose | Exit Criterion |
|---|---|---|---|---|
| **PoC** | 1–4 | 10M URLs | Validate architecture end-to-end at low cost | All Go/No-Go criteria met, signed off in Week 4 |
| **Hardening** | 5–8 | — | Production-grade infrastructure, observability, security | 50M URL stress test clean, runbooks written, on-call trained |
| **Ramp** | 9–12 | 100M → 1B URLs | Gradual load increase, SLAs activated | 1B URL/month sustained, SLAs live |

No stage is skipped under deadline pressure. Each exit criterion is agreed upfront.
Week 4's contingency buffer means a single failed Go/No-Go criterion triggers a
targeted re-test — not a full restart of the schedule.

### What Makes This a High-Quality Release

**Highest-risk blocker tested first.**
B1 (retail bot protection) is the item most likely to require unplanned budget and
schedule changes. It is tested on Week 2 Day 1 — not the morning of the Go/No-Go
meeting. Stakeholders know before the 24h run whether proxy spend is required.

**Human evaluation for topic quality.**
Automated metrics cannot measure semantic relevance for an SEO product. Two reviewers
independently score 100 URLs. A misconfigured YAKE threshold is caught at PoC, not
after a customer complaint in production.

**Failure recovery is a scheduled test, not an assumption.**
Worker OOM, database restart, 429 flood — each has a dedicated test slot in the
schedule. Production-readiness means the system survives known failure modes under
load, not just the happy path in a staging environment.

**Costs agreed at each stage before the next begins.**
PoC: ~$50. Beta: ~$675. Production: ~$2,600/month. Each number is presented and
signed off before further infrastructure is provisioned. No budget surprise at go-live.

**Gradual traffic ramp with defined exit criteria.**
PoC (10M) → Beta (100M) → Production (1B). Canary deployment shifts 10% of
production traffic to the new version, verifies error rate and latency, then promotes
to 100%. Auto-rollback fires within 5 minutes if verification fails.

**Runbooks written before production traffic arrives.**
On-call engineers have written procedures for every known failure mode — queue
backup, Playwright OOM, domain circuit-break, PostgreSQL replication lag — before
the first production URL is crawled. Discovering the procedure during an incident
is not acceptable.
