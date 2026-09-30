import hashlib
import json
import time
import uuid
from contextlib import asynccontextmanager
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import aio_pika
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from api.models import CrawlRequest, CrawlResult, JobResponse, HealthResponse
from config import settings
from crawler.extractor import build_result_dict
from crawler.fetcher import fetch_fast, needs_playwright

_TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
    "ref", "tag", "ie", "qid", "sr", "keywords",
}

redis_client: aioredis.Redis | None = None
rabbitmq_connection: aio_pika.abc.AbstractRobustConnection | None = None
rabbitmq_channel: aio_pika.abc.AbstractChannel | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, rabbitmq_connection, rabbitmq_channel

    redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)

    rabbitmq_connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    rabbitmq_channel = await rabbitmq_connection.channel()

    # Declare queues (idempotent — safe to call on every startup)
    await rabbitmq_channel.declare_queue(
        settings.rabbitmq_queue,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": settings.rabbitmq_dlq,
        },
    )
    await rabbitmq_channel.declare_queue(settings.rabbitmq_dlq, durable=True)

    yield

    await rabbitmq_channel.close()
    await rabbitmq_connection.close()
    await redis_client.aclose()


app = FastAPI(title="Web Crawler API", lifespan=lifespan)


def _cache_key(normalized_url: str) -> str:
    return "cache:" + hashlib.sha256(normalized_url.encode()).hexdigest()


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    qs = {k: v for k, v in parse_qs(parsed.query).items() if k not in _TRACKING_PARAMS}
    return urlunparse((
        parsed.scheme,
        parsed.netloc.lower(),
        parsed.path or "/",
        "",
        urlencode(qs, doseq=True),
        "",  # strip fragment
    ))


def _build_result(
    url: str,
    normalized: str,
    html: str,
    status: int,
    method: str,
    start: float,
) -> CrawlResult:
    return CrawlResult(**build_result_dict(url, normalized, html, status, method, start))


@app.post("/crawl", response_model=CrawlResult)
async def crawl(request: CrawlRequest):
    start = time.monotonic()
    url = request.url
    normalized = normalize_url(url)

    cached = await redis_client.get(_cache_key(normalized))
    if cached:
        return CrawlResult(**json.loads(cached))

    domain = urlparse(normalized).netloc

    raw_cookies = await redis_client.get(f"session:{domain}")
    cookies_in = json.loads(raw_cookies) if raw_cookies else None

    html, status, final_url, cookies_out = await fetch_fast(
        normalized, timeout=settings.httpx_timeout, cookies=cookies_in
    )

    if cookies_out:
        await redis_client.setex(
            f"session:{domain}", settings.job_ttl_seconds, json.dumps(cookies_out)
        )

    if not needs_playwright(html):
        result = _build_result(url, normalized, html, status, "httpx", start)
        await redis_client.setex(
            _cache_key(normalized), settings.job_ttl_seconds, json.dumps(result.model_dump())
        )
        return result

    # Slow path: use the resolved URL so Playwright doesn't re-encounter the http→https redirect
    fetch_url = normalize_url(final_url)
    job_id = str(uuid.uuid4())
    await redis_client.setex(
        f"job:{job_id}",
        settings.job_ttl_seconds,
        json.dumps({"status": "pending"}),
    )
    await rabbitmq_channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps({
                "job_id": job_id,
                "url": url,
                "normalized_url": fetch_url,
                "attempt": 1,
            }).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=settings.rabbitmq_queue,
    )

    return JSONResponse(
        status_code=202,
        content=JobResponse(
            job_id=job_id,
            status="pending",
            poll_url=f"/result/{job_id}",
        ).model_dump(),
    )


@app.get("/result/{job_id}")
async def get_result(job_id: str):
    raw = await redis_client.get(f"job:{job_id}")
    if not raw:
        raise HTTPException(status_code=404, detail="Job not found or expired")

    data = json.loads(raw)
    status = data["status"]

    if status == "pending":
        return JSONResponse(
            status_code=202,
            content=JobResponse(
                job_id=job_id,
                status="pending",
                poll_url=f"/result/{job_id}",
            ).model_dump(),
        )

    result = CrawlResult(**data["result"]) if data.get("result") else None
    if result and status == "complete":
        await redis_client.setex(
            _cache_key(result.normalized_url), settings.job_ttl_seconds, json.dumps(result.model_dump())
        )
    return JobResponse(
        job_id=job_id,
        status=status,
        poll_url=f"/result/{job_id}",
        result=result,
    )


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(status="ok", service="api")
