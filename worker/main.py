import asyncio
import json
import time
from urllib.parse import urlparse

import aio_pika
import redis.asyncio as aioredis
from playwright.async_api import async_playwright

from config import settings
from crawler.extractor import build_result_dict
from crawler.fetcher import fetch_with_browser


async def process_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    browser,
    redis_client: aioredis.Redis,
    channel: aio_pika.abc.AbstractChannel,
) -> None:
    data = json.loads(message.body)
    job_id = data["job_id"]
    url = data["url"]
    normalized = data.get("normalized_url", url)
    attempt = data.get("attempt", 1)

    start = time.monotonic()
    domain = urlparse(normalized).netloc

    try:
        raw_state = await redis_client.get(f"session:{domain}")
        storage_state_in = json.loads(raw_state) if raw_state else None

        html, status, storage_state_out = await fetch_with_browser(
            normalized, browser, settings.playwright_timeout_ms, storage_state_in
        )

        await redis_client.setex(
            f"session:{domain}", settings.job_ttl_seconds, json.dumps(storage_state_out)
        )

        result = build_result_dict(url, normalized, html, status, "playwright", start)
        await redis_client.setex(
            f"job:{job_id}",
            settings.job_ttl_seconds,
            json.dumps({"status": "complete", "result": result}),
        )
        await message.ack()

    except Exception as exc:
        if attempt < settings.max_retries:
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps({**data, "attempt": attempt + 1}).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=settings.rabbitmq_queue,
            )
            await message.ack()  # ack original — replacement is now queued
        else:
            await redis_client.setex(
                f"job:{job_id}",
                settings.job_ttl_seconds,
                json.dumps({"status": "failed", "reason": str(exc)}),
            )
            await message.reject(requeue=False)


async def main() -> None:
    redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    channel = await connection.channel()

    await channel.set_qos(prefetch_count=1)

    queue = await channel.declare_queue(
        settings.rabbitmq_queue,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": settings.rabbitmq_dlq,
        },
    )
    await channel.declare_queue(settings.rabbitmq_dlq, durable=True)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        print(f"Worker ready — consuming from '{settings.rabbitmq_queue}'")

        tasks: set[asyncio.Task] = set()
        async with queue.iterator() as q_iter:
            async for message in q_iter:
                task = asyncio.create_task(
                    process_message(message, browser, redis_client, channel)
                )
                tasks.add(task)
                task.add_done_callback(tasks.discard)

        await browser.close()

    await connection.close()
    await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
