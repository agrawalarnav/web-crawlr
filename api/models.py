from typing import Any, Optional

from pydantic import BaseModel


class CrawlRequest(BaseModel):
    url: str


class CrawlResult(BaseModel):
    url: str
    normalized_url: str
    title: str
    meta_description: str
    meta_keywords: list[str]
    og_tags: dict[str, str]
    canonical_url: str
    h1_tags: list[str]
    structured_data: list[Any]
    language: str
    topics: list[str]
    word_count: int
    http_status: int
    fetch_method: str   # 'httpx' or 'playwright'
    crawl_duration_ms: int
    crawled_at: str     # ISO 8601


class JobResponse(BaseModel):
    job_id: str
    status: str         # pending | complete | failed
    poll_url: str
    result: Optional[CrawlResult] = None


class HealthResponse(BaseModel):
    status: str
    service: str
