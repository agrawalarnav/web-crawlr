import time
from datetime import datetime, timezone

import trafilatura
from langdetect import detect, LangDetectException

from crawler.classifier import extract_topics
from crawler.parser import parse_metadata


def extract_body_text(html: str) -> str:
    text = trafilatura.extract(
        html,
        include_comments=False,
        include_tables=True,
        no_fallback=False,
    )
    return text or ""


def count_words(text: str) -> int:
    return len(text.split()) if text else 0


def detect_language(html_lang: str, body_text: str) -> str:
    if html_lang:
        return html_lang
    if body_text and len(body_text) >= 50:
        try:
            return detect(body_text)
        except LangDetectException:
            pass
    return "unknown"


def build_result_dict(
    url: str,
    normalized: str,
    html: str,
    status: int,
    method: str,
    start: float,
) -> dict:
    meta = parse_metadata(html)
    body = extract_body_text(html)
    return {
        "url": url,
        "normalized_url": normalized,
        "title": meta["title"],
        "meta_description": meta["meta_description"],
        "meta_keywords": meta["meta_keywords"],
        "og_tags": meta["og_tags"],
        "canonical_url": meta["canonical_url"] or normalized,
        "h1_tags": meta["h1_tags"],
        "structured_data": meta["structured_data"],
        "language": detect_language(meta["html_lang"], body),
        "topics": extract_topics(meta["title"], meta["meta_description"], body),
        "word_count": count_words(body),
        "http_status": status,
        "fetch_method": method,
        "crawl_duration_ms": int((time.monotonic() - start) * 1000),
        "crawled_at": datetime.now(timezone.utc).isoformat(),
    }
