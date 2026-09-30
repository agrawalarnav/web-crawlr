import asyncio
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from playwright.async_api import Browser, TimeoutError as PlaywrightTimeoutError
from urllib.parse import urlparse

from config import settings
from crawler.resolver import resolve
from playwright_stealth import stealth_async

MINIMUM_TEXT_CHARS = 200

BLOCKED_RESOURCE_TYPES = {
    "image", "stylesheet", "font", "media",
    "texttrack", "object", "beacon", "csp_report", "imageset",
}

_HTTPX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _url_candidates(url: str) -> list[str]:
    candidates = [url]
    if url.startswith("http://"):
        candidates.append(url.replace("http://", "https://", 1))
    return candidates


def _build_playwright_proxy() -> dict | None:
    if not settings.proxy_url:
        return None
    parsed = urlparse(settings.proxy_url)
    proxy = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
    if parsed.username:
        proxy["username"] = parsed.username
    if parsed.password:
        proxy["password"] = parsed.password
    return proxy


_PLAYWRIGHT_PROXY = _build_playwright_proxy()


async def fetch_fast(
    url: str,
    timeout: float = 10.0,
    cookies: dict | None = None,
) -> tuple[str | None, int, str, dict]:
    """Returns (html, status, final_url, cookies). Tries https if http fails."""
    hostname = urlparse(url).hostname or ""
    if hostname and not await resolve(hostname):
        return None, 0, url, cookies or {}

    async with AsyncSession(impersonate="chrome124") as session:
        if cookies:
            session.cookies.update(cookies)
        for candidate in _url_candidates(url):
            try:
                r = await session.get(
                    candidate,
                    headers=_HTTPX_HEADERS,
                    proxies={"all": settings.proxy_url} if settings.proxy_url else None,
                    allow_redirects=True,
                    timeout=timeout,
                )
                return r.text, r.status_code, r.url, dict(session.cookies)
            except Exception:
                continue
    return None, 0, url, cookies or {}


async def fetch_with_browser(
    url: str,
    browser: Browser,
    timeout_ms: int = 30_000,
    storage_state: dict | None = None,
) -> tuple[str, int, dict]:
    """Playwright fetch — slow path for JS-heavy pages. Returns (html, status, storage_state)."""
    context_kwargs = dict(
        user_agent=_HTTPX_HEADERS["User-Agent"],
        viewport={"width": 1280, "height": 800},
        locale="en-US",
        timezone_id="America/New_York",
        proxy=_PLAYWRIGHT_PROXY,
    )
    if storage_state:
        context_kwargs["storage_state"] = storage_state
    context = await browser.new_context(**context_kwargs)
    try:
        page = await context.new_page()
        await stealth_async(page)

        async def _block(route):
            if route.request.resource_type in BLOCKED_RESOURCE_TYPES:
                await route.abort()
            else:
                await route.continue_()

        await page.route("**/*", _block)

        status = 0
        last_exc: Exception | None = None
        for candidate in _url_candidates(url):
            try:
                response = await page.goto(candidate, wait_until="networkidle", timeout=timeout_ms)
                if response:
                    status = response.status
                break
            except PlaywrightTimeoutError:
                # networkidle timed out — if the page actually loaded, proceed with what we have
                if page.url not in ("about:blank", ""):
                    break
                last_exc = RuntimeError(f"Navigation timed out for {url}")
                continue
            except Exception as exc:
                if page.url in ("about:blank", ""):
                    last_exc = exc
                    continue
                raise
        else:
            raise last_exc or RuntimeError(f"Navigation failed for {url}")

        try:
            await page.wait_for_load_state("load", timeout=timeout_ms)
        except Exception:
            pass

        # page.content() can raise if a redirect is still in flight
        html = None
        for _ in range(3):
            try:
                html = await page.content()
                break
            except Exception:
                await asyncio.sleep(1)
        if html is None:
            raise RuntimeError(f"Could not read page content for {url}")

        state_out = await context.storage_state()
        return html, status, state_out
    finally:
        await context.close()


def needs_playwright(html: str | None) -> bool:
    if not html:
        return True
    soup = BeautifulSoup(html, "lxml")
    body = soup.find("body")
    if not body:
        return True
    return len(body.get_text(strip=True)) < MINIMUM_TEXT_CHARS
