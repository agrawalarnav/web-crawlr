from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    rabbitmq_queue: str = "crawl_jobs"
    rabbitmq_dlq: str = "crawl_jobs.dlq"
    job_ttl_seconds: int = 3600
    httpx_timeout: float = 10.0
    playwright_timeout_ms: int = 30_000
    max_retries: int = 3
    proxy_url: str | None = None  # e.g. http://user:pass@host:port or socks5://...

    class Config:
        env_file = ".env"


settings = Settings()
