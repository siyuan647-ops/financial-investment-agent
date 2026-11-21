"""Bounded, authenticated protocol checks; never expose exception text or URLs."""

import asyncio

import httpx
import psycopg
from redis.asyncio import Redis

from app.core.config import Settings


def database_check(settings: Settings):
    url = settings.database_url.get_secret_value().replace(
        "postgresql+psycopg://", "postgresql://", 1
    )
    with psycopg.connect(url, connect_timeout=2, options="-c statement_timeout=2000") as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
            if cursor.fetchone() is None:
                raise RuntimeError("required extension absent")


async def redis_check(settings: Settings):
    async with Redis.from_url(
        settings.redis_url.get_secret_value(), socket_connect_timeout=2, socket_timeout=2
    ) as client:
        if not await client.ping():
            raise RuntimeError("ping failed")


async def http_check(url: str):
    async with httpx.AsyncClient(timeout=2, trust_env=False) as client:
        response = await client.get(url)
        response.raise_for_status()


async def check_dependencies(settings: Settings) -> dict[str, bool]:
    checks = {
        "database": lambda: asyncio.to_thread(database_check, settings),
        "redis": lambda: redis_check(settings),
        "prefect": lambda: http_check(
            settings.prefect_url.get_secret_value().rstrip("/") + "/health"
        ),
        "worker": lambda: http_check(
            settings.prefect_worker_url.get_secret_value().rstrip("/") + "/health"
        ),
    }

    async def check(name, operation):
        try:
            async with asyncio.timeout(settings.readiness_timeout_seconds):
                await operation()
            return name, True
        except Exception:
            return name, False

    return dict(
        await asyncio.gather(*(check(name, operation) for name, operation in checks.items()))
    )
