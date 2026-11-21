"""Request-scoped external resources for the Step14 API."""

from typing import Annotated

import psycopg
from fastapi import Depends, Request
from redis.asyncio import Redis

from app.services.prefect import PrefectGateway


async def database_connection(request: Request):
    url = request.app.state.settings.database_url.get_secret_value().replace(
        "postgresql+psycopg://", "postgresql://", 1
    )
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as connection:
        yield connection


async def redis_connection(request: Request):
    client = Redis.from_url(
        request.app.state.settings.redis_url.get_secret_value(),
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
    )
    try:
        yield client
    finally:
        await client.aclose()


def prefect_gateway(request: Request):
    settings = request.app.state.settings
    return PrefectGateway(
        settings.prefect_url.get_secret_value(),
        timeout_seconds=min(settings.total_timeout_seconds, 10),
    )


Database = Annotated[object, Depends(database_connection)]
RedisClient = Annotated[object, Depends(redis_connection)]
PrefectClient = Annotated[PrefectGateway, Depends(prefect_gateway)]
