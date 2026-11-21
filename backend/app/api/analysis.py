"""Authenticated five-role research API and internal data tool adapter."""

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, Header, Request
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.api.auth import Identity, current_identity
from app.contracts.analysis import SecurityCandidate
from app.contracts.common import SHANGHAI
from app.contracts.research import ResearchRequest, ResearchTask, ResearchTaskAccepted
from app.contracts.routing import UnifiedAnalysisRequest, UnifiedAnalysisResponse
from app.core.errors import AppError
from app.repositories.api import ApiRepository
from app.repositories.research import ResearchRepository
from app.services.research import ResearchTaskService
from app.services.research_queue import ResearchQueue

router = APIRouter(
    prefix="/api/v1/analysis",
    tags=["analysis"],
    dependencies=[Depends(current_identity)],
)


class RepositorySecurityResolver:
    def __init__(self, database):
        self.repository = ApiRepository(database)

    async def __call__(self, text, *, as_of, on):
        return await self.repository.resolve_security(text, as_of, on_date=on)


@asynccontextmanager
async def _production_target_services(request):
    settings = request.app.state.settings
    url = settings.database_url.get_secret_value().replace(
        "postgresql+psycopg://", "postgresql://", 1
    )
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as database:
        redis = Redis.from_url(
            settings.redis_url.get_secret_value(),
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        try:
            yield (
                RepositorySecurityResolver(database),
                ResearchTaskService(ResearchRepository(database), ResearchQueue(redis)),
            )
        finally:
            await redis.aclose()


@asynccontextmanager
async def _target_services(request):
    override = getattr(request.app.state, "target_research_services", None)
    if override is not None:
        yield override
        return
    async with _production_target_services(request) as services:
        yield services


@router.post("/query", status_code=202, response_model=UnifiedAnalysisResponse)
async def analyze_query(
    payload: UnifiedAnalysisRequest,
    request: Request,
    identity: Annotated[Identity, Depends(current_identity)],
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
):
    as_of = payload.as_of or datetime.now(SHANGHAI)
    try:
        async with _target_services(request) as (resolver, research):
            matches = await resolver(payload.target, as_of=as_of, on=None)
            if not matches:
                raise AppError(
                    422,
                    "RESEARCH_TARGET_NOT_FOUND",
                    "Target must be an exact A-share name or exchange-qualified code",
                )
            if len(matches) != 1:
                raise AppError(
                    409,
                    "RESEARCH_TARGET_AMBIGUOUS",
                    "Target resolved to more than one A-share security",
                )
            record = matches[0]
            security = SecurityCandidate(
                code=record.security.ts_code,
                name=record.name,
                exchange=record.security.exchange,
            )
            accepted = await research.submit(
                payload,
                request_id=request.state.request_id,
                identity=identity,
                idempotency_key=idempotency_key,
                as_of=as_of,
                security=security,
                target=security.code,
            )
            return UnifiedAnalysisResponse(
                request_id=request.state.request_id,
                target=payload.target,
                as_of=as_of,
                security=security,
                task_id=accepted.task_id,
                task_status=accepted.status,
                deduplicated=accepted.deduplicated,
            )
    except RedisError:
        raise AppError(503, "RESEARCH_QUEUE_UNAVAILABLE", "Research queue is unavailable") from None


@asynccontextmanager
async def _production_research_service(request):
    settings = request.app.state.settings
    url = settings.database_url.get_secret_value().replace(
        "postgresql+psycopg://", "postgresql://", 1
    )
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as database:
        redis = Redis.from_url(
            settings.redis_url.get_secret_value(),
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        try:
            yield ResearchTaskService(ResearchRepository(database), ResearchQueue(redis))
        finally:
            await redis.aclose()


@asynccontextmanager
async def _research_service(request):
    override = getattr(request.app.state, "research_task_service", None)
    if override is not None:
        yield override
        return
    async with _production_research_service(request) as service:
        yield service


@router.post("/research", status_code=202, response_model=ResearchTaskAccepted)
async def submit_research(
    payload: ResearchRequest,
    request: Request,
    identity: Annotated[Identity, Depends(current_identity)],
    idempotency_key: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=128,
            pattern=r"^[a-zA-Z0-9_.:-]+$",
        ),
    ] = None,
):
    try:
        async with _research_service(request) as service:
            return await service.submit(
                payload,
                request_id=request.state.request_id,
                identity=identity,
                idempotency_key=idempotency_key,
                as_of=payload.as_of or datetime.now(SHANGHAI),
            )
    except RedisError:
        raise AppError(503, "RESEARCH_QUEUE_UNAVAILABLE", "Research queue is unavailable") from None


@router.get("/research/{task_id}", response_model=ResearchTask)
async def get_research(
    task_id: UUID,
    request: Request,
    identity: Annotated[Identity, Depends(current_identity)],
):
    async with _research_service(request) as service:
        return await service.get(task_id, identity)


@router.post("/research/{task_id}/cancel", response_model=ResearchTask)
async def cancel_research(
    task_id: UUID,
    request: Request,
    identity: Annotated[Identity, Depends(current_identity)],
):
    async with _research_service(request) as service:
        return await service.cancel(task_id, identity)
