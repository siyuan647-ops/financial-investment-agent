"""Long-running Redis Stream consumer for durable Step19 research tasks."""

import asyncio
import logging
from contextlib import suppress
from types import SimpleNamespace
from uuid import UUID, uuid4

import psycopg
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.agents.direct_deep import (
    DirectDeepBudget,
    DirectDeepResearchAgent,
    ResearchBudgetExceeded,
    ResearchCancelled,
)
from app.agents.role_registry import RoleRegistry
from app.agents.specialists import (
    DynamicMultiAgentResearchGraph,
    QwenSpecialistModels,
    SpecialistBudget,
    _current_prompt_digests,
)
from app.api.analysis import RepositorySecurityResolver
from app.api.auth import Identity, Role
from app.contracts.analysis import SecurityCandidate
from app.contracts.research import ResearchPhase
from app.core.config import load_settings
from app.core.logging import configure_logging
from app.observability.metrics import Metrics
from app.providers.qwen import LLMClient, LLMError
from app.providers.tushare import ProviderAdapter
from app.repositories.llm import ModelInvocationRepository
from app.repositories.research import ResearchRepository
from app.repositories.research_checkpoints import ResearchCheckpointStore
from app.services.business_tool_dataops import DataOpsBusinessToolFactory
from app.services.research_queue import ResearchQueue

logger = logging.getLogger(__name__)


class ResearchWorker:
    def __init__(self, settings, connection, redis, *, worker_id=None):
        self.settings = settings
        self.connection = connection
        self.redis = redis
        self.worker_id = worker_id or f"research-{uuid4().hex}"
        self.repository = ResearchRepository(connection)
        self.queue = ResearchQueue(redis)
        self.metrics = Metrics()
        self.role_registry = RoleRegistry(settings.specialist_model_profiles)
        self.role_registry.validate_prompts(_current_prompt_digests())

    async def run_forever(self):
        while True:
            message = await self.queue.receive(
                self.worker_id,
                lease_seconds=self.settings.deep_research_lease_seconds,
            )
            if message is not None:
                await self.process(message)

    async def heartbeat_once(self, task_id, message_id):
        """Refresh Redis only while this worker still owns the PG lease."""
        renewed = await self.repository.heartbeat(
            task_id,
            self.worker_id,
            self.settings.deep_research_lease_seconds,
        )
        if not renewed:
            return False
        try:
            return await self.queue.refresh(self.worker_id, message_id)
        except RedisError:
            return False

    async def process(self, message):
        try:
            task_id = UUID(message.task_id)
        except ValueError:
            await self.queue.ack(message.message_id)
            return
        claimed = await self.repository.claim(
            task_id,
            self.worker_id,
            self.settings.deep_research_lease_seconds,
        )
        if claimed is None:
            state = await self.repository.queue_state(task_id)
            if state is None or state[0] in {"succeeded", "partial", "failed", "cancelled"}:
                await self.queue.ack(message.message_id)
            elif state[0] == "running" and await self.repository.finish_expired_cancel(task_id):
                await self.queue.ack(message.message_id)
            return
        request_id, owner_id, target, as_of, persisted_security = claimed
        generation = await self.repository.claim_generation(task_id, self.worker_id)
        client = LLMClient(
            self.settings,
            ModelInvocationRepository(self.connection),
            metrics=self.metrics,
        )
        identity = Identity(user_id=owner_id, role=Role.USER)
        if persisted_security is None:
            resolver = RepositorySecurityResolver(self.connection)
            matches = await resolver(target, as_of=as_of, on=None)
            if len(matches) != 1:
                terminal = await self.repository.fail(
                    task_id,
                    self.worker_id,
                    "research_security_not_unique",
                    generation,
                )
                if terminal:
                    await self.queue.ack(message.message_id)
                await client.aclose()
                return
            record = matches[0]
            security = SecurityCandidate(
                code=record.security.ts_code,
                name=record.name,
                exchange=record.security.exchange,
            )
        else:
            security = SecurityCandidate.model_validate(persisted_security)
        role_tokens = self.settings.deep_research_role_max_tokens
        role_registry = self.role_registry
        provider = ProviderAdapter(self.settings)
        collaboration = DynamicMultiAgentResearchGraph(
            QwenSpecialistModels(client, role_registry),
            lambda security: DataOpsBusinessToolFactory(
                SimpleNamespace(**security),
                as_of=as_of,
                provider=provider,
                database=self.connection,
            ).registry(),
            budget=SpecialistBudget(
                role_timeout_seconds=self.settings.deep_research_role_timeout_seconds,
                max_role_tokens=role_tokens,
            ),
            checkpoint_store=ResearchCheckpointStore(
                self.connection, task_id, self.worker_id, generation
            ),
            max_task_tokens=self.settings.deep_research_max_tokens,
            registry=role_registry,
        )
        agent = DirectDeepResearchAgent(
            collaboration=collaboration,
            budget=DirectDeepBudget(
                max_seconds=self.settings.deep_research_timeout_seconds,
                max_tokens=self.settings.deep_research_max_tokens,
            ),
        )

        async def progress(phase, status):
            del status
            await self.repository.renew(
                task_id,
                self.worker_id,
                self.settings.deep_research_lease_seconds,
                ResearchPhase(phase),
            )

        async def cancelled():
            return lease_lost.is_set() or await self.repository.cancel_requested(task_id)

        lease_lost = asyncio.Event()

        async def heartbeat():
            interval = max(1, self.settings.deep_research_lease_seconds // 3)
            while True:
                await asyncio.sleep(interval)
                if not await self.heartbeat_once(task_id, message.message_id):
                    lease_lost.set()
                    return

        terminal = False
        heartbeat_task = asyncio.create_task(heartbeat())
        try:
            state = await agent.run(
                target,
                security=security,
                request_id=request_id,
                task_id=str(task_id),
                identity=identity,
                as_of=as_of,
                progress=progress,
                cancelled=cancelled,
            )
            if lease_lost.is_set():
                raise ValueError("research_lease_lost")
            await self.repository.complete(task_id, self.worker_id, state, generation)
            terminal = True
        except ResearchCancelled:
            terminal = await self.repository.finish_cancel(task_id, self.worker_id)
        except Exception as exc:
            if await self.repository.cancel_requested(task_id):
                terminal = await self.repository.finish_cancel(task_id, self.worker_id)
            else:
                code = _failure_code(exc)
                terminal = await self.repository.fail(task_id, self.worker_id, code, generation)
                logger.warning(
                    "",
                    extra={
                        "event": "research_task_failed",
                        "task_id": str(task_id),
                        "code": code,
                    },
                )
        finally:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
            await client.aclose()
            await provider.aclose()
        if terminal:
            await self.queue.ack(message.message_id)


def _failure_code(exc):
    if isinstance(exc, TimeoutError):
        return "research_timeout"
    if isinstance(exc, ResearchBudgetExceeded):
        return "research_budget_exceeded"
    if isinstance(exc, LLMError):
        return "research_model_failed"
    if isinstance(exc, ValueError):
        safe = str(exc)
        if safe.startswith("research_") and safe.replace("_", "").isalnum():
            return safe[:80]
        return "research_validation_failed"
    return "research_internal_error"


async def _main():
    settings = load_settings()
    configure_logging(settings)
    if not settings.qwen_url or not settings.qwen_token:
        raise RuntimeError("research_qwen_not_configured")
    url = settings.database_url.get_secret_value().replace(
        "postgresql+psycopg://", "postgresql://", 1
    )
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as connection:
        redis = Redis.from_url(
            settings.redis_url.get_secret_value(),
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=10,
        )
        try:
            await ResearchWorker(settings, connection, redis).run_forever()
        finally:
            await redis.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
