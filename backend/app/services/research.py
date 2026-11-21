"""Application service for owner-scoped research task submission and recovery."""

from uuid import uuid4

from app.contracts.research import ResearchTaskAccepted
from app.core.errors import AppError


class ResearchTaskService:
    def __init__(self, repository, queue):
        self.repository = repository
        self.queue = queue

    async def submit(
        self,
        payload,
        *,
        request_id,
        identity,
        idempotency_key,
        as_of,
        security=None,
        target=None,
    ):
        key = idempotency_key or uuid4().hex
        research_input = target or payload.question
        try:
            task_id, created = await self.repository.create(
                request_id=request_id,
                owner_id=identity.user_id,
                idempotency_key=key,
                question=research_input,
                as_of=as_of,
                security=security,
            )
        except ValueError as exc:
            if str(exc) == "research_idempotency_conflict":
                raise AppError(
                    409,
                    "RESEARCH_IDEMPOTENCY_CONFLICT",
                    "Idempotency key was already used for a different request",
                ) from None
            raise
        status = "queued"
        if not created:
            existing = await self.repository.get(task_id, identity.user_id)
            status = existing.status if existing is not None else status
        # Re-enqueue queued work as recovery from a prior Redis outage.
        if status == "queued":
            await self.queue.enqueue(task_id)
        return ResearchTaskAccepted(task_id=task_id, status=status, deduplicated=not created)

    async def get(self, task_id, identity):
        task = await self.repository.get(task_id, identity.user_id)
        if task is None:
            raise AppError(404, "RESEARCH_TASK_NOT_FOUND", "Research task was not found")
        return task

    async def cancel(self, task_id, identity):
        changed = await self.repository.cancel(task_id, identity.user_id)
        task = await self.repository.get(task_id, identity.user_id)
        if task is None:
            raise AppError(404, "RESEARCH_TASK_NOT_FOUND", "Research task was not found")
        if not changed and task.status.value not in {"cancelled", "succeeded", "partial", "failed"}:
            raise AppError(409, "RESEARCH_CANCEL_CONFLICT", "Research task could not be cancelled")
        return task
