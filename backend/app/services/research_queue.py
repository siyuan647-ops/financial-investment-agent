"""Redis Streams queue with consumer acknowledgement and stale-message reclaim."""

from dataclasses import dataclass

from redis.exceptions import ResponseError


@dataclass(frozen=True)
class ResearchQueueMessage:
    message_id: str
    task_id: str


class ResearchQueue:
    def __init__(self, redis, *, stream="research:tasks", group="research-workers"):
        self.redis = redis
        self.stream = stream
        self.group = group
        self._ready = False

    async def ensure_group(self):
        if self._ready:
            return
        try:
            await self.redis.xgroup_create(self.stream, self.group, id="0-0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._ready = True

    async def enqueue(self, task_id):
        await self.ensure_group()
        return await self.redis.xadd(self.stream, {"task_id": str(task_id)})

    async def receive(self, consumer, *, lease_seconds, block_ms=5_000):
        await self.ensure_group()
        reclaimed = await self.redis.xautoclaim(
            self.stream,
            self.group,
            consumer,
            min_idle_time=lease_seconds * 1_000,
            start_id="0-0",
            count=1,
        )
        pending = reclaimed[1] if len(reclaimed) > 1 else ()
        if pending:
            return _message(pending[0])
        batches = await self.redis.xreadgroup(
            self.group,
            consumer,
            {self.stream: ">"},
            count=1,
            block=block_ms,
        )
        if not batches:
            return None
        return _message(batches[0][1][0])

    async def ack(self, message_id):
        return await self.redis.xack(self.stream, self.group, message_id)

    async def refresh(self, consumer, message_id):
        """Reset pending-message idle time for the worker holding the PG lease."""
        refreshed = await self.redis.xclaim(
            self.stream,
            self.group,
            consumer,
            min_idle_time=0,
            message_ids=[message_id],
            justid=True,
        )
        return message_id in refreshed


def _message(item):
    message_id, fields = item
    task_id = fields.get("task_id")
    if not task_id:
        raise ValueError("research_queue_message_invalid")
    return ResearchQueueMessage(message_id=message_id, task_id=task_id)
