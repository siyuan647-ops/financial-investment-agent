"""Durable Step19 research-task state, leases, events and idempotent outcomes."""

import argparse
import asyncio
import hashlib
import json
import os
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from app.contracts.research import (
    ResearchEvent,
    ResearchExecution,
    ResearchPhase,
    ResearchPlan,
    ResearchReport,
    ResearchStatus,
    ResearchTask,
    ResearchVerification,
)
from app.repositories.migrate import MIGRATIONS


async def migrate_research(connection, direction="up"):
    if direction not in {"up", "down"}:
        raise ValueError("invalid_migration_direction")
    checksum = hashlib.sha256((MIGRATIONS / "019_research.up.sql").read_bytes()).hexdigest()
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(600600)")
        cursor = await connection.execute(
            "SELECT version,checksum FROM public.schema_migration WHERE version IN ('016','019')"
        )
        versions = dict(await cursor.fetchall())
        if "016" not in versions:
            raise ValueError("step16_migration_required")
        if "019" in versions and versions["019"] != checksum:
            raise ValueError("migration_checksum_mismatch")
        if (direction == "up") == ("019" in versions):
            return False
        await connection.execute((MIGRATIONS / f"019_research.{direction}.sql").read_text("utf-8"))
        if direction == "up":
            await connection.execute(
                "INSERT INTO public.schema_migration(version,checksum) VALUES ('019',%s)",
                (checksum,),
            )
        else:
            await connection.execute("DELETE FROM public.schema_migration WHERE version='019'")
    return True


def payload_hash(question, as_of):
    payload = json.dumps(
        {"question": question, "as_of": as_of.isoformat()},
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


class ResearchRepository:
    def __init__(self, connection):
        self.connection = connection

    async def create(
        self,
        *,
        request_id,
        owner_id,
        idempotency_key,
        question,
        as_of,
        security=None,
    ):
        task_id = uuid4()
        digest = payload_hash(question, as_of)
        async with self.connection.transaction():
            cursor = await self.connection.execute(
                "INSERT INTO storage.research_task "
                "(task_id,request_id,owner_id,idempotency_key,payload_hash,question,"
                "as_of,security) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(owner_id,idempotency_key) "
                "DO NOTHING RETURNING task_id",
                (
                    task_id,
                    request_id,
                    owner_id,
                    idempotency_key,
                    digest,
                    question,
                    as_of,
                    None if security is None else Jsonb(security.model_dump(mode="json")),
                ),
            )
            row = await cursor.fetchone()
            created = row is not None
            if created:
                await self._event(task_id, ResearchPhase.QUEUED, ResearchStatus.QUEUED, "queued")
                return task_id, True
            cursor = await self.connection.execute(
                "SELECT task_id,payload_hash FROM storage.research_task "
                "WHERE owner_id=%s AND idempotency_key=%s FOR UPDATE",
                (owner_id, idempotency_key),
            )
            existing = await cursor.fetchone()
            if existing is None:
                raise ValueError("research_idempotency_race")
            if existing[1] != digest:
                raise ValueError("research_idempotency_conflict")
            return existing[0], False

    async def claim(self, task_id, worker_id, lease_seconds):
        cursor = await self.connection.execute(
            "UPDATE storage.research_task SET status='running',phase='plan',"
            "attempt_count=attempt_count+1,lease_owner=%s,"
            "lease_until=now()+(%s * interval '1 second'),updated_at=now() "
            "WHERE task_id=%s AND NOT cancel_requested AND "
            "(status='queued' OR (status='running' AND lease_until<now())) "
            "RETURNING request_id,owner_id,question,as_of,security",
            (worker_id, lease_seconds, task_id),
        )
        row = await cursor.fetchone()
        if row is not None:
            await self._event(task_id, ResearchPhase.PLAN, ResearchStatus.RUNNING, "claimed")
        return row

    async def queue_state(self, task_id):
        """Read the authoritative task state before discarding a duplicate queue message."""
        cursor = await self.connection.execute(
            "SELECT status,lease_owner,lease_until FROM storage.research_task WHERE task_id=%s",
            (task_id,),
        )
        return await cursor.fetchone()

    async def claim_generation(self, task_id, worker_id):
        cursor = await self.connection.execute(
            "SELECT attempt_count FROM storage.research_task WHERE task_id=%s "
            "AND status='running' AND lease_owner=%s AND lease_until>=now()",
            (task_id, worker_id),
        )
        row = await cursor.fetchone()
        if row is None:
            raise ValueError("research_lease_lost")
        return row[0]

    async def finish_expired_cancel(self, task_id):
        """Close a cancelled task whose previous worker can no longer finish it."""
        cursor = await self.connection.execute(
            "UPDATE storage.research_task SET status='cancelled',phase='completed',"
            "lease_owner=NULL,lease_until=NULL,completed_at=now(),updated_at=now() "
            "WHERE task_id=%s AND status='running' AND cancel_requested "
            "AND lease_until<now() RETURNING task_id",
            (task_id,),
        )
        if await cursor.fetchone() is None:
            return False
        await self._event(task_id, ResearchPhase.COMPLETED, ResearchStatus.CANCELLED, "cancelled")
        return True

    async def renew(self, task_id, worker_id, lease_seconds, phase):
        cursor = await self.connection.execute(
            "UPDATE storage.research_task SET phase=%s,"
            "lease_until=now()+(%s * interval '1 second'),updated_at=now() "
            "WHERE task_id=%s AND status='running' AND lease_owner=%s "
            "AND lease_until>=now() AND NOT cancel_requested",
            (phase.value, lease_seconds, task_id, worker_id),
        )
        if cursor.rowcount != 1:
            raise ValueError("research_lease_lost")
        await self._event(task_id, phase, ResearchStatus.RUNNING, f"{phase.value}_started")

    async def heartbeat(self, task_id, worker_id, lease_seconds):
        cursor = await self.connection.execute(
            "UPDATE storage.research_task SET "
            "lease_until=now()+(%s * interval '1 second'),updated_at=now() "
            "WHERE task_id=%s AND status='running' AND lease_owner=%s "
            "AND lease_until>=now() AND NOT cancel_requested",
            (lease_seconds, task_id, worker_id),
        )
        return cursor.rowcount == 1

    async def complete(self, task_id, worker_id, state, generation=None):
        status = ResearchStatus(state["status"])
        if status not in {ResearchStatus.SUCCEEDED, ResearchStatus.PARTIAL}:
            raise ValueError("invalid_research_completion")
        cursor = await self.connection.execute(
            "UPDATE storage.research_task SET status=%s,phase='completed',plan=%s,security=%s,"
            "execution=%s,report=%s,verification=%s,lease_owner=NULL,lease_until=NULL,"
            "completed_at=now(),updated_at=now() WHERE task_id=%s AND status='running' "
            "AND lease_owner=%s AND lease_until>=now() AND NOT cancel_requested "
            "AND (%s IS NULL OR attempt_count=%s)",
            (
                status.value,
                Jsonb(state["plan"].model_dump(mode="json")),
                Jsonb(state["security"].model_dump(mode="json")),
                Jsonb(state["execution"].model_dump(mode="json")),
                Jsonb(state["report"].model_dump(mode="json")),
                Jsonb(state["verification"].model_dump(mode="json")),
                task_id,
                worker_id,
                generation,
                generation,
            ),
        )
        if cursor.rowcount != 1:
            raise ValueError("research_completion_conflict")
        code = "verified" if status == ResearchStatus.SUCCEEDED else "verification_incomplete"
        await self._event(task_id, ResearchPhase.COMPLETED, status, code)

    async def fail(self, task_id, worker_id, code, generation=None):
        cursor = await self.connection.execute(
            "UPDATE storage.research_task SET status='failed',phase='completed',failure_code=%s,"
            "lease_owner=NULL,lease_until=NULL,completed_at=now(),updated_at=now() "
            "WHERE task_id=%s AND status='running' AND lease_owner=%s AND NOT cancel_requested "
            "AND (%s IS NULL OR attempt_count=%s)",
            (code, task_id, worker_id, generation, generation),
        )
        if cursor.rowcount == 1:
            await self._event(task_id, ResearchPhase.COMPLETED, ResearchStatus.FAILED, code)
        return cursor.rowcount == 1

    async def cancel(self, task_id, owner_id):
        async with self.connection.transaction():
            cursor = await self.connection.execute(
                "UPDATE storage.research_task SET cancel_requested=true,"
                "status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END,"
                "phase=CASE WHEN status='queued' THEN 'completed' ELSE phase END,"
                "completed_at=CASE WHEN status='queued' THEN now() ELSE completed_at END,"
                "updated_at=now() WHERE task_id=%s AND owner_id=%s "
                "AND status IN ('queued','running') RETURNING status",
                (task_id, owner_id),
            )
            row = await cursor.fetchone()
            if row and row[0] == ResearchStatus.CANCELLED.value:
                await self._event(
                    task_id,
                    ResearchPhase.COMPLETED,
                    ResearchStatus.CANCELLED,
                    "cancelled",
                )
            return row is not None

    async def finish_cancel(self, task_id, worker_id):
        cursor = await self.connection.execute(
            "UPDATE storage.research_task SET status='cancelled',phase='completed',"
            "lease_owner=NULL,lease_until=NULL,completed_at=now(),updated_at=now() "
            "WHERE task_id=%s AND status='running' AND lease_owner=%s AND cancel_requested",
            (task_id, worker_id),
        )
        if cursor.rowcount == 1:
            await self._event(
                task_id,
                ResearchPhase.COMPLETED,
                ResearchStatus.CANCELLED,
                "cancelled",
            )
        return cursor.rowcount == 1

    async def cancel_requested(self, task_id):
        cursor = await self.connection.execute(
            "SELECT cancel_requested FROM storage.research_task WHERE task_id=%s", (task_id,)
        )
        row = await cursor.fetchone()
        return row is None or row[0]

    async def get(self, task_id, owner_id):
        cursor = await self.connection.execute(
            "SELECT request_id,owner_id,question,as_of,status,phase,cancel_requested,plan,"
            "security,execution,report,verification,failure_code,created_at,updated_at "
            "FROM storage.research_task WHERE task_id=%s AND owner_id=%s",
            (task_id, owner_id),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        events_cursor = await self.connection.execute(
            "SELECT sequence,phase,status,code,occurred_at FROM storage.research_event "
            "WHERE task_id=%s ORDER BY sequence",
            (task_id,),
        )
        events = tuple(
            ResearchEvent(
                sequence=item[0],
                phase=item[1],
                status=item[2],
                code=item[3],
                occurred_at=item[4],
            )
            for item in await events_cursor.fetchall()
        )
        return ResearchTask(
            task_id=UUID(str(task_id)),
            request_id=row[0],
            owner_id=row[1],
            question=row[2],
            as_of=row[3],
            status=row[4],
            phase=row[5],
            cancel_requested=row[6],
            plan=None if row[7] is None else ResearchPlan.model_validate(row[7]),
            security=row[8],
            execution=None if row[9] is None else ResearchExecution.model_validate(row[9]),
            report=None if row[10] is None else ResearchReport.model_validate(row[10]),
            verification=None if row[11] is None else ResearchVerification.model_validate(row[11]),
            failure_code=row[12],
            events=events,
            created_at=row[13],
            updated_at=row[14],
        )

    async def _event(self, task_id, phase, status, code):
        await self.connection.execute(
            "INSERT INTO storage.research_event(task_id,phase,status,code) VALUES (%s,%s,%s,%s)",
            (task_id, phase.value, status.value, code),
        )


async def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument("direction", choices=("up", "down"))
    parser.add_argument("--allow-data-loss", action="store_true")
    args = parser.parse_args()
    if args.direction == "down" and not args.allow_data_loss:
        parser.error("down requires --allow-data-loss after a verified backup")
    import psycopg

    url = os.environ["APP_DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://", 1)
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as connection:
        changed = await migrate_research(connection, args.direction)
    print("migration_applied" if changed else "migration_unchanged")


if __name__ == "__main__":
    asyncio.run(_main())
