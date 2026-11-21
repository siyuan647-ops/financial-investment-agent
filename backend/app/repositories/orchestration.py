"""Durable Step13 batch leases, failures, alerts and partition watermarks."""

import hashlib
from dataclasses import dataclass
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from app.dataops.orchestration import BatchSpec, FailurePolicy
from app.repositories.migrate import MIGRATIONS


async def migrate_orchestration(connection, direction="up"):
    if direction not in {"up", "down"}:
        raise ValueError("invalid_migration_direction")
    checksum = hashlib.sha256((MIGRATIONS / "013_orchestration.up.sql").read_bytes()).hexdigest()
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(600600)")
        cursor = await connection.execute(
            "SELECT version,checksum FROM public.schema_migration WHERE version IN ('012','013')"
        )
        versions = dict(await cursor.fetchall())
        if "012" not in versions:
            raise ValueError("step12_migration_required")
        if "013" in versions and versions["013"] != checksum:
            raise ValueError("migration_checksum_mismatch")
        if (direction == "up") == ("013" in versions):
            return False
        await connection.execute(
            (MIGRATIONS / f"013_orchestration.{direction}.sql").read_text("utf-8")
        )
        if direction == "up":
            await connection.execute(
                "INSERT INTO public.schema_migration(version,checksum) VALUES ('013',%s)",
                (checksum,),
            )
        else:
            await connection.execute("DELETE FROM public.schema_migration WHERE version='013'")
    return True


@dataclass(frozen=True)
class Claim:
    disposition: str
    batch_id: UUID
    result: dict


class OrchestrationRepository:
    def __init__(self, connection):
        self.connection = connection
        self._locks: set[str] = set()

    async def claim(self, spec: BatchSpec, *, allow_retry=False, force_retry=False):
        cursor = await self.connection.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s,13))", (spec.idempotency_key,)
        )
        if not (await cursor.fetchone())[0]:
            return Claim("locked", UUID(int=0), {})
        self._locks.add(spec.idempotency_key)
        try:
            async with self.connection.transaction():
                batch_id = uuid4()
                cursor = await self.connection.execute(
                    "INSERT INTO storage.orchestration_batch "
                    "(batch_id,idempotency_key,domain,partition_key,cursor_kind,cursor_value,"
                    "status,parameters) VALUES (%s,%s,%s,%s,%s,%s,'queued',%s) "
                    "ON CONFLICT(idempotency_key) DO NOTHING RETURNING "
                    "batch_id,status,result,parameters,domain,partition_key,retryable",
                    (
                        batch_id,
                        spec.idempotency_key,
                        spec.domain,
                        spec.partition_key,
                        spec.cursor_kind,
                        spec.cursor_value,
                        Jsonb(spec.parameters),
                    ),
                )
                row = await cursor.fetchone()
                if row is None:
                    cursor = await self.connection.execute(
                        "SELECT batch_id,status,result,parameters,domain,partition_key,retryable "
                        "FROM storage.orchestration_batch WHERE idempotency_key=%s FOR UPDATE",
                        (spec.idempotency_key,),
                    )
                    row = await cursor.fetchone()
                batch_id, status, result, parameters, domain, partition, retryable = row
                if (parameters, domain, partition) != (
                    spec.parameters,
                    spec.domain,
                    spec.partition_key,
                ):
                    raise ValueError("orchestration_idempotency_conflict")
                if status == "succeeded":
                    return Claim("cached", batch_id, result)
                if status == "deferred" and not force_retry:
                    return Claim("deferred", batch_id, result)
                if status == "failed" and not ((allow_retry and retryable) or force_retry):
                    return Claim("blocked", batch_id, result)
                await self.connection.execute(
                    "UPDATE storage.orchestration_batch SET status='running',"
                    "attempt_count=attempt_count+1,started_at=now(),finished_at=NULL,"
                    "reason_code=NULL,retryable=false,updated_at=now() WHERE batch_id=%s",
                    (batch_id,),
                )
                return Claim("execute", batch_id, {})
        except BaseException:
            await self.release(spec)
            raise

    async def succeed(self, spec: BatchSpec, batch_id, result):
        run_id = result.get("run_id")
        async with self.connection.transaction():
            cursor = await self.connection.execute(
                "UPDATE storage.orchestration_batch SET status='succeeded',last_run_id=%s,"
                "reason_code=NULL,retryable=false,result=%s,finished_at=now(),updated_at=now() "
                "WHERE batch_id=%s AND status='running'",
                (run_id, Jsonb(result), batch_id),
            )
            if cursor.rowcount != 1:
                raise ValueError("orchestration_state_conflict")
            await self.connection.execute(
                "INSERT INTO storage.partition_watermark "
                "(domain,partition_key,cursor_kind,cursor_value,batch_id,run_id) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(domain,partition_key) DO UPDATE SET "
                "cursor_kind=EXCLUDED.cursor_kind,cursor_value=EXCLUDED.cursor_value,"
                "batch_id=EXCLUDED.batch_id,run_id=EXCLUDED.run_id,updated_at=now() "
                "WHERE storage.partition_watermark.cursor_value<=EXCLUDED.cursor_value",
                (
                    spec.domain,
                    spec.partition_key,
                    spec.cursor_kind,
                    spec.cursor_value,
                    batch_id,
                    run_id,
                ),
            )
            await self.connection.execute(
                "UPDATE storage.orchestration_alert SET status='resolved',updated_at=now() "
                "WHERE batch_id=%s AND status='open'",
                (batch_id,),
            )

    async def fail(self, batch_id, policy: FailurePolicy, *, run_id=None):
        async with self.connection.transaction():
            cursor = await self.connection.execute(
                "UPDATE storage.orchestration_batch SET status='failed',last_run_id=%s,"
                "reason_code=%s,retryable=%s,result=%s,finished_at=now(),updated_at=now() "
                "WHERE batch_id=%s AND status='running' RETURNING attempt_count",
                (
                    run_id,
                    policy.reason,
                    policy.retryable,
                    Jsonb({"reason": policy.reason, "kind": policy.kind}),
                    batch_id,
                ),
            )
            row = await cursor.fetchone()
            if row is None:
                raise ValueError("orchestration_state_conflict")
            await self.connection.execute(
                "INSERT INTO storage.orchestration_failure "
                "(failure_id,batch_id,attempt,reason_code,failure_kind,retryable,run_id) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (uuid4(), batch_id, row[0], policy.reason, policy.kind, policy.retryable, run_id),
            )
            if policy.kind == "permission":
                await self.connection.execute(
                    "INSERT INTO storage.orchestration_alert(alert_id,batch_id,kind,detail) "
                    "VALUES (%s,%s,'permission_denied',%s) ON CONFLICT(batch_id,kind) DO NOTHING",
                    (uuid4(), batch_id, Jsonb({"reason": policy.reason})),
                )

    async def defer(self, batch_id, reason):
        cursor = await self.connection.execute(
            "UPDATE storage.orchestration_batch SET status='deferred',reason_code=%s,"
            "retryable=true,result=%s,finished_at=now(),updated_at=now() "
            "WHERE batch_id=%s AND status='running'",
            (reason, Jsonb({"reason": reason}), batch_id),
        )
        if cursor.rowcount != 1:
            raise ValueError("orchestration_state_conflict")

    async def release(self, spec: BatchSpec):
        if spec.idempotency_key in self._locks:
            await self.connection.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s,13))", (spec.idempotency_key,)
            )
            self._locks.remove(spec.idempotency_key)

    async def failed_batches(self, *, retryable_only=False):
        predicate = "AND retryable" if retryable_only else ""
        cursor = await self.connection.execute(
            "SELECT batch_id,domain,partition_key,parameters,reason_code,retryable,attempt_count "
            "FROM storage.orchestration_batch WHERE status='failed' "
            + predicate
            + " ORDER BY updated_at,batch_id"
        )
        return await cursor.fetchall()

    async def batch(self, batch_id):
        cursor = await self.connection.execute(
            "SELECT domain,partition_key,parameters,status,retryable,result,"
            "cursor_kind,cursor_value "
            "FROM storage.orchestration_batch WHERE batch_id=%s",
            (batch_id,),
        )
        return await cursor.fetchone()
