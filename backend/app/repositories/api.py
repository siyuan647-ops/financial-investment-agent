"""Step14 read metadata and durable API task ownership."""

import argparse
import asyncio
import hashlib
import json
import os
from uuid import uuid4

from psycopg import sql
from psycopg.types.json import Jsonb

from app.contracts.domain import Security
from app.repositories.migrate import MIGRATIONS


async def migrate_api(connection, direction="up"):
    if direction not in {"up", "down"}:
        raise ValueError("invalid_migration_direction")
    checksum = hashlib.sha256((MIGRATIONS / "014_api.up.sql").read_bytes()).hexdigest()
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(600600)")
        cursor = await connection.execute(
            "SELECT version,checksum FROM public.schema_migration WHERE version IN ('013','014')"
        )
        versions = dict(await cursor.fetchall())
        if "013" not in versions:
            raise ValueError("step13_migration_required")
        if "014" in versions and versions["014"] != checksum:
            raise ValueError("migration_checksum_mismatch")
        if (direction == "up") == ("014" in versions):
            return False
        await connection.execute((MIGRATIONS / f"014_api.{direction}.sql").read_text("utf-8"))
        if direction == "up":
            await connection.execute(
                "INSERT INTO public.schema_migration(version,checksum) VALUES ('014',%s)",
                (checksum,),
            )
        else:
            await connection.execute("DELETE FROM public.schema_migration WHERE version='014'")
    return True


TABLE_DATES = {
    "price_bar": "trade_date",
    "adjustment_factor": "trade_date",
    "valuation": "trade_date",
    "financial_fact": "period_end",
    "corporate_action": None,
    "industry_membership": None,
}


class ApiRepository:
    def __init__(self, connection):
        self.connection = connection

    async def version_token(self, kinds, code, as_of, *, start=None, end=None):
        versions = []
        for kind in kinds:
            date_column = TABLE_DATES[kind]
            date_filter = sql.SQL("")
            arguments = [code, as_of]
            if date_column and start is not None and end is not None:
                date_filter = sql.SQL("AND d.{} BETWEEN %s AND %s ").format(
                    sql.Identifier(date_column)
                )
                arguments.extend((start, end))
            cursor = await self.connection.execute(
                sql.SQL(
                    "SELECT v.data_version FROM storage.{} d "
                    "JOIN storage.record_version v USING(data_version) "
                    "WHERE d.ts_code=%s AND v.available_at<=%s {} "
                    "ORDER BY v.data_version"
                ).format(sql.Identifier(kind), date_filter),
                tuple(arguments),
            )
            versions.extend(row[0] for row in await cursor.fetchall())
        canonical = json.dumps(sorted(set(versions)), separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()

    async def resolve_security(self, text, as_of, *, exchange=None, on_date=None):
        needle = text.strip()
        if not needle:
            return []
        bare_code = needle if len(needle) == 6 and needle.isdigit() else None
        full_code = needle.upper() if len(needle) == 9 else None
        cursor = await self.connection.execute(
            "WITH alias_codes AS ("
            " SELECT DISTINCT a.ts_code FROM storage.security_alias a "
            " JOIN storage.record_version av USING(data_version) "
            " WHERE lower(a.payload->>'name')=lower(%s) AND av.available_at<=%s "
            " AND (%s::date IS NULL OR ((a.payload->>'effective_from')::date<=%s "
            " AND (a.payload->>'effective_to' IS NULL "
            " OR (a.payload->>'effective_to')::date>=%s)))) "
            "SELECT DISTINCT ON (s.ts_code) s.payload FROM storage.security s "
            "JOIN storage.record_version v USING(data_version) "
            "WHERE v.available_at<=%s AND (s.ts_code=%s OR left(s.ts_code,6)=%s "
            "OR (%s::date IS NULL AND lower(s.payload->>'name')=lower(%s)) "
            "OR s.ts_code IN (SELECT ts_code FROM alias_codes)) "
            "AND (%s::text IS NULL OR s.payload->'security'->>'exchange'=%s) "
            "ORDER BY s.ts_code,v.available_at DESC,v.fetched_at DESC,v.data_version DESC "
            "LIMIT 31",
            (
                needle,
                as_of,
                on_date,
                on_date,
                on_date,
                as_of,
                full_code,
                bare_code,
                on_date,
                needle,
                exchange,
                exchange,
            ),
        )
        return [Security.model_validate(row[0]) for row in await cursor.fetchall()]

    async def create_task(self, owner_id, kind, deployment, parameters, idempotency_key):
        task_id = uuid4()
        cursor = await self.connection.execute(
            "INSERT INTO storage.api_task "
            "(task_id,owner_id,kind,deployment,status,parameters,idempotency_key) "
            "VALUES (%s,%s,%s,%s,'submitting',%s,%s) "
            "ON CONFLICT(idempotency_key) DO NOTHING RETURNING task_id",
            (task_id, owner_id, kind, deployment, Jsonb(parameters), idempotency_key),
        )
        row = await cursor.fetchone()
        if row:
            return row[0], True
        cursor = await self.connection.execute(
            "SELECT task_id FROM storage.api_task WHERE idempotency_key=%s",
            (idempotency_key,),
        )
        return (await cursor.fetchone())[0], False

    async def attach_flow_run(self, task_id, flow_run_id):
        await self.connection.execute(
            "UPDATE storage.api_task SET flow_run_id=%s,status='submitted',updated_at=now() "
            "WHERE task_id=%s AND status='submitting'",
            (flow_run_id, task_id),
        )

    async def fail_submission(self, task_id, reason="prefect_unavailable"):
        await self.connection.execute(
            "UPDATE storage.api_task SET status='submission_failed',result=%s,updated_at=now() "
            "WHERE task_id=%s AND status='submitting'",
            (Jsonb({"reason": reason}), task_id),
        )

    async def task(self, task_id):
        cursor = await self.connection.execute(
            "SELECT task_id,owner_id,kind,status,deployment,flow_run_id,parameters,result,"
            "created_at,updated_at FROM storage.api_task WHERE task_id=%s",
            (task_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        keys = (
            "task_id",
            "owner_id",
            "kind",
            "status",
            "deployment",
            "flow_run_id",
            "parameters",
            "result",
            "created_at",
            "updated_at",
        )
        return dict(zip(keys, row, strict=True))

    async def data_status(self, code):
        cursor = await self.connection.execute(
            "SELECT domain,partition_key,cursor_kind,cursor_value,batch_id,run_id,updated_at "
            "FROM storage.partition_watermark WHERE partition_key=%s "
            "OR partition_key LIKE %s ORDER BY domain,partition_key",
            (code, f"%{code}%"),
        )
        watermarks = [
            dict(
                zip(
                    (
                        "domain",
                        "partition_key",
                        "cursor_kind",
                        "cursor_value",
                        "batch_id",
                        "run_id",
                        "updated_at",
                    ),
                    row,
                    strict=True,
                )
            )
            for row in await cursor.fetchall()
        ]
        cursor = await self.connection.execute(
            "SELECT batch_id,domain,partition_key,status,reason_code,retryable,updated_at "
            "FROM storage.orchestration_batch WHERE partition_key=%s OR partition_key LIKE %s "
            "ORDER BY updated_at DESC LIMIT 20",
            (code, f"%{code}%"),
        )
        batches = [
            dict(
                zip(
                    (
                        "batch_id",
                        "domain",
                        "partition_key",
                        "status",
                        "reason_code",
                        "retryable",
                        "updated_at",
                    ),
                    row,
                    strict=True,
                )
            )
            for row in await cursor.fetchall()
        ]
        return {"watermarks": watermarks, "batches": batches}

    async def orchestration_results(self, idempotency_keys):
        if not idempotency_keys:
            return []
        cursor = await self.connection.execute(
            "SELECT batch_id,idempotency_key,domain,partition_key,status,reason_code,retryable,"
            "result,attempt_count,updated_at FROM storage.orchestration_batch "
            "WHERE idempotency_key=ANY(%s) ORDER BY partition_key,cursor_value,batch_id",
            (list(idempotency_keys),),
        )
        keys = (
            "batch_id",
            "idempotency_key",
            "domain",
            "partition_key",
            "status",
            "reason_code",
            "retryable",
            "result",
            "attempt_count",
            "updated_at",
        )
        return [dict(zip(keys, row, strict=True)) for row in await cursor.fetchall()]


def task_idempotency_key(owner_id, kind, deployment, parameters):
    canonical = json.dumps(
        {"owner": owner_id, "kind": kind, "deployment": deployment, "parameters": parameters},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return "step14:" + hashlib.sha256(canonical.encode()).hexdigest()


async def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument("direction", choices=("up", "down"))
    args = parser.parse_args()
    import psycopg

    url = os.environ["APP_DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://", 1)
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as connection:
        changed = await migrate_api(connection, args.direction)
    print("migration_applied" if changed else "migration_unchanged")


if __name__ == "__main__":
    asyncio.run(_main())
