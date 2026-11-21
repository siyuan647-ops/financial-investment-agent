"""Persistence and as-of reads for the five fundamental business domains."""

import argparse
import asyncio
import hashlib
import os

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from app.contracts.fundamental import DOMAIN_TABLES, FundamentalRecord
from app.contracts.mapping import content_version
from app.repositories.migrate import MIGRATIONS
from app.repositories.reference import ReferenceRepository

MIGRATION_VERSIONS = (
    "032_fundamental_domains",
    "033_fundamental_date_semantics",
    "034_fundamental_business_date",
)


async def migrate_fundamental(connection, direction="up"):
    if direction not in {"up", "down"}:
        raise ValueError("invalid_migration_direction")
    checksums = {
        item.split("_", 1)[0]: hashlib.sha256(
            (MIGRATIONS / f"{item}.up.sql").read_bytes()
        ).hexdigest()
        for item in MIGRATION_VERSIONS
    }
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(600600)")
        cursor = await connection.execute(
            "SELECT version,checksum FROM public.schema_migration "
            "WHERE version IN ('006','032','033','034')"
        )
        versions = dict(await cursor.fetchall())
        if "006" not in versions:
            raise ValueError("step06_migration_required")
        for version, checksum in checksums.items():
            if version in versions and versions[version] != checksum:
                raise ValueError("migration_checksum_mismatch")
        changed = False
        ordered = MIGRATION_VERSIONS if direction == "up" else reversed(MIGRATION_VERSIONS)
        for item in ordered:
            version = item.split("_", 1)[0]
            applied = version in versions
            if (direction == "up" and applied) or (direction == "down" and not applied):
                continue
            await connection.execute((MIGRATIONS / f"{item}.{direction}.sql").read_text("utf-8"))
            if direction == "up":
                await connection.execute(
                    "INSERT INTO public.schema_migration(version,checksum) VALUES (%s,%s)",
                    (version, checksums[version]),
                )
                versions[version] = checksums[version]
            else:
                await connection.execute(
                    "DELETE FROM public.schema_migration WHERE version=%s", (version,)
                )
                versions.pop(version, None)
            changed = True
    return changed


def record_identity(record: FundamentalRecord):
    natural = {
        "provider": record.provenance.provider,
        "source_api": record.provenance.source_api,
        "ts_code": record.security.ts_code,
        "domain": record.domain,
        "record_type": record.record_type,
        "source_key": record.source_key,
    }
    semantic = record.model_dump(mode="json", exclude={"provenance"})
    semantic["source"] = natural
    return DOMAIN_TABLES[record.domain], content_version(natural), content_version(semantic)


def materialize_record_version(record: FundamentalRecord):
    """Return the deterministic stored form before publication.

    Calculated five-domain records use this helper to close their input-version list
    before the repository transaction publishes the inputs and derived outputs.
    """
    table, business_key, semantic_hash = record_identity(record)
    version = content_version(
        {
            "kind": table,
            "business_key": business_key,
            "semantic_hash": semantic_hash,
            "schema_version": record.provenance.schema_version,
            "mapping_version": record.provenance.mapping_version,
        }
    )
    stored = FundamentalRecord.model_validate(
        record.model_copy(
            update={"provenance": record.provenance.model_copy(update={"data_version": version})}
        ).model_dump(mode="python")
    )
    return stored, table, business_key, semantic_hash


class FundamentalRepository(ReferenceRepository):
    async def publish(self, record: FundamentalRecord, raw_id):
        record, table, business_key, semantic_hash = materialize_record_version(record)
        version = record.provenance.data_version
        provenance = record.provenance
        async with self.connection.transaction():
            cursor = await self.connection.execute(
                "SELECT sha256,provider,source_api,fetched_at FROM storage.raw_response "
                "WHERE raw_id=%s",
                (raw_id,),
            )
            raw = await cursor.fetchone()
            expected = (
                provenance.raw_sha256,
                provenance.provider,
                provenance.source_api,
                provenance.timing.fetched_at,
            )
            if raw != expected:
                raise ValueError("raw_provenance_mismatch")
            cursor = await self.connection.execute(
                "INSERT INTO storage.record_version(data_version,kind,business_key,semantic_hash,"
                "raw_id,available_at,fetched_at) VALUES (%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING RETURNING data_version",
                (
                    version,
                    table,
                    business_key,
                    semantic_hash,
                    raw_id,
                    provenance.timing.available_at,
                    provenance.timing.fetched_at,
                ),
            )
            inserted = await cursor.fetchone() is not None
            if inserted:
                await self.connection.execute(
                    sql.SQL(
                        "INSERT INTO storage.{} "
                        "(business_key,data_version,ts_code,record_type,source_key,data_date,"
                        "period_end,announced_on,effective_on,payload) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(sql.Identifier(table)),
                    (
                        business_key,
                        version,
                        record.security.ts_code,
                        record.record_type,
                        record.source_key,
                        record.data_date,
                        record.period_end,
                        record.announced_on,
                        record.effective_on,
                        Jsonb(record.model_dump(mode="json")),
                    ),
                )
            else:
                cursor = await self.connection.execute(
                    "SELECT kind,business_key,semantic_hash FROM storage.record_version "
                    "WHERE data_version=%s",
                    (version,),
                )
                if await cursor.fetchone() != (table, business_key, semantic_hash):
                    raise ValueError("data_version_conflict")
            await self.connection.execute(
                "INSERT INTO storage.lineage(raw_id,data_version) VALUES (%s,%s) "
                "ON CONFLICT DO NOTHING",
                (raw_id, version),
            )
        return inserted

    async def records(
        self,
        domain,
        code,
        query,
        *,
        record_type=None,
        start=None,
        end=None,
        limit=1000,
    ):
        if domain not in DOMAIN_TABLES or not 1 <= limit <= 5000:
            raise ValueError("invalid_fundamental_query")
        if (start is None) != (end is None) or (start and start > end):
            raise ValueError("invalid_fundamental_date_range")
        table = DOMAIN_TABLES[domain]
        cursor = await self.connection.execute(
            sql.SQL(
                "SELECT payload FROM (SELECT DISTINCT ON (d.business_key) "
                "d.business_key,d.data_date,d.payload FROM storage.{} d "
                "JOIN storage.record_version v USING(data_version) "
                "WHERE d.ts_code=%s AND (%s::text IS NULL OR d.record_type=%s) "
                "AND (%s::date IS NULL OR d.data_date BETWEEN %s AND %s) "
                "AND v.available_at<=%s AND (%s='public' OR v.fetched_at<=%s) "
                "ORDER BY d.business_key,v.available_at DESC,v.fetched_at DESC,v.data_version DESC"
                ") selected ORDER BY data_date DESC,business_key LIMIT %s"
            ).format(sql.Identifier(table)),
            (
                code,
                record_type,
                record_type,
                start,
                start,
                end,
                query.as_of,
                query.knowledge,
                query.as_of,
                limit,
            ),
        )
        return [FundamentalRecord.model_validate(row[0]) for row in await cursor.fetchall()]


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("direction", choices=["up", "down"])
    parser.add_argument("--allow-data-loss", action="store_true")
    args = parser.parse_args()
    if args.direction == "down" and not args.allow_data_loss:
        parser.error("down requires --allow-data-loss after a verified backup")
    url = os.environ["APP_DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://", 1)
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as connection:
        changed = await migrate_fundamental(connection, args.direction)
    print("migration_applied" if changed else "migration_unchanged")


if __name__ == "__main__":
    asyncio.run(main())
