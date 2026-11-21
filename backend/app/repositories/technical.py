"""Append-only persistence and as-of reads for technical indicators."""

import hashlib

from psycopg.types.json import Jsonb

from app.contracts.mapping import content_version
from app.contracts.technical import TechnicalIndicatorRecord
from app.repositories.migrate import MIGRATIONS

MIGRATION = "035_technical_indicators"


async def migrate_technical(connection, direction="up"):
    if direction not in {"up", "down"}:
        raise ValueError("invalid_migration_direction")
    checksum = hashlib.sha256((MIGRATIONS / f"{MIGRATION}.up.sql").read_bytes()).hexdigest()
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(600600)")
        cursor = await connection.execute(
            "SELECT version,checksum FROM public.schema_migration WHERE version IN ('006','035')"
        )
        versions = dict(await cursor.fetchall())
        if "006" not in versions:
            raise ValueError("step06_migration_required")
        if "035" in versions and versions["035"] != checksum:
            raise ValueError("migration_checksum_mismatch")
        if (direction == "up") == ("035" in versions):
            return False
        await connection.execute((MIGRATIONS / f"{MIGRATION}.{direction}.sql").read_text("utf-8"))
        if direction == "up":
            await connection.execute(
                "INSERT INTO public.schema_migration(version,checksum) VALUES ('035',%s)",
                (checksum,),
            )
        else:
            await connection.execute("DELETE FROM public.schema_migration WHERE version='035'")
    return True


def indicator_identity(record: TechnicalIndicatorRecord):
    natural = {
        "provider": record.provenance.provider,
        "source_api": record.provenance.source_api,
        "ts_code": record.security.ts_code,
        "trade_date": record.trade_date,
        "indicator": record.indicator,
        "parameters": record.parameters,
        "calculation_version": record.calculation_version,
    }
    semantic = record.model_dump(mode="json", exclude={"provenance"})
    semantic["source"] = natural
    business_key = content_version(natural)
    semantic_hash = content_version(semantic)
    version = content_version(
        {
            "kind": "technical_indicator",
            "business_key": business_key,
            "semantic_hash": semantic_hash,
            "mapping_version": record.provenance.mapping_version,
        }
    )
    return business_key, semantic_hash, version


class TechnicalRepository:
    def __init__(self, connection):
        self.connection = connection

    async def publish(self, record: TechnicalIndicatorRecord):
        business_key, semantic_hash, version = indicator_identity(record)
        record = TechnicalIndicatorRecord.model_validate(
            record.model_copy(
                update={
                    "provenance": record.provenance.model_copy(update={"data_version": version})
                }
            ).model_dump(mode="python")
        )
        cursor = await self.connection.execute(
            "SELECT raw_id FROM storage.record_version WHERE data_version=%s",
            (record.input_versions[-1],),
        )
        row = await cursor.fetchone()
        if row is None:
            raise ValueError("technical_input_version_missing")
        raw_id = row[0]
        cursor = await self.connection.execute(
            "INSERT INTO storage.record_version(data_version,kind,business_key,semantic_hash,"
            "raw_id,available_at,fetched_at) VALUES (%s,'technical_indicator',%s,%s,%s,%s,%s) "
            "ON CONFLICT DO NOTHING RETURNING data_version",
            (
                version,
                business_key,
                semantic_hash,
                raw_id,
                record.provenance.timing.available_at,
                record.provenance.timing.fetched_at,
            ),
        )
        inserted = await cursor.fetchone() is not None
        if inserted:
            await self.connection.execute(
                "INSERT INTO storage.technical_indicator(business_key,data_version,ts_code,"
                "trade_date,indicator_name,indicator_value,payload) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    business_key,
                    version,
                    record.security.ts_code,
                    record.trade_date,
                    record.indicator,
                    record.value,
                    Jsonb(record.model_dump(mode="json")),
                ),
            )
        else:
            cursor = await self.connection.execute(
                "SELECT kind,business_key,semantic_hash FROM storage.record_version "
                "WHERE data_version=%s",
                (version,),
            )
            if await cursor.fetchone() != ("technical_indicator", business_key, semantic_hash):
                raise ValueError("technical_data_version_conflict")
        await self.connection.execute(
            "INSERT INTO storage.lineage(raw_id,data_version) "
            "SELECT DISTINCT v.raw_id,%s FROM storage.record_version v "
            "WHERE v.data_version=ANY(%s) ON CONFLICT DO NOTHING",
            (version, list(record.input_versions)),
        )
        return inserted

    async def indicators(self, code, names, end, query, *, days=30):
        if not names or len(names) > 8 or not 1 <= days <= 30:
            raise ValueError("invalid_indicator_query")
        cursor = await self.connection.execute(
            "SELECT payload FROM (SELECT DISTINCT ON (d.indicator_name,d.trade_date) "
            "d.indicator_name,d.trade_date,d.payload FROM storage.technical_indicator d "
            "JOIN storage.record_version v USING(data_version) WHERE d.ts_code=%s "
            "AND d.indicator_name=ANY(%s) AND d.trade_date<=%s AND v.available_at<=%s "
            "AND (%s='public' OR v.fetched_at<=%s) ORDER BY d.indicator_name,d.trade_date,"
            "v.available_at DESC,v.fetched_at DESC,v.data_version DESC) selected "
            "ORDER BY trade_date DESC,indicator_name",
            (code, list(names), end, query.as_of, query.knowledge, query.as_of),
        )
        records = [
            TechnicalIndicatorRecord.model_validate(row[0]) for row in await cursor.fetchall()
        ]
        dates = sorted({record.trade_date for record in records}, reverse=True)[:days]
        allowed = set(dates)
        return sorted(
            (record for record in records if record.trade_date in allowed),
            key=lambda item: (item.trade_date, item.indicator),
        )
