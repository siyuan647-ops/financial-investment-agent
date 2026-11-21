"""Window-scoped publication watermarks and bounded local market queries."""

import hashlib

from psycopg import sql

from app.repositories.migrate import MIGRATIONS
from app.repositories.reference import ReferenceRepository
from app.repositories.storage import MODELS


async def migrate_market(connection, direction="up"):
    if direction not in {"up", "down"}:
        raise ValueError("invalid_migration_direction")
    checksum = hashlib.sha256((MIGRATIONS / "008_market.up.sql").read_bytes()).hexdigest()
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(600600)")
        cursor = await connection.execute(
            "SELECT version,checksum FROM public.schema_migration WHERE version IN ('007','008')"
        )
        versions = dict(await cursor.fetchall())
        if "007" not in versions:
            raise ValueError("step07_migration_required")
        if "008" in versions and versions["008"] != checksum:
            raise ValueError("migration_checksum_mismatch")
        if (direction == "up") == ("008" in versions):
            return False
        await connection.execute((MIGRATIONS / f"008_market.{direction}.sql").read_text("utf-8"))
        if direction == "up":
            await connection.execute(
                "INSERT INTO public.schema_migration(version,checksum) VALUES ('008',%s)",
                (checksum,),
            )
        else:
            await connection.execute("DELETE FROM public.schema_migration WHERE version='008'")
    return True


class MarketRepository(ReferenceRepository):
    async def advance_watermark(self, provider, code, start, end, run_id):
        # Caller must include this in the domain publication/status transaction.
        await self.connection.execute(
            "INSERT INTO storage.market_watermark "
            "(provider,ts_code,window_start,window_end,published_through,run_id) "
            "VALUES (%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(provider,ts_code,window_start,window_end) DO UPDATE "
            "SET published_through=EXCLUDED.published_through,"
            "run_id=EXCLUDED.run_id,updated_at=now()",
            (provider, code, start, end, end, run_id),
        )

    async def market(self, code, start, end, query, *, provider="tushare_compatible"):
        from app.dataops.market import validate_window

        validate_window(code, start, end)
        result = {}
        # One snapshot across all three tables, even during a concurrent publication.
        async with self.connection.transaction():
            await self.connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            for kind in ("price_bar", "adjustment_factor", "valuation"):
                cursor = await self.connection.execute(
                    sql.SQL(
                        "SELECT payload FROM (SELECT DISTINCT ON (d.business_key) "
                        "d.trade_date,d.business_key,d.payload FROM storage.{} d "
                        "JOIN storage.record_version v USING(data_version) "
                        "WHERE d.ts_code=%s AND d.trade_date BETWEEN %s AND %s "
                        "AND d.payload->'provenance'->>'provider'=%s AND v.available_at<=%s "
                        "AND (%s='public' OR v.fetched_at<=%s) "
                        "ORDER BY d.business_key,v.available_at DESC,v.fetched_at DESC,"
                        "v.data_version DESC) chosen ORDER BY trade_date,business_key"
                    ).format(sql.Identifier(kind)),
                    (code, start, end, provider, query.as_of, query.knowledge, query.as_of),
                )
                result[kind] = [
                    MODELS[kind].model_validate(row[0]).model_dump(mode="json")
                    for row in await cursor.fetchall()
                ]
        return result
