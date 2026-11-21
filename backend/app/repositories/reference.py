"""Bounded reference-data reads and explicit Step07 migration."""

import hashlib

from psycopg import sql

from app.repositories.migrate import MIGRATIONS
from app.repositories.storage import MODELS, Repository


async def migrate_reference(connection, direction="up"):
    if direction not in {"up", "down"}:
        raise ValueError("invalid_migration_direction")
    checksum = hashlib.sha256((MIGRATIONS / "007_reference.up.sql").read_bytes()).hexdigest()
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(600600)")
        cursor = await connection.execute(
            "SELECT version,checksum FROM public.schema_migration WHERE version IN ('006','007')"
        )
        versions = dict(await cursor.fetchall())
        if "006" not in versions:
            raise ValueError("step06_migration_required")
        if "007" in versions and versions["007"] != checksum:
            raise ValueError("migration_checksum_mismatch")
        if (direction == "up") == ("007" in versions):
            return False
        if direction == "down":
            cursor = await connection.execute(
                "SELECT version FROM public.schema_migration WHERE version='008'"
            )
            if await cursor.fetchone():
                raise ValueError("rollback_step08_first")
        await connection.execute((MIGRATIONS / f"007_reference.{direction}.sql").read_text("utf-8"))
        if direction == "up":
            await connection.execute(
                "INSERT INTO public.schema_migration(version,checksum) VALUES ('007',%s)",
                (checksum,),
            )
        else:
            await connection.execute("DELETE FROM public.schema_migration WHERE version='007'")
    return True


class ReferenceRepository(Repository):
    async def snapshot(
        self, kind, query, *, provider="tushare_compatible", codes=(), start=None, end=None
    ):
        if kind not in {"security", "security_alias", "trading_calendar"}:
            raise ValueError("invalid_reference_kind")
        if kind != "trading_calendar" and not 1 <= len(codes) <= 30:
            raise ValueError("reference_codes_required")
        if kind == "trading_calendar" and (
            start is None or end is None or not 0 <= (end - start).days <= 1100
        ):
            raise ValueError("invalid_calendar_range")
        predicate = (
            sql.SQL("d.calendar_date BETWEEN %s AND %s")
            if kind == "trading_calendar"
            else sql.SQL("d.ts_code=ANY(%s)")
        )
        args = (start, end) if kind == "trading_calendar" else (list(codes),)
        cursor = await self.connection.execute(
            sql.SQL(
                "SELECT payload FROM (SELECT DISTINCT ON (d.business_key) d.payload "
                "FROM storage.{} d JOIN storage.record_version v USING(data_version) "
                "WHERE {} AND d.payload->'provenance'->>'provider'=%s "
                "AND v.available_at<=%s AND (%s='public' OR v.fetched_at<=%s) "
                "ORDER BY d.business_key,v.available_at DESC,v.fetched_at DESC,"
                "v.data_version DESC) chosen"
            ).format(sql.Identifier(kind), predicate),
            (*args, provider, query.as_of, query.knowledge, query.as_of),
        )
        return [MODELS[kind].model_validate(row[0]) for row in await cursor.fetchall()]
