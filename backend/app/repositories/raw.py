"""Complete-file-before-registration storage; all writers/GC share a database lock."""

import asyncio
import hashlib
import os
import re
import time
from pathlib import Path
from uuid import uuid4

from psycopg.types.json import Jsonb

RAW_LOCK = 600601
SAFE_PARAMETERS = {
    "ts_code",
    "start_date",
    "end_date",
    "trade_date",
    "period",
    "exchange",
    "list_status",
    "limit",
    "offset",
    "fields",
    "ann_date",
    "report_type",
    "is_new",
    "level",
    "src",
}


def safe_parameters(parameters):
    """Unknown keys and structured values are never persisted as request parameters."""
    return {
        key: value
        if key in SAFE_PARAMETERS
        and isinstance(value, (str, int, bool))
        and len(str(value)) <= 1000
        and re.fullmatch(r"[\w., -]*", str(value))
        else "[REDACTED]"
        for key, value in parameters.items()
        if key in SAFE_PARAMETERS
    }


class RawStore:
    def __init__(self, connection, root: Path):
        if not connection.autocommit:
            raise ValueError("raw_store_requires_autocommit_connection")
        self.connection = connection
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, name):
        if not re.fullmatch(r"[0-9a-f]{32}\.(bin|tmp)", name):
            raise ValueError("invalid_raw_path")
        path = self.root / name
        if path.is_symlink() or path.resolve().parent != self.root:
            raise ValueError("unsafe_raw_path")
        return path

    def _write(self, name, content):
        temporary = self._path(name.replace(".bin", ".tmp"))
        final = self._path(name)
        with temporary.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, final)
        if os.name != "nt":
            fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    async def save(self, *, run_id, provider, source_api, parameters, fetched_at, content: bytes):
        if not isinstance(content, bytes):
            raise TypeError("raw_content_requires_bytes")
        if fetched_at.tzinfo is None or fetched_at.utcoffset() is None:
            raise ValueError("aware_fetched_at_required")
        if not all(re.fullmatch(r"[a-zA-Z0-9_.-]{1,100}", x) for x in (provider, source_api)):
            raise ValueError("source_identifier_required")
        raw_id = uuid4()
        name = raw_id.hex + ".bin"
        digest = hashlib.sha256(content).hexdigest()
        async with self.connection.transaction():
            await self.connection.execute("SELECT pg_advisory_xact_lock(%s)", (RAW_LOCK,))
            # Shield the write and wait on cancellation so the lock cannot outlive the writer.
            task = asyncio.create_task(asyncio.to_thread(self._write, name, content))
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise
            await self.connection.execute(
                "INSERT INTO storage.raw_response "
                "(raw_id,run_id,provider,source_api,parameters,fetched_at,sha256,byte_count,"
                "relative_path) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    raw_id,
                    run_id,
                    provider,
                    source_api,
                    Jsonb(safe_parameters(parameters)),
                    fetched_at,
                    digest,
                    len(content),
                    name,
                ),
            )
        return raw_id

    async def read(self, raw_id):
        cursor = await self.connection.execute(
            "SELECT relative_path,sha256,byte_count FROM storage.raw_response WHERE raw_id=%s",
            (raw_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise KeyError("raw_response_missing")
        content = await asyncio.to_thread(self._path(row[0]).read_bytes)
        if len(content) != row[2] or hashlib.sha256(content).hexdigest() != row[1]:
            raise ValueError("raw_integrity_failure")
        return content

    async def cleanup(self, *, minimum_age_seconds=86400, dry_run=True):
        if minimum_age_seconds < 0:
            raise ValueError("negative_cleanup_age")
        async with self.connection.transaction():
            await self.connection.execute("SELECT pg_advisory_xact_lock(%s)", (RAW_LOCK,))
            cursor = await self.connection.execute("SELECT relative_path FROM storage.raw_response")
            registered = {row[0] for row in await cursor.fetchall()}
            cutoff = time.time() - minimum_age_seconds
            candidates = []
            for entry in self.root.iterdir():
                if not re.fullmatch(r"[0-9a-f]{32}\.(bin|tmp)", entry.name):
                    continue
                path = self._path(entry.name)
                if (
                    entry.name not in registered
                    and path.is_file()
                    and path.stat().st_mtime <= cutoff
                ):
                    candidates.append(entry.name)
                    if not dry_run:
                        path.unlink()
            return sorted(candidates)
