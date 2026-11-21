"""Migration bytes must survive Git checkout without weakening tamper detection."""

import asyncio
import hashlib
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from app.repositories import migrate as migration

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("direction", ["up", "down"])
def test_sql_checkout_preserves_bytes(direction):
    relative = f"backend/migrations/006_storage.{direction}.sql"
    original = (ROOT / relative).read_bytes()
    # Force the Windows setting that exposed the original regression.
    checkout = subprocess.run(
        ["git", "-c", "core.autocrlf=true", "cat-file", "--filters", f"HEAD:{relative}"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    ).stdout
    assert b"\r" not in original
    assert checkout == original


@pytest.mark.parametrize("direction", ["up", "down"])
def test_changed_sql_is_rejected(monkeypatch, direction):
    original = (migration.MIGRATIONS / "006_storage.up.sql").read_bytes()
    checksum = hashlib.sha256(original).hexdigest()
    connection = AsyncMock()
    cursor = AsyncMock()
    cursor.fetchone.return_value = (checksum,)
    connection.execute.return_value = cursor

    @asynccontextmanager
    async def transaction():
        yield

    connection.transaction = transaction
    monkeypatch.setattr(Path, "read_bytes", lambda self: original + b"\nSELECT 1;\n")
    with pytest.raises(ValueError, match="migration_checksum_mismatch"):
        asyncio.run(migration.migrate(connection, direction))
    assert connection.execute.await_count == 3
