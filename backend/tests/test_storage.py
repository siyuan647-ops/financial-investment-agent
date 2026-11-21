import asyncio
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from app.repositories.raw import RawStore, safe_parameters


class Connection:
    autocommit = True


def test_parameter_allowlist():
    assert safe_parameters(
        {
            "token": "secret",
            "url": "https://user:secret@host",
            "ts_code": "000001.SZ",
            "offset": 0,
            "fields": {"token": "secret"},
        }
    ) == {"ts_code": "000001.SZ", "offset": 0, "fields": "[REDACTED]"}


@pytest.fixture
def store():
    # Explicit workspace path: Windows sandbox disallows pytest's global temp directory.
    root = Path(".pytest_cache") / "storage" / uuid4().hex
    instance = RawStore(Connection(), root)
    yield instance
    for path in root.iterdir():
        path.unlink()
    root.rmdir()


@pytest.mark.parametrize("name", ["../escape.bin", "/escape", "x.bin", "a" * 32 + ".json"])
def test_path_escape_rejected(store, name):
    with pytest.raises(ValueError, match="invalid_raw_path"):
        store._path(name)


def test_complete_file_write(store):
    name = uuid4().hex + ".bin"
    store._write(name, b"\x00raw decimal 1.234567890123456789")
    assert store._path(name).read_bytes() == b"\x00raw decimal 1.234567890123456789"
    assert list(store.root.glob("*.tmp")) == []


def test_transaction_connection_required():
    connection = Connection()
    connection.autocommit = False
    with pytest.raises(ValueError, match="autocommit"):
        RawStore(connection, Path("unused"))


def test_raw_metadata_rejected_before_io(store):
    async def run():
        with pytest.raises(ValueError, match="aware"):
            await store.save(
                run_id=uuid4(),
                provider="test",
                source_api="daily",
                parameters={},
                fetched_at=datetime(2026, 1, 1),
                content=b"x",
            )
        with pytest.raises(ValueError, match="source_identifier"):
            await store.save(
                run_id=uuid4(),
                provider="https://secret@host",
                source_api="daily",
                parameters={},
                fetched_at=datetime.now(UTC),
                content=b"x",
            )

    asyncio.run(run())
