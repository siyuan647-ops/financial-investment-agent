import json
from datetime import date
from uuid import uuid4

import anyio
import httpx
import pytest
from pydantic import ValidationError

from app.api.auth import Identity, Role
from app.api.data import _cached_page, _project_units, _submit_task
from app.api.dependencies import database_connection
from app.contracts.api import BackfillRequest, GapFillRequest, quarter_ends
from app.core.config import Settings
from app.main import create_app
from app.repositories.api import task_idempotency_key
from app.services.cache import QueryCache, cache_key
from app.services.prefect import PrefectGateway


class MemoryRedis:
    def __init__(self):
        self.values = {}

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, ex):
        assert ex > 0
        self.values[key] = value


class Request:
    class State:
        request_id = "1" * 32

    def __init__(self, settings):
        self.app = type("App", (), {"state": type("State", (), {"settings": settings})()})()
        self.state = self.State()


class VersionRepository:
    def __init__(self, version="a" * 64):
        self.version = version

    async def version_token(self, *args, **kwargs):
        return self.version


def test_cache_key_covers_semantics_and_version():
    base = {"code": "000001.SZ", "unit": "standard", "adjustment": "none"}
    key = cache_key("prices", base, "a" * 64)
    assert key != cache_key("prices", base | {"unit": "raw"}, "a" * 64)
    assert key != cache_key("prices", base | {"adjustment": "front"}, "a" * 64)
    assert key != cache_key("prices", base, "b" * 64)
    assert "000001" not in key


def test_query_cache_round_trip_and_corrupt_cache_bypass():
    async def run():
        redis = MemoryRedis()
        cache = QueryCache(redis, 60)
        assert await cache.get("x", {"a": 1}, "v") == (None, "miss")
        assert await cache.set("x", {"a": 1}, "v", [{"value": "1"}])
        assert await cache.get("x", {"a": 1}, "v") == ([{"value": "1"}], "hit")
        redis.values[next(iter(redis.values))] = "not-json"
        assert await cache.get("x", {"a": 1}, "v") == (None, "bypass")

    anyio.run(run)


def test_raw_unit_projection_does_not_change_source_fields():
    value = {
        "amount": {
            "value": "10000",
            "unit": "CNY",
            "raw_value": "1",
            "raw_unit": "ten_thousand_CNY",
        }
    }
    projected = _project_units(value, "raw")
    assert projected["amount"]["value"] == "1"
    assert projected["amount"]["unit"] == "ten_thousand_CNY"
    assert value["amount"]["value"] == "10000"


def test_task_and_financial_scope_contracts():
    with pytest.raises(ValidationError):
        BackfillRequest(
            codes=("000001.SZ", "000001.SZ"),
            start="2024-01-01",
            end="2024-01-02",
        )
    with pytest.raises(ValidationError):
        GapFillRequest(codes=("000001.SZ",), start="2024-01-02", end="2024-01-01")
    assert quarter_ends(date(2024, 1, 1), date(2024, 12, 31)) == [
        date(2024, 3, 31),
        date(2024, 6, 30),
        date(2024, 9, 30),
        date(2024, 12, 31),
    ]
    with pytest.raises(ValueError, match="one_to_twelve"):
        quarter_ends(date(2020, 1, 1), date(2024, 12, 31))


def test_cached_page_paginates_and_revision_changes_cache_key(config):
    async def run():
        settings = Settings(**config)
        request, redis = Request(settings), MemoryRedis()
        repository = VersionRepository()
        calls = 0

        async def loader():
            nonlocal calls
            calls += 1
            return [{"id": 1}, {"id": 2}, {"id": 3}]

        from datetime import datetime

        from app.contracts.common import SHANGHAI, AsOfQuery

        query = AsOfQuery(request_id="1" * 32, as_of=datetime(2024, 1, 1, tzinfo=SHANGHAI))
        first = await _cached_page(
            request,
            redis,
            repository,
            namespace="test",
            dimensions={"unit": "standard", "adjustment": "none"},
            kinds=("price_bar",),
            code="000001.SZ",
            query=query,
            loader=loader,
            page=2,
            page_size=2,
        )
        assert [item["id"] for item in first.data] == [3]
        assert first.meta.cache == "miss"
        again = await _cached_page(
            request,
            redis,
            repository,
            namespace="test",
            dimensions={"unit": "standard", "adjustment": "none"},
            kinds=("price_bar",),
            code="000001.SZ",
            query=query,
            loader=loader,
            page=1,
            page_size=2,
        )
        assert again.meta.cache == "hit"
        repository.version = "b" * 64
        revised = await _cached_page(
            request,
            redis,
            repository,
            namespace="test",
            dimensions={"unit": "standard", "adjustment": "none"},
            kinds=("price_bar",),
            code="000001.SZ",
            query=query,
            loader=loader,
            page=1,
            page_size=2,
        )
        assert revised.meta.cache == "miss"
        assert calls == 2

    anyio.run(run)


def test_openapi_contains_step14_surface(config):
    paths = create_app(Settings(**config)).openapi()["paths"]
    expected = {
        "/api/v1/securities/resolve",
        "/api/v1/data/prices/{code}",
        "/api/v1/data/valuations/{code}",
        "/api/v1/data/financials/{code}",
        "/api/v1/data/metrics/{code}",
        "/api/v1/data/dividends/{code}",
        "/api/v1/data/industries/{code}",
        "/api/v1/data/lineage",
        "/api/v1/data/status/{code}",
        "/api/v1/tasks/backfills",
        "/api/v1/tasks/gap-fills",
        "/api/v1/tasks/retries",
        "/api/v1/tasks/{task_id}",
    }
    assert expected <= paths.keys()
    rendered = json.dumps(create_app(Settings(**config)).openapi())
    assert config["data_source_token"] not in rendered


def test_idempotency_is_owner_scoped():
    parameters = {"codes": ["000001.SZ"], "start": "2024-01-01", "end": "2024-01-02"}
    alice = task_idempotency_key("alice", "gap_fill", "bounded-history", parameters)
    assert alice == task_idempotency_key("alice", "gap_fill", "bounded-history", parameters)
    assert alice != task_idempotency_key("bob", "gap_fill", "bounded-history", parameters)


def test_submit_task_deduplicates_before_prefect(monkeypatch):
    task_id, flow_run_id = uuid4(), uuid4()

    class Repository:
        calls = 0

        def __init__(self, database):
            pass

        async def create_task(self, *args):
            type(self).calls += 1
            return task_id, type(self).calls == 1

        async def attach_flow_run(self, *args):
            pass

        async def task(self, task):
            return {"status": "submitted"}

    class Gateway:
        calls = 0

        async def submit(self, *args):
            self.calls += 1
            return flow_run_id

    import app.api.data as module

    monkeypatch.setattr(module, "ApiRepository", Repository)

    async def run():
        gateway = Gateway()
        identity = Identity(user_id="alice", role=Role.USER)
        first = await _submit_task(None, gateway, identity, "gap_fill", "flow", "deployment", {})
        second = await _submit_task(None, gateway, identity, "gap_fill", "flow", "deployment", {})
        assert first["deduplicated"] is False
        assert second == {"task_id": str(task_id), "status": "submitted", "deduplicated": True}
        assert gateway.calls == 1

    anyio.run(run)


def test_prefect_gateway_resolves_deployment_id_before_submission(monkeypatch):
    deployment_id, flow_run_id, task_id = uuid4(), uuid4(), uuid4()
    calls = []

    class Response:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            pass

        def json(self):
            return self.payload

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, url):
            calls.append(("GET", url, None))
            return Response({"id": str(deployment_id)})

        async def post(self, url, json):
            calls.append(("POST", url, json))
            return Response({"id": str(flow_run_id)})

    import app.services.prefect as module

    monkeypatch.setattr(module.httpx, "AsyncClient", Client)

    async def run():
        result = await PrefectGateway("http://prefect/api").submit(
            "historical-backfill", "bounded-history", {"codes": ["000001.SZ"]}, task_id
        )
        assert result == flow_run_id
        assert calls[0][1].endswith("/deployments/name/historical-backfill/bounded-history")
        assert calls[1][1].endswith(f"/deployments/{deployment_id}/create_flow_run")
        assert calls[1][2]["idempotency_key"] == str(task_id)

    anyio.run(run)


def test_auth_and_admin_role_are_checked_before_database(config):
    app = create_app(Settings(**config))

    async def database_must_not_open():
        raise AssertionError("database opened before authorization")
        yield

    app.dependency_overrides[database_connection] = database_must_not_open

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            unauthenticated = await client.get(
                "/api/v1/data/prices/000001.SZ?start=2024-01-01&end=2024-01-02"
            )
            assert unauthenticated.status_code == 401
            user = await client.post(
                "/api/v1/tasks/backfills",
                headers={"Authorization": "Bearer " + config["auth_credentials"][0]["token"]},
                json={
                    "codes": ["000001.SZ"],
                    "start": "2024-01-01",
                    "end": "2024-01-02",
                },
            )
            assert user.status_code == 403

    anyio.run(run)
