"""Target-only research entry tests."""

from types import SimpleNamespace
from uuid import UUID

from fastapi.testclient import TestClient

from app.contracts.research import ResearchTaskAccepted
from app.core.config import Settings
from app.main import create_app

AS_OF = "2026-11-10T12:00:00+08:00"


def test_target_query_resolves_security_without_intent_model(config):
    class Resolver:
        async def __call__(self, target, **kwargs):
            assert target == "平安银行"
            assert kwargs["on"] is None
            return (
                SimpleNamespace(
                    security=SimpleNamespace(ts_code="000001.SZ", exchange="SZSE"),
                    name="平安银行",
                ),
            )

    class Research:
        async def submit(self, payload, **kwargs):
            assert payload.target == "平安银行"
            assert kwargs["target"] == "000001.SZ"
            assert kwargs["security"].name == "平安银行"
            return ResearchTaskAccepted(
                task_id=UUID("11111111-1111-1111-1111-111111111111"),
                status="queued",
                deduplicated=False,
            )

    app = create_app(Settings(**config))
    app.state.target_research_services = (Resolver(), Research())
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/analysis/query",
            headers={"Authorization": "Bearer fixture-user-token-0000000000000001"},
            json={"target": "平安银行", "as_of": AS_OF},
        )
    assert response.status_code == 202
    assert response.json()["security"]["code"] == "000001.SZ"
    assert response.json()["task_status"] == "queued"


def test_target_query_rejects_natural_language(config):
    class Resolver:
        async def __call__(self, target, **kwargs):
            del kwargs
            assert target == "分析平安银行是否值得投资"
            return ()

    app = create_app(Settings(**config))
    app.state.target_research_services = (Resolver(), object())
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/analysis/query",
            headers={"Authorization": "Bearer fixture-user-token-0000000000000001"},
            json={"target": "分析平安银行是否值得投资"},
        )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "RESEARCH_TARGET_NOT_FOUND"
