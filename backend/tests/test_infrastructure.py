import asyncio
from unittest.mock import AsyncMock

import anyio
import httpx
import pytest
from test_baseline import AppClient

from app.core import readiness
from app.core.config import Settings
from app.main import create_app


@pytest.mark.parametrize("failed", [None, "database", "redis", "prefect", "worker"])
def test_readiness_fail_closed(config, monkeypatch, failed):
    async def probes(settings):
        return {name: name != failed for name in ("database", "redis", "prefect", "worker")}

    monkeypatch.setattr("app.api.routes.check_dependencies", probes)
    client = AppClient(create_app(Settings(**config)))
    response = client.get("/health/ready")
    assert response.status_code == (503 if failed else 200)
    assert response.json()["status"] == ("not_ready" if failed else "ready")
    assert client.get("/health/live").status_code == 200


def test_real_probe_orchestration_timeout_and_redaction(config, monkeypatch):
    def broken_database(settings):
        raise RuntimeError("postgresql://secret:secret@private")

    async def slow_redis(settings):
        await asyncio.sleep(10)

    monkeypatch.setattr(readiness, "database_check", broken_database)
    monkeypatch.setattr(readiness, "redis_check", slow_redis)
    monkeypatch.setattr(readiness, "http_check", AsyncMock())
    settings = Settings(**config, readiness_timeout_seconds=0.02)
    result = anyio.run(readiness.check_dependencies, settings)
    assert result == {"database": False, "redis": False, "prefect": True, "worker": True}
    assert "secret" not in str(result)


def test_metrics_count_errors_and_no_sensitive_labels(config, monkeypatch):
    monkeypatch.setattr(
        "app.api.routes.check_dependencies", AsyncMock(return_value={"database": False})
    )
    app = create_app(Settings(**config))
    monkeypatch.setattr(app.state.metrics, "refresh_tasks", AsyncMock())
    monkeypatch.setattr(app.state.metrics, "refresh_dataops", AsyncMock())
    client = AppClient(app)
    client.get("/missing-sensitive-path?token=secret")
    response = client.get("/metrics")
    assert response.status_code == 200
    assert 'app_http_requests_total{status_class="4xx"} 1.0' in response.text
    assert 'app_dependency_up{dependency="database"} 0.0' in response.text
    assert "sensitive" not in response.text and "secret" not in response.text


def test_prefect_metrics_failure_does_not_report_zero(config, monkeypatch):
    app = create_app(Settings(**config))
    monkeypatch.setattr(httpx.AsyncClient, "post", AsyncMock(side_effect=RuntimeError("secret")))
    anyio.run(app.state.metrics.refresh_tasks, app.state.settings)
    from prometheus_client import generate_latest

    output = generate_latest(app.state.metrics.registry).decode()
    assert "prefect_metrics_up 0.0" in output
    assert 'prefect_task_runs{state="all"} NaN' in output
    assert "secret" not in output


def test_dataops_metrics_failure_is_unknown_not_zero(config, monkeypatch):
    app = create_app(Settings(**config))
    monkeypatch.setattr(
        "app.observability.metrics.psycopg.AsyncConnection.connect",
        AsyncMock(side_effect=RuntimeError("postgresql://secret:secret@private")),
    )
    anyio.run(app.state.metrics.refresh_dataops, app.state.settings)
    from prometheus_client import generate_latest

    output = generate_latest(app.state.metrics.registry).decode()
    assert "dataops_metrics_up 0.0" in output
    assert 'dataops_orchestration_batches{state="failed"} NaN' in output
    assert 'dataops_completeness_missing{domain="market"} NaN' in output
    assert 'dataops_update_lag_seconds{domain="financial"} NaN' in output
    assert "secret" not in output
