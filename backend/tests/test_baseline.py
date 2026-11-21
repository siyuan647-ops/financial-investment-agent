import json
import logging
import socket
import traceback

import anyio
import httpx
import pytest
from pydantic import ValidationError
from pytest_socket import SocketConnectBlockedError

from app.api.auth import Identity, Role, require_owner
from app.core.config import ConfigurationError, Settings, load_settings
from app.core.errors import AppError
from app.core.logging import SafeJsonFormatter
from app.main import create_app


class AppClient:
    """In-process ASGI requests; no HTTP server or external connections."""

    def __init__(self, app):
        self.app = app

    def request(self, method, path, **kwargs):
        async def send():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=self.app), base_url="http://test"
            ) as client:
                return await client.request(method, path, **kwargs)

        return anyio.run(send)

    def get(self, path, **kwargs):
        return self.request("GET", path, **kwargs)


def test_missing_configuration_is_safe(monkeypatch):
    monkeypatch.setenv("APP_DATA_SOURCE_TOKEN", "do-not-disclose-this")
    with pytest.raises(ConfigurationError) as result:
        load_settings()
    assert "database_url" in str(result.value)
    assert "redis_url" in str(result.value)
    assert "do-not-disclose-this" not in str(result.value)


def test_malformed_credentials_are_safe(monkeypatch):
    monkeypatch.setenv("APP_AUTH_CREDENTIALS", "secret-malformed-json")
    with pytest.raises(ConfigurationError) as result:
        load_settings()
    assert "secret-malformed-json" not in str(result.value)


def test_unknown_credential_field_is_redacted(config, monkeypatch):
    marker = "synthetic-secret-field"
    config["auth_credentials"][0][marker] = "synthetic-secret-value"
    for name, value in config.items():
        monkeypatch.setenv(
            "APP_" + name.upper(),
            json.dumps(value) if isinstance(value, (list, dict, bool)) else str(value),
        )
    with pytest.raises(ConfigurationError) as result:
        load_settings()
    rendered = "".join(traceback.format_exception(result.value))
    assert "auth_credentials.0.<unknown_field>" in rendered
    assert marker not in rendered
    assert "synthetic-secret-value" not in rendered
    assert config["auth_credentials"][0]["token"] not in rendered


@pytest.mark.parametrize("token", ["", "   ", "\t" * 32, "\n" * 32])
@pytest.mark.parametrize("field", ["data_source_token", "qwen_token", "credential"])
def test_blank_tokens_rejected(config, field, token):
    config.update(
        qwen_url="https://qwen.example.invalid",
        qwen_model="model",
        qwen_token="valid",
    )
    if field == "credential":
        config["auth_credentials"][0]["token"] = token
        expected_location = ("auth_credentials", 0, "token")
    else:
        config[field] = token
        expected_location = (field,)
    with pytest.raises(ValidationError) as result:
        Settings(**config)
    assert expected_location in [error["loc"] for error in result.value.errors()]


def test_tokens_preserve_original_value_and_optional_absence(config):
    config["data_source_token"] = "  valid-token  "
    settings = Settings(**config)
    assert settings.data_source_token.get_secret_value() == "  valid-token  "
    assert settings.qwen_token is None


@pytest.mark.parametrize(
    "host",
    [
        "bad host.invalid",
        "bad\thost",
        "bad\nhost",
        "bad\x00host",
        "bad%20host",
        "-bad.invalid",
        "bad..invalid",
        "999.1.1.1",
    ],
)
def test_invalid_url_hosts_rejected(config, host):
    with pytest.raises(ValidationError):
        Settings(**(config | {"data_source_url": f"https://{host}"}))


@pytest.mark.parametrize(
    "url",
    [
        "https://example.invalid/v1",
        "http://localhost:8000",
        "http://embedding-service:8080",
        "http://127.0.0.1:8080",
        "http://[::1]:8080",
        "https://example.invalid./v1",
    ],
)
def test_valid_url_hosts_preserved(config, url):
    settings = Settings(**(config | {"data_source_url": url}))
    assert settings.data_source_url.get_secret_value() == url


@pytest.mark.parametrize(
    "change",
    [
        {"dev_identity_enabled": True},
        {"auth_credentials": []},
        {"rate_limit_per_minute": 0},
        {"connect_timeout_seconds": -1},
        {"total_timeout_seconds": 1},
        {"database_url": "https://wrong-scheme.invalid"},
        {"redis_url": "redis://localhost:bad-port"},
        {"qwen_url": "https://qwen.example.invalid"},
    ],
)
def test_configuration_boundaries(config, change):
    with pytest.raises(ValidationError):
        Settings(**(config | change))


def test_duplicate_tokens_rejected(config):
    config["auth_credentials"][1]["token"] = config["auth_credentials"][0]["token"]
    with pytest.raises(ValidationError):
        Settings(**config)


@pytest.mark.parametrize("environment", ["test", "production"])
def test_dev_identity_never_enabled_outside_development(config, environment):
    with pytest.raises(ValidationError):
        Settings(**(config | {"environment": environment, "dev_identity_enabled": True}))


@pytest.mark.parametrize(
    "index,expected", [(0, [200, 403, 403]), (1, [200, 200, 403]), (2, [200, 403, 200])]
)
def test_role_matrix(config, index, expected):
    client = AppClient(create_app(Settings(**config)))
    headers = {"Authorization": "Bearer " + config["auth_credentials"][index]["token"]}
    for path, status in zip(["me", "data/access", "system/access"], expected, strict=True):
        response = client.get("/api/v1/" + path, headers=headers)
        assert response.status_code == status
        if status == 403:
            assert response.json()["error"]["code"] == "FORBIDDEN"
            assert response.json()["request_id"] == response.headers["x-request-id"]


@pytest.mark.parametrize("authorization", [None, "Bearer invalid", "Basic invalid", "Bearer"])
def test_no_implicit_auth(config, authorization):
    client = AppClient(create_app(Settings(**config)))
    headers = {"Authorization": authorization} if authorization else {}
    headers["X-User-Role"] = "system_admin"
    response = client.get("/api/v1/me", headers=headers)
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


def test_development_identity_is_explicit_and_not_fallback(config):
    settings = Settings(**(config | {"environment": "development", "dev_identity_enabled": True}))
    client = AppClient(create_app(settings))
    assert client.get("/api/v1/me").json() == {"user_id": "local-user", "role": "user"}
    assert client.get("/api/v1/me", headers={"Authorization": "Bearer invalid"}).status_code == 401


@pytest.mark.parametrize("role", list(Role))
def test_resource_ownership(role):
    identity = Identity(user_id="alice", role=role)
    require_owner(identity, "alice")
    with pytest.raises(AppError) as error:
        require_owner(identity, "bob")
    assert error.value.status == 403


def test_health_and_safe_error_envelopes(config, capsys):
    app = create_app(Settings(**config))

    @app.get("/fail")
    def fail():
        raise RuntimeError("exception-secret-token")

    @app.get("/validate")
    def validate(count: int):
        return count

    client = AppClient(app)
    health = client.get("/health/live")
    assert health.json() == {"status": "alive", "scope": "application"}
    ids = {health.headers["x-request-id"]}
    for path, method, status, code in [
        ("/unknown?token=query-secret", "GET", 404, "NOT_FOUND"),
        ("/health/live", "POST", 405, "METHOD_NOT_ALLOWED"),
        ("/validate?count=input-secret", "GET", 422, "VALIDATION_ERROR"),
        ("/fail", "GET", 500, "INTERNAL_ERROR"),
    ]:
        response = client.request(method, path, headers={"X-Request-ID": "untrusted-secret"})
        assert response.status_code == status
        assert response.json()["error"]["code"] == code
        assert response.json()["request_id"] == response.headers["x-request-id"]
        ids.add(response.headers["x-request-id"])
        assert "secret" not in response.text
    assert len(ids) == 5
    logs = capsys.readouterr().err
    assert "secret" not in logs
    events = [json.loads(line) for line in logs.splitlines()]
    assert any(event["event"] == "request_failed" for event in events)


def test_formatter_drops_arbitrary_messages_and_exception(config):
    formatter = SafeJsonFormatter(Settings(**config))
    record = logging.LogRecord(
        "test", logging.ERROR, "", 0, "Authorization Bearer arbitrary-secret", (), None
    )
    record.token = config["data_source_token"]
    record.event = "arbitrary-secret"
    record.request_id = "arbitrary-secret"
    assert "secret" not in formatter.format(record)
    assert json.loads(formatter.format(record))["event"] == "redacted_log"


def test_suite_blocks_network():
    with socket.socket() as connection:
        with pytest.warns(UserWarning), pytest.raises(SocketConnectBlockedError):
            connection.connect(("192.0.2.1", 443))
