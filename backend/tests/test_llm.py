import asyncio
import json
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest
from prometheus_client import generate_latest
from pydantic import BaseModel, ConfigDict

from app.contracts.llm import LLMMessage, ModelPurpose
from app.core.config import Settings, load_settings
from app.observability.metrics import Metrics
from app.providers.qwen import LLMClient, LLMError


class RecordingSink:
    def __init__(self):
        self.started = []
        self.finished = []

    async def start(self, **values):
        self.started.append(values)

    async def finish(self, **values):
        self.finished.append(values)


class Answer(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: int


def qwen_settings(config, **updates):
    values = config | {
        "qwen_url": "https://qwen.example.invalid/v1",
        "qwen_token": "qwen-test-secret",
        "qwen_model": "qwen-test",
        "qwen_max_retries": 1,
        "qwen_backoff_seconds": 0.001,
        "qwen_connect_timeout_seconds": 0.2,
        "qwen_read_timeout_seconds": 0.2,
        "qwen_total_timeout_seconds": 0.3,
    }
    return Settings(**(values | updates))


def response(content="hello", *, usage=True):
    value = {
        "choices": [{"message": {"role": "assistant", "content": content}, "finish_reason": "stop"}]
    }
    if usage:
        value["usage"] = {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
    return httpx.Response(200, json=value, headers={"content-type": "application/json"})


def tool_response(*, content=None, arguments='{"start":"2026-01-01"}'):
    value = {
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": content,
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {"name": "finance.query", "arguments": arguments},
                        }
                    ],
                },
                "finish_reason": "tool_calls",
            }
        ],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    }
    return httpx.Response(200, json=value, headers={"content-type": "application/json"})


def run(value):
    return asyncio.run(value)


def test_qwen_configuration_requires_endpoint_credentials_and_resolved_profiles(config):
    with pytest.raises(ValueError, match="qwen_url and qwen_token"):
        Settings(**(config | {"qwen_model": "qwen-test"}))
    with pytest.raises(ValueError, match="profile for every purpose"):
        Settings(
            **(
                config
                | {
                    "qwen_url": "https://qwen.example.invalid/v1",
                    "qwen_token": "secret",
                    "qwen_profiles": {"plan": {"model": "qwen-plan"}},
                }
            )
        )
    settings = qwen_settings(
        config,
        qwen_profiles={
            "plan": {
                "model": "qwen-plan",
                "max_output_tokens": 128,
                "input_price_per_million": "2",
                "output_price_per_million": "4",
                "currency": "CNY",
            }
        },
    )
    assert settings.qwen_profiles["plan"].max_output_tokens == 128


def test_qwen_profiles_load_from_environment_json(config, monkeypatch):
    for name, value in config.items():
        monkeypatch.setenv(
            "APP_" + name.upper(), json.dumps(value) if isinstance(value, list) else str(value)
        )
    monkeypatch.setenv("APP_QWEN_URL", "https://qwen.example.invalid/v1")
    monkeypatch.setenv("APP_QWEN_TOKEN", "qwen-test-secret")
    monkeypatch.setenv("APP_QWEN_MODEL", "qwen-default")
    monkeypatch.setenv(
        "APP_QWEN_PROFILES",
        json.dumps({"verify": {"model": "qwen-verify", "max_output_tokens": 256}}),
    )
    settings = load_settings()
    assert settings.qwen_profiles["verify"].model == "qwen-verify"
    assert settings.qwen_profiles["verify"].max_output_tokens == 256


def test_native_tool_call_is_sent_and_parsed(config):
    requests = []

    async def handler(request):
        requests.append(json.loads(request.content))
        return tool_response()

    async def scenario():
        sink = RecordingSink()
        settings = qwen_settings(config)
        async with LLMClient(settings, sink, transport=httpx.MockTransport(handler)) as client:
            return await client.complete_with_tools(
                [LLMMessage(role="user", content="分析")],
                (
                    {
                        "type": "function",
                        "function": {
                            "name": "finance.query",
                            "description": "Read finance data.",
                            "parameters": {"type": "object", "properties": {}},
                        },
                    },
                ),
                purpose=ModelPurpose.GENERATE,
                prompt_version="fundamental-v5",
                request_id="1" * 32,
            )

    output = run(scenario())
    assert requests[0]["tool_choice"] == "auto"
    assert requests[0]["tools"][0]["function"]["name"] == "finance.query"
    assert output.completion.content == ""
    assert output.tool_calls[0].arguments == {"start": "2026-01-01"}


def test_native_tool_call_rejects_malformed_arguments(config):
    async def handler(request):
        del request
        return tool_response(arguments="not-json")

    async def scenario():
        async with LLMClient(
            qwen_settings(config),
            RecordingSink(),
            transport=httpx.MockTransport(handler),
        ) as client:
            await client.complete_with_tools(
                [LLMMessage(role="user", content="分析")],
                (
                    {
                        "type": "function",
                        "function": {
                            "name": "finance.query",
                            "description": "Read finance data.",
                            "parameters": {"type": "object"},
                        },
                    },
                ),
                purpose=ModelPurpose.GENERATE,
                prompt_version="fundamental-v5",
                request_id="1" * 32,
            )

    with pytest.raises(LLMError, match="invalid_tool_calls"):
        run(scenario())


def test_plain_and_structured_calls_validate_and_record_cost(config):
    requests = []

    async def handler(request):
        requests.append(json.loads(request.content))
        if requests[-1].get("response_format"):
            return response('{"value":7}')
        return response()

    sink = RecordingSink()
    settings = qwen_settings(
        config,
        qwen_profiles={
            "plan": {
                "model": "qwen-plan",
                "max_output_tokens": 128,
                "input_price_per_million": "2",
                "output_price_per_million": "4",
            }
        },
    )

    async def scenario():
        async with LLMClient(settings, sink, transport=httpx.MockTransport(handler)) as client:
            plain = await client.complete(
                [LLMMessage(role="user", content="one")],
                purpose=ModelPurpose.PLAN,
                prompt_version="plan-v1",
                request_id="a" * 32,
            )
            structured = await client.complete_structured(
                [LLMMessage(role="user", content="two")],
                Answer,
                purpose=ModelPurpose.PLAN,
                prompt_version="plan-v1",
                request_id="b" * 32,
                use_profile_max_output_tokens=False,
                temperature=0,
            )
        return plain, structured

    plain, structured = run(scenario())
    assert plain.content == "hello"
    assert plain.estimated_cost == Decimal("0.00004000")
    assert structured.parsed.value == 7
    assert requests[0]["max_completion_tokens"] == 128
    assert "max_completion_tokens" not in requests[1]
    assert requests[1]["temperature"] == 0
    schema = requests[1]["response_format"]["json_schema"]
    assert schema["strict"] is True and schema["schema"]["additionalProperties"] is False
    assert [item["status"] for item in sink.finished] == ["succeeded", "succeeded"]
    assert all("messages" not in item and "content" not in item for item in sink.finished)


def test_invalid_structured_json_has_deterministic_failed_record(config):
    sink = RecordingSink()
    client = LLMClient(
        qwen_settings(config), sink, transport=httpx.MockTransport(lambda request: response("bad"))
    )
    with pytest.raises(LLMError, match="invalid_structured_output") as error:
        run(
            client.complete_structured(
                [LLMMessage(role="user", content="return JSON")],
                Answer,
                purpose=ModelPurpose.VERIFY,
                prompt_version="verify-v1",
                request_id="c" * 32,
            )
        )
    run(client.aclose())
    assert error.value.kind == "invalid_structured_output"
    assert sink.finished[0]["status"] == "failed"
    assert sink.finished[0]["error_kind"] == "invalid_structured_output"
    assert sink.finished[0]["usage"].total_tokens == 15


def test_rate_limit_retries_but_invalid_response_does_not(config):
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(429) if calls == 1 else response()

    sink = RecordingSink()

    async def no_wait(seconds):
        return None

    client = LLMClient(
        qwen_settings(config),
        sink,
        transport=httpx.MockTransport(handler),
        sleep=no_wait,
    )
    result = run(
        client.complete(
            [LLMMessage(role="user", content="retry")],
            purpose=ModelPurpose.GENERATE,
            prompt_version="generate-v1",
            request_id="d" * 32,
        )
    )
    run(client.aclose())
    assert result.content == "hello" and calls == 2
    assert sink.finished[0]["attempts"] == 2

    invalid_calls = 0

    def invalid(request):
        nonlocal invalid_calls
        invalid_calls += 1
        return httpx.Response(200, content=b"not-json")

    client = LLMClient(
        qwen_settings(config), sink := RecordingSink(), transport=httpx.MockTransport(invalid)
    )
    with pytest.raises(LLMError, match="invalid_response"):
        run(
            client.complete(
                [LLMMessage(role="user", content="invalid")],
                purpose=ModelPurpose.GENERATE,
                prompt_version="generate-v1",
                request_id="e" * 32,
            )
        )
    run(client.aclose())
    assert invalid_calls == 1 and sink.finished[0]["attempts"] == 1


def test_timeout_and_task_cancellation_have_deterministic_states(config):
    async def slow(request):
        await asyncio.sleep(10)
        return response()

    settings = qwen_settings(
        config,
        qwen_connect_timeout_seconds=0.01,
        qwen_read_timeout_seconds=0.01,
        qwen_total_timeout_seconds=0.02,
        qwen_max_retries=0,
    )
    sink = RecordingSink()
    client = LLMClient(settings, sink, transport=httpx.MockTransport(slow))
    with pytest.raises(LLMError, match="total_timeout"):
        run(
            client.complete(
                [LLMMessage(role="user", content="timeout")],
                purpose=ModelPurpose.VERIFY,
                prompt_version="verify-v1",
                request_id="f" * 32,
            )
        )
    run(client.aclose())
    assert sink.finished[0]["status"] == "failed"
    assert sink.finished[0]["error_kind"] == "total_timeout"

    entered = asyncio.Event()

    async def blocked(request):
        entered.set()
        await asyncio.Event().wait()

    sink = RecordingSink()

    async def cancel_scenario():
        async with LLMClient(
            qwen_settings(config), sink, transport=httpx.MockTransport(blocked)
        ) as client:
            task = asyncio.create_task(
                client.complete(
                    [LLMMessage(role="user", content="cancel")],
                    purpose=ModelPurpose.PLAN,
                    prompt_version="plan-v1",
                    request_id="1" * 32,
                )
            )
            await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    run(cancel_scenario())
    assert sink.finished[0]["status"] == "cancelled"
    assert sink.finished[0]["error_kind"] == "cancelled"


def test_stream_assembles_usage_and_close_is_cancelled(config):
    lines = [
        'data: {"choices":[{"delta":{"content":"你"},"finish_reason":null}]}',
        'data: {"choices":[{"delta":{"content":"好"},"finish_reason":"stop"}]}',
        'data: {"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":2,"total_tokens":4}}',
        "data: [DONE]",
        "",
    ]
    sink = RecordingSink()
    client = LLMClient(
        qwen_settings(config),
        sink,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text="\n\n".join(lines))),
    )

    async def scenario():
        events = []
        async for event in client.stream(
            [LLMMessage(role="user", content="stream")],
            purpose=ModelPurpose.GENERATE,
            prompt_version="generate-v1",
            request_id="2" * 32,
        ):
            events.append(event)
        await client.aclose()
        return events

    events = run(scenario())
    assert [event.delta for event in events[:-1]] == ["你", "好"]
    assert events[-1].completion.content == "你好"
    assert events[-1].completion.usage.total_tokens == 4
    assert sink.finished[0]["status"] == "succeeded"

    sink = RecordingSink()
    client = LLMClient(
        qwen_settings(config),
        sink,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text="\n\n".join(lines))),
    )

    async def close_scenario():
        generator = client.stream(
            [LLMMessage(role="user", content="stop early")],
            purpose=ModelPurpose.GENERATE,
            prompt_version="generate-v1",
            request_id="3" * 32,
        )
        assert (await anext(generator)).delta == "你"
        await generator.aclose()
        await client.aclose()

    run(close_scenario())
    assert sink.finished[0]["status"] == "cancelled"


def test_concurrent_requests_do_not_share_messages(config):
    bodies = []

    async def handler(request):
        value = json.loads(request.content)
        bodies.append(value)
        await asyncio.sleep(0)
        return response(value["messages"][0]["content"])

    sink = RecordingSink()

    async def scenario():
        async with LLMClient(
            qwen_settings(config), sink, transport=httpx.MockTransport(handler)
        ) as client:
            return await asyncio.gather(
                *[
                    client.complete(
                        [LLMMessage(role="user", content=content)],
                        purpose=ModelPurpose.PLAN,
                        prompt_version="plan-v1",
                        request_id=str(index) * 32,
                    )
                    for index, content in ((4, "alpha"), (5, "beta"))
                ]
            )

    results = run(scenario())
    assert {result.content for result in results} == {"alpha", "beta"}
    assert [[message["content"] for message in body["messages"]] for body in bodies] in (
        [["alpha"], ["beta"]],
        [["beta"], ["alpha"]],
    )
    assert len({item["invocation_id"] for item in sink.started}) == 2


def test_missing_usage_or_price_is_unknown_not_zero(config):
    metrics = Metrics()
    sink = RecordingSink()
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        return response(usage=calls > 1)

    client = LLMClient(
        qwen_settings(config),
        sink,
        metrics=metrics,
        transport=httpx.MockTransport(handler),
    )

    async def scenario():
        missing_usage = await client.complete(
            [LLMMessage(role="user", content="unknown usage")],
            purpose=ModelPurpose.PLAN,
            prompt_version="plan-v1",
            request_id="6" * 32,
        )
        missing_price = await client.complete(
            [LLMMessage(role="user", content="unknown price")],
            purpose=ModelPurpose.PLAN,
            prompt_version="plan-v1",
            request_id="7" * 32,
        )
        await client.aclose()
        return missing_usage, missing_price

    missing_usage, missing_price = run(scenario())
    output = generate_latest(metrics.registry).decode()
    assert missing_usage.usage is None and missing_usage.estimated_cost is None
    assert missing_price.usage.total_tokens == 15 and missing_price.estimated_cost is None
    assert (
        'llm_observation_unknown_total{model="qwen-test",purpose="plan",reason="usage"} 1.0'
        in output
    )
    assert (
        'llm_observation_unknown_total{model="qwen-test",purpose="plan",reason="price"} 1.0'
        in output
    )
    assert "llm_estimated_cost_total{" not in output


def test_repository_finish_is_state_guarded_and_never_accepts_sensitive_error():
    from app.repositories.llm import ModelInvocationRepository

    connection = AsyncMock()
    cursor = AsyncMock()
    cursor.fetchone.return_value = ("id",)
    connection.execute.return_value = cursor
    repository = ModelInvocationRepository(connection)
    run(
        repository.finish(
            invocation_id="00000000-0000-0000-0000-000000000001",
            status="failed",
            attempts=1,
            duration_ms=2,
            usage=None,
            estimated_cost=None,
            cost_currency=None,
            error_kind="rate_limited",
        )
    )
    assert connection.execute.await_count == 1
    with pytest.raises(ValueError, match="invalid_invocation_error_kind"):
        run(
            repository.finish(
                invocation_id="00000000-0000-0000-0000-000000000001",
                status="failed",
                attempts=1,
                duration_ms=2,
                usage=None,
                estimated_cost=None,
                cost_currency=None,
                error_kind="secret: upstream body",
            )
        )
