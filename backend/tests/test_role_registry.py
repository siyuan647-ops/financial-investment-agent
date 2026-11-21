"""Five-role registration, prompt locks and per-role model settings."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from app.agents import role_registry, specialists
from app.agents.role_registry import RoleRegistry
from app.agents.specialists import DynamicMultiAgentResearchGraph, QwenSpecialistModels
from app.contracts.llm import LLMMessage
from app.contracts.research import SpecialistRole
from app.core.config import Settings, SpecialistModelProfile
from app.providers.qwen import LLMClient


def test_every_role_has_one_registration_and_explicit_tool_policy():
    registry = RoleRegistry()
    assert set(registry.definitions) == set(SpecialistRole)
    assert registry.get("investment").tool_role is None
    assert registry.get("risk").tool_role is None
    assert registry.get("market").tool_role.value == "market"
    assert all(
        item.responsibility and item.prompt_version for item in registry.definitions.values()
    )


def test_prompt_text_change_requires_version_manifest_update(monkeypatch):
    registry = RoleRegistry()
    actual = specialists._current_prompt_digests()
    registry.validate_prompts(actual)
    assert specialists._prompt_digest(lambda: "original") != specialists._prompt_digest(
        lambda: "changed"
    )
    changed = {**actual, SpecialistRole.NEWS: "0" * 64}
    monkeypatch.setattr(specialists, "_current_prompt_digests", lambda: changed)
    with pytest.raises(ValueError, match="research_prompt_version_not_updated:news"):
        DynamicMultiAgentResearchGraph(object(), object())


def test_prompt_digest_update_without_version_is_rejected(monkeypatch):
    original = role_registry.ROLE_DEFINITIONS
    changed = dict(original)
    changed[SpecialistRole.NEWS] = replace(original[SpecialistRole.NEWS], prompt_digest="f" * 64)
    monkeypatch.setattr(role_registry, "ROLE_DEFINITIONS", changed)
    with pytest.raises(ValueError, match="research_prompt_version_digest_mismatch:news"):
        RoleRegistry()


def test_per_role_model_override_reaches_qwen_request(config):
    requests = []

    async def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "choices": [
                    {"message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}
                ]
            },
        )

    async def scenario():
        settings = Settings(
            **(
                config
                | {
                    "qwen_url": "https://qwen.example.invalid/v1",
                    "qwen_token": "test-token",
                    "qwen_model": "qwen-default",
                    "specialist_model_profiles": {
                        "market": {"model": "qwen-market", "max_output_tokens": 222},
                        "risk": {
                            "model": "qwen-risk",
                            "max_output_tokens": 321,
                            "temperature": 0.4,
                        },
                    },
                }
            )
        )
        registry = RoleRegistry(settings.specialist_model_profiles)
        sink = SimpleNamespace(start=AsyncMock(), finish=AsyncMock())
        async with LLMClient(settings, sink, transport=httpx.MockTransport(handler)) as client:
            models = QwenSpecialistModels(client, registry)
            await client.complete(
                [LLMMessage(role="user", content="test")],
                **models._call_options(SpecialistRole.RISK),
                request_id="1" * 32,
            )
            await client.complete(
                [LLMMessage(role="user", content="test")],
                **models._call_options(SpecialistRole.MARKET),
                request_id="1" * 32,
            )
            await client.complete(
                [LLMMessage(role="user", content="test")],
                **models._call_options(SpecialistRole.FUNDAMENTAL),
                request_id="1" * 32,
            )
        assert requests[0]["model"] == "qwen-risk"
        assert requests[0]["max_completion_tokens"] == 321
        assert requests[0]["temperature"] == 0.4
        assert requests[1]["model"] == "qwen-market"
        assert requests[1]["max_completion_tokens"] == 222
        assert "temperature" not in requests[1]
        assert requests[2]["model"] == "qwen-default"
        assert sink.start.await_args_list[0].kwargs["model"] == "qwen-risk"

    asyncio.run(scenario())


def test_role_profile_validation_rejects_unknown_role_and_bad_temperature(config):
    with pytest.raises(ValueError):
        Settings(
            **(
                config
                | {
                    "specialist_model_profiles": {"strategy": {"model": "qwen-test"}},
                }
            )
        )
    with pytest.raises(ValueError):
        SpecialistModelProfile(model="qwen-risk", temperature=3)
