"""Stateless Qwen OpenAI-compatible client with bounded retries and durable outcomes."""

import asyncio
import json
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from time import perf_counter
from typing import Protocol, TypeVar
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, ValidationError

from app.contracts.llm import (
    LLMCompletion,
    LLMMessage,
    LLMStreamEvent,
    LLMToolCall,
    ModelPurpose,
    StructuredCompletion,
    TokenUsage,
    ToolCompletion,
)
from app.core.config import QwenProfile, Settings

SAFE_ID = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")
SAFE_VERSION = re.compile(r"^[a-zA-Z0-9_.:-]{1,128}$")
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
StructuredValue = TypeVar("StructuredValue", bound=BaseModel)


class InvocationRecorder(Protocol):
    async def start(
        self,
        *,
        invocation_id: UUID,
        request_id: str,
        task_id: str | None,
        purpose: ModelPurpose,
        model: str,
        prompt_version: str,
        stream: bool,
    ) -> None: ...

    async def finish(
        self,
        *,
        invocation_id: UUID,
        status: str,
        attempts: int,
        duration_ms: int,
        usage: TokenUsage | None,
        estimated_cost: Decimal | None,
        cost_currency: str | None,
        error_kind: str | None,
    ) -> None: ...


class LLMError(RuntimeError):
    def __init__(self, kind: str, *, retryable: bool = False, http_status: int | None = None):
        self.kind = kind
        self.retryable = retryable
        self.http_status = http_status
        super().__init__(kind)


@dataclass(frozen=True)
class ResolvedProfile:
    model: str
    max_output_tokens: int
    input_price_per_million: Decimal | None
    output_price_per_million: Decimal | None
    currency: str


@dataclass
class InvocationState:
    invocation_id: UUID
    purpose: ModelPurpose
    profile: ResolvedProfile
    started: float
    attempts: int = 0
    usage: TokenUsage | None = None


class LLMClient:
    """One process-level transport; no conversation or request content is retained."""

    def __init__(
        self,
        settings: Settings,
        recorder: InvocationRecorder,
        *,
        metrics=None,
        transport=None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        if not settings.qwen_url or not settings.qwen_token:
            raise ValueError("qwen_not_configured")
        self.settings = settings
        self.recorder = recorder
        self.metrics = metrics
        self._sleep = sleep
        self._client = httpx.AsyncClient(
            transport=transport,
            follow_redirects=False,
            trust_env=False,
            timeout=httpx.Timeout(
                settings.qwen_read_timeout_seconds,
                connect=settings.qwen_connect_timeout_seconds,
            ),
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.aclose()

    async def aclose(self):
        await self._client.aclose()

    async def complete(
        self,
        messages: Sequence[LLMMessage],
        *,
        purpose: ModelPurpose,
        prompt_version: str,
        request_id: str,
        task_id: str | None = None,
        profile_override: QwenProfile | None = None,
        temperature: float | None = None,
    ) -> LLMCompletion:
        completion, _ = await self._complete(
            messages,
            purpose=purpose,
            prompt_version=prompt_version,
            request_id=request_id,
            task_id=task_id,
            response_model=None,
            profile_override=profile_override,
            temperature=temperature,
        )
        return completion

    async def complete_structured(
        self,
        messages: Sequence[LLMMessage],
        response_model: type[StructuredValue],
        *,
        purpose: ModelPurpose,
        prompt_version: str,
        request_id: str,
        task_id: str | None = None,
        use_profile_max_output_tokens: bool = True,
        temperature: float | None = None,
        profile_override: QwenProfile | None = None,
    ) -> StructuredCompletion[StructuredValue]:
        completion, parsed = await self._complete(
            messages,
            purpose=purpose,
            prompt_version=prompt_version,
            request_id=request_id,
            task_id=task_id,
            response_model=response_model,
            use_profile_max_output_tokens=use_profile_max_output_tokens,
            temperature=temperature,
            profile_override=profile_override,
        )
        return StructuredCompletion(completion=completion, parsed=parsed)

    async def complete_with_tools(
        self,
        messages: Sequence[LLMMessage],
        tools: Sequence[dict],
        *,
        purpose: ModelPurpose,
        prompt_version: str,
        request_id: str,
        task_id: str | None = None,
        profile_override: QwenProfile | None = None,
        temperature: float | None = None,
    ) -> ToolCompletion:
        """Complete once with native OpenAI-compatible function tools enabled."""
        state, payload = await self._begin(
            messages,
            purpose=purpose,
            prompt_version=prompt_version,
            request_id=request_id,
            task_id=task_id,
            stream=False,
            response_model=None,
            profile_override=profile_override,
            temperature=temperature,
        )
        if not tools:
            raise ValueError("tools_required")
        payload["tools"] = [self._validate_tool_definition(item) for item in tools]
        payload["tool_choice"] = "auto"
        self._active(state, 1)
        try:
            try:
                async with asyncio.timeout(self.settings.qwen_total_timeout_seconds):
                    value = await self._retry_json(state, payload)
                    content, finish_reason, usage, tool_calls = self._parse_tool_completion(value)
                    state.usage = usage
                completion = self._completion(state, content, finish_reason, usage)
                await self._finish(state, "succeeded", completion=completion)
                return ToolCompletion(completion=completion, tool_calls=tool_calls)
            except TimeoutError:
                raise LLMError("total_timeout", retryable=True) from None
        except asyncio.CancelledError:
            await self._finish(state, "cancelled", error_kind="cancelled")
            raise
        except LLMError as exc:
            await self._finish(state, "failed", error_kind=exc.kind)
            raise
        finally:
            self._active(state, -1)

    async def _complete(
        self,
        messages,
        *,
        purpose,
        prompt_version,
        request_id,
        task_id,
        response_model,
        use_profile_max_output_tokens=True,
        temperature=None,
        profile_override=None,
    ):
        state, payload = await self._begin(
            messages,
            purpose=purpose,
            prompt_version=prompt_version,
            request_id=request_id,
            task_id=task_id,
            stream=False,
            response_model=response_model,
            use_profile_max_output_tokens=use_profile_max_output_tokens,
            temperature=temperature,
            profile_override=profile_override,
        )
        self._active(state, 1)
        try:
            try:
                async with asyncio.timeout(self.settings.qwen_total_timeout_seconds):
                    value = await self._retry_json(state, payload)
                    content, finish_reason, usage = self._parse_completion(value)
                    state.usage = usage
                    parsed = None
                    if response_model is not None:
                        try:
                            parsed = response_model.model_validate_json(content)
                        except (ValidationError, ValueError, TypeError):
                            raise LLMError("invalid_structured_output") from None
                completion = self._completion(state, content, finish_reason, usage)
                await self._finish(state, "succeeded", completion=completion)
                return completion, parsed
            except TimeoutError:
                raise LLMError("total_timeout", retryable=True) from None
        except asyncio.CancelledError:
            await self._finish(state, "cancelled", error_kind="cancelled")
            raise
        except LLMError as exc:
            await self._finish(state, "failed", error_kind=exc.kind)
            raise
        finally:
            self._active(state, -1)

    async def stream(
        self,
        messages: Sequence[LLMMessage],
        *,
        purpose: ModelPurpose,
        prompt_version: str,
        request_id: str,
        task_id: str | None = None,
    ) -> AsyncIterator[LLMStreamEvent]:
        state, payload = await self._begin(
            messages,
            purpose=purpose,
            prompt_version=prompt_version,
            request_id=request_id,
            task_id=task_id,
            stream=True,
            response_model=None,
        )
        self._active(state, 1)
        try:
            try:
                async with asyncio.timeout(self.settings.qwen_total_timeout_seconds):
                    content_parts = []
                    finish_reason = None
                    usage = None
                    async for delta, reason, chunk_usage in self._retry_stream(state, payload):
                        if reason is not None:
                            finish_reason = reason
                        if chunk_usage is not None:
                            usage = chunk_usage
                            state.usage = chunk_usage
                        if delta:
                            content_parts.append(delta)
                            yield LLMStreamEvent(kind="delta", delta=delta)
                completion = self._completion(state, "".join(content_parts), finish_reason, usage)
                await self._finish(state, "succeeded", completion=completion)
                yield LLMStreamEvent(kind="completed", completion=completion)
            except TimeoutError:
                raise LLMError("total_timeout", retryable=True) from None
        except (asyncio.CancelledError, GeneratorExit):
            await self._finish(state, "cancelled", error_kind="cancelled")
            raise
        except LLMError as exc:
            await self._finish(state, "failed", error_kind=exc.kind)
            raise
        finally:
            self._active(state, -1)

    async def _begin(
        self,
        messages,
        *,
        purpose,
        prompt_version,
        request_id,
        task_id,
        stream,
        response_model,
        use_profile_max_output_tokens=True,
        temperature=None,
        profile_override=None,
    ):
        if not isinstance(purpose, ModelPurpose):
            purpose = ModelPurpose(purpose)
        self._validate_metadata(request_id, task_id, prompt_version)
        clean_messages = [LLMMessage.model_validate(message) for message in messages]
        if not clean_messages:
            raise ValueError("messages_required")
        profile = (
            self._profile(purpose)
            if profile_override is None
            else ResolvedProfile(
                model=profile_override.model,
                max_output_tokens=profile_override.max_output_tokens,
                input_price_per_million=profile_override.input_price_per_million,
                output_price_per_million=profile_override.output_price_per_million,
                currency=profile_override.currency,
            )
        )
        state = InvocationState(uuid4(), purpose, profile, perf_counter())
        payload = {
            "model": profile.model,
            "messages": [message.model_dump(mode="json") for message in clean_messages],
            "stream": stream,
        }
        if use_profile_max_output_tokens:
            payload["max_completion_tokens"] = profile.max_output_tokens
        if temperature is not None:
            if not 0 <= temperature <= 2:
                raise ValueError("temperature_out_of_range")
            payload["temperature"] = temperature
        if stream:
            payload["stream_options"] = {"include_usage": True}
        if response_model is not None:
            schema_name = re.sub(r"[^a-zA-Z0-9_-]", "_", response_model.__name__)[:64]
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": schema_name or "structured_output",
                    "strict": True,
                    "schema": response_model.model_json_schema(),
                },
            }
        await self.recorder.start(
            invocation_id=state.invocation_id,
            request_id=request_id,
            task_id=task_id,
            purpose=purpose,
            model=profile.model,
            prompt_version=prompt_version,
            stream=stream,
        )
        return state, payload

    async def _retry_json(self, state, payload):
        for attempt in range(self.settings.qwen_max_retries + 1):
            state.attempts = attempt + 1
            try:
                return await self._request_json(payload)
            except LLMError as exc:
                if not exc.retryable or attempt == self.settings.qwen_max_retries:
                    raise
                await self._sleep(min(30, self.settings.qwen_backoff_seconds * 2**attempt))
        raise AssertionError("unreachable")

    async def _request_json(self, payload):
        try:
            async with self._client.stream(
                "POST",
                self._url(),
                headers=self._headers(),
                json=payload,
            ) as response:
                self._check_status(response.status_code)
                self._check_encoding(response)
                body = await self._bounded_body(response)
        except httpx.TimeoutException:
            raise LLMError("transport_timeout", retryable=True) from None
        except httpx.HTTPError:
            raise LLMError("transport_error", retryable=True) from None
        try:
            value = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError, RecursionError):
            raise LLMError("invalid_response") from None
        if not isinstance(value, dict):
            raise LLMError("invalid_response")
        return value

    async def _retry_stream(self, state, payload):
        received_event = False
        for attempt in range(self.settings.qwen_max_retries + 1):
            state.attempts = attempt + 1
            try:
                async for event in self._request_stream(payload):
                    received_event = True
                    yield event
                return
            except LLMError as exc:
                if received_event:
                    raise LLMError("stream_interrupted") from None
                if not exc.retryable or attempt == self.settings.qwen_max_retries:
                    raise
                await self._sleep(min(30, self.settings.qwen_backoff_seconds * 2**attempt))
        raise AssertionError("unreachable")

    async def _request_stream(self, payload):
        saw_done = False
        size = 0
        try:
            async with self._client.stream(
                "POST",
                self._url(),
                headers=self._headers(),
                json=payload,
            ) as response:
                self._check_status(response.status_code)
                self._check_encoding(response)
                async for line in response.aiter_lines():
                    size += len(line.encode("utf-8")) + 1
                    if size > self.settings.qwen_max_response_bytes:
                        raise LLMError("response_too_large")
                    if not line or line.startswith(":"):
                        continue
                    if not line.startswith("data:"):
                        raise LLMError("invalid_stream")
                    data = line[5:].strip()
                    if data == "[DONE]":
                        saw_done = True
                        break
                    try:
                        value = json.loads(data)
                        yield self._parse_stream_chunk(value)
                    except (json.JSONDecodeError, UnicodeDecodeError, RecursionError):
                        raise LLMError("invalid_stream") from None
        except httpx.TimeoutException:
            raise LLMError("transport_timeout", retryable=True) from None
        except httpx.HTTPError:
            raise LLMError("transport_error", retryable=True) from None
        if not saw_done:
            raise LLMError("stream_incomplete", retryable=True)

    def _parse_completion(self, value):
        try:
            choices = value["choices"]
            choice = choices[0]
            content = choice["message"]["content"]
            finish_reason = choice.get("finish_reason")
            if not isinstance(choices, list) or len(choices) != 1 or not isinstance(content, str):
                raise TypeError
            if finish_reason is not None and not isinstance(finish_reason, str):
                raise TypeError
            usage = self._parse_usage(value.get("usage"))
            return content, finish_reason, usage
        except (KeyError, IndexError, TypeError, ValidationError):
            raise LLMError("invalid_response") from None

    def _parse_tool_completion(self, value):
        try:
            choices = value["choices"]
            if not isinstance(choices, list) or len(choices) != 1:
                raise TypeError
            choice = choices[0]
            message = choice["message"]
            content = message.get("content") or ""
            finish_reason = choice.get("finish_reason")
            raw_calls = message.get("tool_calls") or []
            if not isinstance(content, str) or not isinstance(raw_calls, list):
                raise TypeError
            if finish_reason is not None and not isinstance(finish_reason, str):
                raise TypeError
            calls = []
            for raw_call in raw_calls:
                function = raw_call["function"]
                arguments = json.loads(function["arguments"])
                if not isinstance(arguments, dict):
                    raise TypeError
                calls.append(
                    LLMToolCall(
                        id=raw_call["id"],
                        name=function["name"],
                        arguments=arguments,
                    )
                )
            if not calls and not content.strip():
                raise TypeError
            return content, finish_reason, self._parse_usage(value.get("usage")), tuple(calls)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, ValidationError):
            raise LLMError("invalid_tool_calls") from None

    @staticmethod
    def _validate_tool_definition(value):
        try:
            if value.get("type") != "function":
                raise TypeError
            function = value["function"]
            if not SAFE_VERSION.fullmatch(function["name"]):
                raise TypeError
            if not isinstance(function["description"], str):
                raise TypeError
            parameters = function["parameters"]
            if not isinstance(parameters, dict):
                raise TypeError
            return {
                "type": "function",
                "function": {
                    "name": function["name"],
                    "description": function["description"],
                    "parameters": parameters,
                },
            }
        except (AttributeError, KeyError, TypeError):
            raise ValueError("invalid_tool_definition") from None

    def _parse_stream_chunk(self, value):
        if not isinstance(value, dict):
            raise LLMError("invalid_stream")
        usage = self._parse_usage(value.get("usage"))
        choices = value.get("choices")
        if choices == []:
            if usage is None:
                raise LLMError("invalid_stream")
            return "", None, usage
        try:
            if not isinstance(choices, list) or len(choices) != 1:
                raise TypeError
            choice = choices[0]
            delta = choice["delta"].get("content") or ""
            reason = choice.get("finish_reason")
            if not isinstance(delta, str) or (reason is not None and not isinstance(reason, str)):
                raise TypeError
            return delta, reason, usage
        except (KeyError, TypeError):
            raise LLMError("invalid_stream") from None

    @staticmethod
    def _parse_usage(value):
        if value is None:
            return None
        try:
            return TokenUsage(
                input_tokens=value["prompt_tokens"],
                output_tokens=value["completion_tokens"],
                total_tokens=value["total_tokens"],
            )
        except (KeyError, TypeError, ValidationError):
            raise LLMError("invalid_usage") from None

    def _completion(self, state, content, finish_reason, usage):
        cost, currency = self._cost(state.profile, usage)
        return LLMCompletion(
            invocation_id=state.invocation_id,
            purpose=state.purpose,
            model=state.profile.model,
            content=content,
            finish_reason=finish_reason,
            usage=usage,
            estimated_cost=cost,
            cost_currency=currency,
        )

    async def _finish(self, state, status, *, completion=None, error_kind=None):
        duration_ms = max(0, round((perf_counter() - state.started) * 1000))
        usage = completion.usage if completion else state.usage
        if completion:
            cost, currency = completion.estimated_cost, completion.cost_currency
        else:
            cost, currency = self._cost(state.profile, usage)
        await self.recorder.finish(
            invocation_id=state.invocation_id,
            status=status,
            attempts=state.attempts,
            duration_ms=duration_ms,
            usage=usage,
            estimated_cost=cost,
            cost_currency=currency,
            error_kind=error_kind,
        )
        if self.metrics is not None:
            unknown_reason = None
            if status == "succeeded" and usage is None:
                unknown_reason = "usage"
            elif status == "succeeded" and cost is None:
                unknown_reason = "price"
            self.metrics.record_llm(
                purpose=state.purpose.value,
                model=state.profile.model,
                status=status,
                duration_seconds=duration_ms / 1000,
                usage=usage,
                estimated_cost=cost,
                currency=currency,
                unknown_reason=unknown_reason,
            )

    def _active(self, state, amount):
        if self.metrics is not None:
            self.metrics.llm_active.labels(state.purpose.value, state.profile.model).inc(amount)

    def _profile(self, purpose):
        configured: QwenProfile | None = self.settings.qwen_profiles.get(purpose.value)
        if configured is None:
            if not self.settings.qwen_model:
                raise ValueError("qwen_profile_missing")
            return ResolvedProfile(
                model=self.settings.qwen_model,
                max_output_tokens=self.settings.qwen_default_max_output_tokens,
                input_price_per_million=None,
                output_price_per_million=None,
                currency="CNY",
            )
        return ResolvedProfile(
            model=configured.model,
            max_output_tokens=configured.max_output_tokens,
            input_price_per_million=configured.input_price_per_million,
            output_price_per_million=configured.output_price_per_million,
            currency=configured.currency,
        )

    @staticmethod
    def _cost(profile, usage):
        if (
            usage is None
            or profile.input_price_per_million is None
            or profile.output_price_per_million is None
        ):
            return None, None
        cost = (
            Decimal(usage.input_tokens) * profile.input_price_per_million
            + Decimal(usage.output_tokens) * profile.output_price_per_million
        ) / Decimal(1_000_000)
        return cost.quantize(Decimal("0.00000001")), profile.currency

    @staticmethod
    def _validate_metadata(request_id, task_id, prompt_version):
        if not SAFE_ID.fullmatch(request_id):
            raise ValueError("invalid_request_id")
        if task_id is not None and not SAFE_ID.fullmatch(task_id):
            raise ValueError("invalid_task_id")
        if not SAFE_VERSION.fullmatch(prompt_version):
            raise ValueError("invalid_prompt_version")

    def _url(self):
        return self.settings.qwen_url.get_secret_value().rstrip("/") + "/chat/completions"

    def _headers(self):
        return {
            "Authorization": "Bearer " + self.settings.qwen_token.get_secret_value(),
            "Accept": "text/event-stream, application/json",
            "Accept-Encoding": "identity",
        }

    @staticmethod
    def _check_status(status):
        if status in (401, 403):
            raise LLMError("permission_denied", http_status=status)
        if status in RETRYABLE_STATUSES:
            kind = "rate_limited" if status == 429 else "upstream_failure"
            raise LLMError(kind, retryable=True, http_status=status)
        if status != 200:
            raise LLMError("http_error", http_status=status)

    @staticmethod
    def _check_encoding(response):
        if response.headers.get("content-encoding", "identity") != "identity":
            raise LLMError("unsupported_encoding")

    async def _bounded_body(self, response):
        body = bytearray()
        async for chunk in response.aiter_bytes(chunk_size=65536):
            if len(body) + len(chunk) > self.settings.qwen_max_response_bytes:
                raise LLMError("response_too_large")
            body.extend(chunk)
        return bytes(body)
