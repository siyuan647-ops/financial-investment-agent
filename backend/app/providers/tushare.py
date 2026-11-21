"""Bounded asynchronous Tushare-compatible transport; no upstream error text escapes."""

import asyncio
import json
import time
from dataclasses import dataclass, field

import httpx

from app.core.config import Settings


class ProviderError(RuntimeError):
    def __init__(
        self,
        kind: str,
        *,
        retryable: bool = False,
        http_status: int | None = None,
        business_code: int | None = None,
    ):
        self.kind = kind
        self.retryable = retryable
        self.http_status = http_status
        self.business_code = business_code
        super().__init__(kind)


@dataclass(frozen=True)
class ProviderResult:
    fields: tuple[str, ...]
    items: tuple[tuple, ...]
    raw_content: bytes = field(default=b"", repr=False, compare=False)

    @property
    def rows(self) -> list[dict]:
        return [dict(zip(self.fields, row, strict=True)) for row in self.items]


class ProviderAdapter:
    """One adapter per process/event loop. Rate gate and circuit shared by all calls.

    Serialization bounds concurrency to one and allows exactly one recovery trial.
    Permission failures do not trip the source circuit. No redirects or env proxies.
    """

    def __init__(self, settings: Settings, *, transport=None):
        self.settings = settings
        self._client = httpx.AsyncClient(
            transport=transport,
            follow_redirects=False,
            trust_env=False,
            timeout=httpx.Timeout(
                settings.read_timeout_seconds, connect=settings.connect_timeout_seconds
            ),
        )
        self._lock = asyncio.Lock()
        self._next_request = 0.0
        self._failures = 0
        self._open_until = 0.0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.aclose()

    async def aclose(self):
        await self._client.aclose()

    async def query(
        self, api_name: str, params: dict | None = None, fields: str = ""
    ) -> ProviderResult:
        active = False
        try:
            async with asyncio.timeout(self.settings.total_timeout_seconds):
                async with self._lock:
                    if time.monotonic() < self._open_until:
                        raise ProviderError("circuit_open")
                    active = True
                    try:
                        result = await self._retry(api_name, params or {}, fields)
                    except ProviderError as exc:
                        if exc.retryable:
                            self._record_failure()
                        else:
                            self._failures = 0
                        raise
                    self._failures = 0
                    self._open_until = 0
                    return result
        except TimeoutError:
            if active:
                self._record_failure()
            raise ProviderError("total_timeout", retryable=True) from None

    def _record_failure(self):
        self._failures += 1
        if self._failures >= self.settings.provider_failure_threshold:
            self._open_until = time.monotonic() + self.settings.provider_cooldown_seconds

    async def _retry(self, api_name, params, fields):
        for attempt in range(self.settings.provider_max_retries + 1):
            await asyncio.sleep(max(0, self._next_request - time.monotonic()))
            self._next_request = time.monotonic() + 60 / self.settings.rate_limit_per_minute
            try:
                return await self._request(api_name, params, fields)
            except ProviderError as exc:
                if not exc.retryable or attempt == self.settings.provider_max_retries:
                    raise
                await asyncio.sleep(min(30, self.settings.provider_backoff_seconds * 2**attempt))
        raise AssertionError("unreachable")

    async def _request(self, api_name, params, fields):
        payload = {
            "api_name": api_name,
            "token": self.settings.data_source_token.get_secret_value(),
            "params": params,
            "fields": fields,
        }
        try:
            async with self._client.stream(
                "POST",
                self.settings.data_source_url.get_secret_value(),
                json=payload,
                headers={"Accept-Encoding": "identity"},
            ) as response:
                status = response.status_code
                if status in (401, 403):
                    raise ProviderError("permission_denied", http_status=status)
                if status == 429 or status in (500, 502, 503, 504):
                    raise ProviderError(
                        "rate_limited" if status == 429 else "source_failure",
                        retryable=True,
                        http_status=status,
                    )
                if status != 200:
                    raise ProviderError("http_error", http_status=status)
                # Reject compression so decompression cannot allocate an unbounded body.
                if response.headers.get("content-encoding", "identity") != "identity":
                    raise ProviderError("unsupported_encoding")
                body = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    if len(body) + len(chunk) > self.settings.provider_max_response_bytes:
                        raise ProviderError("response_too_large")
                    body.extend(chunk)
        except httpx.TimeoutException:
            raise ProviderError("transport_timeout", retryable=True) from None
        except httpx.HTTPError:
            raise ProviderError("transport_error", retryable=True) from None
        try:
            value = json.loads(body)
            code = value["code"]
            if type(code) is not int:
                raise ValueError
            if code in (2002, 401, 403):
                raise ProviderError("permission_denied", http_status=200, business_code=code)
            if code != 0:
                # Unknown third-party codes must not be guessed to be transient.
                raise ProviderError("business_error", http_status=200, business_code=code)
            data = value["data"]
            names, items = data["fields"], data["items"]
            if (
                not isinstance(names, list)
                or not all(isinstance(n, str) for n in names)
                or len(names) != len(set(names))
                or not isinstance(items, list)
                or any(not isinstance(r, list) or len(r) != len(names) for r in items)
            ):
                raise ValueError
            return ProviderResult(tuple(names), tuple(tuple(r) for r in items), bytes(body))
        except (ValueError, KeyError, TypeError, RecursionError):
            raise ProviderError("invalid_response") from None
