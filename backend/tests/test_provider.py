import asyncio
import json

import httpx
import pytest

from app.core.config import Settings
from app.providers.probe import assess
from app.providers.tushare import ProviderAdapter, ProviderError, ProviderResult


def settings(config, **overrides):
    return Settings(
        **{
            **config,
            "rate_limit_per_minute": 100000,
            "provider_backoff_seconds": 0.001,
            **overrides,
        }
    )


def ok():
    return httpx.Response(
        200,
        json={
            "code": 0,
            "data": {"fields": ["ts_code", "trade_date"], "items": [["000001.SZ", "20260930"]]},
        },
    )


def test_protocol_and_retry(config):
    requests = []

    def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(503) if len(requests) < 3 else ok()

    async def run():
        async with ProviderAdapter(settings(config), transport=httpx.MockTransport(handler)) as p:
            result = await p.query("daily", {"ts_code": "000001.SZ"}, "ts_code,trade_date")
            assert result.rows[0]["trade_date"] == "20260930"
            assert result.raw_content == ok().content
            assert "raw_content" not in repr(result)

    asyncio.run(run())
    assert len(requests) == 3
    assert requests[0]["api_name"] == "daily"
    assert requests[0]["params"] == {"ts_code": "000001.SZ"}
    assert requests[0]["fields"] == "ts_code,trade_date"
    assert requests[0]["token"] == config["data_source_token"]


@pytest.mark.parametrize(
    ("response", "kind"),
    [
        (httpx.Response(403), "permission_denied"),
        (httpx.Response(200, json={"code": 2002, "msg": "secret"}), "permission_denied"),
        (httpx.Response(200, json={"code": 999, "msg": "secret"}), "business_error"),
        (httpx.Response(302, headers={"location": "https://other.invalid"}), "http_error"),
        (httpx.Response(200, text="secret"), "invalid_response"),
        (
            httpx.Response(200, json={"code": 0, "data": {"fields": ["a"], "items": [[1, 2]]}}),
            "invalid_response",
        ),
        (httpx.Response(200, text="x" * 1025), "response_too_large"),
    ],
)
def test_permanent_errors_never_retry_or_leak(config, response, kind):
    calls = []

    def handler(request):
        calls.append(request)
        return response

    async def run():
        async with ProviderAdapter(
            settings(config, provider_max_response_bytes=1024),
            transport=httpx.MockTransport(handler),
        ) as p:
            with pytest.raises(ProviderError, match=f"^{kind}$"):
                await p.query("daily")

    asyncio.run(run())
    assert len(calls) == 1


def test_endpoint_denial_does_not_trip_circuit(config):
    def handler(request):
        return httpx.Response(403) if json.loads(request.content)["api_name"] == "rt_k" else ok()

    async def run():
        async with ProviderAdapter(
            settings(config, provider_failure_threshold=1), transport=httpx.MockTransport(handler)
        ) as p:
            with pytest.raises(ProviderError, match="permission_denied"):
                await p.query("rt_k")
            assert (await p.query("daily")).items

    asyncio.run(run())


def test_circuit_opens_and_recovers(config):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(503) if len(calls) == 1 else ok()

    async def run():
        async with ProviderAdapter(
            settings(
                config,
                provider_failure_threshold=1,
                provider_max_retries=0,
                provider_cooldown_seconds=0.01,
            ),
            transport=httpx.MockTransport(handler),
        ) as p:
            with pytest.raises(ProviderError, match="source_failure"):
                await p.query("daily")
            with pytest.raises(ProviderError, match="circuit_open"):
                await p.query("daily")
            assert len(calls) == 1
            await asyncio.sleep(0.02)
            assert (await p.query("daily")).items

    asyncio.run(run())


def test_total_timeout_includes_waiting_and_retry(config):
    async def handler(request):
        await asyncio.sleep(1)
        return ok()

    async def run():
        async with ProviderAdapter(
            settings(
                config,
                total_timeout_seconds=0.03,
                read_timeout_seconds=0.01,
                connect_timeout_seconds=0.01,
            ),
            transport=httpx.MockTransport(handler),
        ) as p:
            with pytest.raises(ProviderError, match="total_timeout"):
                await p.query("daily")

    asyncio.run(run())


def test_filter_mismatch_and_empty_are_distinct():
    params = {"ts_code": "000001.SZ", "start_date": "20260901", "end_date": "20260930", "limit": 1}
    wrong = ProviderResult(("ts_code", "trade_date"), (("600000.SH", "20260101"),))
    checks = assess(wrong, params, "ts_code,trade_date", "trade_date")
    assert checks["security"] == checks["date"] == "ignored_or_mismatched"
    empty = assess(ProviderResult(wrong.fields, ()), params, "ts_code,trade_date", "trade_date")
    assert empty["security"] == empty["date"] == empty["limit"] == "pending"


def test_rate_gate_spaces_concurrent_requests(config):
    times = []

    def handler(request):
        times.append(asyncio.get_running_loop().time())
        return ok()

    async def run():
        async with ProviderAdapter(
            settings(config, rate_limit_per_minute=1200), transport=httpx.MockTransport(handler)
        ) as p:
            await asyncio.gather(p.query("daily"), p.query("daily"), p.query("daily"))

    asyncio.run(run())
    assert all(b - a >= 0.045 for a, b in zip(times, times[1:], strict=False))


@pytest.mark.parametrize("failure", ["timeout", "connection", "rate"])
def test_transient_errors_have_bounded_retries(config, failure):
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        if failure == "timeout":
            raise httpx.ReadTimeout("secret", request=request)
        if failure == "connection":
            raise httpx.ConnectError("secret", request=request)
        return httpx.Response(429)

    async def run():
        async with ProviderAdapter(settings(config), transport=httpx.MockTransport(handler)) as p:
            with pytest.raises(ProviderError) as caught:
                await p.query("daily")
            assert "secret" not in str(caught.value)
            assert caught.value.retryable

    asyncio.run(run())
    assert calls == 3


def test_total_timeout_trips_circuit_and_cancellation_releases_lock(config):
    async def handler(request):
        await asyncio.sleep(1)
        return ok()

    async def run():
        async with ProviderAdapter(
            settings(
                config,
                total_timeout_seconds=0.03,
                read_timeout_seconds=0.01,
                connect_timeout_seconds=0.01,
                provider_failure_threshold=1,
            ),
            transport=httpx.MockTransport(handler),
        ) as p:
            task = asyncio.create_task(p.query("daily"))
            await asyncio.sleep(0.005)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not p._lock.locked()
            assert p._failures == 0
            with pytest.raises(ProviderError, match="total_timeout"):
                await p.query("daily")
            with pytest.raises(ProviderError, match="circuit_open"):
                await p.query("daily")

    asyncio.run(run())


def test_stream_size_limit_closes_response(config):
    class Stream(httpx.AsyncByteStream):
        closed = False

        async def __aiter__(self):
            yield b"x" * 65536
            yield b"x" * 65536
            pytest.fail("read continued past response budget")

        async def aclose(self):
            self.closed = True

    stream = Stream()

    async def run():
        transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=stream))
        async with ProviderAdapter(
            settings(config, provider_max_response_bytes=65536), transport=transport
        ) as p:
            with pytest.raises(ProviderError, match="response_too_large"):
                await p.query("daily")
        assert stream.closed

    asyncio.run(run())


def test_probe_denial_empty_and_redaction(config, monkeypatch):
    from app.providers import probe as module

    class FakeAdapter:
        def __init__(self, settings):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def query(self, api, params, fields):
            if api == "rt_k":
                raise ProviderError("permission_denied", http_status=403)
            if api == "dividend":
                return ProviderResult(tuple(fields.split(",")), ())
            return ProviderResult(("ts_code",), ((config["data_source_token"],),))

    monkeypatch.setattr(module, "ProviderAdapter", FakeAdapter)

    class Output:
        text = ""

        @property
        def parent(self):
            return self

        def mkdir(self, **kwargs):
            pass

        def write_text(self, text, **kwargs):
            self.text = text

    output = Output()
    asyncio.run(module.probe(settings(config), output, ["stock_basic", "dividend", "rt_k"]))
    text = output.text
    assert config["data_source_token"] not in text
    records = {r["api"]: r for r in json.loads(text)["records"]}
    assert records["rt_k"]["status"] == "unavailable"
    assert records["dividend"]["observation"] == "success_empty"
    assert records["dividend"]["status"] == "pending"
    assert records["stock_basic"]["observation"] == "filter_mismatch"
