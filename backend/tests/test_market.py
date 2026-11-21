import asyncio
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from app.contracts.common import SHANGHAI, Availability, Provenance
from app.dataops.market import FIELDS, decimal_rows, normalize, validate_response, validate_window
from evals.market_fixture import MarketFixture

START, END = date(2026, 9, 1), date(2026, 9, 28)


def test_original_json_numbers_preserve_decimal_precision():
    content = b'{"data":{"fields":["close"],"items":[[1.234567890123456789]]}}'
    assert decimal_rows(content)[0]["close"] == Decimal("1.234567890123456789")
    with pytest.raises(ValueError):
        decimal_rows(b'{"data":{"fields":["close","close"],"items":[[1,2]]}}')


def rows(api):
    return asyncio.run(
        MarketFixture().query(
            api,
            {"ts_code": "000001.SZ", "start_date": "20260901", "end_date": "20260928"},
            FIELDS[api],
        )
    ).rows


def provenance(api):
    now = datetime(2026, 11, 8, tzinfo=SHANGHAI)
    return Provenance(
        provider="test",
        source_api=api,
        source_ref="fixture",
        raw_sha256="0" * 64,
        request_id="0" * 32,
        data_version="1" * 64,
        mapping_version="step08-v1",
        timing=Availability(fetched_at=now, available_at=now, basis="observed"),
    )


def test_units_null_and_negative_pe():
    price = normalize("daily", rows("daily")[0], provenance("daily"))
    assert price.volume.value == Decimal("250") and price.volume.raw_unit == "lot"
    assert price.amount.value == Decimal("3250") and price.adjustment == "none"
    value = normalize("daily_basic", rows("daily_basic")[0], provenance("daily_basic"))
    assert value.pe.value is None and value.pe.status == "null_value"
    assert value.pe_ttm.value == -3
    assert value.total_market_cap.value == 1000000
    assert value.dividend_yield_ttm.value == Decimal("0.02")


@pytest.mark.parametrize(
    "api,field,value",
    [
        ("daily", "open", "-1"),
        ("daily", "close", "13"),
        ("daily", "low", "11"),
        ("daily", "high", "8"),
        ("daily", "vol", "-1"),
        ("daily", "open", None),
        ("daily", "close", "NaN"),
        ("daily", "close", 1.2),
        ("adj_factor", "adj_factor", "0"),
        ("adj_factor", "adj_factor", None),
        ("daily_basic", "total_mv", "-1"),
    ],
)
def test_invalid_measures_block(api, field, value):
    row = rows(api)[0]
    row[field] = value
    with pytest.raises(ValueError):
        normalize(api, row, provenance(api))


@pytest.mark.parametrize("api", list(FIELDS))
@pytest.mark.parametrize("fault", ["duplicate", "missing", "wrong_code", "out_of_range", "field"])
def test_source_scope_and_completeness(api, fault):
    batch = rows(api)
    expected = {
        START + timedelta(days=i) for i in range(28) if (START + timedelta(days=i)).weekday() < 5
    }
    validate_response(api, batch, "000001.SZ", START, END, expected)
    if fault == "duplicate":
        batch.append(batch[0])
    elif fault == "missing":
        batch.pop()
    elif fault == "wrong_code":
        batch[0]["ts_code"] = "600036.SH"
    elif fault == "out_of_range":
        batch[0]["trade_date"] = "20260831"
    else:
        batch[0].pop("trade_date")
    with pytest.raises(ValueError):
        validate_response(api, batch, "000001.SZ", START, END, expected)


def test_window_is_bounded():
    validate_window("000001.SZ", START, END)
    for code, end in [
        ("000001", END),
        ("000001.SZ", START - timedelta(days=1)),
        ("000001.SZ", START + timedelta(days=46)),
    ]:
        with pytest.raises(ValueError):
            validate_window(code, START, end)
