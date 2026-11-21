import asyncio
from datetime import date, datetime

import pytest

from app.contracts.common import SHANGHAI, Availability, Provenance
from app.contracts.domain import SecurityAlias
from app.dataops.reference import normalize, required_exchanges, validate_response, validate_window
from app.repositories.storage import identity
from app.services.securities import coverage, resolve_security
from evals.reference_fixture import ReferenceFixture


@pytest.fixture
def provenance():
    now = datetime(2026, 11, 8, tzinfo=SHANGHAI)
    return Provenance(
        provider="test",
        source_api="stock_basic",
        source_ref="fixture",
        raw_sha256="0" * 64,
        request_id="0" * 32,
        data_version="1" * 64,
        timing=Availability(fetched_at=now, available_at=now, basis="observed"),
    )


def security(provenance, **updates):
    row = dict(
        ts_code="000001.SZ",
        exchange="SZSE",
        name="平安银行",
        list_status="L",
        list_date="19910403",
        delist_date=None,
    )
    row.update(updates)
    return normalize("stock_basic", row, provenance)


@pytest.mark.parametrize("status,expected", [("L", "listed"), ("D", "delisted"), ("P", "paused")])
def test_status_mapping(provenance, status, expected):
    assert security(provenance, list_status=status).listing_status == expected


def test_resolver_ambiguity_and_exchange(provenance):
    first = security(provenance)
    second = security(provenance, ts_code="000001.SH", exchange="SSE")
    assert len(resolve_security("平安银行", [first, second])) == 2
    assert len(resolve_security("000001", [first, second])) == 2
    assert resolve_security("000001.sz", [first, second]) == [first]
    assert resolve_security("000001", [first, second], exchange="SSE") == [second]
    assert resolve_security("平安", [first]) == []
    assert resolve_security(" ", [first]) == []


def test_alias_dates_and_current_name_not_backfilled(provenance):
    item = security(provenance)
    alias = SecurityAlias(
        security=item.security,
        name="深发展A",
        effective_from=date(1991, 4, 3),
        effective_to=date(2012, 8, 1),
        provenance=provenance,
    )
    assert resolve_security("深发展A", [item], [alias]) == [item]
    assert resolve_security("深发展A", [item], [alias], on=date(2012, 8, 1)) == [item]
    assert resolve_security("深发展A", [item], [alias], on=date(2012, 8, 2)) == []
    assert resolve_security("平安银行", [item], on=date(2000, 1, 1)) == []
    revision = alias.model_copy(update={"effective_to": date(2012, 8, 2)})
    assert identity(alias)[1] == identity(revision)[1]
    assert identity(alias)[2] != identity(revision)[2]
    with pytest.raises(ValueError):
        SecurityAlias(
            security=item.security,
            name="bad",
            effective_from=date(2012, 8, 2),
            effective_to=date(2012, 8, 1),
            provenance=provenance,
        )


@pytest.mark.parametrize("value", ["false", "True", 2, True, 1.0, None])
def test_invalid_open_flags(provenance, value):
    with pytest.raises(ValueError):
        normalize(
            "trade_cal",
            dict(exchange="SSE", cal_date="20261001", is_open=value, pretrade_date=None),
            provenance,
        )


def test_coverage_holiday_listing_and_unknown(provenance):
    source = ReferenceFixture()
    params = dict(exchange="SZSE", start_date="20260930", end_date="20261004")
    response = asyncio.run(source.query("trade_cal", params, ""))
    calendars = [normalize("trade_cal", row, provenance) for row in response.rows]
    item = security(provenance, list_date="20261001", delist_date="20261003", list_status="D")
    days = coverage([item], calendars, date(2026, 9, 30), date(2026, 10, 4))
    assert [d.state for d in days] == [
        "before_listing",
        "non_trading_day",
        "expected_status_unverified",
        "non_trading_day",
        "after_delisting",
    ]
    assert coverage([item], [], date(2026, 10, 2), date(2026, 10, 2))[0].state == "calendar_missing"
    unknown = security(provenance, list_date=None)
    assert (
        coverage([unknown], calendars, date(2026, 10, 2), date(2026, 10, 2))[0].state
        == "listing_date_unknown"
    )
    paused = security(provenance, list_status="P")
    assert (
        coverage([paused], calendars, date(2026, 10, 2), date(2026, 10, 2))[0].state
        == "expected_status_unverified"
    )


@pytest.mark.parametrize("fault", ["gap", "duplicate", "exchange", "range"])
def test_calendar_response_validation(fault):
    params = dict(exchange="SSE", start_date="20261001", end_date="20261002")
    rows = asyncio.run(ReferenceFixture().query("trade_cal", params, "")).rows
    if fault == "gap":
        rows.pop()
    elif fault == "duplicate":
        rows.append(rows[0])
    elif fault == "exchange":
        rows[0]["exchange"] = "SZSE"
    else:
        rows[0]["cal_date"] = "20260930"
    with pytest.raises(ValueError):
        validate_response("trade_cal", rows, params)


def test_stock_filters_and_projection():
    params = dict(ts_code="000001.SZ", list_status="L")
    rows = asyncio.run(ReferenceFixture().query("stock_basic", params, "")).rows
    validate_response("stock_basic", rows, params)
    for change in ({"ts_code": "600036.SH"}, {"list_status": "D"}):
        with pytest.raises(ValueError):
            validate_response("stock_basic", [{**rows[0], **change}], params)
    with pytest.raises(ValueError):
        validate_response("stock_basic", [{"ts_code": "000001.SZ"}], params)


@pytest.mark.parametrize("codes", [[], ["000001"], ["000001.BJ"], ["000001.SZ"] * 2])
def test_window_codes(codes):
    with pytest.raises(ValueError):
        validate_window(codes, date(2026, 1, 1), date(2026, 1, 2))


def test_reference_calendar_is_scoped_to_requested_exchanges():
    assert required_exchanges(["600519.SH", "600036.SH"]) == ("SSE",)
    assert required_exchanges(["000001.SZ", "600519.SH"]) == ("SSE", "SZSE")
