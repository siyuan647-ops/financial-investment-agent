import hashlib
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from app.contracts.common import SHANGHAI, Availability, DataStatus, Measure, Provenance
from app.contracts.domain import PriceBar, SecurityKey
from app.dataops.technical import calculate_technical_indicators
from app.services.business_tool_dataops import _indicator_window_gap

NOW = datetime(2026, 11, 15, 20, tzinfo=SHANGHAI)
SECURITY = SecurityKey(exchange="SZSE", ts_code="000001.SZ")


def measure(value, unit):
    return Measure(
        value=value,
        raw_value=value,
        unit=unit,
        raw_unit=unit,
        status=DataStatus.OK,
    )


def bar(index):
    close = Decimal(10) + Decimal(index) / Decimal(10)
    version = hashlib.sha256(str(index).encode()).hexdigest()
    return PriceBar(
        security=SECURITY,
        trade_date=date(2026, 8, 18) + timedelta(days=index),
        open=measure(close - Decimal("0.1"), "CNY/share"),
        high=measure(close + Decimal("0.5"), "CNY/share"),
        low=measure(close - Decimal("0.5"), "CNY/share"),
        close=measure(close, "CNY/share"),
        volume=measure(Decimal(1000 + index), "share"),
        amount=measure(Decimal(10000 + index), "CNY"),
        provenance=Provenance(
            provider="test",
            source_api="daily",
            source_ref=f"fixture:{index}",
            raw_sha256="a" * 64,
            request_id="0" * 32,
            data_version=version,
            mapping_version="step08-v1",
            quality="valid",
            timing=Availability(fetched_at=NOW, available_at=NOW, basis="observed"),
        ),
    )


def test_ninety_bars_produce_all_eleven_indicators_for_latest_thirty_days():
    records = calculate_technical_indicators(tuple(bar(index) for index in range(90)))
    assert len(records) == 30 * 11
    assert len({record.trade_date for record in records}) == 30
    assert {record.indicator for record in records} == {
        "close_50_sma",
        "close_10_ema",
        "macd",
        "macds",
        "macdh",
        "rsi",
        "boll",
        "boll_ub",
        "boll_lb",
        "atr",
        "vwma",
    }
    latest = {record.indicator: record for record in records[-11:]}
    assert latest["rsi"].value == Decimal(100)
    assert latest["boll_lb"].value < latest["boll"].value < latest["boll_ub"].value
    assert latest["atr"].value > 0
    assert len(latest["macdh"].input_versions) == 90


def test_technical_migration_is_append_only_and_versioned():
    root = Path(__file__).resolve().parents[1]
    sql = (root / "migrations/035_technical_indicators.up.sql").read_text("utf-8")
    assert "storage.technical_indicator" in sql
    assert "storage.record_version" in sql
    assert "storage.reject_mutation()" in sql


def test_indicator_window_must_end_on_latest_published_price_date():
    days = [date(2026, 10, 1) + timedelta(days=index) for index in range(30)]
    complete = [(day, {"rsi": object()}) for day in days]

    assert (
        _indicator_window_gap(
            complete,
            [SimpleNamespace(trade_date=days[-1] + timedelta(days=1))],
        )
        == "indicator_price_date_mismatch"
    )
    assert _indicator_window_gap(complete, [SimpleNamespace(trade_date=days[-1])]) is None
