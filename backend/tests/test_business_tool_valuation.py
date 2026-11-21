"""Persisted company metrics keep valuation and report dates distinct."""

from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

import anyio

from app.contracts.common import SHANGHAI, Availability, Provenance
from app.contracts.fundamental import FundamentalRecord
from app.dataops import fundamental_domains
from app.dataops.reference import security_key
from app.repositories.fundamental import materialize_record_version
from app.services.business_tool_dataops import _select_fundamental_window


def _metric_record():
    now = datetime(2026, 11, 21, 2, tzinfo=SHANGHAI)
    record = FundamentalRecord(
        provenance=Provenance(
            provider="tushare_compatible",
            source_api="fina_indicator",
            source_ref="601628.SH:raw",
            raw_sha256="a" * 64,
            request_id="1" * 32,
            data_version="0" * 64,
            mapping_version="fundamental-analysis-window-v1",
            timing=Availability(fetched_at=now, available_at=now, basis="observed"),
            quality="valid",
        ),
        security=security_key("601628.SH"),
        domain="company_profile",
        record_type="company_key_metrics",
        source_key="base",
        data_date=date(2026, 10, 31),
        period_end=date(2026, 9, 30),
        announced_on=date(2026, 10, 31),
        values={"ts_code": "601628.SH", "bps": Decimal("23.0605")},
    )
    return materialize_record_version(record)[0]


def test_dataops_materializes_pe_ttm_and_static_pb(monkeypatch):
    trade_date = date(2026, 11, 18)
    valuation = SimpleNamespace(
        trade_date=trade_date,
        pe_ttm=SimpleNamespace(value=Decimal("13.2169")),
        provenance=SimpleNamespace(data_version="b" * 64),
    )
    price = SimpleNamespace(
        close=SimpleNamespace(value=Decimal("35.40")),
        provenance=SimpleNamespace(data_version="c" * 64),
    )

    class MarketRows:
        def __init__(self, connection):
            self.connection = connection

        async def valuations(self, *args, **kwargs):
            return [valuation]

        async def prices(self, *args, **kwargs):
            assert args[1:3] == (trade_date, trade_date)
            return [price]

    monkeypatch.setattr(fundamental_domains, "Repository", MarketRows)
    base = _metric_record()
    derived = anyio.run(
        fundamental_domains._published_company_valuation,
        SimpleNamespace(connection=object()),
        "601628.SH",
        date(2026, 11, 21),
        base,
    )

    assert derived.origin == "calculated"
    assert derived.values["pe_ttm"] == Decimal("13.2169")
    assert derived.values["pb"] == Decimal("1.5351")
    assert derived.values["valuation_trade_date"] == "2026-11-18"
    assert derived.values["pb_bps_period_end"] == "2026-09-30"
    assert derived.input_versions == (base.provenance.data_version, "b" * 64, "c" * 64)
    assert derived.period_end == base.period_end
    assert derived.data_date == trade_date


def test_dataops_skips_valuation_without_published_market_data(monkeypatch):
    class MarketRows:
        def __init__(self, connection):
            self.connection = connection

        async def valuations(self, *args, **kwargs):
            return []

    monkeypatch.setattr(fundamental_domains, "Repository", MarketRows)
    result = anyio.run(
        fundamental_domains._published_company_valuation,
        SimpleNamespace(connection=object()),
        "601628.SH",
        date(2026, 11, 21),
        _metric_record(),
    )
    assert result is None


def test_company_profile_reads_latest_persisted_snapshot_only():
    availability = SimpleNamespace(available_at=datetime(2026, 11, 21, 2, tzinfo=SHANGHAI))

    def metric(period, data_date, origin):
        return SimpleNamespace(
            record_type="company_key_metrics",
            period_end=period,
            data_date=data_date,
            origin=origin,
            source_key=f"{period}-{origin}",
            provenance=SimpleNamespace(timing=availability),
        )

    q2 = date(2026, 6, 30)
    q3 = date(2026, 9, 30)
    records = [
        metric(q2, date(2026, 8, 28), "direct"),
        metric(q2, date(2026, 9, 15), "calculated"),
        metric(q3, date(2026, 10, 31), "direct"),
        metric(q3, date(2026, 11, 18), "calculated"),
    ]
    selected, _ = _select_fundamental_window("company_profile", records, date(2026, 11, 21))
    metrics = {
        item.period_end: item for item in selected if item.record_type == "company_key_metrics"
    }
    assert metrics[q2].origin == "direct"
    assert metrics[q3].origin == "calculated"
