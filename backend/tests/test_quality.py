import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from app.contracts.common import SHANGHAI, Availability, Provenance
from app.contracts.domain import FinancialPeriod
from app.dataops.financial import FIELDS as FINANCIAL_FIELDS
from app.dataops.financial import normalize as normalize_financial
from app.dataops.market import FIELDS as MARKET_FIELDS
from app.dataops.market import normalize as normalize_market
from app.dataops.quality import (
    ENGINE,
    PROCESSING_VERSION,
    RULES,
    CompletenessOutcome,
    ExceptionReason,
    Severity,
)
from app.repositories.quality import QualityRepository, node_id
from evals.financial_fixture import FinancialFixture
from evals.market_fixture import MarketFixture


def provenance(api, mapping_version):
    now = datetime(2026, 11, 9, tzinfo=SHANGHAI)
    return Provenance(
        provider="test",
        source_api=api,
        source_ref="fixture",
        raw_sha256="0" * 64,
        request_id="0" * 32,
        data_version="1" * 64,
        mapping_version=mapping_version,
        quality="valid",
        timing=Availability(fetched_at=now, available_at=now, basis="observed"),
    )


def test_rule_catalog_covers_existing_flows_and_has_fixed_severity():
    assert {
        "reference_response_invalid",
        "market_response_invalid",
        "financial_response_invalid",
        "dividend_response_invalid",
        "industry_catalog_invalid",
        "basic_industry_invalid",
        "industry_membership_invalid",
    } <= RULES.keys()
    assert (
        ENGINE.issue(
            "market_response_invalid",
            subject_kind="daily",
            subject_key="000001.SZ",
            reason_code="invalid_ohlc_relationship",
        ).severity
        is Severity.BLOCKING
    )
    with pytest.raises(ValueError, match="unknown_quality_rule"):
        ENGINE.issue(
            "invented_rule",
            subject_kind="daily",
            subject_key="000001.SZ",
            reason_code="invalid",
        )


def test_missing_duplicate_optional_and_not_applicable_are_distinct():
    rows = [
        {"code": "000001.SZ", "period": "2024-12-31", "required": "1", "optional": None},
        {"code": "000001.SZ", "period": "2024-12-31", "required": None, "optional": None},
        {"code": "600000.SH", "period": "2024-12-31", "required": None, "optional": None},
    ]
    issues = ENGINE.rows(
        rows,
        subject_kind="financial",
        key_fields=("code", "period"),
        required_fields=("required",),
        optional_fields=("optional",),
        not_applicable={"600000.SH|2024-12-31": {"required"}},
    )
    assert [item.rule_id for item in issues] == [
        "duplicate_record",
        "required_field_missing",
        "field_not_applicable",
    ]
    assert all(item.detail.get("field") != "optional" for item in issues)
    assert issues[-1].severity is Severity.NOT_APPLICABLE


def test_bad_price_unit_and_period_are_identified():
    market_row = asyncio.run(
        MarketFixture().query(
            "daily",
            {"ts_code": "000001.SZ", "start_date": "20260901", "end_date": "20260928"},
            MARKET_FIELDS["daily"],
        )
    ).rows[0]
    bar = normalize_market("daily", market_row, provenance("daily", "step08-v1"))
    bad_high = bar.high.model_copy(update={"value": Decimal("8"), "raw_value": Decimal("8")})
    bad_bar = bar.model_copy(update={"high": bad_high})
    assert ENGINE.price_bar(bad_bar)[0].reason_code == "invalid_ohlc_relationship"
    assert (
        ENGINE.unit(
            bar.amount,
            "share",
            subject_kind="price_bar",
            subject_key="000001.SZ:2026-09-01",
            field_name="amount",
        )[0].rule_id
        == "unit_mismatch"
    )

    financial_row = asyncio.run(
        FinancialFixture().query(
            "income",
            {"ts_code": "000001.SZ", "period": "20241231", "report_type": "1"},
            FINANCIAL_FIELDS["income"],
        )
    ).rows[0]
    fact = normalize_financial("income", financial_row, provenance("income", "step09-v1"))[0]
    invalid_period = FinancialPeriod.model_construct(
        start=date(2024, 2, 1), end=date(2024, 12, 30), basis="annual"
    )
    assert ENGINE.period(fact.model_copy(update={"period": invalid_period}))[0].rule_id == (
        "invalid_period"
    )


def test_reconciliation_tolerance_and_legal_nulls():
    assert not ENGINE.reconcile(
        subject_key="000001.SZ:2024-12-31",
        left=Decimal("100.00"),
        right=Decimal("100.009"),
        relation="assets=liabilities+equity",
    )
    failed = ENGINE.reconcile(
        subject_key="000001.SZ:2024-12-31",
        left=Decimal("100"),
        right=Decimal("99"),
        relation="assets=liabilities+equity",
    )
    assert failed[0].reason_code == "outside_rounding_tolerance"
    assert not ENGINE.reconcile(
        subject_key="bank:cashflow",
        left=None,
        right=Decimal("1"),
        relation="industry_optional_relation",
    )
    not_applicable = ENGINE.reconcile(
        subject_key="bank:cashflow",
        left=None,
        right=None,
        relation="industry_optional_relation",
        applicable=False,
    )
    assert not_applicable[0].severity is Severity.NOT_APPLICABLE


def test_completeness_uses_expected_set_and_justified_exceptions():
    report = ENGINE.completeness(
        "daily",
        expected=("2026-10-01", "2026-10-08", "2026-10-09", "2026-10-10"),
        observed=("2026-10-08", "2026-10-11"),
        exceptions={
            "2026-10-01": ExceptionReason.NON_TRADING_DAY,
            "2026-10-10": ExceptionReason.SUSPENDED,
        },
    )
    assert report.expected_count == 4
    assert report.eligible_count == 2
    assert report.received_count == 1
    assert report.exception_count == 2
    assert report.missing_count == 1
    assert report.unexpected_count == 1
    assert report.completeness == Decimal("0.5")
    outcomes = {item.subject_key: item.outcome for item in report.items}
    assert outcomes["2026-10-01"] is CompletenessOutcome.EXCEPTION
    assert outcomes["2026-10-09"] is CompletenessOutcome.MISSING
    with pytest.raises(ValueError, match="exception_outside_expected_set"):
        ENGINE.completeness("daily", ("a",), (), {"b": "non_trading_day"})


def test_freshness_and_summary_do_not_block_warnings_or_not_applicable():
    due = datetime(2026, 11, 9, 18, tzinfo=SHANGHAI)
    warning = ENGINE.freshness(
        due + timedelta(hours=2),
        due,
        subject_kind="daily",
        subject_key="000001.SZ:2026-11-09",
        grace=timedelta(minutes=30),
    )
    not_applicable = ENGINE.reconcile(
        subject_key="bank:cashflow",
        left=None,
        right=None,
        relation="cashflow_ratio",
        applicable=False,
    )
    summary = ENGINE.summarize((*warning, *not_applicable))
    assert summary.publishable
    assert summary.warnings == 1 and summary.not_applicable == 1


class FakeConnection:
    def __init__(self):
        self.execute = AsyncMock()

    @asynccontextmanager
    async def transaction(self):
        yield


def test_quarantine_is_append_only_and_records_processing_version():
    connection = FakeConnection()
    repository = QualityRepository(connection)
    issue = ENGINE.issue(
        "duplicate_record",
        subject_kind="price_bar",
        subject_key="000001.SZ:2026-09-01",
        reason_code="duplicate_natural_key",
        original_value={"close": Decimal("10.01")},
    )
    quarantine_id = asyncio.run(repository.quarantine(UUID(int=1), issue))
    assert isinstance(quarantine_id, UUID)
    assert connection.execute.await_count == 2
    first = connection.execute.await_args_list[0]
    assert "quality_quarantine" in first.args[0]
    assert PROCESSING_VERSION in first.args[1]
    event_id = asyncio.run(
        repository.quarantine_event(
            quarantine_id,
            "retry_requested",
            UUID(int=2),
            detail={"repair": "deduplicate"},
        )
    )
    assert isinstance(event_id, UUID)
    assert "quality_quarantine_event" in connection.execute.await_args.args[0]


def test_lineage_node_identity_and_relation_validation():
    assert node_id("standard", "a" * 64) == node_id("standard", "a" * 64)
    assert node_id("raw", "a" * 64) != node_id("standard", "a" * 64)
    with pytest.raises(ValueError, match="invalid_lineage_node"):
        node_id("unknown", "key")
    repository = QualityRepository(FakeConnection())
    with pytest.raises(ValueError, match="invalid_lineage_relation"):
        asyncio.run(repository.link("report", "r1", "raw", "x1", UUID(int=1)))
