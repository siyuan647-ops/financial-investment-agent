from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import anyio
import pytest
from pydantic import ValidationError

from app.contracts.common import SHANGHAI, Availability, Provenance
from app.contracts.fundamental import FundamentalRecord
from app.dataops.fundamental_domains import (
    COMPANY_KEY_METRIC_FIELDS,
    SOURCES,
    derive_single_quarter_cash_flow,
    derive_single_quarter_records,
    fundamental_analysis_period_plan,
    normalize,
    profitability_period_plan,
)
from app.repositories.fundamental import materialize_record_version
from app.services.business_tool_dataops import _select_fundamental_window
from app.workflows import fundamental as fundamental_workflow
from app.workflows.fundamental import latest_completed_quarter

NOW = datetime(2026, 11, 14, 12, tzinfo=SHANGHAI)


def test_daily_fundamental_refresh_populates_every_agent_tool_window(monkeypatch):
    calls = []

    async def current(*args):
        calls.append(("current", args[3], args[4], args[5]))
        return {"kind": "current"}

    async def profitability(*args):
        calls.append(("profitability", args[3], args[4]))
        return {"kind": "profitability"}

    async def analysis(*args):
        calls.append(("analysis", args[3], args[4]))
        return {"kind": "analysis"}

    monkeypatch.setattr(fundamental_workflow, "collect_fundamental_domains", current)
    monkeypatch.setattr(fundamental_workflow, "collect_profitability_window", profitability)
    monkeypatch.setattr(fundamental_workflow, "collect_fundamental_analysis_window", analysis)

    result = anyio.run(
        fundamental_workflow.refresh_fundamental_agent_domains,
        object(),
        object(),
        object(),
        "600519.SH",
        date(2026, 9, 30),
        date(2026, 11, 17),
    )

    assert [item[0] for item in calls] == ["current", "profitability", "analysis"]
    assert result["ts_code"] == "600519.SH"
    assert set(result) == {
        "ts_code",
        "current_domains",
        "profitability_window",
        "analysis_window",
    }


def provenance(api="income"):
    return Provenance(
        provider="tushare_compatible",
        source_api=api,
        source_ref=f"000001.SZ:{'1' * 32}",
        raw_sha256="2" * 64,
        request_id="3" * 32,
        data_version="0" * 64,
        mapping_version="fundamental-v1",
        quality="valid",
        timing=Availability(fetched_at=NOW, available_at=NOW, basis="observed"),
    )


def test_direct_normalization_keeps_dates_and_drops_nulls():
    spec = next(item for item in SOURCES if item.api == "income")
    row = {field: None for field in spec.fields}
    row.update(
        ts_code="000001.SZ",
        ann_date="20250315",
        f_ann_date="20250317",
        end_date="20241231",
        report_type="1",
        comp_type="2",
        total_revenue=Decimal("146695000000"),
        revenue=Decimal("146695000000"),
    )
    record = normalize(spec, row, provenance())
    assert record.domain == "profitability"
    assert record.period_end == date(2024, 12, 31)
    assert record.announced_on == date(2025, 3, 17)
    assert record.data_date == date(2025, 3, 17)
    assert record.values["total_revenue"] == Decimal("146695000000")
    assert "rd_exp" not in record.values


def test_contract_rejects_null_placeholders_and_future_business_date():
    base = dict(
        provenance=provenance(),
        security={"exchange": "SZSE", "ts_code": "000001.SZ"},
        domain="profitability",
        record_type="income_statement",
        source_key="source",
        data_date="2025-03-17",
        period_end="2024-12-31",
        announced_on="2025-03-17",
        values={"revenue": Decimal("1")},
    )
    assert FundamentalRecord.model_validate(base).origin == "direct"
    with pytest.raises(ValidationError):
        FundamentalRecord.model_validate({**base, "values": {"revenue": None}})
    with pytest.raises(ValidationError):
        FundamentalRecord.model_validate({**base, "data_date": "2026-12-31"})


def test_dividend_announcement_may_precede_source_period_end():
    spec = next(item for item in SOURCES if item.api == "dividend")
    row = {field: None for field in spec.fields}
    row.update(
        ts_code="000001.SZ",
        end_date="20261231",
        ann_date="20261020",
        div_proc="预案",
        cash_div=Decimal("0.1"),
        ex_date="20270115",
    )
    record = normalize(spec, row, provenance("dividend"))
    assert record.data_date == date(2026, 10, 20)
    assert record.period_end == date(2026, 12, 31)
    assert record.effective_on == date(2027, 1, 15)


@pytest.mark.parametrize(
    "day,expected",
    [
        (date(2026, 1, 1), date(2025, 12, 31)),
        (date(2026, 3, 31), date(2026, 3, 31)),
        (date(2026, 11, 14), date(2026, 9, 30)),
    ],
)
def test_latest_completed_quarter(day, expected):
    assert latest_completed_quarter(day) == expected


def _profitability_record(api, period, values):
    spec = next(item for item in SOURCES if item.api == api)
    row = {field: None for field in spec.fields}
    row.update(
        ts_code="000001.SZ",
        ann_date=period.replace(year=period.year + (period.month == 12)).strftime("%Y%m%d"),
        end_date=period.strftime("%Y%m%d"),
        update_flag="1",
        **values,
    )
    if api in {"income", "balancesheet", "cashflow"}:
        row.update(report_type="1", comp_type="2")
    return materialize_record_version(normalize(spec, row, provenance(api)))[0]


def test_profitability_period_plan_has_four_single_quarters_and_two_annuals():
    plan = profitability_period_plan(date(2026, 11, 15))
    assert plan.annual_periods == (date(2024, 12, 31), date(2025, 12, 31))
    assert plan.quarter_periods == (
        date(2025, 12, 31),
        date(2026, 3, 31),
        date(2026, 6, 30),
        date(2026, 9, 30),
    )
    assert plan.source_periods == (
        date(2024, 12, 31),
        date(2025, 9, 30),
        date(2025, 12, 31),
        date(2026, 3, 31),
        date(2026, 6, 30),
        date(2026, 9, 30),
    )


def test_fundamental_analysis_period_plan_matches_confirmed_windows():
    plan = fundamental_analysis_period_plan(date(2026, 11, 15))
    assert plan.company_metric_periods == (
        date(2026, 3, 31),
        date(2026, 6, 30),
        date(2026, 9, 30),
    )
    assert plan.balance_sheet_periods == (
        date(2025, 12, 31),
        date(2026, 3, 31),
        date(2026, 6, 30),
        date(2026, 9, 30),
    )
    assert plan.cash_flow_periods == plan.balance_sheet_periods
    assert plan.cash_flow_source_periods == (
        date(2025, 9, 30),
        date(2025, 12, 31),
        date(2026, 3, 31),
        date(2026, 6, 30),
        date(2026, 9, 30),
    )
    assert COMPANY_KEY_METRIC_FIELDS == (
        "eps",
        "dt_eps",
        "diluted2_eps",
        "bps",
        "roe",
        "roe_waa",
        "roe_dt",
        "debt_to_assets",
    )


def _window_record(record_type, period, *, domain="profitability", suffix=""):
    record = _profitability_record("income", date(2024, 12, 31), {"revenue": Decimal("1")})
    return record.model_copy(
        update={
            "domain": domain,
            "record_type": record_type,
            "source_key": f"{record_type}:{period}:{suffix}",
            "period_end": period,
        }
    )


@pytest.mark.parametrize(
    ("domain", "record_type", "count"),
    [
        ("balance_sheet", "balance_sheet_point_in_time", 4),
        ("cash_flow", "cash_flow_statement_single_quarter", 4),
    ],
)
def test_fixed_statement_windows_keep_latest_four_distinct_periods(domain, record_type, count):
    periods = (
        date(2025, 9, 30),
        date(2025, 12, 31),
        date(2026, 3, 31),
        date(2026, 6, 30),
        date(2026, 9, 30),
    )
    records = [_window_record(record_type, period, domain=domain) for period in periods]
    records.append(_window_record(record_type, periods[-1], domain=domain, suffix="duplicate"))

    selected, missing = _select_fundamental_window(domain, records, date(2026, 11, 15))

    assert missing == ()
    assert len(selected) == count
    assert {item.period_end for item in selected} == set(periods[-count:])


def test_profitability_window_keeps_two_annuals_and_four_independent_quarters():
    annual_periods = (date(2023, 12, 31), date(2024, 12, 31), date(2025, 12, 31))
    quarter_periods = (
        date(2025, 9, 30),
        date(2025, 12, 31),
        date(2026, 3, 31),
        date(2026, 6, 30),
        date(2026, 9, 30),
    )
    records = []
    for record_type in ("income_statement_annual", "financial_indicator_annual"):
        records.extend(_window_record(record_type, period) for period in annual_periods)
    for record_type in (
        "income_statement_single_quarter",
        "financial_indicator_single_quarter",
    ):
        records.extend(_window_record(record_type, period) for period in quarter_periods)

    selected, missing = _select_fundamental_window("profitability", records, date(2026, 11, 15))

    assert missing == ()
    by_type = {}
    for item in selected:
        by_type.setdefault(item.record_type, set()).add(item.period_end)
    assert by_type["income_statement_annual"] == set(annual_periods[-2:])
    assert by_type["financial_indicator_annual"] == set(annual_periods[-2:])
    assert by_type["income_statement_single_quarter"] == set(quarter_periods[-4:])
    assert by_type["financial_indicator_single_quarter"] == set(quarter_periods[-4:])


def test_company_profile_window_keeps_latest_three_key_metric_periods():
    records = [
        _window_record("security_profile", date(2026, 9, 30), domain="company_profile"),
        _window_record("company_description", date(2026, 9, 30), domain="company_profile"),
        _window_record("sw_industry", date(2026, 9, 30), domain="company_profile"),
        _window_record("citic_industry", date(2026, 9, 30), domain="company_profile"),
    ]
    for record_type in ("business_segment", "top10_holder", "top10_float_holder"):
        records.append(_window_record(record_type, date(2026, 9, 30), domain="company_profile"))
    metric_periods = (
        date(2025, 12, 31),
        date(2026, 3, 31),
        date(2026, 6, 30),
        date(2026, 9, 30),
    )
    records.extend(
        _window_record("company_key_metrics", period, domain="company_profile")
        for period in metric_periods
    )

    selected, missing = _select_fundamental_window("company_profile", records, date(2026, 11, 15))

    assert missing == ()
    assert {
        item.period_end for item in selected if item.record_type == "company_key_metrics"
    } == set(metric_periods[-3:])


def test_fixed_window_rejects_stale_periods_even_when_record_count_is_sufficient():
    stale = (
        date(2025, 6, 30),
        date(2025, 9, 30),
        date(2025, 12, 31),
        date(2026, 3, 31),
    )
    records = [
        _window_record("balance_sheet_point_in_time", period, domain="balance_sheet")
        for period in stale
    ]

    selected, missing = _select_fundamental_window("balance_sheet", records, date(2026, 11, 15))

    assert missing == ("balance_sheet_point_in_time",)
    assert {item.period_end for item in selected} == {
        date(2025, 12, 31),
        date(2026, 3, 31),
    }


def test_single_quarter_derivation_subtracts_amounts_and_recomputes_ratios():
    previous_income = _profitability_record(
        "income",
        date(2026, 3, 31),
        {
            "total_revenue": Decimal("100"),
            "revenue": Decimal("100"),
            "admin_exp": Decimal("20"),
            "n_income_attr_p": Decimal("30"),
        },
    )
    current_income = _profitability_record(
        "income",
        date(2026, 6, 30),
        {
            "total_revenue": Decimal("240"),
            "revenue": Decimal("240"),
            "admin_exp": Decimal("48"),
            "n_income_attr_p": Decimal("72"),
        },
    )
    previous_indicator = _profitability_record(
        "fina_indicator",
        date(2026, 3, 31),
        {"profit_dedt": Decimal("29"), "netprofit_margin": Decimal("30")},
    )
    current_indicator = _profitability_record(
        "fina_indicator",
        date(2026, 6, 30),
        {"profit_dedt": Decimal("69"), "netprofit_margin": Decimal("30")},
    )
    income, indicator = derive_single_quarter_records(
        date(2026, 6, 30),
        current_income,
        previous_income,
        current_indicator,
        previous_indicator,
    )
    assert income.record_type == "income_statement_single_quarter"
    assert income.values["quarter"] == "2026Q2"
    assert income.values["total_revenue"] == Decimal("140")
    assert income.values["n_income_attr_p"] == Decimal("42")
    assert income.origin == "calculated"
    assert len(income.input_versions) == 2
    assert indicator.values["profit_dedt"] == Decimal("40")
    assert indicator.values["netprofit_margin"] == Decimal("30")
    assert indicator.values["adminexp_of_gr"] == Decimal("20")
    assert len(indicator.input_versions) == 3


def test_single_quarter_cash_flow_subtracts_flows_but_not_cash_balances():
    previous = _profitability_record(
        "cashflow",
        date(2026, 6, 30),
        {
            "n_cashflow_act": Decimal("100"),
            "n_cashflow_inv_act": Decimal("-20"),
            "n_cash_flows_fnc_act": Decimal("10"),
            "c_cash_equ_beg_period": Decimal("900"),
            "c_cash_equ_end_period": Decimal("990"),
        },
    )
    current = _profitability_record(
        "cashflow",
        date(2026, 9, 30),
        {
            "n_cashflow_act": Decimal("160"),
            "n_cashflow_inv_act": Decimal("-35"),
            "n_cash_flows_fnc_act": Decimal("4"),
            "c_cash_equ_beg_period": Decimal("900"),
            "c_cash_equ_end_period": Decimal("1029"),
        },
    )
    result = derive_single_quarter_cash_flow(date(2026, 9, 30), current, previous)
    assert result.record_type == "cash_flow_statement_single_quarter"
    assert result.values["quarter"] == "2026Q3"
    assert result.values["n_cashflow_act"] == Decimal("60")
    assert result.values["n_cashflow_inv_act"] == Decimal("-15")
    assert result.values["n_cash_flows_fnc_act"] == Decimal("-6")
    assert "c_cash_equ_beg_period" not in result.values
    assert "c_cash_equ_end_period" not in result.values
    assert result.origin == "calculated"
    assert len(result.input_versions) == 2


def test_migration_has_five_dated_append_only_tables():
    root = Path(__file__).resolve().parents[1]
    sql = (root / "migrations/032_fundamental_domains.up.sql").read_text("utf-8")
    for table in (
        "company_profile_record",
        "profitability_record",
        "balance_sheet_record",
        "cash_flow_record",
        "fundamental_aux_record",
    ):
        assert table in sql
    for column in ("data_date date NOT NULL", "period_end date", "announced_on date"):
        assert column in sql
    assert "storage.reject_mutation()" in sql
