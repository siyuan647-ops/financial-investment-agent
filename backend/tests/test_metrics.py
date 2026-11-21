from datetime import date, datetime
from decimal import Decimal

from app.contracts.common import SHANGHAI, Availability, DataStatus, Measure, Provenance
from app.contracts.domain import (
    AdjustmentFactor,
    FinancialFact,
    FinancialPeriod,
    PriceBar,
    SecurityKey,
)
from app.services.metrics import (
    FORMULAS,
    adjusted_prices,
    cumulative_to_quarter,
    financial_input,
    financial_ratio,
    price_input,
    recalculate_valuation,
    traced_measure,
    trailing_twelve_months,
    year_over_year,
)

NOW = datetime(2026, 11, 9, tzinfo=SHANGHAI)
SECURITY = SecurityKey(exchange="SZSE", ts_code="000001.SZ")


def provenance(version, api="income"):
    return Provenance(
        provider="test",
        source_api=api,
        source_ref="fixture",
        raw_sha256="a" * 64,
        request_id="0" * 32,
        data_version=version * 64,
        mapping_version="step09-v1" if api != "daily" else "step08-v1",
        quality="valid",
        timing=Availability(fetched_at=NOW, available_at=NOW, basis="observed"),
    )


def measure(value, unit="CNY", status=DataStatus.OK, reason=None):
    return Measure(
        value=value,
        raw_value=value,
        unit=unit,
        raw_unit=unit,
        status=status,
        reason=reason,
    )


def period(end, basis):
    if basis == "instant":
        start = None
    elif basis == "quarter":
        start = date(end.year, end.month - 2, 1)
    elif basis in {"annual", "ytd"}:
        start = date(end.year, 1, 1)
    else:
        month = end.month % 12 + 1
        start = date(end.year if month == 1 else end.year - 1, month, 1)
    return FinancialPeriod(start=start, end=end, basis=basis)


def fact(metric, value, end, basis, version, *, company_type="industrial"):
    statement = {
        "total_assets": "balancesheet",
        "total_liabilities": "balancesheet",
        "operating_cash_flow": "cashflow",
        "investing_cash_flow": "cashflow",
        "financing_cash_flow": "cashflow",
    }.get(metric, "income")
    report_type = "2" if basis == "quarter" else "1"
    return FinancialFact(
        security=SECURITY,
        metric=metric,
        statement=statement,
        period=period(end, basis),
        scope="consolidated",
        company_type=company_type,
        report_type=report_type,
        update_flag="0",
        revision="reported",
        amount=measure(value),
        provenance=provenance(version, statement),
    )


def price_bar(day, close, version):
    return PriceBar(
        security=SECURITY,
        trade_date=day,
        open=measure(close, "CNY/share"),
        high=measure(close + 1, "CNY/share"),
        low=measure(close - 1, "CNY/share"),
        close=measure(close, "CNY/share"),
        volume=measure(100, "share"),
        amount=measure(1000, "CNY"),
        provenance=provenance(version, "daily"),
    )


def adjustment_factor(day, value, version):
    return AdjustmentFactor(
        security=SECURITY,
        trade_date=day,
        factor=measure(value, "factor"),
        provenance=provenance(version, "adj_factor").model_copy(
            update={"mapping_version": "step08-v1"}
        ),
    )


def test_cumulative_to_quarter_q1_q4_and_missing_period_status():
    q1 = fact("revenue", Decimal("10"), date(2024, 3, 31), "ytd", "1")
    assert cumulative_to_quarter(q1).measure.value == 10

    q3 = fact("revenue", Decimal("70"), date(2024, 9, 30), "ytd", "2")
    annual = fact("revenue", Decimal("100"), date(2024, 12, 31), "annual", "3")
    q4 = cumulative_to_quarter(annual, q3)
    assert q4.measure.value == 30
    assert q4.period.basis == "quarter"
    assert q4.input_versions == ("2" * 64, "3" * 64)
    assert q4.evidence_id.startswith("ev_")

    missing = cumulative_to_quarter(annual)
    assert missing.measure.status == "no_data"
    assert missing.measure.reason == "missing_previous_cumulative_period"


def test_ttm_cross_year_and_incomplete_window():
    quarters = [
        fact("revenue", Decimal("10"), date(2023, 12, 31), "quarter", "1"),
        fact("revenue", Decimal("20"), date(2024, 3, 31), "quarter", "2"),
        fact("revenue", Decimal("30"), date(2024, 6, 30), "quarter", "3"),
        fact("revenue", Decimal("40"), date(2024, 9, 30), "quarter", "4"),
    ]
    result = trailing_twelve_months(quarters)
    assert result.measure.value == 100
    assert result.period == period(date(2024, 9, 30), "ttm")
    assert result.metric == "revenue_ttm"

    incomplete = trailing_twelve_months(quarters[1:])
    assert incomplete.measure.status == "no_data"
    assert incomplete.measure.reason == "four_quarters_required"


def test_yoy_handles_zero_negative_and_revised_input_lineage():
    prior = fact("net_profit_parent", Decimal("100"), date(2023, 12, 31), "annual", "1")
    current = fact("net_profit_parent", Decimal("120"), date(2024, 12, 31), "annual", "2")
    result = year_over_year(current, prior)
    assert result.measure.value == Decimal("0.2")
    assert result.input_versions == ("1" * 64, "2" * 64)

    zero = fact("net_profit_parent", Decimal(0), date(2023, 12, 31), "annual", "3")
    assert year_over_year(current, zero).measure.reason == "division_by_zero"
    negative = fact("net_profit_parent", Decimal(-1), date(2023, 12, 31), "annual", "4")
    assert year_over_year(current, negative).measure.reason == "negative_comparison_base"

    revised = fact("net_profit_parent", Decimal("130"), date(2024, 12, 31), "annual", "5")
    revised_result = year_over_year(revised, prior)
    assert revised_result.measure.value == Decimal("0.3")
    assert revised_result.evidence_id != result.evidence_id
    assert revised_result.input_versions == ("1" * 64, "5" * 64)


def test_industry_aware_margin_leverage_and_cash_flow_indicators():
    end = date(2024, 12, 31)
    profit = fact("net_profit_parent", Decimal("10"), end, "annual", "1")
    revenue = fact("revenue", Decimal("100"), end, "annual", "2")
    assert financial_ratio("net_profit_margin", profit, revenue).measure.value == Decimal("0.1")

    liabilities = fact("total_liabilities", Decimal("40"), end, "instant", "3")
    assets = fact("total_assets", Decimal("100"), end, "instant", "4")
    assert financial_ratio("liability_ratio", liabilities, assets).measure.value == Decimal("0.4")

    cash = fact("operating_cash_flow", Decimal("15"), end, "annual", "5")
    assert financial_ratio("operating_cash_flow_margin", cash, revenue).measure.value == Decimal(
        "0.15"
    )
    assert financial_ratio("operating_cash_flow_to_profit", cash, profit).measure.value == Decimal(
        "1.5"
    )

    bank_cash = fact("operating_cash_flow", Decimal("15"), end, "annual", "6", company_type="bank")
    bank_revenue = fact("total_revenue", Decimal("100"), end, "annual", "7", company_type="bank")
    bank_profit = fact("net_profit_parent", Decimal("20"), end, "annual", "8", company_type="bank")
    assert financial_ratio("net_profit_margin", bank_profit, bank_revenue).measure.value == Decimal(
        "0.2"
    )
    bank_result = financial_ratio("operating_cash_flow_margin", bank_cash, bank_revenue)
    assert bank_result.measure.status == "not_applicable"
    assert bank_result.measure.reason == "indicator_not_applicable_to_industry"


def test_repeating_ratio_is_bounded_to_the_decimal_contract():
    end = date(2024, 12, 31)
    profit = fact("net_profit_parent", Decimal("1"), end, "annual", "1")
    revenue = fact("revenue", Decimal("3"), end, "annual", "2")

    result = financial_ratio("net_profit_margin", profit, revenue)

    assert result.measure.value == Decimal("0." + "3" * 38)
    assert result.measure.raw_value == result.measure.value


def test_negative_profit_and_zero_denominator_are_explainable():
    end = date(2024, 12, 31)
    cash = fact("operating_cash_flow", Decimal("15"), end, "annual", "1")
    loss = fact("net_profit_parent", Decimal("-10"), end, "annual", "2")
    result = financial_ratio("operating_cash_flow_to_profit", cash, loss)
    assert result.measure.status == "not_applicable"
    assert result.measure.reason == "negative_earnings"

    zero_revenue = fact("revenue", Decimal(0), end, "annual", "3")
    margin = financial_ratio("net_profit_margin", loss, zero_revenue)
    assert margin.measure.status == "quality_error"
    assert margin.measure.reason == "division_by_zero"

    quarter_revenue = fact("revenue", Decimal("100"), end, "quarter", "4")
    period_conflict = financial_ratio("net_profit_margin", loss, quarter_revenue)
    assert period_conflict.measure.status == "quality_error"
    assert period_conflict.measure.reason == "period_basis_conflict"


def test_recalculated_valuation_records_price_shares_denominator_and_lineage():
    day = date(2024, 12, 31)
    bar = price_bar(day, Decimal("10"), "1")
    shares = traced_measure(
        "total_shares",
        day,
        measure(Decimal("100"), "share"),
        "2" * 64,
        "share_capital:000001.SZ:2024-12-31",
    )
    earnings_fact = fact("net_profit_parent", Decimal("50"), day, "annual", "3")
    result = recalculate_valuation("pe", price_input(bar), shares, financial_input(earnings_fact))
    assert result.market_value.value == 1000
    assert result.multiple.value == 20
    assert result.price_date == day and result.financial_period_end == day
    assert result.input_versions == ("1" * 64, "2" * 64, "3" * 64)
    assert len(result.input_evidence_ids) == 3

    loss = fact("net_profit_parent", Decimal("-1"), day, "annual", "4")
    loss_result = recalculate_valuation("pe", price_input(bar), shares, financial_input(loss))
    assert loss_result.multiple.status == "not_applicable"
    assert loss_result.multiple.reason == "negative_earnings"

    stale_shares = shares.model_copy(update={"as_of_date": date(2024, 12, 30)})
    mismatch = recalculate_valuation(
        "pe", price_input(bar), stale_shares, financial_input(earnings_fact)
    )
    assert mismatch.multiple.status == "quality_error"
    assert mismatch.multiple.reason == "share_date_conflict"


def test_pb_recalculation_uses_explicit_book_equity_input():
    day = date(2024, 12, 31)
    bar = price_bar(day, Decimal("10"), "1")
    shares = traced_measure(
        "total_shares",
        day,
        measure(Decimal("100"), "share"),
        "2" * 64,
        "share_capital:000001.SZ:2024-12-31",
    )
    equity = traced_measure(
        "book_equity",
        day,
        measure(Decimal("250")),
        "3" * 64,
        "book_equity:000001.SZ:2024-12-31",
    )
    assert recalculate_valuation("pb", price_input(bar), shares, equity).multiple.value == 4


def test_unadjusted_front_and_back_adjusted_prices_preserve_raw_records():
    first_day, last_day = date(2024, 1, 2), date(2024, 1, 3)
    bars = [
        price_bar(first_day, Decimal("10"), "1"),
        price_bar(last_day, Decimal("20"), "2"),
    ]
    factors = [
        adjustment_factor(first_day, Decimal("1"), "3"),
        adjustment_factor(last_day, Decimal("2"), "4"),
    ]
    raw = adjusted_prices(bars, adjustment="none")
    front = adjusted_prices(bars, factors, adjustment="front")
    back = adjusted_prices(bars, factors, adjustment="back")

    assert raw.base_date is None and [point.close for point in raw.points] == [10, 20]
    assert front.base_date == last_day and [point.close for point in front.points] == [5, 20]
    assert back.base_date == first_day and [point.close for point in back.points] == [10, 40]
    assert [bar.close.value for bar in bars] == [10, 20]
    assert all(len(point.input_versions) == 2 for point in front.points)


def test_adjustment_missing_or_invalid_factor_returns_explainable_status():
    first_day, last_day = date(2024, 1, 2), date(2024, 1, 3)
    bars = [
        price_bar(first_day, Decimal("10"), "1"),
        price_bar(last_day, Decimal("20"), "2"),
    ]
    missing = adjusted_prices(
        bars, [adjustment_factor(first_day, Decimal("1"), "3")], adjustment="front"
    )
    assert missing.status == "no_data" and missing.reason == "missing_adjustment_factor"

    invalid = adjusted_prices(
        bars,
        [
            adjustment_factor(first_day, Decimal("1"), "3"),
            adjustment_factor(last_day, Decimal("0"), "4"),
        ],
        adjustment="front",
    )
    assert invalid.status == "quality_error" and invalid.reason == "invalid_adjustment_factor"


def test_repeating_adjustment_is_bounded_to_the_decimal_contract():
    first_day, last_day = date(2024, 1, 2), date(2024, 1, 3)
    bars = [
        price_bar(first_day, Decimal("10"), "1"),
        price_bar(last_day, Decimal("20"), "2"),
    ]
    factors = [
        adjustment_factor(first_day, Decimal("1"), "3"),
        adjustment_factor(last_day, Decimal("3"), "4"),
    ]

    front = adjusted_prices(bars, factors, adjustment="front")

    assert front.points[0].close == Decimal("3." + "3" * 37)
    assert len(front.points[0].close.as_tuple().digits) == 38


def test_formula_dictionary_is_versioned_and_complete():
    assert {
        "cumulative_to_quarter_v1",
        "ttm_sum_four_quarters_v1",
        "yoy_growth_v1",
        "net_profit_margin_v1",
        "liability_ratio_v1",
        "operating_cash_flow_margin_v1",
        "operating_cash_flow_to_profit_v1",
        "pe_recalculated_v1",
        "pb_recalculated_v1",
        "identity_price_v1",
        "adjusted_price_v1",
    } <= FORMULAS.keys()
