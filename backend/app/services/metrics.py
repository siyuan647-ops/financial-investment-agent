"""Deterministic Step11 financial metrics, valuation and price adjustment.

The service is intentionally side-effect free.  Callers must first select the
appropriate as-of revisions; every result then carries the exact input versions
and evidence identifiers used for the calculation.
"""

from datetime import date
from decimal import Decimal, localcontext
from typing import Annotated, Literal

from pydantic import Field, model_validator

from app.contracts.common import CalendarDate, Contract, DataStatus, Digest, Measure, Number, Text
from app.contracts.domain import (
    FinancialFact,
    FinancialPeriod,
    PriceBar,
    Scope,
    SecurityKey,
)
from app.contracts.mapping import evidence_id

EvidenceId = Annotated[str, Field(pattern=r"^ev_[0-9a-f]{64}$")]
CompanyType = Literal["industrial", "bank", "insurance", "securities", "other_finance", "unknown"]
Indicator = Literal[
    "net_profit_margin",
    "liability_ratio",
    "operating_cash_flow_margin",
    "operating_cash_flow_to_profit",
]
DERIVED_DECIMAL_PRECISION = 38


class FormulaDefinition(Contract):
    formula_id: Text
    expression: Text
    output_unit: Literal["input", "ratio", "multiple", "CNY/share"]
    applicable_company_types: tuple[CompanyType, ...]
    notes: Text


FORMULAS = {
    "cumulative_to_quarter_v1": FormulaDefinition(
        formula_id="cumulative_to_quarter_v1",
        expression="current_cumulative - previous_cumulative; Q1 = current_cumulative",
        output_unit="input",
        applicable_company_types=(
            "industrial",
            "bank",
            "insurance",
            "securities",
            "other_finance",
        ),
        notes="Only flow facts are eligible; annual Q4 subtracts Q3 YTD.",
    ),
    "ttm_sum_four_quarters_v1": FormulaDefinition(
        formula_id="ttm_sum_four_quarters_v1",
        expression="q0 + q1 + q2 + q3",
        output_unit="input",
        applicable_company_types=(
            "industrial",
            "bank",
            "insurance",
            "securities",
            "other_finance",
        ),
        notes="Requires four consecutive single-quarter flow values.",
    ),
    "yoy_growth_v1": FormulaDefinition(
        formula_id="yoy_growth_v1",
        expression="current / prior_comparable - 1",
        output_unit="ratio",
        applicable_company_types=(
            "industrial",
            "bank",
            "insurance",
            "securities",
            "other_finance",
        ),
        notes="A zero or negative comparison base returns an explainable non-value status.",
    ),
    "net_profit_margin_v1": FormulaDefinition(
        formula_id="net_profit_margin_v1",
        expression="net_profit / revenue_basis",
        output_unit="ratio",
        applicable_company_types=(
            "industrial",
            "bank",
            "insurance",
            "securities",
            "other_finance",
        ),
        notes="Industrial companies use revenue; financial companies use total_revenue.",
    ),
    "liability_ratio_v1": FormulaDefinition(
        formula_id="liability_ratio_v1",
        expression="total_liabilities / total_assets",
        output_unit="ratio",
        applicable_company_types=(
            "industrial",
            "bank",
            "insurance",
            "securities",
            "other_finance",
        ),
        notes="The ratio is deterministic; interpretation remains industry-specific.",
    ),
    "operating_cash_flow_margin_v1": FormulaDefinition(
        formula_id="operating_cash_flow_margin_v1",
        expression="operating_cash_flow / revenue",
        output_unit="ratio",
        applicable_company_types=("industrial",),
        notes="Not applied to financial companies because their cash-flow semantics differ.",
    ),
    "operating_cash_flow_to_profit_v1": FormulaDefinition(
        formula_id="operating_cash_flow_to_profit_v1",
        expression="operating_cash_flow / net_profit",
        output_unit="ratio",
        applicable_company_types=("industrial",),
        notes="Non-positive profit returns an explainable non-value status.",
    ),
    "pe_recalculated_v1": FormulaDefinition(
        formula_id="pe_recalculated_v1",
        expression="price * shares / earnings",
        output_unit="multiple",
        applicable_company_types=(
            "industrial",
            "bank",
            "insurance",
            "securities",
            "other_finance",
        ),
        notes="Explicit recalculation; it is never asserted equal to provider-reported PE.",
    ),
    "pb_recalculated_v1": FormulaDefinition(
        formula_id="pb_recalculated_v1",
        expression="price * shares / book_equity",
        output_unit="multiple",
        applicable_company_types=(
            "industrial",
            "bank",
            "insurance",
            "securities",
            "other_finance",
        ),
        notes="Explicit recalculation; it is never asserted equal to provider-reported PB.",
    ),
    "adjusted_price_v1": FormulaDefinition(
        formula_id="adjusted_price_v1",
        expression="raw_price * factor_on_date / factor_on_base_date",
        output_unit="CNY/share",
        applicable_company_types=(
            "industrial",
            "bank",
            "insurance",
            "securities",
            "other_finance",
            "unknown",
        ),
        notes="Front uses the latest base date; back uses the earliest base date.",
    ),
    "identity_price_v1": FormulaDefinition(
        formula_id="identity_price_v1",
        expression="raw_price",
        output_unit="CNY/share",
        applicable_company_types=(
            "industrial",
            "bank",
            "insurance",
            "securities",
            "other_finance",
            "unknown",
        ),
        notes="Unadjusted prices are returned unchanged and have no adjustment base date.",
    ),
}


class DerivedMetric(Contract):
    security: SecurityKey
    metric: Text
    period: FinancialPeriod
    scope: Scope
    company_type: CompanyType
    measure: Measure
    formula_id: Text
    formula_expression: Text
    input_versions: Annotated[tuple[Digest, ...], Field(min_length=1)]
    input_evidence_ids: Annotated[tuple[EvidenceId, ...], Field(min_length=1)]
    evidence_id: EvidenceId
    calculation_version: Literal["step11-v1"] = "step11-v1"

    @model_validator(mode="after")
    def lineage_identity(self):
        expected = evidence_id(
            self.input_versions,
            _derived_locator(self.security, self.metric, self.period.end),
            formula=self.formula_id,
            inputs=self.input_evidence_ids,
        )
        if self.evidence_id != expected:
            raise ValueError("derived_evidence_identity_mismatch")
        return self


class TracedMeasure(Contract):
    label: Text
    as_of_date: CalendarDate
    measure: Measure
    data_version: Digest
    evidence_id: EvidenceId


class ValuationCalculation(Contract):
    metric: Literal["pe", "pb"]
    price_date: CalendarDate
    financial_period_end: CalendarDate
    price: TracedMeasure
    shares: TracedMeasure
    financial_denominator: TracedMeasure
    market_value: Measure
    multiple: Measure
    formula_id: Text
    formula_expression: Text
    input_versions: Annotated[tuple[Digest, ...], Field(min_length=3, max_length=3)]
    input_evidence_ids: Annotated[tuple[EvidenceId, ...], Field(min_length=3, max_length=3)]
    evidence_id: EvidenceId
    calculation_version: Literal["step11-v1"] = "step11-v1"

    @model_validator(mode="after")
    def complete_inputs(self):
        inputs = (self.price, self.shares, self.financial_denominator)
        if self.input_versions != tuple(item.data_version for item in inputs):
            raise ValueError("valuation_input_versions_mismatch")
        if self.input_evidence_ids != tuple(item.evidence_id for item in inputs):
            raise ValueError("valuation_input_evidence_mismatch")
        expected = evidence_id(
            self.input_versions,
            f"valuation:{self.metric}:{self.price_date}:{self.financial_period_end}",
            formula=self.formula_id,
            inputs=self.input_evidence_ids,
        )
        if self.evidence_id != expected:
            raise ValueError("valuation_evidence_identity_mismatch")
        return self


class AdjustedPricePoint(Contract):
    trade_date: CalendarDate
    open: Number
    high: Number
    low: Number
    close: Number
    factor: Number | None
    input_versions: Annotated[tuple[Digest, ...], Field(min_length=1)]
    input_evidence_ids: Annotated[tuple[EvidenceId, ...], Field(min_length=1)]
    evidence_id: EvidenceId


class AdjustedPriceSeries(Contract):
    security: SecurityKey
    adjustment: Literal["none", "front", "back"]
    base_date: CalendarDate | None
    unit: Literal["CNY/share"] = "CNY/share"
    status: DataStatus
    reason: Text | None = None
    formula_id: Text
    formula_expression: Text
    points: tuple[AdjustedPricePoint, ...]
    calculation_version: Literal["step11-v1"] = "step11-v1"

    @model_validator(mode="after")
    def status_consistency(self):
        if self.status == DataStatus.OK and not self.points:
            raise ValueError("ok_price_series_requires_points")
        if self.status != DataStatus.OK and (self.reason is None or self.points):
            raise ValueError("failed_price_series_requires_reason_and_no_points")
        if self.adjustment == "none" and self.base_date is not None:
            raise ValueError("unadjusted_series_has_no_base")
        if self.adjustment != "none" and self.status == DataStatus.OK and self.base_date is None:
            raise ValueError("adjusted_series_requires_base")
        return self


Operand = FinancialFact | DerivedMetric


def _derived_locator(security, metric, period_end):
    return f"derived_metric:{security.ts_code}:{metric}:{period_end.isoformat()}"


def _source_evidence(version, locator):
    return evidence_id((version,), locator)


def _fact_evidence(fact):
    return _source_evidence(
        fact.provenance.data_version,
        f"financial_fact:{fact.security.ts_code}:{fact.metric}:{fact.period.end}:{fact.scope}",
    )


def _operand_parts(item: Operand):
    if isinstance(item, FinancialFact):
        return (
            item.security,
            item.metric,
            item.period,
            item.scope,
            item.company_type,
            item.amount,
            (item.provenance.data_version,),
            (_fact_evidence(item),),
        )
    return (
        item.security,
        item.metric,
        item.period,
        item.scope,
        item.company_type,
        item.measure,
        item.input_versions,
        (item.evidence_id,),
    )


def _lineage(items):
    parts = [_operand_parts(item) for item in items]
    return (
        tuple(sorted({version for part in parts for version in part[6]})),
        tuple(sorted({item for part in parts for item in part[7]})),
    )


def _bounded_decimal(value):
    if not isinstance(value, Decimal):
        return value
    with localcontext() as context:
        # Derived division may repeat indefinitely. Keep the result inside the
        # shared Number contract without converting through binary floats.
        context.prec = DERIVED_DECIMAL_PRECISION
        return +value


def _measure(value, unit, status=DataStatus.OK, reason=None):
    value = _bounded_decimal(value)
    return Measure(
        value=value,
        raw_value=value,
        unit=unit,
        raw_unit=unit,
        status=status,
        reason=reason,
    )


def _missing(unit, status, reason):
    return Measure(
        value=None,
        raw_value=None,
        unit=unit,
        raw_unit=unit,
        status=status,
        reason=reason,
    )


def _result(source, metric, period, formula_id, measure, inputs):
    security, _, _, scope, company_type, _, _, _ = _operand_parts(source)
    versions, evidence = _lineage(inputs)
    locator = _derived_locator(security, metric, period.end)
    return DerivedMetric(
        security=security,
        metric=metric,
        period=period,
        scope=scope,
        company_type=company_type,
        measure=measure,
        formula_id=formula_id,
        formula_expression=FORMULAS[formula_id].expression,
        input_versions=versions,
        input_evidence_ids=evidence,
        evidence_id=evidence_id(versions, locator, formula=formula_id, inputs=evidence),
    )


def _same_context(items, *, include_metric=True):
    parts = [_operand_parts(item) for item in items]
    positions = (0, 1, 3, 4) if include_metric else (0, 3, 4)
    return len({tuple(part[position] for position in positions) for part in parts}) == 1


def _input_failure(items, unit):
    for item in items:
        measure = _operand_parts(item)[5]
        if measure.value is None:
            return _missing(unit, DataStatus.NO_DATA, f"input_{measure.status.value}")
    return None


def _previous_quarter_end(value):
    if value.month == 3:
        return date(value.year - 1, 12, 31)
    month_day = {6: (3, 31), 9: (6, 30), 12: (9, 30)}[value.month]
    return date(value.year, *month_day)


def cumulative_to_quarter(current: FinancialFact, previous: FinancialFact | None = None):
    """Convert a cumulative flow fact to a single-quarter derived value."""
    formula_id = "cumulative_to_quarter_v1"
    end = current.period.end
    start = date(end.year, end.month - 2, 1)
    period = FinancialPeriod(start=start, end=end, basis="quarter")
    inputs = [current] + ([previous] if previous else [])
    if current.statement == "balancesheet" or current.period.basis not in {"ytd", "annual"}:
        measure = _missing("CNY", DataStatus.NOT_APPLICABLE, "cumulative_flow_required")
    elif current.company_type == "unknown":
        measure = _missing("CNY", DataStatus.NOT_APPLICABLE, "unknown_company_type")
    elif end.month == 3:
        measure = (
            _measure(current.amount.value, "CNY")
            if current.amount.value is not None
            else _missing("CNY", DataStatus.NO_DATA, f"input_{current.amount.status.value}")
        )
    elif previous is None:
        measure = _missing("CNY", DataStatus.NO_DATA, "missing_previous_cumulative_period")
    elif not _same_context([current, previous]):
        measure = _missing("CNY", DataStatus.QUALITY_ERROR, "incompatible_financial_context")
    elif previous.period.end != _previous_quarter_end(end) or previous.period.basis != "ytd":
        measure = _missing("CNY", DataStatus.QUALITY_ERROR, "nonconsecutive_cumulative_periods")
    elif failure := _input_failure([current, previous], "CNY"):
        measure = failure
    else:
        with localcontext() as context:
            context.prec = 50
            measure = _measure(current.amount.value - previous.amount.value, "CNY")
    return _result(current, f"{current.metric}_single_quarter", period, formula_id, measure, inputs)


def trailing_twelve_months(quarters: list[Operand] | tuple[Operand, ...]):
    """Sum four consecutive single-quarter flow values."""
    if not quarters:
        raise ValueError("at_least_one_quarter_required_for_lineage")
    ordered = sorted(quarters, key=lambda item: _operand_parts(item)[2].end)
    last = ordered[-1]
    _, metric, last_period, _, _, _, _, _ = _operand_parts(last)
    end = last_period.end
    start_month = end.month % 12 + 1
    period = FinancialPeriod(
        start=date(end.year if start_month == 1 else end.year - 1, start_month, 1),
        end=end,
        basis="ttm",
    )
    if len(ordered) != 4:
        measure = _missing("CNY", DataStatus.NO_DATA, "four_quarters_required")
    elif not _same_context(ordered):
        measure = _missing("CNY", DataStatus.QUALITY_ERROR, "incompatible_financial_context")
    elif any(_operand_parts(item)[2].basis != "quarter" for item in ordered):
        measure = _missing("CNY", DataStatus.QUALITY_ERROR, "single_quarter_inputs_required")
    elif any(
        _operand_parts(ordered[index - 1])[2].end
        != _previous_quarter_end(_operand_parts(ordered[index])[2].end)
        for index in range(1, 4)
    ):
        measure = _missing("CNY", DataStatus.NO_DATA, "missing_consecutive_quarter")
    elif failure := _input_failure(ordered, "CNY"):
        measure = failure
    else:
        with localcontext() as context:
            context.prec = 50
            total = sum((_operand_parts(item)[5].value for item in ordered), Decimal(0))
            measure = _measure(total, "CNY")
    base_metric = metric.removesuffix("_single_quarter")
    return _result(last, f"{base_metric}_ttm", period, "ttm_sum_four_quarters_v1", measure, ordered)


def year_over_year(current: Operand, prior: Operand):
    """Compare equivalent periods one year apart without hiding bad bases."""
    c = _operand_parts(current)
    p = _operand_parts(prior)
    formula_id = "yoy_growth_v1"
    if not _same_context([current, prior]):
        measure = _missing("ratio", DataStatus.QUALITY_ERROR, "incompatible_financial_context")
    elif c[2].basis != p[2].basis or c[2].end != date(
        p[2].end.year + 1, p[2].end.month, p[2].end.day
    ):
        measure = _missing("ratio", DataStatus.QUALITY_ERROR, "noncomparable_prior_period")
    elif failure := _input_failure([current, prior], "ratio"):
        measure = failure
    elif p[5].unit != c[5].unit:
        measure = _missing("ratio", DataStatus.QUALITY_ERROR, "unit_conflict")
    elif p[5].value == 0:
        measure = _missing("ratio", DataStatus.QUALITY_ERROR, "division_by_zero")
    elif p[5].value < 0:
        measure = _missing("ratio", DataStatus.QUALITY_ERROR, "negative_comparison_base")
    else:
        with localcontext() as context:
            context.prec = 50
            measure = _measure(c[5].value / p[5].value - 1, "ratio")
    return _result(current, f"{c[1]}_yoy", c[2], formula_id, measure, [current, prior])


def financial_ratio(indicator: Indicator, numerator: Operand, denominator: Operand):
    """Calculate a named ratio with metric and industry applicability checks."""
    n = _operand_parts(numerator)
    d = _operand_parts(denominator)
    formula_id = f"{indicator}_v1"
    allowed = FORMULAS[formula_id].applicable_company_types
    expected = {
        "net_profit_margin": ({"net_profit_parent", "net_profit_total"}, None),
        "liability_ratio": ({"total_liabilities"}, {"total_assets"}),
        "operating_cash_flow_margin": ({"operating_cash_flow"}, {"revenue"}),
        "operating_cash_flow_to_profit": (
            {"operating_cash_flow"},
            {"net_profit_parent", "net_profit_total"},
        ),
    }[indicator]
    denominator_metrics = expected[1]
    if indicator == "net_profit_margin":
        denominator_metrics = {"revenue"} if n[4] == "industrial" else {"total_revenue"}
    if n[4] not in allowed:
        measure = _missing(
            "ratio", DataStatus.NOT_APPLICABLE, "indicator_not_applicable_to_industry"
        )
    elif not _same_context([numerator, denominator], include_metric=False):
        measure = _missing("ratio", DataStatus.QUALITY_ERROR, "incompatible_financial_context")
    elif n[2].end != d[2].end:
        measure = _missing("ratio", DataStatus.QUALITY_ERROR, "period_end_conflict")
    elif n[2].basis != d[2].basis:
        measure = _missing("ratio", DataStatus.QUALITY_ERROR, "period_basis_conflict")
    elif n[1] not in expected[0] or d[1] not in denominator_metrics:
        measure = _missing("ratio", DataStatus.QUALITY_ERROR, "wrong_indicator_inputs")
    elif failure := _input_failure([numerator, denominator], "ratio"):
        measure = failure
    elif n[5].unit != "CNY" or d[5].unit != "CNY":
        measure = _missing("ratio", DataStatus.QUALITY_ERROR, "unit_conflict")
    elif d[5].value == 0:
        measure = _missing("ratio", DataStatus.QUALITY_ERROR, "division_by_zero")
    elif indicator == "operating_cash_flow_to_profit" and d[5].value < 0:
        measure = _missing("ratio", DataStatus.NOT_APPLICABLE, "negative_earnings")
    elif (
        indicator in {"liability_ratio", "net_profit_margin", "operating_cash_flow_margin"}
        and d[5].value < 0
    ):
        measure = _missing("ratio", DataStatus.QUALITY_ERROR, "negative_denominator")
    else:
        with localcontext() as context:
            context.prec = 50
            measure = _measure(n[5].value / d[5].value, "ratio")
    return _result(numerator, indicator, n[2], formula_id, measure, [numerator, denominator])


def traced_measure(label, as_of_date, measure, data_version, locator):
    """Create an explicit input for valuation recalculation."""
    return TracedMeasure(
        label=label,
        as_of_date=as_of_date,
        measure=measure,
        data_version=data_version,
        evidence_id=_source_evidence(data_version, locator),
    )


def price_input(bar: PriceBar):
    return traced_measure(
        "close_price",
        bar.trade_date,
        bar.close,
        bar.provenance.data_version,
        f"price_bar:{bar.security.ts_code}:{bar.trade_date}:close:none",
    )


def financial_input(fact: FinancialFact):
    return TracedMeasure(
        label=fact.metric,
        as_of_date=fact.period.end,
        measure=fact.amount,
        data_version=fact.provenance.data_version,
        evidence_id=_fact_evidence(fact),
    )


def recalculate_valuation(metric: Literal["pe", "pb"], price, shares, denominator):
    """Recalculate PE/PB from explicit traced inputs, never replacing provider values."""
    formula_id = f"{metric}_recalculated_v1"
    inputs = (price, shares, denominator)
    versions = tuple(item.data_version for item in inputs)
    evidence = tuple(item.evidence_id for item in inputs)
    if price.measure.value is None or shares.measure.value is None:
        market_value = _missing("CNY", DataStatus.NO_DATA, "missing_price_or_shares")
    elif price.measure.unit != "CNY/share" or shares.measure.unit != "share":
        market_value = _missing("CNY", DataStatus.QUALITY_ERROR, "price_or_share_unit_conflict")
    elif price.measure.value < 0 or shares.measure.value <= 0:
        market_value = _missing("CNY", DataStatus.QUALITY_ERROR, "invalid_price_or_shares")
    else:
        with localcontext() as context:
            context.prec = 50
            market_value = _measure(price.measure.value * shares.measure.value, "CNY")
    if market_value.value is None:
        multiple = _missing("multiple", market_value.status, market_value.reason)
    elif shares.label not in {"shares", "total_shares"}:
        multiple = _missing("multiple", DataStatus.QUALITY_ERROR, "share_input_label_conflict")
    elif shares.as_of_date != price.as_of_date:
        multiple = _missing("multiple", DataStatus.QUALITY_ERROR, "share_date_conflict")
    elif metric == "pe" and denominator.label not in {
        "earnings",
        "earnings_ttm",
        "net_profit_parent",
        "net_profit_parent_ttm",
        "net_profit_total",
        "net_profit_total_ttm",
    }:
        multiple = _missing("multiple", DataStatus.QUALITY_ERROR, "earnings_denominator_required")
    elif metric == "pb" and denominator.label not in {
        "book_equity",
        "equity_parent",
        "total_equity",
    }:
        multiple = _missing(
            "multiple", DataStatus.QUALITY_ERROR, "book_equity_denominator_required"
        )
    elif denominator.measure.value is None:
        multiple = _missing("multiple", DataStatus.NO_DATA, "missing_financial_denominator")
    elif denominator.measure.unit != "CNY":
        multiple = _missing("multiple", DataStatus.QUALITY_ERROR, "financial_unit_conflict")
    elif denominator.measure.value == 0:
        multiple = _missing("multiple", DataStatus.QUALITY_ERROR, "division_by_zero")
    elif metric == "pe" and denominator.measure.value < 0:
        multiple = _missing("multiple", DataStatus.NOT_APPLICABLE, "negative_earnings")
    elif metric == "pb" and denominator.measure.value < 0:
        multiple = _missing("multiple", DataStatus.QUALITY_ERROR, "negative_book_equity")
    else:
        with localcontext() as context:
            context.prec = 50
            multiple = _measure(market_value.value / denominator.measure.value, "multiple")
    locator = f"valuation:{metric}:{price.as_of_date}:{denominator.as_of_date}"
    return ValuationCalculation(
        metric=metric,
        price_date=price.as_of_date,
        financial_period_end=denominator.as_of_date,
        price=price,
        shares=shares,
        financial_denominator=denominator,
        market_value=market_value,
        multiple=multiple,
        formula_id=formula_id,
        formula_expression=FORMULAS[formula_id].expression,
        input_versions=versions,
        input_evidence_ids=evidence,
        evidence_id=evidence_id(versions, locator, formula=formula_id, inputs=evidence),
    )


def adjusted_prices(bars, factors=(), *, adjustment: Literal["none", "front", "back"]):
    """Return a complete adjusted series or an explainable all-or-nothing failure."""
    formula_id = "adjusted_price_v1"
    bars = sorted(bars, key=lambda item: item.trade_date)
    factors = () if adjustment == "none" else tuple(factors)
    if not bars:
        raise ValueError("at_least_one_price_bar_required")
    security = bars[0].security
    if len({bar.trade_date for bar in bars}) != len(bars) or any(
        bar.security != security for bar in bars
    ):
        return AdjustedPriceSeries(
            security=security,
            adjustment=adjustment,
            base_date=None,
            status=DataStatus.QUALITY_ERROR,
            reason="duplicate_date_or_security_conflict",
            formula_id="identity_price_v1" if adjustment == "none" else formula_id,
            formula_expression=(
                "raw_price" if adjustment == "none" else FORMULAS[formula_id].expression
            ),
            points=(),
        )
    factor_by_date = {item.trade_date: item for item in factors}
    if len(factor_by_date) != len(factors) or any(item.security != security for item in factors):
        return AdjustedPriceSeries(
            security=security,
            adjustment=adjustment,
            base_date=None,
            status=DataStatus.QUALITY_ERROR,
            reason="duplicate_factor_or_security_conflict",
            formula_id=formula_id,
            formula_expression=FORMULAS[formula_id].expression,
            points=(),
        )
    base_date = (
        None
        if adjustment == "none"
        else (bars[-1] if adjustment == "front" else bars[0]).trade_date
    )
    if adjustment != "none" and any(bar.trade_date not in factor_by_date for bar in bars):
        return AdjustedPriceSeries(
            security=security,
            adjustment=adjustment,
            base_date=None,
            status=DataStatus.NO_DATA,
            reason="missing_adjustment_factor",
            formula_id=formula_id,
            formula_expression=FORMULAS[formula_id].expression,
            points=(),
        )
    base_factor = None if base_date is None else factor_by_date[base_date].factor.value
    if adjustment != "none" and (
        base_factor is None
        or base_factor <= 0
        or any(
            factor_by_date[bar.trade_date].factor.value is None
            or factor_by_date[bar.trade_date].factor.value <= 0
            for bar in bars
        )
    ):
        return AdjustedPriceSeries(
            security=security,
            adjustment=adjustment,
            base_date=None,
            status=DataStatus.QUALITY_ERROR,
            reason="invalid_adjustment_factor",
            formula_id=formula_id,
            formula_expression=FORMULAS[formula_id].expression,
            points=(),
        )
    points = []
    for bar in bars:
        factor_record = factor_by_date.get(bar.trade_date)
        versions = [bar.provenance.data_version]
        evidence = [
            _source_evidence(
                bar.provenance.data_version,
                f"price_bar:{bar.security.ts_code}:{bar.trade_date}:none",
            )
        ]
        factor_value = None
        multiplier = Decimal(1)
        if factor_record is not None and adjustment != "none":
            factor_value = factor_record.factor.value
            with localcontext() as context:
                context.prec = 50
                multiplier = factor_value / base_factor
            versions.append(factor_record.provenance.data_version)
            evidence.append(
                _source_evidence(
                    factor_record.provenance.data_version,
                    f"adjustment_factor:{bar.security.ts_code}:{bar.trade_date}",
                )
            )
        versions_tuple = tuple(sorted(set(versions)))
        evidence_tuple = tuple(sorted(set(evidence)))
        point_locator = (
            f"adjusted_price:{security.ts_code}:{bar.trade_date}:{adjustment}:{base_date}"
        )
        values = [getattr(bar, name).value for name in ("open", "high", "low", "close")]
        if any(value is None for value in values):
            return AdjustedPriceSeries(
                security=security,
                adjustment=adjustment,
                base_date=None,
                status=DataStatus.NO_DATA,
                reason="missing_raw_price",
                formula_id=formula_id,
                formula_expression=FORMULAS[formula_id].expression,
                points=(),
            )
        with localcontext() as context:
            context.prec = 50
            adjusted = [_bounded_decimal(value * multiplier) for value in values]
        points.append(
            AdjustedPricePoint(
                trade_date=bar.trade_date,
                open=adjusted[0],
                high=adjusted[1],
                low=adjusted[2],
                close=adjusted[3],
                factor=factor_value,
                input_versions=versions_tuple,
                input_evidence_ids=evidence_tuple,
                evidence_id=evidence_id(
                    versions_tuple,
                    point_locator,
                    formula="identity_price_v1" if adjustment == "none" else formula_id,
                    inputs=evidence_tuple,
                ),
            )
        )
    return AdjustedPriceSeries(
        security=security,
        adjustment=adjustment,
        base_date=base_date,
        status=DataStatus.OK,
        formula_id="identity_price_v1" if adjustment == "none" else formula_id,
        formula_expression="raw_price" if adjustment == "none" else FORMULAS[formula_id].expression,
        points=tuple(points),
    )
