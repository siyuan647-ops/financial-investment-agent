"""Bounded ingestion for the five directly obtainable fundamental domains."""

import asyncio
import hashlib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from uuid import uuid4

from pydantic import ValidationError

from app.contracts.common import SHANGHAI, AsOfQuery, Availability, Provenance
from app.contracts.fundamental import FundamentalRecord, canonical_data_date
from app.contracts.mapping import content_version, upstream_date
from app.dataops.market import decimal_rows
from app.dataops.reference import PROVIDER, security_key
from app.providers.tushare import ProviderError
from app.repositories.fundamental import materialize_record_version
from app.repositories.storage import Repository


class FundamentalDomainError(ValueError):
    """Safe fixed reason code."""


@dataclass(frozen=True)
class SourceSpec:
    api: str
    domain: str
    record_type: str
    fields: tuple[str, ...]
    period_parameter: bool = False
    market_date_parameter: bool = False
    extra_params: tuple[tuple[str, str], ...] = ()


SOURCES = (
    SourceSpec(
        "stock_basic",
        "company_profile",
        "security_profile",
        (
            "ts_code",
            "symbol",
            "name",
            "fullname",
            "market",
            "exchange",
            "industry",
            "list_status",
            "list_date",
            "delist_date",
            "act_name",
            "act_ent_type",
        ),
    ),
    SourceSpec(
        "stock_company",
        "company_profile",
        "company_description",
        (
            "ts_code",
            "com_name",
            "chairman",
            "manager",
            "secretary",
            "province",
            "city",
            "introduction",
            "website",
            "office",
            "employees",
            "main_business",
            "business_scope",
        ),
    ),
    SourceSpec(
        "index_member_all",
        "company_profile",
        "sw_industry",
        (
            "ts_code",
            "l1_code",
            "l1_name",
            "l2_code",
            "l2_name",
            "l3_code",
            "l3_name",
            "in_date",
            "out_date",
            "is_new",
        ),
        extra_params=(("is_new", "Y"),),
    ),
    SourceSpec(
        "ci_index_member",
        "company_profile",
        "citic_industry",
        (
            "l1_code",
            "l1_name",
            "l2_code",
            "l2_name",
            "l3_code",
            "l3_name",
            "ts_code",
            "name",
            "in_date",
            "out_date",
            "is_new",
        ),
        extra_params=(("is_new", "Y"),),
    ),
    SourceSpec(
        "fina_mainbz",
        "company_profile",
        "business_segment",
        (
            "ts_code",
            "end_date",
            "bz_item",
            "bz_code",
            "bz_sales",
            "bz_profit",
            "bz_cost",
            "curr_type",
            "update_flag",
        ),
        period_parameter=True,
        extra_params=(("type", "P"),),
    ),
    SourceSpec(
        "top10_holders",
        "company_profile",
        "top10_holder",
        (
            "ts_code",
            "ann_date",
            "end_date",
            "holder_name",
            "hold_amount",
            "hold_ratio",
            "holder_type",
        ),
        period_parameter=True,
    ),
    SourceSpec(
        "top10_floatholders",
        "company_profile",
        "top10_float_holder",
        ("ts_code", "ann_date", "end_date", "holder_name", "hold_amount"),
        period_parameter=True,
    ),
    SourceSpec(
        "income",
        "profitability",
        "income_statement",
        (
            "ts_code",
            "ann_date",
            "f_ann_date",
            "end_date",
            "report_type",
            "comp_type",
            "end_type",
            "total_revenue",
            "revenue",
            "int_income",
            "int_exp",
            "comm_income",
            "comm_exp",
            "n_commis_income",
            "total_cogs",
            "oper_cost",
            "sell_exp",
            "admin_exp",
            "fin_exp",
            "rd_exp",
            "operate_profit",
            "total_profit",
            "n_income",
            "n_income_attr_p",
            "non_oper_income",
            "non_oper_exp",
            "asset_disp_income",
            "fv_value_chg_gain",
            "invest_income",
            "update_flag",
        ),
        period_parameter=True,
        extra_params=(("report_type", "1"),),
    ),
    SourceSpec(
        "fina_indicator",
        "profitability",
        "financial_indicator",
        (
            "ts_code",
            "ann_date",
            "end_date",
            "extra_item",
            "eps",
            "dt_eps",
            "diluted2_eps",
            "bps",
            "roe",
            "roe_waa",
            "roe_dt",
            "debt_to_assets",
            "profit_dedt",
            "gross_margin",
            "grossprofit_margin",
            "netprofit_margin",
            "expense_of_sales",
            "saleexp_to_gr",
            "adminexp_of_gr",
            "finaexp_of_gr",
            "netprofit_yoy",
            "dt_netprofit_yoy",
            "tr_yoy",
            "or_yoy",
            "rd_exp",
            "update_flag",
        ),
        period_parameter=True,
    ),
    SourceSpec(
        "balancesheet",
        "balance_sheet",
        "balance_sheet",
        (
            "ts_code",
            "ann_date",
            "f_ann_date",
            "end_date",
            "report_type",
            "comp_type",
            "end_type",
            "total_share",
            "cap_rese",
            "surplus_rese",
            "undistr_porfit",
            "money_cap",
            "accounts_receiv",
            "cash_reser_cb",
            "depos_in_oth_bfi",
            "decr_in_disbur",
            "trad_asset",
            "fair_value_fin_assets",
            "debt_invest",
            "oth_debt_invest",
            "oth_eq_invest",
            "fix_assets_total",
            "total_assets",
            "depos_ib_deposits",
            "loan_oth_bank",
            "bond_payable",
            "depos_oth_bfi",
            "depos",
            "total_liab",
            "ordin_risk_reser",
            "oth_comp_income",
            "total_hldr_eqy_exc_min_int",
            "total_hldr_eqy_inc_min_int",
            "update_flag",
        ),
        period_parameter=True,
        extra_params=(("report_type", "1"),),
    ),
    SourceSpec(
        "cashflow",
        "cash_flow",
        "cash_flow_statement",
        (
            "ts_code",
            "ann_date",
            "f_ann_date",
            "end_date",
            "report_type",
            "comp_type",
            "end_type",
            "c_inf_fr_operate_a",
            "st_cash_out_act",
            "n_cashflow_act",
            "stot_inflows_inv_act",
            "stot_out_inv_act",
            "n_cashflow_inv_act",
            "c_recp_borrow",
            "proc_issue_bonds",
            "oth_cash_recp_ral_fnc_act",
            "stot_cash_in_fnc_act",
            "c_prepay_amt_borr",
            "c_pay_dist_dpcp_int_exp",
            "oth_cashpay_ral_fnc_act",
            "stot_cashout_fnc_act",
            "n_cash_flows_fnc_act",
            "c_cash_equ_beg_period",
            "c_cash_equ_end_period",
            "c_recp_cap_contrib",
            "net_cash_rece_sec",
            "update_flag",
        ),
        period_parameter=True,
        extra_params=(("report_type", "1"),),
    ),
    SourceSpec(
        "dividend",
        "auxiliary",
        "dividend",
        (
            "ts_code",
            "end_date",
            "ann_date",
            "div_proc",
            "stk_div",
            "stk_bo_rate",
            "stk_co_rate",
            "cash_div",
            "cash_div_tax",
            "record_date",
            "ex_date",
            "pay_date",
            "imp_ann_date",
            "base_date",
            "base_share",
        ),
    ),
    SourceSpec(
        "daily_basic",
        "auxiliary",
        "dividend_yield",
        ("ts_code", "trade_date", "dv_ratio", "dv_ttm"),
        market_date_parameter=True,
    ),
)

DATE_FIELDS = {
    "ann_date",
    "f_ann_date",
    "end_date",
    "in_date",
    "out_date",
    "list_date",
    "delist_date",
    "record_date",
    "ex_date",
    "pay_date",
    "imp_ann_date",
    "base_date",
    "trade_date",
}

PROFITABILITY_SOURCES = tuple(spec for spec in SOURCES if spec.domain == "profitability")
FINANCIAL_INDICATOR_SOURCE = next(spec for spec in SOURCES if spec.api == "fina_indicator")
BALANCE_SHEET_SOURCE = next(spec for spec in SOURCES if spec.api == "balancesheet")
CASH_FLOW_SOURCE = next(spec for spec in SOURCES if spec.api == "cashflow")
INCOME_FLOW_FIELDS = frozenset(
    {
        "total_revenue",
        "revenue",
        "int_income",
        "int_exp",
        "comm_income",
        "comm_exp",
        "n_commis_income",
        "total_cogs",
        "oper_cost",
        "sell_exp",
        "admin_exp",
        "fin_exp",
        "rd_exp",
        "operate_profit",
        "total_profit",
        "n_income",
        "n_income_attr_p",
        "non_oper_income",
        "non_oper_exp",
        "asset_disp_income",
        "fv_value_chg_gain",
        "invest_income",
    }
)
INDICATOR_FLOW_FIELDS = frozenset({"extra_item", "profit_dedt", "rd_exp"})
CASH_FLOW_FIELDS = frozenset(
    field
    for field in CASH_FLOW_SOURCE.fields
    if field
    not in {
        "ts_code",
        "ann_date",
        "f_ann_date",
        "end_date",
        "report_type",
        "comp_type",
        "end_type",
        "c_cash_equ_beg_period",
        "c_cash_equ_end_period",
        "update_flag",
    }
)
COMPANY_KEY_METRIC_FIELDS = (
    "eps",
    "dt_eps",
    "diluted2_eps",
    "bps",
    "roe",
    "roe_waa",
    "roe_dt",
    "debt_to_assets",
)


@dataclass(frozen=True)
class ProfitabilityPeriodPlan:
    annual_periods: tuple[date, ...]
    quarter_periods: tuple[date, ...]
    source_periods: tuple[date, ...]


@dataclass(frozen=True)
class FundamentalAnalysisPeriodPlan:
    company_metric_periods: tuple[date, ...]
    balance_sheet_periods: tuple[date, ...]
    cash_flow_periods: tuple[date, ...]
    cash_flow_source_periods: tuple[date, ...]


def previous_quarter(period: date) -> date:
    if (period.month, period.day) == (3, 31):
        return date(period.year - 1, 12, 31)
    if (period.month, period.day) == (6, 30):
        return date(period.year, 3, 31)
    if (period.month, period.day) == (9, 30):
        return date(period.year, 6, 30)
    if (period.month, period.day) == (12, 31):
        return date(period.year, 9, 30)
    raise FundamentalDomainError("quarter_end_required")


def latest_completed_quarter_for_day(day: date) -> date:
    candidates = (
        date(day.year, 3, 31),
        date(day.year, 6, 30),
        date(day.year, 9, 30),
        date(day.year, 12, 31),
    )
    earlier = [candidate for candidate in candidates if candidate <= day]
    return earlier[-1] if earlier else date(day.year - 1, 12, 31)


def profitability_period_plan(day: date) -> ProfitabilityPeriodPlan:
    if type(day) is not date:
        raise FundamentalDomainError("as_of_date_required")
    last_annual_year = day.year - 1
    annuals = (date(last_annual_year - 1, 12, 31), date(last_annual_year, 12, 31))
    quarters = []
    cursor = latest_completed_quarter_for_day(day)
    for _ in range(4):
        quarters.append(cursor)
        cursor = previous_quarter(cursor)
    quarters = tuple(reversed(quarters))
    required = set(annuals)
    for quarter in quarters:
        required.add(quarter)
        if quarter.month != 3:
            required.add(previous_quarter(quarter))
    return ProfitabilityPeriodPlan(annuals, quarters, tuple(sorted(required)))


def fundamental_analysis_period_plan(day: date) -> FundamentalAnalysisPeriodPlan:
    if type(day) is not date:
        raise FundamentalDomainError("as_of_date_required")
    quarters = []
    cursor = latest_completed_quarter_for_day(day)
    for _ in range(4):
        quarters.append(cursor)
        cursor = previous_quarter(cursor)
    quarters = tuple(reversed(quarters))
    cash_sources = set(quarters)
    for quarter in quarters:
        if quarter.month != 3:
            cash_sources.add(previous_quarter(quarter))
    return FundamentalAnalysisPeriodPlan(
        company_metric_periods=quarters[-3:],
        balance_sheet_periods=quarters,
        cash_flow_periods=quarters,
        cash_flow_source_periods=tuple(sorted(cash_sources)),
    )


def validate_request(code: str, period: date, market_date: date | None):
    security_key(code)
    if type(period) is not date or (period.month, period.day) not in {
        (3, 31),
        (6, 30),
        (9, 30),
        (12, 31),
    }:
        raise FundamentalDomainError("quarter_end_required")
    if market_date is not None and type(market_date) is not date:
        raise FundamentalDomainError("market_date_required")


def _optional_date(row, key):
    value = row.get(key)
    return upstream_date(value) if value else None


def _source_key(spec: SourceSpec, row: dict):
    identity_fields = {
        key: row.get(key)
        for key in (
            "ts_code",
            "end_date",
            "ann_date",
            "f_ann_date",
            "report_type",
            "comp_type",
            "update_flag",
            "in_date",
            "out_date",
            "l3_code",
            "holder_name",
            "bz_code",
            "bz_item",
            "div_proc",
            "record_date",
            "trade_date",
        )
        if key in row
    }
    return content_version(
        {"api": spec.api, "record_type": spec.record_type, "identity": identity_fields}
    )


def normalize(spec: SourceSpec, row: dict, provenance: Provenance):
    if row.get("ts_code") != provenance.source_ref.split(":", 1)[0]:
        raise FundamentalDomainError("security_filter_ignored")
    period_end = _optional_date(row, "end_date")
    announced_on = _optional_date(row, "f_ann_date") or _optional_date(row, "ann_date")
    effective_on = (
        _optional_date(row, "trade_date")
        or _optional_date(row, "in_date")
        or _optional_date(row, "ex_date")
        or _optional_date(row, "list_date")
    )
    values = {
        key: value
        for key, value in row.items()
        if value is not None and value != "" and isinstance(value, (str, int, Decimal, bool))
    }
    for key in DATE_FIELDS & values.keys():
        upstream_date(values[key])
    fetched_on = provenance.timing.fetched_at.date()
    return FundamentalRecord(
        provenance=provenance,
        security=security_key(row["ts_code"]),
        domain=spec.domain,
        record_type=spec.record_type,
        source_key=_source_key(spec, row),
        data_date=canonical_data_date(
            fetched_on=fetched_on,
            period_end=period_end,
            announced_on=announced_on,
            effective_on=effective_on,
        ),
        period_end=period_end,
        announced_on=announced_on,
        effective_on=effective_on,
        values=values,
    )


def validate_response(spec: SourceSpec, rows: list[dict], code: str, period: date):
    if len(rows) >= 2000:
        raise FundamentalDomainError("response_limit_or_truncation_unverified")
    expected = set(spec.fields)
    unique = {}
    for row in rows:
        if not expected <= row.keys():
            raise FundamentalDomainError("required_projection_missing")
        if row.get("ts_code") != code:
            raise FundamentalDomainError("security_filter_ignored")
        if spec.period_parameter and row.get("end_date") != period.strftime("%Y%m%d"):
            raise FundamentalDomainError("period_filter_ignored")
        unique[content_version(row)] = row
    return list(unique.values())


def _selected_financial_row(spec: SourceSpec, rows: list[dict]):
    if spec.api in {"income", "balancesheet", "cashflow"}:
        rows = [row for row in rows if row.get("report_type") == "1"]
    if not rows:
        raise FundamentalDomainError(f"{spec.api}_period_missing")
    preferred = [row for row in rows if str(row.get("update_flag") or "") == "1"]
    candidates = preferred or rows
    latest_disclosure = max(
        str(row.get("f_ann_date") or row.get("ann_date") or "") for row in candidates
    )
    candidates = [
        row
        for row in candidates
        if str(row.get("f_ann_date") or row.get("ann_date") or "") == latest_disclosure
    ]
    semantic = {content_version(row): row for row in candidates}
    if len(semantic) != 1:
        raise FundamentalDomainError(f"ambiguous_{spec.api}_revision")
    return next(iter(semantic.values()))


def _selected_profitability_row(spec: SourceSpec, rows: list[dict]):
    return _selected_financial_row(spec, rows)


def _retag_direct_record(record: FundamentalRecord, record_type: str):
    return FundamentalRecord.model_validate(
        record.model_copy(
            update={
                "record_type": record_type,
                "source_key": content_version(
                    {
                        "source_key": record.source_key,
                        "record_type": record_type,
                    }
                ),
            }
        ).model_dump(mode="python")
    )


def _rehome_direct_record(record: FundamentalRecord, domain: str, record_type: str, values: dict):
    return FundamentalRecord.model_validate(
        record.model_copy(
            update={
                "domain": domain,
                "record_type": record_type,
                "source_key": content_version(
                    {
                        "source_key": record.source_key,
                        "domain": domain,
                        "record_type": record_type,
                    }
                ),
                "values": values,
            }
        ).model_dump(mode="python")
    )


async def _published_company_valuation(repository, code, as_of_date, metric_record):
    """Materialize a dated valuation from published market and financial versions."""
    query = AsOfQuery(
        request_id=metric_record.provenance.request_id,
        as_of=datetime.now(SHANGHAI),
        knowledge="public",
    )
    market = Repository(repository.connection)
    valuations = await market.valuations(
        code, as_of_date - timedelta(days=45), as_of_date, query, limit=46
    )
    valuation = max(valuations, key=lambda row: row.trade_date, default=None)
    if valuation is None or valuation.trade_date < metric_record.announced_on:
        return None
    prices = await market.prices(code, valuation.trade_date, valuation.trade_date, query, limit=1)
    if not prices or valuation.pe_ttm.value is None or prices[0].close.value is None:
        return None
    bps = Decimal(str(metric_record.values["bps"]))
    if bps <= 0:
        return None
    pb = (prices[0].close.value / bps).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    values = {
        **metric_record.values,
        "pe_ttm": valuation.pe_ttm.value,
        "pb": pb,
        "valuation_trade_date": valuation.trade_date.isoformat(),
        "pb_bps_period_end": metric_record.period_end.isoformat(),
    }
    derived = metric_record.model_copy(
        update={
            "source_key": content_version(
                {
                    "base": metric_record.source_key,
                    "valuation_date": valuation.trade_date.isoformat(),
                }
            ),
            "data_date": valuation.trade_date,
            "effective_on": valuation.trade_date,
            "values": values,
            "origin": "calculated",
            "formula": "pe_ttm=daily_basic.pe_ttm;pb=daily.close/company_key_metrics.bps",
            "input_versions": (
                metric_record.provenance.data_version,
                valuation.provenance.data_version,
                prices[0].provenance.data_version,
            ),
        }
    )
    return materialize_record_version(FundamentalRecord.model_validate(derived))[0]


def _quarter_label(period: date):
    quarter = {3: 1, 6: 2, 9: 3, 12: 4}[period.month]
    return f"{period.year}Q{quarter}"


def _quarter_amounts(current, previous, fields):
    values = {}
    for field in fields:
        current_value = current.values.get(field)
        if not isinstance(current_value, (int, Decimal)) or isinstance(current_value, bool):
            continue
        if previous is None:
            values[field] = current_value
            continue
        previous_value = previous.values.get(field)
        if not isinstance(previous_value, (int, Decimal)) or isinstance(previous_value, bool):
            continue
        values[field] = Decimal(current_value) - Decimal(previous_value)
    return values


def derive_single_quarter_records(
    period: date,
    current_income: FundamentalRecord,
    previous_income: FundamentalRecord | None,
    current_indicator: FundamentalRecord,
    previous_indicator: FundamentalRecord | None,
):
    if period.month == 3:
        if previous_income is not None or previous_indicator is not None:
            raise FundamentalDomainError("q1_must_not_subtract_prior_year")
        formula = "q1_cumulative_identity_v1"
    else:
        if previous_income is None or previous_indicator is None:
            raise FundamentalDomainError("prior_cumulative_period_required")
        formula = "current_cumulative_minus_previous_cumulative_v1"

    income_inputs = tuple(
        item.provenance.data_version
        for item in (current_income, previous_income)
        if item is not None
    )
    indicator_inputs = tuple(
        item.provenance.data_version
        for item in (current_indicator, previous_indicator)
        if item is not None
    )
    income_values = _quarter_amounts(current_income, previous_income, INCOME_FLOW_FIELDS)
    for required in ("total_revenue", "n_income_attr_p"):
        if required not in income_values:
            raise FundamentalDomainError(f"single_quarter_{required}_required")
    income_values.update(
        {
            "ts_code": current_income.security.ts_code,
            "end_date": period.strftime("%Y%m%d"),
            "period_basis": "quarter",
            "quarter": _quarter_label(period),
        }
    )
    income_provenance = current_income.provenance.model_copy(
        update={"mapping_version": "fundamental-profitability-window-v1"}
    )
    income = FundamentalRecord(
        provenance=income_provenance,
        security=current_income.security,
        domain="profitability",
        record_type="income_statement_single_quarter",
        source_key=content_version(
            {
                "record_type": "income_statement_single_quarter",
                "period": period.isoformat(),
                "inputs": income_inputs,
            }
        ),
        data_date=current_income.data_date,
        period_end=period,
        announced_on=current_income.announced_on,
        values=income_values,
        origin="calculated",
        formula=formula,
        input_versions=income_inputs,
    )
    income = materialize_record_version(income)[0]

    indicator_values = _quarter_amounts(
        current_indicator, previous_indicator, INDICATOR_FLOW_FIELDS
    )
    revenue = Decimal(income.values["total_revenue"])
    if revenue == 0:
        raise FundamentalDomainError("single_quarter_revenue_zero")
    indicator_values["netprofit_margin"] = (
        Decimal(income.values["n_income_attr_p"]) / revenue * Decimal("100")
    )
    admin_exp = income.values.get("admin_exp")
    if isinstance(admin_exp, (int, Decimal)) and not isinstance(admin_exp, bool):
        indicator_values["adminexp_of_gr"] = Decimal(admin_exp) / revenue * Decimal("100")
    indicator_values.update(
        {
            "ts_code": current_indicator.security.ts_code,
            "end_date": period.strftime("%Y%m%d"),
            "period_basis": "quarter",
            "quarter": _quarter_label(period),
        }
    )
    all_indicator_inputs = tuple(dict.fromkeys((*indicator_inputs, income.provenance.data_version)))
    indicator_provenance = current_indicator.provenance.model_copy(
        update={"mapping_version": "fundamental-profitability-window-v1"}
    )
    indicator = FundamentalRecord(
        provenance=indicator_provenance,
        security=current_indicator.security,
        domain="profitability",
        record_type="financial_indicator_single_quarter",
        source_key=content_version(
            {
                "record_type": "financial_indicator_single_quarter",
                "period": period.isoformat(),
                "inputs": all_indicator_inputs,
            }
        ),
        data_date=current_indicator.data_date,
        period_end=period,
        announced_on=current_indicator.announced_on,
        values=indicator_values,
        origin="calculated",
        formula=f"{formula};ratios_recomputed_from_single_quarter_amounts_v1",
        input_versions=all_indicator_inputs,
    )
    return income, indicator


def derive_single_quarter_cash_flow(
    period: date,
    current: FundamentalRecord,
    previous: FundamentalRecord | None,
):
    if period.month == 3:
        if previous is not None:
            raise FundamentalDomainError("cash_flow_q1_must_not_subtract_prior_year")
        formula = "q1_cumulative_identity_v1"
    else:
        if previous is None:
            raise FundamentalDomainError("cash_flow_prior_cumulative_period_required")
        formula = "current_cumulative_minus_previous_cumulative_v1"
    values = _quarter_amounts(current, previous, CASH_FLOW_FIELDS)
    for required in (
        "n_cashflow_act",
        "n_cashflow_inv_act",
        "n_cash_flows_fnc_act",
    ):
        if required not in values:
            raise FundamentalDomainError(f"single_quarter_{required}_required")
    values.update(
        {
            "ts_code": current.security.ts_code,
            "end_date": period.strftime("%Y%m%d"),
            "period_basis": "quarter",
            "quarter": _quarter_label(period),
        }
    )
    inputs = tuple(item.provenance.data_version for item in (current, previous) if item is not None)
    record = FundamentalRecord(
        provenance=current.provenance.model_copy(
            update={"mapping_version": "fundamental-analysis-window-v1"}
        ),
        security=current.security,
        domain="cash_flow",
        record_type="cash_flow_statement_single_quarter",
        source_key=content_version(
            {
                "record_type": "cash_flow_statement_single_quarter",
                "period": period.isoformat(),
                "inputs": inputs,
            }
        ),
        data_date=current.data_date,
        period_end=period,
        announced_on=current.announced_on,
        values=values,
        origin="calculated",
        formula=formula,
        input_versions=inputs,
    )
    return materialize_record_version(record)[0]


async def collect_profitability_window(
    adapter,
    repository,
    raw_store,
    code: str,
    as_of_date: date,
    *,
    timeout_seconds=600,
):
    security_key(code)
    plan = profitability_period_plan(as_of_date)
    run_id = await repository.create_run(
        "profitability-window-v1:" + uuid4().hex,
        {
            "ts_code": code,
            "as_of_date": as_of_date.isoformat(),
            "annual_periods": [item.isoformat() for item in plan.annual_periods],
            "quarter_periods": [item.isoformat() for item in plan.quarter_periods],
        },
    )
    await repository.transition_run(run_id, "queued", "running")
    direct = {}
    raw_ids = {}
    batches = []
    try:
        async with asyncio.timeout(timeout_seconds):
            securities = await repository.snapshot(
                "security",
                AsOfQuery(request_id=run_id.hex, as_of=datetime.now(SHANGHAI)),
                codes=[code],
            )
            if {item.security.ts_code for item in securities} != {code}:
                raise FundamentalDomainError("security_reference_required")
            for period in plan.source_periods:
                for spec in PROFITABILITY_SOURCES:
                    params = {
                        "ts_code": code,
                        "period": period.strftime("%Y%m%d"),
                        "limit": 2000,
                        "offset": 0,
                        **dict(spec.extra_params),
                    }
                    response = await adapter.query(spec.api, params, ",".join(spec.fields))
                    fetched = datetime.now(SHANGHAI)
                    if not response.raw_content:
                        raise FundamentalDomainError("original_response_bytes_required")
                    raw_id = await raw_store.save(
                        run_id=run_id,
                        provider=PROVIDER,
                        source_api=spec.api,
                        parameters={**params, "fields": ",".join(spec.fields)},
                        fetched_at=fetched,
                        content=response.raw_content,
                    )
                    rows = validate_response(spec, decimal_rows(response.raw_content), code, period)
                    selected = _selected_profitability_row(spec, rows)
                    provenance = Provenance(
                        provider=PROVIDER,
                        source_api=spec.api,
                        source_ref=f"{code}:{raw_id}",
                        raw_sha256=hashlib.sha256(response.raw_content).hexdigest(),
                        request_id=run_id.hex,
                        data_version="0" * 64,
                        mapping_version="fundamental-profitability-window-v1",
                        quality="valid",
                        timing=Availability(
                            fetched_at=fetched,
                            available_at=fetched,
                            basis="observed",
                        ),
                    )
                    record = normalize(spec, selected, provenance)
                    suffix = "annual" if period in plan.annual_periods else "cumulative_input"
                    record = _retag_direct_record(record, f"{spec.record_type}_{suffix}")
                    record = materialize_record_version(record)[0]
                    direct[(spec.api, period)] = record
                    raw_ids[(spec.api, period)] = raw_id
                    batches.append(
                        {
                            "api": spec.api,
                            "period": period.isoformat(),
                            "status": "received",
                            "rows": len(rows),
                            "selected_update_flag": selected.get("update_flag"),
                        }
                    )

            derived = []
            for period in plan.quarter_periods:
                previous = None if period.month == 3 else previous_quarter(period)
                income, indicator = derive_single_quarter_records(
                    period,
                    direct[("income", period)],
                    None if previous is None else direct[("income", previous)],
                    direct[("fina_indicator", period)],
                    None if previous is None else direct[("fina_indicator", previous)],
                )
                derived.extend(
                    (
                        (income, raw_ids[("income", period)]),
                        (indicator, raw_ids[("fina_indicator", period)]),
                    )
                )

            inserted = 0
            async with repository.connection.transaction():
                await repository.connection.execute("SELECT pg_advisory_xact_lock(600635)")
                for key, record in direct.items():
                    inserted += await repository.publish(record, raw_ids[key])
                for record, raw_id in derived:
                    inserted += await repository.publish(record, raw_id)
                result = {
                    "run_id": str(run_id),
                    "ts_code": code,
                    "annual_periods": [item.isoformat() for item in plan.annual_periods],
                    "quarter_periods": [item.isoformat() for item in plan.quarter_periods],
                    "source_periods": [item.isoformat() for item in plan.source_periods],
                    "records": len(direct) + len(derived),
                    "inserted": inserted,
                    "batches": batches,
                }
                await repository.transition_run(run_id, "running", "succeeded", result=result)
            return result
    except BaseException as exc:
        reason = (
            exc.kind
            if isinstance(exc, ProviderError)
            else str(exc)
            if isinstance(exc, FundamentalDomainError)
            else type(exc).__name__
        )
        await repository.transition_run(
            run_id,
            "running",
            "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed",
            result={"reason": reason},
        )
        if isinstance(exc, asyncio.CancelledError):
            raise
        raise FundamentalDomainError(f"profitability_window_pipeline_failed:{reason}") from None


async def collect_fundamental_analysis_window(
    adapter,
    repository,
    raw_store,
    code: str,
    as_of_date: date,
    *,
    timeout_seconds=600,
):
    """Collect the confirmed 3-period metrics, 4 balance dates and 4 cash quarters."""
    security_key(code)
    plan = fundamental_analysis_period_plan(as_of_date)
    run_id = await repository.create_run(
        "fundamental-analysis-window-v1:" + uuid4().hex,
        {
            "ts_code": code,
            "as_of_date": as_of_date.isoformat(),
            "company_metric_periods": [item.isoformat() for item in plan.company_metric_periods],
            "balance_sheet_periods": [item.isoformat() for item in plan.balance_sheet_periods],
            "cash_flow_periods": [item.isoformat() for item in plan.cash_flow_periods],
        },
    )
    await repository.transition_run(run_id, "queued", "running")
    staged = []
    cash_inputs = {}
    cash_raw_ids = {}
    batches = []

    async def fetch(spec: SourceSpec, period: date):
        params = {
            "ts_code": code,
            "period": period.strftime("%Y%m%d"),
            "limit": 2000,
            "offset": 0,
            **dict(spec.extra_params),
        }
        response = await adapter.query(spec.api, params, ",".join(spec.fields))
        fetched = datetime.now(SHANGHAI)
        if not response.raw_content:
            raise FundamentalDomainError("original_response_bytes_required")
        raw_id = await raw_store.save(
            run_id=run_id,
            provider=PROVIDER,
            source_api=spec.api,
            parameters={**params, "fields": ",".join(spec.fields)},
            fetched_at=fetched,
            content=response.raw_content,
        )
        rows = validate_response(spec, decimal_rows(response.raw_content), code, period)
        selected = _selected_financial_row(spec, rows)
        provenance = Provenance(
            provider=PROVIDER,
            source_api=spec.api,
            source_ref=f"{code}:{raw_id}",
            raw_sha256=hashlib.sha256(response.raw_content).hexdigest(),
            request_id=run_id.hex,
            data_version="0" * 64,
            mapping_version="fundamental-analysis-window-v1",
            quality="valid",
            timing=Availability(fetched_at=fetched, available_at=fetched, basis="observed"),
        )
        batches.append(
            {
                "api": spec.api,
                "period": period.isoformat(),
                "status": "received",
                "rows": len(rows),
                "selected_update_flag": selected.get("update_flag"),
            }
        )
        return normalize(spec, selected, provenance), raw_id

    try:
        async with asyncio.timeout(timeout_seconds):
            latest_metric_period = plan.company_metric_periods[-1]
            primary_metric = None
            for period in plan.company_metric_periods:
                record, raw_id = await fetch(FINANCIAL_INDICATOR_SOURCE, period)
                missing = [
                    field for field in COMPANY_KEY_METRIC_FIELDS if field not in record.values
                ]
                if missing:
                    raise FundamentalDomainError("company_key_metrics_missing:" + ",".join(missing))
                values = {
                    "ts_code": code,
                    "end_date": period.strftime("%Y%m%d"),
                    "period_basis": "report_cumulative",
                    "is_primary": period == latest_metric_period,
                    **{field: record.values[field] for field in COMPANY_KEY_METRIC_FIELDS},
                }
                record = _rehome_direct_record(
                    record, "company_profile", "company_key_metrics", values
                )
                published_metric = materialize_record_version(record)[0]
                staged.append((published_metric, raw_id))
                if period == latest_metric_period:
                    primary_metric = (published_metric, raw_id)

            if primary_metric is not None:
                derived = await _published_company_valuation(
                    repository, code, as_of_date, primary_metric[0]
                )
                if derived is not None:
                    staged.append((derived, primary_metric[1]))

            for period in plan.balance_sheet_periods:
                record, raw_id = await fetch(BALANCE_SHEET_SOURCE, period)
                record = _retag_direct_record(record, "balance_sheet_point_in_time")
                staged.append((materialize_record_version(record)[0], raw_id))

            for period in plan.cash_flow_source_periods:
                record, raw_id = await fetch(CASH_FLOW_SOURCE, period)
                record = _retag_direct_record(record, "cash_flow_statement_cumulative_input")
                record = materialize_record_version(record)[0]
                cash_inputs[period] = record
                cash_raw_ids[period] = raw_id
                staged.append((record, raw_id))

            for period in plan.cash_flow_periods:
                previous = None if period.month == 3 else previous_quarter(period)
                record = derive_single_quarter_cash_flow(
                    period,
                    cash_inputs[period],
                    None if previous is None else cash_inputs[previous],
                )
                staged.append((record, cash_raw_ids[period]))

            inserted = 0
            async with repository.connection.transaction():
                await repository.connection.execute("SELECT pg_advisory_xact_lock(600636)")
                for record, raw_id in staged:
                    inserted += await repository.publish(record, raw_id)
                result = {
                    "run_id": str(run_id),
                    "ts_code": code,
                    "company_metric_periods": [
                        item.isoformat() for item in plan.company_metric_periods
                    ],
                    "balance_sheet_periods": [
                        item.isoformat() for item in plan.balance_sheet_periods
                    ],
                    "cash_flow_periods": [item.isoformat() for item in plan.cash_flow_periods],
                    "cash_flow_source_periods": [
                        item.isoformat() for item in plan.cash_flow_source_periods
                    ],
                    "records": len(staged),
                    "inserted": inserted,
                    "batches": batches,
                }
                await repository.transition_run(run_id, "running", "succeeded", result=result)
            return result
    except BaseException as exc:
        reason = (
            exc.kind
            if isinstance(exc, ProviderError)
            else str(exc)
            if isinstance(exc, FundamentalDomainError)
            else type(exc).__name__
        )
        await repository.transition_run(
            run_id,
            "running",
            "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed",
            result={"reason": reason},
        )
        if isinstance(exc, asyncio.CancelledError):
            raise
        raise FundamentalDomainError(
            f"fundamental_analysis_window_pipeline_failed:{reason}"
        ) from None


async def collect_fundamental_domains(
    adapter,
    repository,
    raw_store,
    code: str,
    period: date,
    market_date: date | None = None,
    *,
    timeout_seconds=600,
):
    validate_request(code, period, market_date)
    run_id = await repository.create_run(
        "fundamental-v1:" + uuid4().hex,
        {
            "ts_code": code,
            "period": period.isoformat(),
            "trade_date": market_date.isoformat() if market_date else "",
        },
    )
    await repository.transition_run(run_id, "queued", "running")
    staged = []
    batches = []
    try:
        async with asyncio.timeout(timeout_seconds):
            securities = await repository.snapshot(
                "security",
                AsOfQuery(request_id=run_id.hex, as_of=datetime.now(SHANGHAI)),
                codes=[code],
            )
            if {item.security.ts_code for item in securities} != {code}:
                raise FundamentalDomainError("security_reference_required")
            for spec in SOURCES:
                if spec.market_date_parameter and market_date is None:
                    batches.append(
                        {"api": spec.api, "status": "skipped_market_date_not_supplied", "rows": 0}
                    )
                    continue
                params = {"ts_code": code, "limit": 2000, "offset": 0, **dict(spec.extra_params)}
                if spec.period_parameter:
                    params["period"] = period.strftime("%Y%m%d")
                if spec.market_date_parameter:
                    params["trade_date"] = market_date.strftime("%Y%m%d")
                try:
                    response = await adapter.query(spec.api, params, ",".join(spec.fields))
                except ProviderError as exc:
                    batches.append(
                        {"api": spec.api, "status": exc.kind, "rows": 0, "code": exc.business_code}
                    )
                    continue
                fetched = datetime.now(SHANGHAI)
                if not response.raw_content:
                    raise FundamentalDomainError("original_response_bytes_required")
                raw_id = await raw_store.save(
                    run_id=run_id,
                    provider=PROVIDER,
                    source_api=spec.api,
                    parameters={**params, "fields": ",".join(spec.fields)},
                    fetched_at=fetched,
                    content=response.raw_content,
                )
                rows = validate_response(spec, decimal_rows(response.raw_content), code, period)
                base = Provenance(
                    provider=PROVIDER,
                    source_api=spec.api,
                    source_ref=f"{code}:{raw_id}",
                    raw_sha256=hashlib.sha256(response.raw_content).hexdigest(),
                    request_id=run_id.hex,
                    data_version="0" * 64,
                    mapping_version="fundamental-v1",
                    quality="valid",
                    timing=Availability(
                        fetched_at=fetched,
                        available_at=fetched,
                        basis="observed",
                    ),
                )
                try:
                    normalized = [normalize(spec, row, base) for row in rows]
                except ValidationError as exc:
                    detail = ";".join(
                        f"{'.'.join(map(str, item['loc']))}:{item['type']}"
                        for item in exc.errors(include_input=False, include_context=False)
                    )
                    raise FundamentalDomainError(f"invalid_{spec.api}_record:{detail}") from None
                staged.extend((record, raw_id) for record in normalized)
                batches.append(
                    {
                        "api": spec.api,
                        "status": "received" if rows else "no_data_unverified",
                        "rows": len(rows),
                    }
                )
            if not staged:
                raise FundamentalDomainError("no_obtainable_domain_records")
            inserted = 0
            async with repository.connection.transaction():
                await repository.connection.execute("SELECT pg_advisory_xact_lock(600632)")
                for record, raw_id in staged:
                    inserted += await repository.publish(record, raw_id)
                result = {
                    "run_id": str(run_id),
                    "ts_code": code,
                    "period": period.isoformat(),
                    "market_date": market_date.isoformat() if market_date else None,
                    "records": len(staged),
                    "inserted": inserted,
                    "domains": sorted({record.domain for record, _ in staged}),
                    "batches": batches,
                }
                await repository.transition_run(run_id, "running", "succeeded", result=result)
            return result
    except BaseException as exc:
        reason = (
            exc.kind
            if isinstance(exc, ProviderError)
            else str(exc)
            if isinstance(exc, FundamentalDomainError)
            else type(exc).__name__
        )
        await repository.transition_run(
            run_id,
            "running",
            "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed",
            result={"reason": reason},
        )
        if isinstance(exc, asyncio.CancelledError):
            raise
        raise FundamentalDomainError(f"fundamental_domain_pipeline_failed:{reason}") from None
