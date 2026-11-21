"""Small pure mapping primitives, not a collection or publication pipeline."""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from pydantic import TypeAdapter

from app.contracts.common import UNIT_RULES, DataStatus, Measure, Number, RawUnit, scale


@dataclass(frozen=True)
class NumericField:
    api: str
    upstream: str
    target: str
    meaning: str
    raw_unit: RawUnit
    industries: str = "all; subject to source applicability"
    required: bool = False


NUMERIC_FIELDS = (
    NumericField("daily", "open", "open", "开盘价", "CNY/share", required=True),
    NumericField("daily", "high", "high", "最高价", "CNY/share", required=True),
    NumericField("daily", "low", "low", "最低价", "CNY/share", required=True),
    NumericField("daily", "close", "close", "收盘价", "CNY/share", required=True),
    NumericField("daily", "vol", "volume", "成交量", "lot", required=True),
    NumericField("daily", "amount", "amount", "成交额", "thousand_CNY"),
    NumericField("adj_factor", "adj_factor", "factor", "复权因子", "factor", required=True),
    NumericField("daily_basic", "pe", "pe", "供应商市盈率", "multiple"),
    NumericField("daily_basic", "pe_ttm", "pe_ttm", "供应商滚动市盈率", "multiple"),
    NumericField("daily_basic", "pb", "pb", "供应商市净率", "multiple"),
    NumericField("daily_basic", "total_mv", "total_market_cap", "总市值", "ten_thousand_CNY"),
    NumericField("daily_basic", "circ_mv", "float_market_cap", "流通市值", "ten_thousand_CNY"),
    NumericField("daily_basic", "dv_ratio", "dividend_yield", "股息率", "percent"),
    NumericField("daily_basic", "dv_ttm", "dividend_yield_ttm", "滚动股息率", "percent"),
    NumericField("income", "revenue", "revenue", "营业收入", "CNY"),
    NumericField("income", "total_revenue", "total_revenue", "营业总收入", "CNY"),
    NumericField("income", "n_income_attr_p", "net_profit_parent", "归母净利润", "CNY"),
    NumericField("income", "n_income", "net_profit_total", "总净利润", "CNY"),
    NumericField("balancesheet", "total_assets", "total_assets", "总资产", "CNY"),
    NumericField("balancesheet", "total_liab", "total_liabilities", "总负债", "CNY"),
    NumericField("cashflow", "n_cashflow_act", "operating_cash_flow", "经营现金流净额", "CNY"),
    NumericField("cashflow", "n_cashflow_inv_act", "investing_cash_flow", "投资现金流净额", "CNY"),
    NumericField(
        "cashflow", "n_cash_flows_fnc_act", "financing_cash_flow", "筹资现金流净额", "CNY"
    ),
    NumericField("dividend", "cash_div", "cash_after_tax", "税后每股现金分红", "CNY/share"),
    NumericField("dividend", "cash_div_tax", "cash_before_tax", "税前每股现金分红", "CNY/share"),
    NumericField("dividend", "stk_div", "stock_distribution", "每股送转比例", "ratio"),
)
_FIELDS = {(item.api, item.upstream): item for item in NUMERIC_FIELDS}
_NUMBER = TypeAdapter(Number)


def map_measure(api: str, field: str, row: dict) -> Measure:
    """Missing projection and source null are different; neither becomes zero.

    Parse raw JSON with parse_float=Decimal. Binary floats are deliberately rejected.
    Permissions, applicability and failed requests cannot be inferred from a row.
    """
    spec = _FIELDS[api, field]
    unit, multiplier = UNIT_RULES[spec.raw_unit]
    raw = row.get(field)
    if field not in row:
        status, reason = DataStatus.NOT_REQUESTED, "field_absent_from_projection"
    elif raw is None:
        status, reason = DataStatus.NULL_VALUE, "source_returned_null"
    else:
        status, reason = DataStatus.OK, None
        raw = _NUMBER.validate_python(raw)
    return Measure(
        value=None if raw is None else scale(raw, multiplier),
        raw_value=raw,
        raw_unit=spec.raw_unit,
        unit=unit,
        status=status,
        reason=reason,
    )


def upstream_date(value: str) -> date:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{8}", value):
        raise ValueError("expected_YYYYMMDD")
    return date(int(value[:4]), int(value[4:6]), int(value[6:]))


def _canonical(value):
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("nonfinite_identity")
        # Avoid Decimal.normalize(), which rounds under the active context.
        text = format(value, "f")
        text = text.rstrip("0").rstrip(".") if "." in text else text
        return "0" if value == 0 else text
    if isinstance(value, datetime):
        raise ValueError("identity_datetime_requires_explicit_ISO_string")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise ValueError("identity_requires_string_keys")
        return {key: _canonical(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_canonical(item) for item in value]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    raise ValueError("unsupported_identity_value")


def content_version(payload: dict) -> str:
    """Caller supplies the complete semantic identity documented in data-dictionary.md."""
    encoded = json.dumps(
        _canonical(payload),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def evidence_id(
    record_versions: tuple[str, ...],
    locator: str,
    *,
    formula: str | None = None,
    inputs: tuple[str, ...] = (),
) -> str:
    return "ev_" + content_version(
        {
            "versions": sorted(set(record_versions)),
            "locator": locator,
            "formula": formula,
            "inputs": sorted(set(inputs)),
        }
    )
