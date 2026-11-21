import asyncio
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from app.contracts.common import SHANGHAI, AsOfQuery, Availability, Provenance
from app.dataops.financial import FIELDS, normalize, validate_request, validate_response
from app.repositories.financial import select_versions
from evals.financial_fixture import FinancialFixture

PERIOD = date(2024, 12, 31)
NOW = datetime(2026, 11, 8, tzinfo=SHANGHAI)


def provenance(api="income", now=NOW):
    return Provenance(
        provider="test",
        source_api=api,
        source_ref="fixture",
        raw_sha256="0" * 64,
        request_id="0" * 32,
        data_version="1" * 64,
        mapping_version="step09-v1",
        quality="valid",
        timing=Availability(fetched_at=now, available_at=now, basis="observed"),
    )


def row(api="income", report_type="1"):
    return asyncio.run(
        FinancialFixture().query(
            api,
            {"ts_code": "000001.SZ", "period": "20241231", "report_type": report_type},
            FIELDS[api],
        )
    ).rows[0]


@pytest.mark.parametrize("api", FIELDS)
@pytest.mark.parametrize(
    "report_type,scope,basis,revision",
    [
        ("1", "consolidated", "annual", "reported"),
        ("2", "consolidated", "quarter", "reported"),
        ("4", "consolidated", "annual", "adjusted"),
        ("6", "parent", "annual", "reported"),
        ("8", "parent", "quarter", "adjusted"),
    ],
)
def test_semantics(api, report_type, scope, basis, revision):
    facts = normalize(api, row(api, report_type), provenance(api))
    assert all(f.scope == scope and f.revision == revision for f in facts)
    assert all(f.period.basis == ("instant" if api == "balancesheet" else basis) for f in facts)
    assert all(f.provenance.timing.available_at == NOW for f in facts)


def test_null_precision_ytd_and_negative_cash():
    data = row()
    data["end_date"] = "20240930"
    facts = normalize("income", data, provenance())
    assert facts[0].amount.value == Decimal("100.123456789012345678")
    assert facts[0].period.basis == "ytd"
    assert facts[1].amount.value is None and facts[1].amount.status == "null_value"
    assert normalize("cashflow", row("cashflow"), provenance("cashflow"))[1].amount.value == -10


@pytest.mark.parametrize(
    "field,value",
    [
        ("report_type", "11"),
        ("comp_type", "99"),
        ("update_flag", "9"),
        ("ann_date", "20200101"),
        ("f_ann_date", "20990101"),
        ("end_date", "20241230"),
        ("revenue", "NaN"),
        ("revenue", 1.1),
    ],
)
def test_invalid_rows(field, value):
    data = row()
    data[field] = value
    with pytest.raises(ValueError):
        normalize("income", data, provenance())


def test_duplicates_and_conflicting_revision():
    data = row()
    assert len(validate_response("income", [data, dict(data)], "000001.SZ", PERIOD, "1")) == 1
    changed = {**data, "revenue": "200"}
    with pytest.raises(ValueError, match="conflicting_same_revision"):
        validate_response("income", [data, changed], "000001.SZ", PERIOD, "1")


@pytest.mark.parametrize(
    "field,value", [("ts_code", "600519.SH"), ("end_date", "20231231"), ("report_type", "6")]
)
def test_filters(field, value):
    with pytest.raises(ValueError):
        validate_response("income", [{**row(), field: value}], "000001.SZ", PERIOD, "1")


def test_projection_and_limits():
    data = row()
    del data["total_revenue"]
    with pytest.raises(ValueError, match="projection"):
        validate_response("income", [data], "000001.SZ", PERIOD, "1")
    with pytest.raises(ValueError, match="truncation"):
        validate_response("income", [row()] * 100, "000001.SZ", PERIOD, "1")
    assert validate_response("income", [], "000001.SZ", PERIOD, "1") == []


def test_revision_order_and_as_of():
    data = row()
    old = normalize("income", data, provenance())[0]
    revised = normalize(
        "income", {**data, "revenue": "200"}, provenance(now=NOW + timedelta(seconds=1))
    )[0]
    query = AsOfQuery(request_id="0" * 32, as_of=NOW)
    assert select_versions([revised, old], query) == [old]
    assert select_versions([old], query.model_copy(update={"as_of": NOW - timedelta(days=1)})) == []
    latest = query.model_copy(update={"as_of": NOW + timedelta(seconds=2)})
    assert select_versions([old, revised], latest) == [revised]
    backfill = normalize(
        "income",
        {**data, "ann_date": "20250301", "f_ann_date": "20250301"},
        provenance(now=NOW + timedelta(seconds=2)),
    )[0]
    assert select_versions([backfill, revised, old], latest) == [revised]


@pytest.mark.parametrize(
    "codes,periods,types",
    [
        ([], [PERIOD], ["1"]),
        (["000001.SZ"] * 2, [PERIOD], ["1"]),
        (["000001.SZ"], [], ["1"]),
        (["000001.SZ"], [date(2024, 12, 30)], ["1"]),
        (["000001.SZ"], [PERIOD], ["11"]),
        (["000001.SZ"], [PERIOD], ["1", "1"]),
    ],
)
def test_request_bounds(codes, periods, types):
    with pytest.raises(ValueError):
        validate_request(codes, periods, types)


def test_multiple_disclosures_and_update_flags_do_not_depend_on_row_order():
    data = row()
    old = {**data, "ann_date": "20250301", "f_ann_date": "20250301", "update_flag": "0"}
    rows = validate_response("income", [data, old], "000001.SZ", PERIOD, "1")
    facts = [normalize("income", r, provenance())[0] for r in rows]
    q = AsOfQuery(request_id="0" * 32, as_of=NOW)
    assert select_versions(facts, q) == select_versions(list(reversed(facts)), q) == [facts[0]]
    missing = normalize("income", {**data, "ann_date": None, "f_ann_date": None}, provenance())[0]
    assert missing.provenance.timing.basis == "observed"
    assert missing.provenance.timing.disclosed_on is None


def test_query_rejects_tied_contradictory_versions():
    first = normalize("income", row(), provenance())[0]
    second = normalize("income", {**row(), "revenue": "999"}, provenance())[0]
    with pytest.raises(ValueError, match="ambiguous"):
        select_versions([first, second], AsOfQuery(request_id="0" * 32, as_of=NOW))
