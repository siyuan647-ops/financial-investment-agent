import asyncio
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from app.contracts.common import SHANGHAI, Availability, Provenance
from app.dataops.corporate import (
    BASIC_VERSION,
    CATALOG_FIELDS,
    DIVIDEND_FIELDS,
    MEMBER_FIELDS,
    CorporateError,
    normalize_basic_industry,
    normalize_dividend,
    normalize_memberships,
    validate_catalog,
    validate_codes,
    validate_dividend_response,
    validate_hierarchy,
    validate_member_response,
)
from app.dataops.market import decimal_rows
from app.repositories.corporate import select_actions, select_memberships
from app.repositories.storage import identity
from evals.corporate_fixture import CorporateFixture

NOW = datetime(2026, 11, 9, 12, tzinfo=SHANGHAI)


def provenance(api, *, now=NOW):
    return Provenance(
        provider="tushare_compatible",
        source_api=api,
        source_ref="fixture",
        raw_sha256="0" * 64,
        request_id="0" * 32,
        data_version="1" * 64,
        mapping_version="step10-v1",
        quality="valid",
        timing=Availability(fetched_at=now, available_at=now, basis="observed"),
    )


def response(api, params, fields):
    return asyncio.run(CorporateFixture().query(api, params, fields))


def catalogs():
    result = {}
    for level in ("L1", "L2", "L3"):
        raw = response("index_classify", {"level": level}, CATALOG_FIELDS).raw_content
        result[level] = validate_catalog(decimal_rows(raw), level)
    validate_hierarchy(result)
    return result


def dividend_rows():
    raw = response("dividend", {"ts_code": "000001.SZ"}, DIVIDEND_FIELDS).raw_content
    return decimal_rows(raw)


def test_dividend_stages_share_event_and_never_claim_payment():
    actions = [normalize_dividend(row, provenance("dividend")) for row in dividend_rows()]
    assert {item.stage for item in actions} == {"proposed", "approved", "implemented"}
    assert len({item.event_id for item in actions}) == 1
    assert all(item.payment_status == "unverified" for item in actions)
    current = select_actions(actions)
    assert len(current) == 1 and current[0].stage == "implemented"
    assert len(select_actions(actions, history=True)) == 3
    assert current[0].cash_before_tax.value == Decimal("0.25")


def test_unknown_dividend_stage_is_explicitly_unverified():
    row = dividend_rows()[0]
    row["div_proc"] = "等待监管确认"
    action = normalize_dividend(row, provenance("dividend"))
    assert action.stage == "unknown" and action.provenance.quality == "unverified"


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("ts_code", "600000.SH", "security_filter_ignored"),
        ("end_date", "20240230", "day is out of range"),
    ],
)
def test_dividend_response_rejects_ignored_filters_and_bad_dates(field, value, reason):
    row = dividend_rows()[0]
    row[field] = value
    with pytest.raises(ValueError, match=reason):
        validate_dividend_response([row], "000001.SZ")


def test_dividend_revision_slot_retains_variants_and_deduplicates_exact_rows():
    row = dividend_rows()[0]
    assert validate_dividend_response([row, dict(row)], "000001.SZ") == [row]
    changed = {**row, "cash_div_tax": Decimal("9")}
    assert validate_dividend_response([row, changed], "000001.SZ") == [row, changed]


def test_dividend_dates_and_response_limit_are_conservative():
    row = dividend_rows()[0]
    row["ann_date"] = "20271231"
    with pytest.raises(CorporateError, match="announcement"):
        normalize_dividend(row, provenance("dividend"))
    with pytest.raises(CorporateError, match="limit"):
        validate_dividend_response([dividend_rows()[0]] * 2000, "000001.SZ")


def test_basic_industry_is_unversioned_current_snapshot():
    row = {"ts_code": "000001.SZ", "industry": "银行"}
    membership = normalize_basic_industry(row, provenance("stock_basic"))
    assert membership.classification_version == BASIC_VERSION
    assert membership.history == "current_snapshot"
    assert membership.effective_from is membership.effective_to is None


def test_sw_hierarchy_and_intervals():
    catalog = catalogs()
    raw = response(
        "index_member_all", {"ts_code": "000001.SZ", "is_new": "Y"}, MEMBER_FIELDS
    ).raw_content
    rows = validate_member_response(decimal_rows(raw), "000001.SZ", "Y", catalog)
    records = normalize_memberships(rows[0], provenance("index_member_all"))
    assert [item.level for item in records] == [1, 2, 3]
    assert all(item.classification_version == "SW2021" for item in records)
    assert all(item.history == "interval_unverified" for item in records)


def test_hierarchy_rejects_wrong_parent_and_member_name():
    catalog = catalogs()
    catalog["L3"]["851911.SI"]["parent_code"] = "missing"
    with pytest.raises(CorporateError, match="parent"):
        validate_hierarchy(catalog)
    catalog = catalogs()
    raw = response(
        "index_member_all", {"ts_code": "000001.SZ", "is_new": "Y"}, MEMBER_FIELDS
    ).raw_content
    row = decimal_rows(raw)[0]
    row["l2_name"] = "错误层级"
    with pytest.raises(CorporateError, match="versioned_hierarchy"):
        validate_member_response([row], "000001.SZ", "Y", catalog)


def test_current_filter_and_interval_requirements():
    catalog = catalogs()
    raw = response(
        "index_member_all", {"ts_code": "000001.SZ", "is_new": "Y"}, MEMBER_FIELDS
    ).raw_content
    row = decimal_rows(raw)[0]
    row["is_new"] = "N"
    with pytest.raises(CorporateError, match="filter_ignored"):
        validate_member_response([row], "000001.SZ", "Y", catalog)
    row["is_new"] = "Y"
    row["out_date"] = "20220101"
    with pytest.raises(CorporateError, match="current_industry_has_end_date"):
        validate_member_response([row], "000001.SZ", "Y", catalog)


def test_membership_query_does_not_backdate_current_snapshot():
    basic = normalize_basic_industry(
        {"ts_code": "000001.SZ", "industry": "银行"}, provenance("stock_basic")
    )
    raw = response(
        "index_member_all", {"ts_code": "000001.SZ", "is_new": "Y"}, MEMBER_FIELDS
    ).raw_content
    sw = normalize_memberships(decimal_rows(raw)[0], provenance("index_member_all"))
    current = select_memberships([basic, *sw], as_of_date=date(2026, 11, 9))
    assert len(current) == 4
    historical = select_memberships(
        [basic, *sw], on_date=date(2023, 1, 1), as_of_date=date(2026, 11, 9)
    )
    assert len(historical) == 3
    assert all(item.classification == "SW" for item in historical)


def test_membership_business_key_tracks_relation_across_revisions():
    raw = response(
        "index_member_all", {"ts_code": "000001.SZ", "is_new": "Y"}, MEMBER_FIELDS
    ).raw_content
    membership = normalize_memberships(decimal_rows(raw)[0], provenance("index_member_all"))[0]
    renamed = membership.model_copy(update={"industry_name": "银行（修订名）"})
    closed = membership.model_copy(update={"effective_to": date(2026, 1, 1)})
    assert identity(membership)[1] == identity(renamed)[1] == identity(closed)[1]
    assert identity(membership)[2] != identity(renamed)[2] != identity(closed)[2]


def test_action_selector_respects_observation_history():
    action = normalize_dividend(dividend_rows()[0], provenance("dividend"))
    later = action.model_copy(
        update={
            "cash_before_tax": action.cash_before_tax.model_copy(
                update={"value": Decimal("0.3"), "raw_value": Decimal("0.3")}
            ),
            "provenance": provenance("dividend", now=NOW + timedelta(seconds=1)),
        }
    )
    assert select_actions([later, action])[0] == later


@pytest.mark.parametrize("codes", [[], ["000001.SZ"] * 2, ["000001.SZ"] * 4, ["000001.BJ"]])
def test_request_bounds(codes):
    with pytest.raises(ValueError):
        validate_codes(codes)
