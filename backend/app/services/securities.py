"""Exact reference lookup and explicit expected-day classification."""

import re
from datetime import timedelta

from app.contracts.common import CalendarDate, Contract
from app.contracts.domain import Security, SecurityKey


class CoverageDay(Contract):
    security: SecurityKey
    date: CalendarDate
    state: str


def resolve_security(text, securities, aliases=(), *, on=None, exchange=None):
    """Return all matches. Bare digits are matched to stored codes, never prefixes.

    With `on`, names require dated alias evidence; a current name is not projected
    backwards. Without `on`, historical names are useful discovery aliases.
    """
    needle = text.strip().casefold()
    if not needle:
        return []
    codes = set()
    for alias in aliases:
        if alias.name.casefold() == needle and (
            on is None
            or alias.effective_from <= on
            and (alias.effective_to is None or on <= alias.effective_to)
        ):
            codes.add(alias.security.ts_code)
    result = {}
    for item in securities:
        key = item.security
        if exchange is not None and exchange != key.exchange:
            continue
        code_match = needle == key.ts_code.casefold() or (
            re.fullmatch(r"[0-9]{6}", needle) and needle == key.ts_code[:6]
        )
        if code_match or key.ts_code in codes or (on is None and item.name.casefold() == needle):
            result[key.ts_code] = item
    return [result[key] for key in sorted(result)]


def coverage(securities: list[Security], calendars, start, end):
    if not 0 <= (end - start).days <= 1100 or len(securities) > 30:
        raise ValueError("coverage_range_exceeded")
    indexed = {}
    for day in calendars:
        key = (day.exchange, day.calendar_date)
        if key in indexed:
            raise ValueError("duplicate_calendar_day")
        indexed[key] = day
    if len({s.security.ts_code for s in securities}) != len(securities):
        raise ValueError("duplicate_security")
    result = []
    for item in securities:
        day = start
        while day <= end:
            cal = indexed.get((item.security.exchange, day))
            if item.list_date and day < item.list_date:
                state = "before_listing"
            elif item.delist_date and day > item.delist_date:
                state = "after_delisting"
            elif cal is None:
                state = "calendar_missing"
            elif not cal.is_open:
                state = "non_trading_day"
            elif item.list_date is None:
                state = "listing_date_unknown"
            elif item.listing_status == "delisted" and item.delist_date is None:
                state = "delisting_date_unknown"
            elif item.listing_status == "unknown":
                state = "listing_status_unknown"
            else:
                # A current P status does not reveal historical daily suspensions.
                state = "expected_status_unverified"
            result.append(CoverageDay(security=item.security, date=day, state=state))
            day += timedelta(days=1)
    return result


class SecurityService:
    """Resolve against a declared local sample universe and knowable DB versions."""

    def __init__(self, repository, codes):
        self.repository = repository
        self.codes = tuple(codes)

    async def resolve(self, text, query, *, on=None, exchange=None):
        securities = await self.repository.snapshot("security", query, codes=self.codes)
        aliases = await self.repository.snapshot("security_alias", query, codes=self.codes)
        return resolve_security(text, securities, aliases, on=on, exchange=exchange)

    async def coverage(self, start, end, query):
        securities = await self.repository.snapshot("security", query, codes=self.codes)
        if {item.security.ts_code for item in securities} != set(self.codes):
            raise ValueError("requested_security_snapshot_missing")
        calendars = await self.repository.snapshot("trading_calendar", query, start=start, end=end)
        return coverage(securities, calendars, start, end)
