"""Deterministic source for offline tests and isolated PostgreSQL development drills."""

import json
from datetime import timedelta

from app.contracts.mapping import upstream_date
from app.dataops.reference import FIELDS
from app.providers.tushare import ProviderError, ProviderResult


class ReferenceFixture:
    def __init__(self):
        self.name = "平安银行"
        self.bad_calendar = False
        self.alias_denied = False
        self.calls = []

    async def query(self, api, params, fields):
        self.calls.append((api, params))
        names = FIELDS[api].split(",")
        if api == "stock_basic":
            rows = [[params["ts_code"], self.name, "SZSE", "L", "19910403", None]]
            if params["list_status"] != "L":
                rows = []
        elif api == "namechange":
            if self.alias_denied:
                raise ProviderError("permission_denied")
            rows = [[params["ts_code"], "深发展A", "19910403", "20120801"]]
        else:
            rows = []
            day, end = upstream_date(params["start_date"]), upstream_date(params["end_date"])
            while day <= end:
                # Deliberately synthetic; holiday is explicit, not inferred by production code.
                opened = day.weekday() < 5 and day.strftime("%m%d") != "1001"
                rows.append([params["exchange"], day.strftime("%Y%m%d"), int(opened), None])
                day += timedelta(days=1)
            if self.bad_calendar:
                rows = rows[:-1]
        content = json.dumps(
            {"code": 0, "data": {"fields": names, "items": rows}}, ensure_ascii=False
        ).encode("utf-8")
        return ProviderResult(tuple(names), tuple(tuple(row) for row in rows), content)
