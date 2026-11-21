"""Financial revision persistence and explicit report-type selection, using Step06 tables."""

from datetime import date

from app.contracts.domain import FinancialFact
from app.contracts.mapping import content_version
from app.dataops.financial import validate_request
from app.repositories.reference import ReferenceRepository
from app.repositories.storage import identity


def revision_rank(fact):
    return (
        fact.actual_announced_on or fact.announced_on or date.min,
        fact.update_flag == "1",
        fact.provenance.timing.available_at,
    )


def select_versions(facts, query):
    chosen = {}
    for fact in facts:
        if not query.includes(fact.provenance):
            continue
        key = identity(fact)[1]
        if key not in chosen or revision_rank(fact) > revision_rank(chosen[key]):
            chosen[key] = fact
        elif revision_rank(fact) == revision_rank(chosen[key]) and fact != chosen[key]:
            raise ValueError("ambiguous_financial_revision")
    return sorted(chosen.values(), key=lambda f: (f.period.end, f.statement, f.metric))


class FinancialRepository(ReferenceRepository):
    async def publish_fact(self, fact, raw_id):
        # Compare within a source revision slot, not against whichever historical row arrived last.
        _, key, semantic = identity(fact)
        cursor = await self.connection.execute(
            "SELECT v.semantic_hash,v.data_version FROM storage.financial_fact d "
            "JOIN storage.record_version v USING(data_version) WHERE d.business_key=%s "
            "AND d.payload->>'announced_on' IS NOT DISTINCT FROM %s "
            "AND d.payload->>'actual_announced_on' IS NOT DISTINCT FROM %s "
            "AND d.payload->>'update_flag' IS NOT DISTINCT FROM %s "
            "ORDER BY v.available_at DESC,v.fetched_at DESC LIMIT 1",
            (
                key,
                fact.announced_on.isoformat() if fact.announced_on else None,
                fact.actual_announced_on.isoformat() if fact.actual_announced_on else None,
                fact.update_flag,
            ),
        )
        previous = await cursor.fetchone()
        version = (
            previous[1]
            if previous and previous[0] == semantic
            else content_version(
                {"semantic": semantic, "observed_at": fact.provenance.timing.fetched_at.isoformat()}
            )
        )
        fact = fact.model_copy(
            update={"provenance": fact.provenance.model_copy(update={"data_version": version})}
        )
        return await self.put(fact, raw_id)

    async def financials(
        self, code, periods, query, *, report_type="1", history=False, provider="tushare_compatible"
    ):
        validate_request([code], periods, [report_type])
        cursor = await self.connection.execute(
            "SELECT d.payload FROM storage.financial_fact d "
            "JOIN storage.record_version v USING(data_version) "
            "WHERE d.ts_code=%s AND d.period_end=ANY(%s) "
            "AND d.payload->>'report_type'=%s "
            "AND d.payload->'provenance'->>'provider'=%s "
            "AND d.payload->'provenance'->>'mapping_version'='step09-v1' "
            "AND v.available_at<=%s AND (%s='public' OR v.fetched_at<=%s) "
            "ORDER BY d.period_end,d.metric,v.available_at,v.data_version LIMIT 10001",
            (code, list(periods), report_type, provider, query.as_of, query.knowledge, query.as_of),
        )
        rows = await cursor.fetchall()
        if len(rows) > 10000:
            raise ValueError("financial_query_limit_exceeded_narrow_periods")
        facts = [FinancialFact.model_validate(row[0]) for row in rows]
        return [
            fact.model_dump(mode="json")
            for fact in (facts if history else select_versions(facts, query))
        ]
