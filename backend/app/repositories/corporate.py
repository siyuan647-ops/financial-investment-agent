"""Step10 version publication and bounded local company-action/industry queries."""

from datetime import date

from app.contracts.domain import CorporateAction, IndustryMembership
from app.contracts.mapping import content_version
from app.repositories.reference import ReferenceRepository
from app.repositories.storage import identity

STAGE_RANK = {"unknown": 0, "proposed": 1, "approved": 2, "implemented": 3, "cancelled": 4}
RAW_STAGE_RANK = {"预披露": 0, "预案": 1, "董事会预案": 1, "股东大会预案": 1}


def _action_rank(action):
    return (
        STAGE_RANK[action.stage],
        RAW_STAGE_RANK.get(action.raw_stage, 2),
        action.pay_date or date.min,
        action.ex_date or date.min,
        action.record_date or date.min,
        action.announced_on or date.min,
        action.provenance.timing.available_at,
        action.provenance.timing.fetched_at,
    )


def select_actions(actions, *, history=False):
    ordered = sorted(
        actions,
        key=lambda item: (
            item.report_end,
            item.event_id,
            item.provenance.timing.available_at,
            item.provenance.data_version,
        ),
    )
    if history:
        return ordered
    chosen = {}
    for action in ordered:
        if action.event_id not in chosen or _action_rank(action) > _action_rank(
            chosen[action.event_id]
        ):
            chosen[action.event_id] = action
        elif (
            _action_rank(action) == _action_rank(chosen[action.event_id])
            and action != chosen[action.event_id]
        ):
            raise ValueError("ambiguous_dividend_revision")
    return sorted(chosen.values(), key=lambda item: (item.report_end, item.event_id))


def select_memberships(memberships, *, history=False, on_date=None, as_of_date=None):
    ordered = sorted(
        memberships,
        key=lambda item: (
            item.classification,
            item.classification_version or "",
            item.level or 0,
            item.effective_from or date.min,
            item.provenance.timing.available_at,
            item.provenance.data_version,
        ),
    )
    if history:
        return ordered
    candidates = []
    for membership in ordered:
        if membership.history == "current_snapshot":
            if on_date is None:
                candidates.append(membership)
            continue
        day = on_date or as_of_date or date.today()
        if membership.effective_from <= day and (
            membership.effective_to is None or membership.effective_to >= day
        ):
            candidates.append(membership)
    chosen = {}
    for membership in candidates:
        key = (
            membership.classification,
            membership.classification_version,
            membership.level,
        )
        current = chosen.get(key)
        if current is None:
            chosen[key] = membership
            continue
        current_rank = (
            current.effective_from or date.min,
            current.provenance.timing.available_at,
            current.provenance.data_version,
        )
        rank = (
            membership.effective_from or date.min,
            membership.provenance.timing.available_at,
            membership.provenance.data_version,
        )
        if rank > current_rank:
            chosen[key] = membership
        elif rank == current_rank and membership.industry_code != current.industry_code:
            raise ValueError("ambiguous_industry_membership")
    return sorted(chosen.values(), key=lambda item: (item.classification, item.level or 0))


class CorporateRepository(ReferenceRepository):
    async def _publish_version(self, record, raw_id, previous):
        _, _, semantic = identity(record)
        version = (
            previous[1]
            if previous and previous[0] == semantic
            else content_version(
                {
                    "semantic": semantic,
                    "observed_at": record.provenance.timing.fetched_at.isoformat(),
                }
            )
        )
        record = record.model_copy(
            update={"provenance": record.provenance.model_copy(update={"data_version": version})}
        )
        return await self.put(record, raw_id)

    async def publish_action(self, action, raw_id):
        # Stages are source revision slots of one stable event, not separate dividend events.
        _, key, semantic = identity(action)
        cursor = await self.connection.execute(
            "SELECT v.semantic_hash,v.data_version FROM storage.corporate_action d "
            "JOIN storage.record_version v USING(data_version) WHERE d.business_key=%s "
            "AND v.semantic_hash=%s "
            "ORDER BY v.available_at DESC,v.fetched_at DESC,v.data_version DESC LIMIT 1",
            (key, semantic),
        )
        return await self._publish_version(action, raw_id, await cursor.fetchone())

    async def publish_membership(self, membership, raw_id):
        _, key, _ = identity(membership)
        cursor = await self.connection.execute(
            "SELECT semantic_hash,data_version FROM storage.record_version "
            "WHERE kind='industry_membership' AND business_key=%s "
            "ORDER BY available_at DESC,fetched_at DESC,data_version DESC LIMIT 1",
            (key,),
        )
        return await self._publish_version(membership, raw_id, await cursor.fetchone())

    async def corporate_actions(
        self, code, query, *, history=False, provider="tushare_compatible", report_ends=()
    ):
        predicate = "AND (d.payload->>'report_end')::date=ANY(%s) " if report_ends else ""
        arguments = [code]
        if report_ends:
            arguments.append(list(report_ends))
        arguments.extend((provider, query.as_of, query.knowledge, query.as_of))
        cursor = await self.connection.execute(
            "SELECT d.payload FROM storage.corporate_action d "
            "JOIN storage.record_version v USING(data_version) WHERE d.ts_code=%s "
            + predicate
            + "AND d.payload->'provenance'->>'provider'=%s "
            "AND d.payload->'provenance'->>'mapping_version'='step10-v1' "
            "AND v.available_at<=%s AND (%s='public' OR v.fetched_at<=%s) "
            "ORDER BY d.event_id,v.available_at,v.data_version LIMIT 10001",
            tuple(arguments),
        )
        rows = await cursor.fetchall()
        if len(rows) > 10000:
            raise ValueError("corporate_action_query_limit_exceeded")
        records = [CorporateAction.model_validate(row[0]) for row in rows]
        return [item.model_dump(mode="json") for item in select_actions(records, history=history)]

    async def industries(
        self,
        code,
        query,
        *,
        history=False,
        on_date=None,
        classification=None,
        provider="tushare_compatible",
    ):
        if classification not in {None, "provider_basic", "SW"}:
            raise ValueError("invalid_industry_classification")
        predicate = "AND d.payload->>'classification'=%s " if classification else ""
        arguments = [code]
        if classification:
            arguments.append(classification)
        arguments.extend((provider, query.as_of, query.knowledge, query.as_of))
        cursor = await self.connection.execute(
            "SELECT d.payload FROM storage.industry_membership d "
            "JOIN storage.record_version v USING(data_version) WHERE d.ts_code=%s "
            + predicate
            + "AND d.payload->'provenance'->>'provider'=%s "
            "AND d.payload->'provenance'->>'mapping_version'='step10-v1' "
            "AND v.available_at<=%s AND (%s='public' OR v.fetched_at<=%s) "
            "ORDER BY v.available_at,v.data_version LIMIT 10001",
            tuple(arguments),
        )
        rows = await cursor.fetchall()
        if len(rows) > 10000:
            raise ValueError("industry_query_limit_exceeded")
        records = [IndustryMembership.model_validate(row[0]) for row in rows]
        return [
            item.model_dump(mode="json")
            for item in select_memberships(
                records, history=history, on_date=on_date, as_of_date=query.as_of.date()
            )
        ]
