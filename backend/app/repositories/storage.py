"""Async append-only repositories. Transactions encompass version, fact and lineage."""

import re
from uuid import uuid4

from psycopg import sql
from psycopg.types.json import Jsonb

from app.contracts.domain import (
    AdjustmentFactor,
    CorporateAction,
    FinancialFact,
    IndustryMembership,
    PriceBar,
    Security,
    SecurityAlias,
    TradingCalendar,
    Valuation,
)
from app.contracts.mapping import content_version
from app.dataops.quality import PROCESSING_VERSION, RULES
from app.repositories.raw import safe_parameters

MODELS = {
    "security": Security,
    "security_alias": SecurityAlias,
    "trading_calendar": TradingCalendar,
    "price_bar": PriceBar,
    "adjustment_factor": AdjustmentFactor,
    "valuation": Valuation,
    "financial_fact": FinancialFact,
    "corporate_action": CorporateAction,
    "industry_membership": IndustryMembership,
}


def identity(record):
    kind = next((k for k, v in MODELS.items() if type(record) is v), None)
    if kind is None:
        raise TypeError("unsupported_storage_contract")
    payload = record.model_dump(mode="json")
    natural = {"provider": record.provenance.provider, "source_api": record.provenance.source_api}
    if isinstance(record, TradingCalendar):
        natural.update(exchange=record.exchange, calendar_date=payload["calendar_date"])
    else:
        natural["security"] = payload["security"]
    if isinstance(record, (PriceBar, AdjustmentFactor, Valuation)):
        natural["trade_date"] = payload["trade_date"]
    if isinstance(record, FinancialFact):
        for key in ("metric", "statement", "period", "scope", "report_type", "company_type"):
            natural[key] = payload[key]
    if isinstance(record, SecurityAlias):
        natural.update(name=record.name, effective_from=payload["effective_from"])
    if isinstance(record, CorporateAction):
        natural["event_id"] = record.event_id
    if isinstance(record, IndustryMembership):
        for key in (
            "classification",
            "classification_version",
            "level",
            "industry_code",
            "effective_from",
        ):
            natural[key] = payload[key]
    semantic = {k: v for k, v in payload.items() if k != "provenance"}
    semantic["source"] = natural
    semantic["schema_version"] = record.provenance.schema_version
    semantic["mapping_version"] = record.provenance.mapping_version
    return kind, content_version(natural), content_version(semantic)


class Repository:
    def __init__(self, connection):
        self.connection = connection

    async def create_run(self, idempotency_key, parameters):
        clean = safe_parameters(parameters)
        async with self.connection.transaction():
            cursor = await self.connection.execute(
                "INSERT INTO storage.pipeline_run(run_id,idempotency_key,status,parameters) "
                "VALUES (%s,%s,'queued',%s) ON CONFLICT(idempotency_key) DO NOTHING "
                "RETURNING run_id",
                (uuid4(), idempotency_key, Jsonb(clean)),
            )
            row = await cursor.fetchone()
            if row:
                return row[0]
            cursor = await self.connection.execute(
                "SELECT run_id,parameters FROM storage.pipeline_run WHERE idempotency_key=%s",
                (idempotency_key,),
            )
            row = await cursor.fetchone()
            if row[1] != clean:
                raise ValueError("run_idempotency_conflict")
            return row[0]

    async def transition_run(self, run_id, expected, target, *, result=None):
        transitions = {
            "queued": {"running", "cancelled"},
            "running": {"succeeded", "failed", "cancelled"},
        }
        if target not in transitions.get(expected, set()):
            raise ValueError("invalid_run_transition")
        cursor = await self.connection.execute(
            "UPDATE storage.pipeline_run SET status=%s,result=%s,updated_at=now() "
            "WHERE run_id=%s AND status=%s RETURNING run_id",
            (target, Jsonb(result or {}), run_id, expected),
        )
        if await cursor.fetchone() is None:
            raise ValueError("run_state_conflict")

    async def put(self, record, raw_id):
        # Revalidate even model_copy/model_construct callers before persistence.
        kind, key, semantic_hash = identity(record)
        record = MODELS[kind].model_validate(record.model_dump(mode="json"))
        p = record.provenance
        payload = record.model_dump(mode="json")
        async with self.connection.transaction():
            cursor = await self.connection.execute(
                "SELECT sha256,provider,source_api,fetched_at FROM storage.raw_response "
                "WHERE raw_id=%s",
                (raw_id,),
            )
            raw = await cursor.fetchone()
            if raw != (p.raw_sha256, p.provider, p.source_api, p.timing.fetched_at):
                raise ValueError("raw_provenance_mismatch")
            previous = getattr(record, "supersedes_version", None)
            if previous:
                cursor = await self.connection.execute(
                    "SELECT kind,business_key,available_at FROM storage.record_version "
                    "WHERE data_version=%s",
                    (previous,),
                )
                row = await cursor.fetchone()
                if not row or row[:2] != (kind, key) or row[2] > p.timing.available_at:
                    raise ValueError("invalid_superseded_version")
            cursor = await self.connection.execute(
                "INSERT INTO storage.record_version(data_version,kind,business_key,semantic_hash,"
                "raw_id,available_at,fetched_at,supersedes_version) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING data_version",
                (
                    p.data_version,
                    kind,
                    key,
                    semantic_hash,
                    raw_id,
                    p.timing.available_at,
                    p.timing.fetched_at,
                    previous,
                ),
            )
            inserted = await cursor.fetchone() is not None
            if not inserted:
                cursor = await self.connection.execute(
                    "SELECT kind,business_key,semantic_hash FROM storage.record_version "
                    "WHERE data_version=%s",
                    (p.data_version,),
                )
                if await cursor.fetchone() != (kind, key, semantic_hash):
                    raise ValueError("data_version_conflict")
            else:
                columns = {
                    "business_key": key,
                    "data_version": p.data_version,
                    "payload": Jsonb(payload),
                }
                if isinstance(record, TradingCalendar):
                    columns.update(exchange=record.exchange, calendar_date=record.calendar_date)
                else:
                    columns["ts_code"] = record.security.ts_code
                if isinstance(record, (PriceBar, AdjustmentFactor, Valuation)):
                    columns["trade_date"] = record.trade_date
                if isinstance(record, PriceBar):
                    columns["close_value"] = record.close.value
                if isinstance(record, FinancialFact):
                    columns.update(
                        period_end=record.period.end,
                        metric=record.metric,
                        amount_value=record.amount.value,
                    )
                if isinstance(record, CorporateAction):
                    columns["event_id"] = record.event_id
                await self.connection.execute(
                    sql.SQL("INSERT INTO storage.{} ({}) VALUES ({})").format(
                        sql.Identifier(kind),
                        sql.SQL(",").join(map(sql.Identifier, columns)),
                        sql.SQL(",").join(sql.Placeholder() for _ in columns),
                    ),
                    tuple(columns.values()),
                )
            await self.connection.execute(
                "INSERT INTO storage.lineage(raw_id,data_version) VALUES (%s,%s) "
                "ON CONFLICT DO NOTHING",
                (raw_id, p.data_version),
            )
        return inserted

    async def current(self, kind, business_key, query):
        """Exact natural key, latest knowable version; never mix reporting scopes/types."""
        model = MODELS[kind]
        cursor = await self.connection.execute(
            sql.SQL(
                "SELECT d.payload FROM storage.{} d JOIN storage.record_version v "
                "USING(data_version) WHERE v.kind=%s AND v.business_key=%s "
                "AND v.available_at<=%s AND (%s='public' OR v.fetched_at<=%s) "
                "AND NOT EXISTS (SELECT 1 FROM storage.record_version successor "
                "WHERE successor.supersedes_version=v.data_version "
                "AND successor.available_at<=%s "
                "AND (%s='public' OR successor.fetched_at<=%s)) "
                "ORDER BY v.available_at DESC,v.fetched_at DESC,v.data_version DESC LIMIT 1"
            ).format(sql.Identifier(kind)),
            (
                kind,
                business_key,
                query.as_of,
                query.knowledge,
                query.as_of,
                query.as_of,
                query.knowledge,
                query.as_of,
            ),
        )
        row = await cursor.fetchone()
        return model.model_validate(row[0]) if row else None

    async def prices(self, ts_code, start, end, query, *, limit=1000):
        if start > end or not 1 <= limit <= 10000:
            raise ValueError("invalid_price_query_range")
        cursor = await self.connection.execute(
            "SELECT payload FROM (SELECT DISTINCT ON (p.trade_date,p.business_key) "
            "p.trade_date,p.business_key,p.payload FROM storage.price_bar p "
            "JOIN storage.record_version v USING(data_version) WHERE p.ts_code=%s "
            "AND p.trade_date BETWEEN %s AND %s AND v.available_at<=%s "
            "AND (%s='public' OR v.fetched_at<=%s) "
            "ORDER BY p.trade_date,p.business_key,v.available_at DESC,v.fetched_at DESC,"
            "v.data_version DESC) chosen ORDER BY trade_date,business_key LIMIT %s",
            (ts_code, start, end, query.as_of, query.knowledge, query.as_of, limit),
        )
        return [PriceBar.model_validate(row[0]) for row in await cursor.fetchall()]

    async def valuations(self, ts_code, start, end, query, *, limit=1000):
        if start > end or not 1 <= limit <= 10000:
            raise ValueError("invalid_valuation_query_range")
        cursor = await self.connection.execute(
            "SELECT payload FROM (SELECT DISTINCT ON (v.trade_date,v.business_key) "
            "v.trade_date,v.business_key,v.payload FROM storage.valuation v "
            "JOIN storage.record_version rv USING(data_version) WHERE v.ts_code=%s "
            "AND v.trade_date BETWEEN %s AND %s AND rv.available_at<=%s "
            "AND (%s='public' OR rv.fetched_at<=%s) "
            "ORDER BY v.trade_date,v.business_key,rv.available_at DESC,rv.fetched_at DESC,"
            "rv.data_version DESC) chosen ORDER BY trade_date,business_key LIMIT %s",
            (ts_code, start, end, query.as_of, query.knowledge, query.as_of, limit),
        )
        return [Valuation.model_validate(row[0]) for row in await cursor.fetchall()]

    async def add_quality_issue(
        self, *, run_id, rule, severity, detail, raw_id=None, data_version=None
    ):
        definition = RULES.get(rule)
        if definition is None:
            raise ValueError("unknown_quality_rule")
        if definition.severity.value != severity:
            raise ValueError("quality_rule_severity_mismatch")
        issue_id, quarantine_id = uuid4(), uuid4()
        async with self.connection.transaction():
            await self.connection.execute(
                "INSERT INTO storage.quality_issue "
                "(issue_id,run_id,raw_id,data_version,rule,severity,detail) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (issue_id, run_id, raw_id, data_version, rule, severity, Jsonb(detail)),
            )
            cursor = await self.connection.execute(
                "SELECT to_regclass('storage.quality_quarantine')"
            )
            if (await cursor.fetchone())[0] is not None:
                reason = detail.get("reason", rule)
                if not isinstance(reason, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", reason):
                    reason = rule
                subject_kind = detail.get("source_api", "pipeline")
                subject_key = str(detail.get("ts_code") or raw_id or data_version or run_id)
                await self.connection.execute(
                    "INSERT INTO storage.quality_quarantine "
                    "(quarantine_id,run_id,raw_id,data_version,rule_id,rule_version,severity,"
                    "subject_kind,subject_key,reason_code,original_value,detail) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        quarantine_id,
                        run_id,
                        raw_id,
                        data_version,
                        rule,
                        PROCESSING_VERSION,
                        severity,
                        subject_kind,
                        subject_key,
                        reason,
                        Jsonb({"raw_id": str(raw_id)}) if raw_id else Jsonb(detail),
                        Jsonb(detail),
                    ),
                )
                await self.connection.execute(
                    "INSERT INTO storage.quality_quarantine_event "
                    "(event_id,quarantine_id,event_type,run_id,processing_version,detail) "
                    "VALUES (%s,%s,'quarantined',%s,%s,'{}')",
                    (uuid4(), quarantine_id, run_id, PROCESSING_VERSION),
                )
        return issue_id
