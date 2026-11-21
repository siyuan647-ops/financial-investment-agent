"""Append-only Step12 quality, quarantine, completeness and lineage storage."""

import hashlib
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from uuid import uuid4

from psycopg.types.json import Jsonb

from app.dataops.quality import PROCESSING_VERSION, RULES, CompletenessReport, QualityIssue
from app.repositories.migrate import MIGRATIONS

NODE_TYPES = {"raw", "standard", "derived", "evidence", "report", "alert"}
RELATIONS = {
    ("raw", "standard"): "normalized_to",
    ("standard", "derived"): "derived_to",
    ("derived", "evidence"): "evidenced_by",
    ("standard", "evidence"): "evidenced_by",
    ("evidence", "report"): "consumed_by",
    ("evidence", "alert"): "consumed_by",
}


def _jsonable(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return value


def node_id(node_type, reference_key):
    if node_type not in NODE_TYPES or not reference_key:
        raise ValueError("invalid_lineage_node")
    return hashlib.sha256(f"{node_type}\0{reference_key}".encode()).hexdigest()


async def _insert_catalog(connection):
    for rule in RULES.values():
        await connection.execute(
            "INSERT INTO storage.quality_rule_catalog "
            "(rule_id,processing_version,category,severity,description) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (
                rule.rule_id,
                PROCESSING_VERSION,
                rule.category.value,
                rule.severity.value,
                rule.description,
            ),
        )


async def migrate_quality(connection, direction="up"):
    if direction not in {"up", "down"}:
        raise ValueError("invalid_migration_direction")
    checksum = hashlib.sha256((MIGRATIONS / "012_quality.up.sql").read_bytes()).hexdigest()
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(600600)")
        cursor = await connection.execute(
            "SELECT version,checksum FROM public.schema_migration WHERE version IN ('008','012')"
        )
        versions = dict(await cursor.fetchall())
        if "008" not in versions:
            raise ValueError("step08_migration_required")
        if "012" in versions and versions["012"] != checksum:
            raise ValueError("migration_checksum_mismatch")
        if (direction == "up") == ("012" in versions):
            return False
        await connection.execute((MIGRATIONS / f"012_quality.{direction}.sql").read_text("utf-8"))
        if direction == "up":
            await _insert_catalog(connection)
            await connection.execute(
                "INSERT INTO public.schema_migration(version,checksum) VALUES ('012',%s)",
                (checksum,),
            )
        else:
            await connection.execute("DELETE FROM public.schema_migration WHERE version='012'")
    return True


class QualityRepository:
    def __init__(self, connection):
        self.connection = connection

    async def register_catalog(self):
        await _insert_catalog(self.connection)

    async def quarantine(
        self,
        run_id,
        issue: QualityIssue,
        *,
        raw_id=None,
        data_version=None,
        processing_version=PROCESSING_VERSION,
    ):
        if issue.rule_id not in RULES or RULES[issue.rule_id].severity != issue.severity:
            raise ValueError("quality_rule_severity_mismatch")
        if raw_id is None and data_version is None and issue.original_value is None:
            raise ValueError("quarantine_requires_original_reference_or_value")
        quarantine_id, event_id = uuid4(), uuid4()
        async with self.connection.transaction():
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
                    issue.rule_id,
                    processing_version,
                    issue.severity.value,
                    issue.subject_kind,
                    issue.subject_key,
                    issue.reason_code,
                    Jsonb(_jsonable(issue.original_value))
                    if issue.original_value is not None
                    else None,
                    Jsonb(_jsonable(dict(issue.detail))),
                ),
            )
            await self.connection.execute(
                "INSERT INTO storage.quality_quarantine_event "
                "(event_id,quarantine_id,event_type,run_id,processing_version,detail) "
                "VALUES (%s,%s,'quarantined',%s,%s,'{}')",
                (event_id, quarantine_id, run_id, processing_version),
            )
        return quarantine_id

    async def quarantine_event(
        self,
        quarantine_id,
        event_type,
        run_id,
        *,
        detail=None,
        processing_version=PROCESSING_VERSION,
    ):
        if event_type not in {"retry_requested", "retry_failed", "resolved"}:
            raise ValueError("invalid_quarantine_event")
        event_id = uuid4()
        cursor = await self.connection.execute(
            "INSERT INTO storage.quality_quarantine_event "
            "(event_id,quarantine_id,event_type,run_id,processing_version,detail) "
            "SELECT %s,quarantine_id,%s,%s,%s,%s FROM storage.quality_quarantine "
            "WHERE quarantine_id=%s RETURNING event_id",
            (
                event_id,
                event_type,
                run_id,
                processing_version,
                Jsonb(_jsonable(detail or {})),
                quarantine_id,
            ),
        )
        if await cursor.fetchone() is None:
            raise ValueError("unknown_quarantine")
        return event_id

    async def save_completeness(
        self,
        run_id,
        report: CompletenessReport,
        *,
        processing_version=PROCESSING_VERSION,
    ):
        assessment_id = uuid4()
        async with self.connection.transaction():
            await self.connection.execute(
                "INSERT INTO storage.completeness_assessment "
                "(assessment_id,run_id,domain,processing_version,expected_count,eligible_count,"
                "received_count,exception_count,missing_count,unexpected_count,completeness) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    assessment_id,
                    run_id,
                    report.domain,
                    processing_version,
                    report.expected_count,
                    report.eligible_count,
                    report.received_count,
                    report.exception_count,
                    report.missing_count,
                    report.unexpected_count,
                    report.completeness,
                ),
            )
            for item in report.items:
                await self.connection.execute(
                    "INSERT INTO storage.completeness_item "
                    "(assessment_id,subject_key,outcome,reason_code) VALUES (%s,%s,%s,%s)",
                    (assessment_id, item.subject_key, item.outcome.value, item.reason_code),
                )
        return assessment_id

    async def register_node(self, node_type, reference_key, metadata=None):
        identifier = node_id(node_type, reference_key)
        await self.connection.execute(
            "INSERT INTO storage.lineage_node(node_id,node_type,reference_key,metadata) "
            "VALUES (%s,%s,%s,%s) ON CONFLICT(node_type,reference_key) DO NOTHING",
            (identifier, node_type, reference_key, Jsonb(_jsonable(metadata or {}))),
        )
        return identifier

    async def link(
        self,
        parent_type,
        parent_reference,
        child_type,
        child_reference,
        run_id,
        *,
        parent_metadata=None,
        child_metadata=None,
        processing_version=PROCESSING_VERSION,
    ):
        relation = RELATIONS.get((parent_type, child_type))
        if relation is None:
            raise ValueError("invalid_lineage_relation")
        async with self.connection.transaction():
            parent_id = await self.register_node(parent_type, parent_reference, parent_metadata)
            child_id = await self.register_node(child_type, child_reference, child_metadata)
            await self.connection.execute(
                "INSERT INTO storage.lineage_edge "
                "(parent_node_id,child_node_id,relation,run_id,processing_version) "
                "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (parent_id, child_id, relation, run_id, processing_version),
            )
        return parent_id, child_id

    async def import_standard_lineage(self, run_id):
        """Mirror existing raw -> record lineage into the extensible graph."""
        cursor = await self.connection.execute(
            "SELECT l.raw_id,l.data_version FROM storage.lineage l "
            "JOIN storage.raw_response r USING(raw_id) WHERE r.run_id=%s",
            (run_id,),
        )
        count = 0
        for raw_id, data_version in await cursor.fetchall():
            await self.link("raw", str(raw_id), "standard", data_version, run_id)
            count += 1
        return count

    async def trace(self, node_type, reference_key):
        """Return the connected lineage component for one record/evidence/output."""
        identifier = node_id(node_type, reference_key)
        cursor = await self.connection.execute(
            "WITH RECURSIVE adjacency(a,b) AS ("
            " SELECT parent_node_id,child_node_id FROM storage.lineage_edge"
            " UNION SELECT child_node_id,parent_node_id FROM storage.lineage_edge),"
            "connected(node_id) AS (VALUES (%s) UNION "
            " SELECT adjacency.b FROM adjacency JOIN connected ON adjacency.a=connected.node_id) "
            "SELECT node_id,node_type,reference_key,metadata,created_at "
            "FROM storage.lineage_node WHERE node_id IN (SELECT node_id FROM connected) "
            "ORDER BY created_at,node_type,reference_key",
            (identifier,),
        )
        nodes = await cursor.fetchall()
        if not nodes:
            return {"nodes": [], "edges": []}
        node_ids = [row[0] for row in nodes]
        cursor = await self.connection.execute(
            "SELECT parent_node_id,child_node_id,relation,run_id,processing_version,created_at "
            "FROM storage.lineage_edge WHERE parent_node_id=ANY(%s) AND child_node_id=ANY(%s) "
            "ORDER BY created_at,parent_node_id,child_node_id",
            (node_ids, node_ids),
        )
        edges = await cursor.fetchall()
        return {
            "nodes": [
                {
                    "node_id": row[0],
                    "node_type": row[1],
                    "reference_key": row[2],
                    "metadata": row[3],
                    "created_at": row[4],
                }
                for row in nodes
            ],
            "edges": [
                {
                    "parent_node_id": row[0],
                    "child_node_id": row[1],
                    "relation": row[2],
                    "run_id": row[3],
                    "processing_version": row[4],
                    "created_at": row[5],
                }
                for row in edges
            ],
        }

    async def statistics(self, run_id):
        cursor = await self.connection.execute(
            "SELECT severity,rule,count(*) FROM storage.quality_issue WHERE run_id=%s "
            "GROUP BY severity,rule ORDER BY severity,rule",
            (run_id,),
        )
        issues = [
            {"severity": row[0], "rule": row[1], "count": row[2]} for row in await cursor.fetchall()
        ]
        cursor = await self.connection.execute(
            "SELECT event_type,count(*) FROM (SELECT DISTINCT ON (q.quarantine_id) "
            "e.event_type,q.quarantine_id FROM storage.quality_quarantine q "
            "JOIN storage.quality_quarantine_event e USING(quarantine_id) "
            "WHERE q.run_id=%s ORDER BY q.quarantine_id,e.created_at DESC,e.event_id DESC) latest "
            "GROUP BY event_type ORDER BY event_type",
            (run_id,),
        )
        quarantine = {row[0]: row[1] for row in await cursor.fetchall()}
        cursor = await self.connection.execute(
            "SELECT domain,expected_count,eligible_count,received_count,exception_count,"
            "missing_count,unexpected_count,completeness FROM ("
            "SELECT DISTINCT ON (domain) * FROM storage.completeness_assessment "
            "WHERE run_id=%s ORDER BY domain,created_at DESC,assessment_id DESC) latest "
            "ORDER BY domain",
            (run_id,),
        )
        completeness = [
            {
                "domain": row[0],
                "expected": row[1],
                "eligible": row[2],
                "received": row[3],
                "exceptions": row[4],
                "missing": row[5],
                "unexpected": row[6],
                "completeness": row[7],
            }
            for row in await cursor.fetchall()
        ]
        return {"issues": issues, "quarantine": quarantine, "completeness": completeness}
