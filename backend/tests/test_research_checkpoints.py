"""Node recovery and lease fencing regressions."""

from datetime import UTC, datetime
from types import MethodType
from uuid import uuid4

import anyio
import pytest

from app.agents.specialists import (
    DynamicMultiAgentResearchGraph,
    SpecialistOutcome,
)
from app.contracts.research import (
    RoleStatus,
    SpecialistAssessment,
    SpecialistInvocationTrace,
    SpecialistRole,
)
from app.repositories.research_checkpoints import ResearchCheckpointStore


class MemoryStore:
    def __init__(self):
        self.rows = {}
        self.active_calls = 0

    async def active(self):
        self.active_calls += 1

    async def load(self, role, digest, prompt, schema):
        row = self.rows.get(role)
        if row and row[:3] == (digest, prompt, schema):
            return row[3]
        return None

    async def save(self, role, digest, prompt, schema, output, tokens):
        self.rows[role] = (digest, prompt, schema, output, tokens)


def graph(store, calls, fail=None, *, question="question", max_tokens=100):
    value = DynamicMultiAgentResearchGraph(
        object(), object(), checkpoint_store=store, max_task_tokens=max_tokens
    )

    def node(role):
        async def execute(self, state):
            del state
            calls.append(role)
            if role == fail:
                raise RuntimeError("worker_interrupted")
            trace = SpecialistInvocationTrace(
                role=SpecialistRole(role),
                status=RoleStatus.SUCCEEDED,
                prompt_version="test",
                tokens=2,
            )
            if role in {"fundamental", "news", "market"}:
                assessment = SpecialistAssessment(
                    role=role,
                    summary="summary",
                    report_markdown="# Report\nsummary",
                    coverage_status="partial",
                )
                return {f"{role}_outcome": SpecialistOutcome(assessment, (), trace)}
            return {role: f"{role} output", f"{role}_trace": trace}

        return MethodType(execute, value)

    for role in ("fundamental", "news", "market", "investment", "risk"):
        setattr(value, f"_{role}", node(role))
    return value


async def run(value, question="question"):
    return await value.run(
        question,
        {},
        {"code": "000001.SZ"},
        None,
        request_id="1" * 32,
        task_id=str(uuid4()),
        as_of=datetime(2026, 11, 19, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    ("failed_role", "expected_retry"),
    [
        ("market", {"market", "investment", "risk"}),
        ("investment", {"investment", "risk"}),
        ("risk", {"risk"}),
    ],
)
def test_worker_restart_runs_only_unfinished_nodes(failed_role, expected_retry):
    async def scenario():
        store = MemoryStore()
        calls = []
        with pytest.raises(RuntimeError, match="worker_interrupted"):
            await run(graph(store, calls, failed_role))
        first = calls[:]
        await run(graph(store, calls))
        assert set(calls[len(first) :]) == expected_retry
        assert set(store.rows) == {"fundamental", "news", "market", "investment", "risk"}

    anyio.run(scenario)


def test_finished_risk_restarts_without_model_calls_and_input_change_invalidates_all():
    async def scenario():
        store = MemoryStore()
        calls = []
        result, tokens = await run(graph(store, calls))
        assert tokens == 10 and result.risk == "risk output"
        calls.clear()
        await run(graph(store, calls))
        assert calls == []
        await run(graph(store, calls), question="changed")
        assert set(calls) == {"fundamental", "news", "market", "investment", "risk"}

    anyio.run(scenario)


def test_recovered_tokens_enforce_total_budget():
    async def scenario():
        store = MemoryStore()
        await run(graph(store, []))
        with pytest.raises(ValueError, match="research_token_budget_exceeded"):
            await run(graph(store, [], max_tokens=9))

    anyio.run(scenario)


def test_stale_generation_cannot_save_checkpoint():
    class Cursor:
        async def fetchone(self):
            return None

    class Connection:
        async def execute(self, sql, params):
            assert "attempt_count=%s" in sql
            assert "lease_owner=%s" in sql
            assert "FOR UPDATE" in sql
            assert params[-1] == 1
            return Cursor()

    async def scenario():
        store = ResearchCheckpointStore(Connection(), uuid4(), "old-worker", 1)
        with pytest.raises(ValueError, match="research_lease_lost_or_cancelled"):
            await store.save("risk", "a" * 64, "v1", 1, {"text": "late"}, 1)

    anyio.run(scenario)
