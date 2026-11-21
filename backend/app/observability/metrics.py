import httpx
import psycopg
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

ORCHESTRATION_STATES = ("queued", "running", "succeeded", "failed", "cancelled", "deferred")
DATAOPS_DOMAINS = ("reference", "market", "financial", "dividend", "industry")


class Metrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.requests = Counter(
            "app_http_requests",
            "HTTP responses by status class",
            ["status_class"],
            registry=self.registry,
        )
        self.dependencies = Gauge(
            "app_dependency_up",
            "Last protocol probe result",
            ["dependency"],
            registry=self.registry,
        )
        self.tasks = Gauge(
            "prefect_task_runs",
            "Current persisted task count by state (not a monotonic counter)",
            ["state"],
            registry=self.registry,
        )
        self.prefect_metrics_up = Gauge(
            "prefect_metrics_up",
            "Whether the last task-count query succeeded",
            registry=self.registry,
        )
        self.dataops_batches = Gauge(
            "dataops_orchestration_batches",
            "Persisted orchestration batch count by bounded state",
            ["state"],
            registry=self.registry,
        )
        self.dataops_missing = Gauge(
            "dataops_completeness_missing",
            "Missing items in the latest completeness assessment by domain",
            ["domain"],
            registry=self.registry,
        )
        self.dataops_update_lag = Gauge(
            "dataops_update_lag_seconds",
            "Seconds since the latest successful orchestration update by domain",
            ["domain"],
            registry=self.registry,
        )
        self.dataops_metrics_up = Gauge(
            "dataops_metrics_up",
            "Whether the last DataOps metrics query succeeded",
            registry=self.registry,
        )
        self.llm_invocations = Counter(
            "llm_invocations",
            "Completed model invocations by bounded purpose, configured model and status",
            ["purpose", "model", "status"],
            registry=self.registry,
        )
        self.llm_duration = Histogram(
            "llm_invocation_duration_seconds",
            "End-to-end model invocation duration",
            ["purpose", "model"],
            buckets=(0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600),
            registry=self.registry,
        )
        self.llm_tokens = Counter(
            "llm_tokens",
            "Reported model tokens by direction",
            ["purpose", "model", "direction"],
            registry=self.registry,
        )
        self.llm_estimated_cost = Counter(
            "llm_estimated_cost",
            "Estimated model cost from configured prices",
            ["purpose", "model", "currency"],
            registry=self.registry,
        )
        self.llm_unknown = Counter(
            "llm_observation_unknown",
            "Successful model calls with unavailable usage or price",
            ["purpose", "model", "reason"],
            registry=self.registry,
        )
        self.llm_active = Gauge(
            "llm_active_invocations",
            "Currently active model invocations",
            ["purpose", "model"],
            registry=self.registry,
        )

    def record_llm(
        self,
        *,
        purpose,
        model,
        status,
        duration_seconds,
        usage,
        estimated_cost,
        currency,
        unknown_reason,
    ):
        self.llm_invocations.labels(purpose, model, status).inc()
        self.llm_duration.labels(purpose, model).observe(duration_seconds)
        if usage is not None:
            self.llm_tokens.labels(purpose, model, "input").inc(usage.input_tokens)
            self.llm_tokens.labels(purpose, model, "output").inc(usage.output_tokens)
        if estimated_cost is not None:
            self.llm_estimated_cost.labels(purpose, model, currency).inc(float(estimated_cost))
        if unknown_reason is not None:
            self.llm_unknown.labels(purpose, model, unknown_reason).inc()

    async def refresh_tasks(self, settings):
        try:
            async with httpx.AsyncClient(timeout=2, trust_env=False) as client:
                response = await client.post(
                    settings.prefect_url.get_secret_value().rstrip("/") + "/task_runs/count",
                    json={},
                )
                response.raise_for_status()
                count = response.json()
                if type(count) is not int or count < 0:
                    raise ValueError("invalid task count")
                self.tasks.labels("all").set(count)
            self.prefect_metrics_up.set(1)
        except Exception:
            self.prefect_metrics_up.set(0)
            self.tasks.labels("all").set(float("nan"))

    async def refresh_dataops(self, settings):
        """Expose failure, gap and update-lag signals with only low-cardinality labels."""
        try:
            async with await psycopg.AsyncConnection.connect(
                settings.database_url.get_secret_value(), autocommit=True
            ) as connection:
                cursor = await connection.execute(
                    "SELECT status,count(*) FROM storage.orchestration_batch GROUP BY status"
                )
                batches = dict(await cursor.fetchall())
                cursor = await connection.execute(
                    "SELECT DISTINCT ON (domain) domain,missing_count "
                    "FROM storage.completeness_assessment "
                    "ORDER BY domain,created_at DESC,assessment_id DESC"
                )
                missing = dict(await cursor.fetchall())
                cursor = await connection.execute(
                    "SELECT domain,EXTRACT(EPOCH FROM (now()-max(updated_at))) "
                    "FROM storage.orchestration_batch WHERE status='succeeded' GROUP BY domain"
                )
                lag = dict(await cursor.fetchall())
            for state in ORCHESTRATION_STATES:
                self.dataops_batches.labels(state).set(int(batches.get(state, 0)))
            for domain in DATAOPS_DOMAINS:
                value = missing.get(domain)
                self.dataops_missing.labels(domain).set(
                    float("nan") if value is None else int(value)
                )
                value = lag.get(domain)
                self.dataops_update_lag.labels(domain).set(
                    float("nan") if value is None else max(0, float(value))
                )
            self.dataops_metrics_up.set(1)
        except Exception:
            self.dataops_metrics_up.set(0)
            for state in ORCHESTRATION_STATES:
                self.dataops_batches.labels(state).set(float("nan"))
            for domain in DATAOPS_DOMAINS:
                self.dataops_missing.labels(domain).set(float("nan"))
                self.dataops_update_lag.labels(domain).set(float("nan"))
