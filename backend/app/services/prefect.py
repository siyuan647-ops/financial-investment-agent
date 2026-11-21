"""Small Prefect REST adapter; provider credentials never enter submitted parameters."""

from urllib.parse import quote
from uuid import UUID

import httpx


class PrefectUnavailable(RuntimeError):
    pass


class PrefectGateway:
    def __init__(self, base_url: str, timeout_seconds: float = 10):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    async def submit(self, flow_name, deployment_name, parameters, task_id):
        flow = quote(flow_name, safe="")
        deployment = quote(deployment_name, safe="")
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.get(f"{self.base_url}/deployments/name/{flow}/{deployment}")
                response.raise_for_status()
                deployment_id = UUID(response.json()["id"])
                payload = {
                    "parameters": parameters,
                    "name": f"api-{task_id}",
                    "idempotency_key": str(task_id),
                }
                response = await client.post(
                    f"{self.base_url}/deployments/{deployment_id}/create_flow_run",
                    json=payload,
                )
                response.raise_for_status()
                return UUID(response.json()["id"])
        except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
            raise PrefectUnavailable("prefect_submission_failed") from exc

    async def flow_run(self, flow_run_id):
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.get(f"{self.base_url}/flow_runs/{flow_run_id}")
                response.raise_for_status()
                payload = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise PrefectUnavailable("prefect_status_unavailable") from exc
        state = payload.get("state") or {}
        return {
            "flow_run_id": str(flow_run_id),
            "name": payload.get("name"),
            "state": state.get("type", "UNKNOWN").lower(),
            "state_name": state.get("name"),
        }
