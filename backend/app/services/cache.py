"""Redis query cache whose keys include all semantics and a database version token."""

import hashlib
import json


def cache_key(namespace: str, dimensions: dict, version_token: str) -> str:
    canonical = json.dumps(
        {"namespace": namespace, "dimensions": dimensions, "version": version_token},
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return "step14:data:v1:" + hashlib.sha256(canonical.encode()).hexdigest()


class QueryCache:
    def __init__(self, client, ttl_seconds: int):
        self.client = client
        self.ttl_seconds = ttl_seconds

    async def get(self, namespace, dimensions, version_token):
        key = cache_key(namespace, dimensions, version_token)
        try:
            value = await self.client.get(key)
            return (json.loads(value), "hit") if value is not None else (None, "miss")
        except Exception:
            return None, "bypass"

    async def set(self, namespace, dimensions, version_token, value):
        key = cache_key(namespace, dimensions, version_token)
        try:
            await self.client.set(
                key,
                json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str),
                ex=self.ttl_seconds,
            )
        except Exception:
            return False
        return True
