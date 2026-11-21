"""Allowlisted JSON events: free-form messages/exception bodies are intentionally dropped."""

import json
import logging
import re
from datetime import UTC, datetime

from app.core.config import Settings


class SafeJsonFormatter(logging.Formatter):
    def __init__(self, settings: Settings):
        super().__init__()
        self.secrets = [c.token.get_secret_value() for c in settings.auth_credentials]
        for name in type(settings).model_fields:
            value = getattr(settings, name)
            if hasattr(value, "get_secret_value"):
                self.secrets.append(value.get_secret_value())

    def format(self, record):
        event = getattr(record, "event", "redacted_log")
        if event not in {"request_finished", "request_failed", "application_started"}:
            event = "redacted_log"
        data = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "event": event,
        }
        request_id = getattr(record, "request_id", None)
        if isinstance(request_id, str) and re.fullmatch(r"[0-9a-f]{32}", request_id):
            data["request_id"] = request_id
        for key in ("status", "duration_ms"):
            value = getattr(record, key, None)
            if isinstance(value, (int, float)):
                data[key] = value
        result = json.dumps(data, ensure_ascii=False)
        for secret in self.secrets:
            if secret:
                result = result.replace(secret, "[REDACTED]")
        return result


def configure_logging(settings: Settings):
    handler = logging.StreamHandler()
    handler.setFormatter(SafeJsonFormatter(settings))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers = []
        logger.propagate = True
