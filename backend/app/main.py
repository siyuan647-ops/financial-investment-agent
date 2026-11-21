import logging
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException

from app.api.auth import EnvironmentIdentityProvider
from app.api.routes import router
from app.core.config import Settings, load_settings
from app.core.errors import (
    AppError,
    app_error_handler,
    error_response,
    http_error_handler,
    validation_error_handler,
)
from app.core.logging import configure_logging
from app.observability.metrics import Metrics

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    configure_logging(settings)
    app = FastAPI(
        title="A-share research API",
        version="0.1.0",
        docs_url="/docs" if settings.environment == "development" else None,
        redoc_url=None,
        openapi_url="/openapi.json" if settings.environment == "development" else None,
    )
    app.state.settings = settings
    app.state.metrics = Metrics()
    app.state.identity_provider = EnvironmentIdentityProvider(settings)
    app.add_exception_handler(AppError, app_error_handler)
    app.add_exception_handler(HTTPException, http_error_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        # Generate server-side IDs; untrusted client IDs can contain secrets or log injections.
        request.state.request_id = uuid4().hex
        start = perf_counter()
        event = "request_finished"
        try:
            response = await call_next(request)
        except Exception:
            event = "request_failed"
            response = error_response(request, 500, "INTERNAL_ERROR", "Internal server error")
        response.headers["X-Request-ID"] = request.state.request_id
        app.state.metrics.requests.labels(f"{response.status_code // 100}xx").inc()
        logger.info(
            "",
            extra={
                "event": event,
                "request_id": request.state.request_id,
                "status": response.status_code,
                "duration_ms": round((perf_counter() - start) * 1000, 2),
            },
        )
        return response

    app.include_router(router)
    logger.info("", extra={"event": "application_started"})
    return app
