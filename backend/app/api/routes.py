from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api.analysis import router as analysis_router
from app.api.auth import Identity, Role, current_identity, require_role
from app.api.data import router as data_router
from app.api.fundamental import router as fundamental_router
from app.core.readiness import check_dependencies

router = APIRouter()
router.include_router(data_router)
router.include_router(analysis_router)
router.include_router(fundamental_router)


@router.get("/health/live")
def live():
    return {"status": "alive", "scope": "application"}


@router.get("/health/ready")
async def ready(request: Request):
    checks = await check_dependencies(request.app.state.settings)
    healthy = all(checks.values())
    return JSONResponse(
        {"status": "ready" if healthy else "not_ready", "dependencies": checks},
        status_code=200 if healthy else 503,
    )


@router.get("/metrics", include_in_schema=False)
async def metrics(request: Request):
    metrics = request.app.state.metrics
    checks = await check_dependencies(request.app.state.settings)
    for name, healthy in checks.items():
        metrics.dependencies.labels(name).set(int(healthy))
    await metrics.refresh_tasks(request.app.state.settings)
    await metrics.refresh_dataops(request.app.state.settings)
    return Response(
        generate_latest(metrics.registry), headers={"Content-Type": CONTENT_TYPE_LATEST}
    )


@router.get("/api/v1/me", response_model=Identity)
def me(identity: Annotated[Identity, Depends(current_identity)]):
    return identity


@router.get("/api/v1/data/access")
def data_access(identity: Annotated[Identity, Depends(require_role(Role.DATA_ADMIN))]):
    return {"permission": "data:manage", "user_id": identity.user_id}


@router.get("/api/v1/system/access")
def system_access(identity: Annotated[Identity, Depends(require_role(Role.SYSTEM_ADMIN))]):
    return {"permission": "system:manage", "user_id": identity.user_id}
