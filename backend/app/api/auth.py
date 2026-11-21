import hashlib
import hmac
from enum import StrEnum
from typing import Annotated, Protocol

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from app.core.config import Settings
from app.core.errors import AppError


class Role(StrEnum):
    USER = "user"
    DATA_ADMIN = "data_admin"
    SYSTEM_ADMIN = "system_admin"


class Identity(BaseModel):
    user_id: str
    role: Role


class IdentityProvider(Protocol):
    def authenticate(self, token: str) -> Identity | None: ...


class EnvironmentIdentityProvider:
    def __init__(self, settings: Settings):
        self.credentials = settings.auth_credentials

    def authenticate(self, token: str) -> Identity | None:
        digest = hashlib.sha256(token.encode()).digest()
        for credential in self.credentials:
            expected = hashlib.sha256(credential.token.get_secret_value().encode()).digest()
            if hmac.compare_digest(digest, expected):
                return Identity(user_id=credential.user_id, role=Role(credential.role))
        return None


bearer = HTTPBearer(auto_error=False)


def current_identity(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
) -> Identity:
    settings = request.app.state.settings
    # Invalid explicit credentials must never fall back to the development identity.
    if request.headers.get("authorization") is not None:
        identity = (
            request.app.state.identity_provider.authenticate(credentials.credentials)
            if credentials
            else None
        )
        if identity is None:
            raise AppError(401, "UNAUTHENTICATED", "Valid Bearer credentials required")
        return identity
    if settings.environment == "development" and settings.dev_identity_enabled:
        return Identity(user_id=settings.dev_user_id, role=Role(settings.dev_role))
    raise AppError(401, "UNAUTHENTICATED", "Bearer credentials required")


def require_role(role: Role):
    def dependency(identity: Annotated[Identity, Depends(current_identity)]) -> Identity:
        if identity.role != role:
            raise AppError(403, "FORBIDDEN", "Insufficient permissions")
        return identity

    return dependency


def require_owner(identity: Identity, owner_id: str):
    if identity.user_id != owner_id:
        raise AppError(403, "FORBIDDEN", "Resource belongs to another user")
