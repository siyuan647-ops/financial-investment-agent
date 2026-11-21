"""Environment-only configuration. Never print raw validation exceptions."""

import re
from decimal import Decimal
from ipaddress import ip_address
from typing import Literal
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


def nonblank_token(value: SecretStr | None) -> SecretStr | None:
    if value is not None and not value.get_secret_value().strip():
        raise ValueError("token must not be blank")
    return value


def valid_host(host: str | None) -> bool:
    if not host:
        return False
    try:
        ip_address(host)
        return True
    except ValueError:
        pass
    # Numeric dotted hosts must be valid IP addresses, not fallback DNS names.
    if re.fullmatch(r"[0-9.]+", host):
        return False
    try:
        ascii_host = host.encode("idna").decode("ascii").removesuffix(".")
    except UnicodeError:
        return False
    return len(ascii_host) <= 253 and all(
        re.fullmatch(r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?", label)
        for label in ascii_host.split(".")
    )


class Credential(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    token: SecretStr = Field(min_length=32)
    user_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    role: Literal["user", "data_admin", "system_admin"]

    @field_validator("token")
    @classmethod
    def validate_token(cls, value):
        return nonblank_token(value)


class QwenProfile(BaseModel):
    """Per-purpose model and token/cost budget. Prices are per million tokens."""

    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    model: str = Field(min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9_.:-]+$")
    max_output_tokens: int = Field(default=2048, ge=1, le=131072)
    input_price_per_million: Decimal | None = Field(default=None, ge=0, le=1_000_000)
    output_price_per_million: Decimal | None = Field(default=None, ge=0, le=1_000_000)
    currency: str = Field(default="CNY", pattern=r"^[A-Z]{3}$")

    @model_validator(mode="after")
    def validate_prices(self):
        if (self.input_price_per_million is None) != (self.output_price_per_million is None):
            raise ValueError("input and output prices must be configured together")
        return self


class SpecialistModelProfile(QwenProfile):
    """Optional complete model profile for one research role."""

    temperature: float | None = Field(default=None, ge=0, le=2)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="APP_", extra="ignore", hide_input_in_errors=True)
    environment: Literal["development", "test", "production"] = "production"
    database_url: SecretStr
    redis_url: SecretStr
    prefect_url: SecretStr = SecretStr("http://127.0.0.1:4200/api")
    prefect_worker_url: SecretStr = SecretStr("http://127.0.0.1:8080")
    readiness_timeout_seconds: float = Field(default=3, gt=0, le=10)
    data_source_url: SecretStr
    data_source_token: SecretStr = Field(min_length=1)
    qwen_url: SecretStr | None = None
    qwen_token: SecretStr | None = None
    qwen_model: str | None = None
    qwen_profiles: dict[Literal["plan", "generate", "verify"], QwenProfile] = Field(
        default_factory=dict
    )
    specialist_model_profiles: dict[
        Literal["fundamental", "news", "market", "investment", "risk"],
        SpecialistModelProfile,
    ] = Field(default_factory=dict)
    qwen_default_max_output_tokens: int = Field(default=2048, ge=1, le=131072)
    qwen_connect_timeout_seconds: float = Field(default=5, gt=0, le=60)
    qwen_read_timeout_seconds: float = Field(default=120, gt=0, le=600)
    qwen_total_timeout_seconds: float = Field(default=180, gt=0, le=1800)
    qwen_max_retries: int = Field(default=2, ge=0, le=5)
    qwen_backoff_seconds: float = Field(default=1, gt=0, le=30)
    qwen_max_response_bytes: int = Field(default=2097152, ge=1024, le=16777216)
    deep_research_timeout_seconds: float = Field(default=300, ge=30, le=1800)
    deep_research_max_tokens: int = Field(default=50000, ge=1000, le=1000000)
    deep_research_max_supplemental_rounds: int = Field(default=1, ge=0, le=3)
    deep_research_lease_seconds: int = Field(default=60, ge=10, le=600)
    deep_research_role_timeout_seconds: float = Field(default=120, gt=0, le=600)
    deep_research_role_max_tokens: int = Field(default=16000, ge=100, le=131072)
    rate_limit_per_minute: int = Field(default=60, ge=1, le=100000)
    connect_timeout_seconds: float = Field(default=5, gt=0, le=60)
    read_timeout_seconds: float = Field(default=30, gt=0, le=300)
    total_timeout_seconds: float = Field(default=60, gt=0, le=600)
    provider_max_retries: int = Field(default=2, ge=0, le=5)
    provider_backoff_seconds: float = Field(default=1, gt=0, le=30)
    provider_max_response_bytes: int = Field(default=2097152, ge=1024, le=16777216)
    provider_failure_threshold: int = Field(default=3, ge=1, le=20)
    provider_cooldown_seconds: float = Field(default=30, gt=0, le=600)
    query_cache_ttl_seconds: int = Field(default=300, ge=1, le=86400)
    api_max_page_size: int = Field(default=200, ge=1, le=1000)
    auth_credentials: list[Credential] = Field(default_factory=list)
    dev_identity_enabled: bool = False
    dev_user_id: str = Field(default="local-user", pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    dev_role: Literal["user", "data_admin", "system_admin"] = "user"

    @field_validator("data_source_token", "qwen_token")
    @classmethod
    def validate_token(cls, value):
        return nonblank_token(value)

    @field_validator("qwen_model")
    @classmethod
    def validate_model_name(cls, value):
        if value is not None and not re.fullmatch(r"[a-zA-Z0-9_./:-]{1,200}", value):
            raise ValueError("invalid model name")
        return value

    @field_validator(
        "database_url",
        "redis_url",
        "data_source_url",
        "qwen_url",
        "prefect_url",
        "prefect_worker_url",
    )
    @classmethod
    def validate_url(cls, value: SecretStr | None, info):
        if value is None:
            return value
        schemes = {
            "database_url": {"postgresql", "postgresql+psycopg"},
            "redis_url": {"redis", "rediss"},
        }.get(info.field_name, {"http", "https"})
        try:
            raw = value.get_secret_value()
            if any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in raw):
                raise ValueError("invalid service URL")
            parsed = urlsplit(raw)
            valid = parsed.scheme in schemes and valid_host(parsed.hostname) and not parsed.fragment
            _ = parsed.port
        except ValueError:
            valid = False
        if not valid:
            raise ValueError("invalid service URL")
        return value

    @model_validator(mode="after")
    def validate_boundaries(self):
        if self.dev_identity_enabled and self.environment != "development":
            raise ValueError("dev_identity_enabled requires development environment")
        if not self.auth_credentials and not self.dev_identity_enabled:
            raise ValueError("auth_credentials required unless development identity is enabled")
        tokens = [item.token.get_secret_value() for item in self.auth_credentials]
        if len(tokens) != len(set(tokens)):
            raise ValueError("auth_credentials tokens must be unique")
        users = [item.user_id for item in self.auth_credentials]
        if len(users) != len(set(users)):
            raise ValueError("auth_credentials user IDs must be unique")
        if self.total_timeout_seconds < max(
            self.connect_timeout_seconds, self.read_timeout_seconds
        ):
            raise ValueError("total_timeout_seconds must cover connect and read timeouts")
        qwen_enabled = any(
            (
                self.qwen_url,
                self.qwen_token,
                self.qwen_model,
                self.qwen_profiles,
                self.specialist_model_profiles,
            )
        )
        if qwen_enabled:
            if not self.qwen_url or not self.qwen_token:
                raise ValueError("qwen_url and qwen_token are required when Qwen is configured")
            missing = {
                purpose
                for purpose in ("plan", "generate", "verify")
                if purpose not in self.qwen_profiles and not self.qwen_model
            }
            if missing:
                raise ValueError("qwen_model or a profile for every purpose is required")
        if self.qwen_total_timeout_seconds < max(
            self.qwen_connect_timeout_seconds, self.qwen_read_timeout_seconds
        ):
            raise ValueError("qwen_total_timeout_seconds must cover Qwen connect and read timeouts")
        return self


class ConfigurationError(RuntimeError):
    """Safe startup failure with field names, never supplied values."""


def safe_error_location(location: tuple) -> str:
    if not location:
        return "configuration"
    root, *tail = location
    if root not in Settings.model_fields:
        return "<unknown_field>"
    parts = [root]
    for index, part in enumerate(tail):
        if root == "auth_credentials" and index == 0 and isinstance(part, int):
            parts.append(str(part))
        elif root == "auth_credentials" and index == 1 and part in Credential.model_fields:
            parts.append(part)
        else:
            parts.append("<unknown_field>")
    return ".".join(parts)


def load_settings() -> Settings:
    try:
        return Settings()
    except ValidationError as exc:
        issues = [
            f"{safe_error_location(e['loc'])}: {e['msg']}"
            for e in exc.errors(include_input=False, include_context=False, include_url=False)
        ]
        raise ConfigurationError("CONFIGURATION_INVALID: " + "; ".join(issues)) from None
    except Exception:
        # SettingsError may include malformed JSON or environment values.
        raise ConfigurationError("CONFIGURATION_INVALID: invalid environment encoding") from None
