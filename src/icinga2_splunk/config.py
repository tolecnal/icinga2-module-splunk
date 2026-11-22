"""Configuration handling for the Icinga2 -> Splunk integration.

Configuration is read from a JSON file (``/etc/icinga2/splunk_config.json`` by
default, overridable with ``ICINGA2_SPLUNK_CONFIG``) and may be overridden by
environment variables. The result is a validated, typed :class:`Config`.
"""

from __future__ import annotations

import json
import logging
import os
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, TypeAlias
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "/etc/icinga2/splunk_config.json"

#: ``verify`` value accepted by :mod:`requests`: a bool or a path to a CA bundle.
VerifySetting: TypeAlias = bool | str

DEFAULT_EVENT_TYPES = (
    "StateChange",
    "Notification",
    "AcknowledgementSet",
    "AcknowledgementCleared",
    "DowntimeAdded",
    "DowntimeRemoved",
)

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

# Older key names that were documented or accepted in earlier releases.
_LEGACY_KEYS = {
    "event_stream_types": "event_types",
    "splunk_batch_size": "batch_size",
    "splunk_batch_interval": "batch_timeout",
}

_HEC_ENDPOINT_SUFFIXES = (
    "/services/collector/event/1.0",
    "/services/collector/event",
    "/services/collector/raw",
    "/services/collector",
)


class ConfigError(ValueError):
    """Raised when the configuration cannot be loaded or is invalid."""


def _parse_bool(value: str) -> bool:
    lowered = value.strip().lower()
    if lowered in ("1", "true", "yes", "on"):
        return True
    if lowered in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"not a boolean: {value!r}")


def _parse_verify(value: str) -> VerifySetting:
    """Parse a TLS verify setting: a boolean, or a path to a CA bundle."""
    try:
        return _parse_bool(value)
    except ValueError:
        return value


def _parse_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_int_list(value: str) -> list[int]:
    return [int(item) for item in _parse_list(value)]


# Environment variable -> (config key, parser). Values are parsed per key so
# that e.g. a numeric password is never turned into an integer.
_ENV_OVERRIDES: dict[str, tuple[str, Callable[[str], Any]]] = {
    "ICINGA2_API_URL": ("icinga2_api_url", str),
    "ICINGA2_API_USERNAME": ("icinga2_api_username", str),
    "ICINGA2_API_PASSWORD": ("icinga2_api_password", str),
    "ICINGA2_API_CLIENT_CERT": ("icinga2_api_client_cert", str),
    "ICINGA2_API_CLIENT_KEY": ("icinga2_api_client_key", str),
    "ICINGA2_API_VERIFY_SSL": ("icinga2_api_verify_ssl", _parse_verify),
    "SPLUNK_HEC_URL": ("splunk_hec_url", str),
    "SPLUNK_HEC_TOKEN": ("splunk_hec_token", str),
    "SPLUNK_VERIFY_SSL": ("splunk_verify_ssl", _parse_verify),
    "SPLUNK_INDEX_EVENTS": ("splunk_index_events", str),
    "SPLUNK_INDEX_PERFDATA": ("splunk_index_perfdata", str),
    "EVENT_TYPES": ("event_types", _parse_list),
    "EVENT_STREAM_QUEUE": ("event_stream_queue", str),
    "EVENT_STREAM_FILTER": ("event_stream_filter", str),
    "INCLUDE_CHECK_RESULTS": ("include_check_results", _parse_bool),
    "CHECK_RESULT_STATE_FILTER": ("check_result_state_filter", _parse_int_list),
    "INCLUDE_PERFDATA": ("include_perfdata", _parse_bool),
    "BATCH_SIZE": ("batch_size", int),
    "BATCH_TIMEOUT": ("batch_timeout", float),
    "MAX_QUEUE_SIZE": ("max_queue_size", int),
    "MAX_RETRIES": ("max_retries", int),
    "HEARTBEAT_TIMEOUT": ("heartbeat_timeout", float),
    "LOG_LEVEL": ("log_level", str),
}


def normalize_hec_url(url: str) -> str:
    """Return the HEC base URL, stripping any ``/services/collector...`` suffix."""
    url = url.strip().rstrip("/")
    for suffix in _HEC_ENDPOINT_SUFFIXES:
        if url.endswith(suffix):
            return url[: -len(suffix)]
    return url


@dataclass
class Config:
    """Validated configuration for the Icinga2 -> Splunk integration.

    Secrets are excluded from ``repr()`` so the object can be logged safely.
    """

    icinga2_api_url: str
    splunk_hec_url: str
    splunk_hec_token: str = field(repr=False)

    icinga2_api_username: str | None = None
    icinga2_api_password: str | None = field(default=None, repr=False)
    icinga2_api_client_cert: str | None = None
    icinga2_api_client_key: str | None = None
    icinga2_api_verify_ssl: VerifySetting = True

    splunk_verify_ssl: VerifySetting = True
    splunk_index_events: str = "icinga2_events"
    splunk_index_perfdata: str = "icinga2_perfdata"

    event_types: list[str] = field(default_factory=lambda: list(DEFAULT_EVENT_TYPES))
    event_stream_queue: str = "splunk"
    event_stream_filter: str = ""
    include_check_results: bool = False
    check_result_state_filter: list[int] = field(default_factory=lambda: [1, 2, 3])
    include_perfdata: bool = True

    batch_size: int = 100
    batch_timeout: float = 10.0
    max_queue_size: int = 10_000
    max_retries: int = 3
    heartbeat_timeout: float = 300.0
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        """Normalize values and validate the configuration."""
        self.icinga2_api_url = self.icinga2_api_url.strip().rstrip("/")
        self.splunk_hec_url = normalize_hec_url(self.splunk_hec_url)
        self.log_level = self.log_level.upper()
        self._validate()

    # ------------------------------------------------------------------ loading

    @classmethod
    def load(
        cls,
        path: str | os.PathLike[str] | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> Config:
        """Load configuration from a JSON file and environment overrides.

        Args:
            path: Config file path. Defaults to ``$ICINGA2_SPLUNK_CONFIG`` or
                ``/etc/icinga2/splunk_config.json``.
            environ: Environment mapping (defaults to :data:`os.environ`).

        Raises:
            ConfigError: If the file cannot be parsed or the result is invalid.
        """
        env = os.environ if environ is None else environ
        config_path = Path(path or env.get("ICINGA2_SPLUNK_CONFIG", DEFAULT_CONFIG_PATH))

        data = _read_config_file(config_path)
        data = _apply_legacy_keys(data)
        data.update(_env_overrides(env))
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Config:
        """Build a config from a plain mapping, ignoring (but logging) unknown keys."""
        known = {f.name for f in fields(cls)}
        unknown = sorted(set(data) - known)
        if unknown:
            logger.warning("Ignoring unknown configuration keys: %s", ", ".join(unknown))

        required = ("icinga2_api_url", "splunk_hec_url", "splunk_hec_token")
        missing = [key for key in required if not data.get(key)]
        if missing:
            raise ConfigError(f"Missing required configuration: {', '.join(missing)}")

        kwargs = {key: value for key, value in data.items() if key in known}
        try:
            return cls(**kwargs)
        except (TypeError, AttributeError) as exc:
            raise ConfigError(f"Invalid configuration: {exc}") from exc

    # --------------------------------------------------------------- validation

    def _validate(self) -> None:
        errors = [*self._connection_errors(), *self._option_errors()]
        if errors:
            raise ConfigError("Invalid configuration: " + "; ".join(errors))
        self._warn_insecure()

    def _connection_errors(self) -> list[str]:
        errors: list[str] = []
        for name in ("icinga2_api_url", "splunk_hec_url"):
            parts = urlsplit(getattr(self, name))
            if parts.scheme not in ("http", "https") or not parts.netloc:
                errors.append(f"{name} must be an http(s) URL")

        if self.icinga2_auth is None and self.icinga2_cert is None:
            errors.append(
                "Icinga2 API authentication required: provide either "
                "icinga2_api_username/icinga2_api_password or "
                "icinga2_api_client_cert/icinga2_api_client_key"
            )

        errors.extend(
            f"{name} must be a boolean or a CA bundle path"
            for name in ("icinga2_api_verify_ssl", "splunk_verify_ssl")
            if not isinstance(getattr(self, name), (bool, str))
        )
        return errors

    def _option_errors(self) -> list[str]:
        def is_int(value: object) -> bool:
            return isinstance(value, int) and not isinstance(value, bool)

        def is_number(value: object) -> bool:
            return isinstance(value, (int, float)) and not isinstance(value, bool)

        checks: list[tuple[bool, str]] = [
            (
                isinstance(self.include_check_results, bool),
                "include_check_results must be a boolean",
            ),
            (isinstance(self.include_perfdata, bool), "include_perfdata must be a boolean"),
            (
                is_int(self.batch_size) and self.batch_size > 0,
                "batch_size must be a positive integer",
            ),
            (
                is_int(self.max_queue_size) and self.max_queue_size > 0,
                "max_queue_size must be a positive integer",
            ),
            (
                is_int(self.max_retries) and self.max_retries >= 0,
                "max_retries must be a non-negative integer",
            ),
            (
                is_number(self.batch_timeout) and self.batch_timeout > 0,
                "batch_timeout must be a positive number",
            ),
            (
                is_number(self.heartbeat_timeout) and self.heartbeat_timeout > 0,
                "heartbeat_timeout must be a positive number",
            ),
            (
                isinstance(self.event_types, list)
                and all(isinstance(t, str) for t in self.event_types),
                "event_types must be a list of strings",
            ),
            (
                isinstance(self.check_result_state_filter, list)
                and all(is_int(s) for s in self.check_result_state_filter),
                "check_result_state_filter must be a list of integers",
            ),
            (self.log_level in LOG_LEVELS, f"log_level must be one of {', '.join(LOG_LEVELS)}"),
        ]
        return [message for ok, message in checks if not ok]

    def _warn_insecure(self) -> None:
        if self.icinga2_api_verify_ssl is False:
            logger.warning(
                "TLS verification for the Icinga2 API is disabled; set "
                "icinga2_api_verify_ssl to the Icinga2 CA path (e.g. "
                "/var/lib/icinga2/certs/ca.crt) instead"
            )
        if self.splunk_verify_ssl is False:
            logger.warning("TLS verification for Splunk HEC is disabled")
        if self.splunk_hec_url.startswith("http://"):
            logger.warning("Splunk HEC URL uses plain HTTP; the HEC token is sent unencrypted")
        if self.icinga2_api_url.startswith("http://"):
            logger.warning("Icinga2 API URL uses plain HTTP; credentials are sent unencrypted")

    # ------------------------------------------------------------------ helpers

    @property
    def icinga2_auth(self) -> tuple[str, str] | None:
        """HTTP basic auth tuple for the Icinga2 API, if configured."""
        if self.icinga2_api_username and self.icinga2_api_password:
            return (self.icinga2_api_username, self.icinga2_api_password)
        return None

    @property
    def icinga2_cert(self) -> tuple[str, str] | None:
        """Client certificate/key tuple for the Icinga2 API, if configured."""
        if self.icinga2_api_client_cert and self.icinga2_api_client_key:
            return (self.icinga2_api_client_cert, self.icinga2_api_client_key)
        return None


def _read_config_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        logger.warning("Config file not found: %s, using environment variables only", path)
        return {}

    try:
        mode = path.stat().st_mode
        if mode & stat.S_IROTH:
            logger.warning(
                "Config file %s is world-readable and may contain secrets; "
                "consider 'chmod 0640 %s'",
                path,
                path,
            )
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"Failed to load config from {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError(f"Config file {path} must contain a JSON object")
    return data


def _apply_legacy_keys(data: dict[str, Any]) -> dict[str, Any]:
    result = dict(data)
    for old, new in _LEGACY_KEYS.items():
        if old in result:
            value = result.pop(old)
            if new in result:
                logger.warning("Ignoring deprecated key %r because %r is also set", old, new)
            else:
                logger.warning("Config key %r is deprecated, use %r instead", old, new)
                result[new] = value
    return result


def _env_overrides(env: Mapping[str, str]) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    for var, (key, parse) in _ENV_OVERRIDES.items():
        raw = env.get(var)
        if raw is None:
            continue
        try:
            overrides[key] = parse(raw)
        except ValueError as exc:
            raise ConfigError(f"Invalid value for environment variable {var}: {exc}") from exc
    return overrides
