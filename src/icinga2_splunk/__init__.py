"""Icinga2 to Splunk integration.

Streams Icinga2 events and performance data to Splunk using the Icinga2 API
event stream and the Splunk HTTP Event Collector (HEC).
"""

from __future__ import annotations

from typing import Any

__version__ = "2.0.2"

# Submodules are imported lazily so that ``python -m icinga2_splunk.<module>``
# doesn't import the module twice (which makes runpy emit a RuntimeWarning).
_LAZY = {
    "Config": "icinga2_splunk.config",
    "ConfigError": "icinga2_splunk.config",
    "EventStreamClient": "icinga2_splunk.event_stream_client",
    "Metric": "icinga2_splunk.perfdata_parser",
    "parse_perfdata": "icinga2_splunk.perfdata_parser",
    "SplunkHECClient": "icinga2_splunk.splunk_hec_client",
}

__all__ = [
    "Config",
    "ConfigError",
    "EventStreamClient",
    "Metric",
    "SplunkHECClient",
    "__version__",
    "parse_perfdata",
]


def __getattr__(name: str) -> Any:
    """Import public names on first access."""
    module_name = _LAZY.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib  # noqa: PLC0415

    return getattr(importlib.import_module(module_name), name)


def __dir__() -> list[str]:
    """List module attributes including lazily imported ones."""
    return sorted(set(globals()) | set(__all__))
