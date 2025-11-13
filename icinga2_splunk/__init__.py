"""
Icinga2 to Splunk Integration Module

This module provides real-time integration between Icinga2 and Splunk
using Icinga2 Event Streams and Splunk HTTP Event Collector (HEC).
"""

__version__ = "1.0.0"
__author__ = "Icinga2-Splunk Integration"

# Avoid importing submodules at package import time. When the package is
# executed as a module (``python -m icinga2_splunk.event_stream_client``),
# importing submodules here can cause them to be loaded twice which
# triggers a RuntimeWarning from runpy. Provide lazy attribute access so
# consumers can still do ``from icinga2_splunk import Config`` without
# importing all submodules immediately.

__all__ = ["Config", "EventStreamClient", "SplunkHECClient", "PerfdataParser"]


def __getattr__(name: str):
    """Lazily import package attributes on first access.

    This avoids side-effects when the package is imported purely to locate
    a submodule executed with ``python -m``.
    """
    if name == "Config":
        from .config import Config as _Config

        return _Config
    if name == "EventStreamClient":
        from .event_stream_client import EventStreamClient as _EventStreamClient

        return _EventStreamClient
    if name == "SplunkHECClient":
        from .splunk_hec_client import SplunkHECClient as _SplunkHECClient

        return _SplunkHECClient
    if name == "PerfdataParser":
        from .perfdata_parser import PerfdataParser as _PerfdataParser

        return _PerfdataParser
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals().keys()) + __all__)
