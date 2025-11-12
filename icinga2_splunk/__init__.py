"""
Icinga2 to Splunk Integration Module
"""

__version__ = "1.0.0"

from .config import Config
from .splunk_hec_client import SplunkHECClient
from .event_stream_client import EventStreamClient
from .perfdata_parser import parse_perfdata

__all__ = [
    "Config",
    "SplunkHECClient",
    "EventStreamClient",
    "parse_perfdata",
]
