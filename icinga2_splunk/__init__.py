"""
Icinga2 to Splunk Integration Module

This module provides real-time integration between Icinga2 and Splunk
using Icinga2 Event Streams and Splunk HTTP Event Collector (HEC).
"""

__version__ = '1.0.0'
__author__ = 'Icinga2-Splunk Integration'

from .config import Config
from .event_stream_client import EventStreamClient
from .splunk_hec_client import SplunkHECClient
from .perfdata_parser import PerfdataParser

__all__ = [
    'Config',
    'EventStreamClient',
    'SplunkHECClient',
    'PerfdataParser'
]

