from __future__ import annotations

from typing import Any

import pytest

from icinga2_splunk.config import Config


@pytest.fixture
def base_settings() -> dict[str, Any]:
    return {
        "icinga2_api_url": "https://icinga.example.com:5665",
        "icinga2_api_username": "splunk",
        "icinga2_api_password": "secret",
        "splunk_hec_url": "https://splunk.example.com:8088",
        "splunk_hec_token": "token-123",
    }


@pytest.fixture
def make_config(base_settings: dict[str, Any]):
    def factory(**overrides: Any) -> Config:
        return Config.from_dict({**base_settings, **overrides})

    return factory


class RecordingSplunk:
    """Stand-in for SplunkHECClient that records queued events."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.started = False
        self.closed = False
        self.sent_count = 0
        self.dropped_count = 0

    def start(self) -> None:
        self.started = True

    def close(self) -> None:
        self.closed = True

    def send(self, event: Any, **kwargs: Any) -> None:
        self.events.append({"event": event, **kwargs})

    def by_sourcetype(self, sourcetype: str) -> list[dict[str, Any]]:
        return [e for e in self.events if e["sourcetype"] == sourcetype]


@pytest.fixture
def splunk() -> RecordingSplunk:
    return RecordingSplunk()
