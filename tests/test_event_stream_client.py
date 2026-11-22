from __future__ import annotations

import json
import signal
from typing import Any
from unittest import mock

import pytest
import requests

from icinga2_splunk import event_stream_client as esc
from icinga2_splunk.event_stream_client import EventStreamClient


@pytest.fixture
def client(make_config, splunk) -> EventStreamClient:
    return EventStreamClient(make_config(), splunk=splunk)


# ---------------------------------------------------------------- subscription


def test_subscription_adds_check_result_for_perfdata(make_config, splunk) -> None:
    cfg = make_config(event_types=["StateChange", "StateChange"], event_stream_filter="")
    payload = EventStreamClient(cfg, splunk=splunk).subscription()
    assert payload == {"queue": "splunk", "types": ["StateChange", "CheckResult"]}


def test_subscription_without_check_results(make_config, splunk) -> None:
    cfg = make_config(
        event_types=["StateChange"],
        include_perfdata=False,
        event_stream_filter='host.vars.os == "Linux"',
        event_stream_queue="q",
    )
    payload = EventStreamClient(cfg, splunk=splunk).subscription()
    assert payload == {"queue": "q", "types": ["StateChange"], "filter": 'host.vars.os == "Linux"'}


# -------------------------------------------------------------------- handlers

CHECK_RESULT = {
    "type": "CheckResult",
    "timestamp": 1700000000.5,
    "host": "web1",
    "service": "http",
    "downtime_depth": 0,
    "acknowledgement": False,
    "check_result": {
        "state": 2,
        "exit_status": 2,
        "output": "HTTP CRITICAL",
        "execution_start": 1699999999.0,
        "execution_end": 1700000000.0,
        "command": ["/usr/lib/nagios/plugins/check_http", "-a", "user:hunter2"],
        "performance_data": ["time=0.5s;1;2;0", "size=100B;;;0"],
        "vars_before": {"state": 0},
    },
}


def test_check_result_perfdata_only_by_default(client, splunk) -> None:
    client.handle_event(CHECK_RESULT)
    assert splunk.by_sourcetype("icinga2:checkresult") == []
    perf = splunk.by_sourcetype("icinga2:perfdata")
    assert [p["event"]["metric"] for p in perf] == ["time", "size"]
    first = perf[0]
    assert first["index"] == "icinga2_perfdata"
    assert first["host"] == "web1"
    assert first["timestamp"] == 1700000000.0
    assert first["event"] == {
        "host": "web1",
        "service": "http",
        "object_name": "web1!http",
        "timestamp": 1700000000.0,
        "metric": "time",
        "value": 0.5,
        "unit": "s",
        "warning": 1.0,
        "critical": 2.0,
        "min": 0.0,
        "max": None,
    }


def test_check_result_event_when_enabled(make_config, splunk) -> None:
    client = EventStreamClient(
        make_config(include_check_results=True, include_perfdata=False), splunk=splunk
    )
    client.handle_event(CHECK_RESULT)
    [sent] = splunk.events
    assert sent["sourcetype"] == "icinga2:checkresult"
    assert sent["index"] == "icinga2_events"
    event = sent["event"]
    assert event["state_name"] == "CRITICAL"
    assert event["object_name"] == "web1!http"
    # Command lines can contain credentials and must not be forwarded.
    assert "command" not in event
    assert "hunter2" not in json.dumps(sent)


def test_check_result_state_filter(make_config, splunk) -> None:
    client = EventStreamClient(
        make_config(include_check_results=True, include_perfdata=False), splunk=splunk
    )
    ok = {**CHECK_RESULT, "check_result": {**CHECK_RESULT["check_result"], "state": 0}}
    client.handle_event(ok)
    assert splunk.events == []


def test_empty_state_filter_sends_everything(make_config, splunk) -> None:
    cfg = make_config(
        include_check_results=True, include_perfdata=False, check_result_state_filter=[]
    )
    client = EventStreamClient(cfg, splunk=splunk)
    client.handle_event({**CHECK_RESULT, "check_result": {"state": 0}})
    assert len(splunk.events) == 1


def test_check_result_without_execution_end_uses_event_timestamp(client, splunk) -> None:
    client.handle_event(
        {
            "type": "CheckResult",
            "host": "h",
            "timestamp": 5,
            "check_result": {"performance_data": "a=1"},
        }
    )
    [perf] = splunk.events
    assert perf["timestamp"] == 5.0
    assert perf["event"]["object_name"] == "h"


def test_state_change(client, splunk) -> None:
    client.handle_event(
        {
            "type": "StateChange",
            "timestamp": 1700000000,
            "host": "web1",
            "service": "http",
            "state": 2,
            "state_type": 1,
            "downtime_depth": 0,
            "acknowledgement": False,
            "check_result": {
                "output": "down",
                "performance_data": ["time=1s"],
                "vars_before": {"state": 0, "state_type": 1},
                "vars_after": {"state": 2, "attempt": 3},
            },
        }
    )
    # Regression: perfdata must not be duplicated from StateChange events.
    [sent] = splunk.events
    assert sent["sourcetype"] == "icinga2:statechange"
    event = sent["event"]
    assert event["state_name"] == "CRITICAL"
    assert event["state_type_name"] == "HARD"
    assert event["previous_state"] == 0
    assert event["previous_state_name"] == "OK"
    assert event["attempt"] == 3
    assert event["output"] == "down"


def test_host_state_names(client, splunk) -> None:
    client.handle_event({"type": "StateChange", "host": "h", "state": 1.0, "state_type": 0.0})
    event = splunk.events[0]["event"]
    assert event["state_name"] == "DOWN"
    assert event["state_type_name"] == "SOFT"
    assert event["service"] is None
    assert event["object_name"] == "h"


def test_unknown_state_values(client, splunk) -> None:
    client.handle_event({"type": "StateChange", "host": "h", "state": "x", "state_type": 7})
    event = splunk.events[0]["event"]
    assert event["state_name"] is None
    assert event["state_type_name"] is None


def test_notification(client, splunk) -> None:
    client.handle_event(
        {
            "type": "Notification",
            "timestamp": 10,
            "host": "h",
            "service": "s",
            "notification_type": "PROBLEM",
            "users": ["alice"],
            "author": "",
            "text": "",
            "command": "mail-service-notification",
            "check_result": {"state": 1, "output": "warn"},
        }
    )
    [sent] = splunk.events
    assert sent["sourcetype"] == "icinga2:notification"
    assert sent["event"]["users"] == ["alice"]
    assert sent["event"]["state_name"] == "WARNING"


@pytest.mark.parametrize("event_type", ["AcknowledgementSet", "AcknowledgementCleared"])
def test_acknowledgement(client, splunk, event_type: str) -> None:
    client.handle_event(
        {
            "type": event_type,
            "host": "h",
            "author": "bob",
            "comment": "on it",
            "acknowledgement_cleared_by": "carol",
        }
    )
    [sent] = splunk.events
    assert sent["sourcetype"] == "icinga2:acknowledgement"
    assert sent["event"]["event_type"] == event_type
    assert sent["event"]["author"] == "bob"
    assert sent["event"]["cleared_by"] == "carol"


def test_comment_reads_nested_object(client, splunk) -> None:
    # Regression: host/service live inside the "comment" object.
    client.handle_event(
        {
            "type": "CommentAdded",
            "timestamp": 1,
            "comment": {"host_name": "h", "service_name": "", "author": "a", "text": "t"},
        }
    )
    [sent] = splunk.events
    assert sent["sourcetype"] == "icinga2:comment"
    assert sent["host"] == "h"
    assert sent["event"]["service"] is None
    assert sent["event"]["text"] == "t"


@pytest.mark.parametrize(
    "event_type", ["DowntimeAdded", "DowntimeRemoved", "DowntimeStarted", "DowntimeTriggered"]
)
def test_downtime_reads_nested_object(client, splunk, event_type: str) -> None:
    client.handle_event(
        {
            "type": event_type,
            "timestamp": 1,
            "downtime": {
                "host_name": "h",
                "service_name": "s",
                "author": "a",
                "comment": "maint",
                "start_time": 10,
                "end_time": 20,
                "fixed": True,
            },
        }
    )
    [sent] = splunk.events
    assert sent["sourcetype"] == "icinga2:downtime"
    assert sent["event"]["object_name"] == "h!s"
    assert sent["event"]["end_time"] == 20


def test_unknown_event_type_forwarded_raw(client, splunk) -> None:
    client.handle_event({"type": "ObjectCreated", "object_type": "Host", "object_name": "x"})
    [sent] = splunk.events
    assert sent["sourcetype"] == "icinga2:objectcreated"
    assert sent["host"] is None
    assert sent["event"]["object_type"] == "Host"


def test_event_without_type_ignored(client, splunk) -> None:
    client.handle_event({"host": "h"})
    assert splunk.events == []


# ------------------------------------------------------------------ connection


class FakeResponse:
    def __init__(self, status: int, lines: list[bytes] | Exception = (), text: str = "") -> None:
        self.status_code = status
        self._lines = lines
        self.text = text

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_: Any) -> None:
        pass

    def iter_lines(self):
        if isinstance(self._lines, Exception):
            raise self._lines
        yield from self._lines


class FakeSession:
    def __init__(self, result: FakeResponse | Exception) -> None:
        self.result = result
        self.calls: list[dict[str, Any]] = []

    def __enter__(self) -> FakeSession:
        return self

    def __exit__(self, *_: Any) -> None:
        pass

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def make_stream_client(make_config, splunk, result, **overrides):
    session = FakeSession(result)
    return EventStreamClient(
        make_config(**overrides), splunk=splunk, session_factory=lambda: session
    ), session


def test_consume_processes_lines(make_config, splunk, caplog) -> None:
    lines = [
        b"",
        json.dumps({"type": "StateChange", "host": "h", "state": 0}).encode(),
        b"not json",
        b"[1, 2]",
        json.dumps(
            {"type": "CheckResult", "host": "h", "check_result": {"performance_data": 5}}
        ).encode(),
    ]
    client, session = make_stream_client(
        make_config, splunk, FakeResponse(200, lines), icinga2_api_verify_ssl="/ca.crt"
    )
    assert client._consume() is True

    call = session.calls[0]
    assert call["url"] == "https://icinga.example.com:5665/v1/events"
    assert call["auth"] == ("splunk", "secret")
    assert call["verify"] == "/ca.crt"
    assert call["stream"] is True
    assert call["timeout"] == (esc.CONNECT_TIMEOUT, 300.0)
    assert len(splunk.events) == 1
    assert "Failed to decode event JSON" in caplog.text
    assert "non-object event" in caplog.text
    assert "closed by Icinga2" in caplog.text


def test_consume_survives_handler_errors(make_config, splunk, caplog) -> None:
    line = json.dumps({"type": "StateChange", "host": "h"}).encode()
    client, _ = make_stream_client(make_config, splunk, FakeResponse(200, [line, line]))
    with mock.patch.object(client, "_handle_state_change", side_effect=[RuntimeError("x"), None]):
        client._handlers["StateChange"] = client._handle_state_change
        assert client._consume() is True
    assert "Error processing StateChange event" in caplog.text


def test_consume_http_error(make_config, splunk, caplog) -> None:
    client, _ = make_stream_client(make_config, splunk, FakeResponse(401, text="Unauthorized"))
    assert client._consume() is False
    assert "HTTP 401" in caplog.text


def test_consume_connection_error(make_config, splunk, caplog) -> None:
    client, _ = make_stream_client(make_config, splunk, requests.ConnectionError("refused"))
    assert client._consume() is False
    assert "refused" in caplog.text


def test_consume_read_timeout_after_connect_resets_backoff(make_config, splunk) -> None:
    client, _ = make_stream_client(
        make_config, splunk, FakeResponse(200, requests.ConnectionError("Read timed out"))
    )
    assert client._consume() is True


def test_consume_stops_when_requested(make_config, splunk) -> None:
    line = json.dumps({"type": "StateChange", "host": "h"}).encode()
    client, _ = make_stream_client(make_config, splunk, FakeResponse(200, [line, line]))
    client.stop()
    assert client._consume() is True
    assert splunk.events == []


def test_run_reconnects_with_backoff(make_config, splunk) -> None:
    client = EventStreamClient(make_config(), splunk=splunk)
    results = iter([False, False, True, False])
    delays: list[float] = []

    def consume() -> bool:
        try:
            return next(results)
        except StopIteration:
            client.stop()
            return False

    with (
        mock.patch.object(client, "_consume", side_effect=consume),
        mock.patch.object(client._stop, "wait", side_effect=delays.append),
    ):
        client.run()

    assert splunk.started
    # 1, 2 after failures; reset to 1 after a successful connection; then 2.
    assert delays == [1.0, 2.0, 1.0, 2.0]


def test_backoff_is_capped(make_config, splunk) -> None:
    client = EventStreamClient(make_config(), splunk=splunk)
    delays: list[float] = []

    def wait(delay: float) -> None:
        delays.append(delay)
        if len(delays) == 10:
            client.stop()

    with (
        mock.patch.object(client, "_consume", return_value=False),
        mock.patch.object(client._stop, "wait", side_effect=wait),
    ):
        client.run()
    assert max(delays) == esc.RECONNECT_DELAY_MAX


def test_close_flushes_splunk(client, splunk) -> None:
    client.close()
    assert splunk.closed


def test_default_splunk_client_built_from_config(make_config) -> None:
    client = EventStreamClient(make_config(batch_size=7, splunk_verify_ssl="/ca.pem"))
    assert client.splunk.batch_size == 7
    assert client.splunk.endpoint == "https://splunk.example.com:8088/services/collector/event"


# ------------------------------------------------------------------------ main


def test_main_config_error(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("SPLUNK_HEC_TOKEN", raising=False)
    assert esc.main(["--config", str(tmp_path / "missing.json")]) == esc.EXIT_CONFIG_ERROR


def test_main_runs_and_flushes_on_signal(tmp_path, base_settings, monkeypatch) -> None:
    path = tmp_path / "c.json"
    path.write_text(json.dumps({**base_settings, "log_level": "DEBUG"}))
    path.chmod(0o600)

    instances: list[mock.Mock] = []

    def fake_client(_config):
        instance = mock.Mock()
        instance.run.side_effect = lambda: esc._raise_shutdown(signal.SIGTERM, None)
        instances.append(instance)
        return instance

    original = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    monkeypatch.setattr(esc, "EventStreamClient", fake_client)
    try:
        assert esc.main(["-c", str(path)]) == 0
    finally:
        for sig, handler in original.items():
            signal.signal(sig, handler)
    instances[0].close.assert_called_once()
