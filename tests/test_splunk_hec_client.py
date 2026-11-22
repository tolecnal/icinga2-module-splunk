from __future__ import annotations

import json
import threading
import time
from typing import Any
from unittest import mock

import pytest
import requests

from icinga2_splunk.splunk_hec_client import SplunkHECClient


def response(status: int, text: str = "") -> mock.Mock:
    resp = mock.Mock(spec=requests.Response)
    resp.status_code = status
    resp.ok = 200 <= status < 400
    resp.text = text
    return resp


def make_client(*responses: Any, **kwargs: Any) -> tuple[SplunkHECClient, mock.Mock]:
    session = mock.Mock(spec=requests.Session)
    session.headers = {}
    session.post.side_effect = list(responses) if responses else None
    if not responses:
        session.post.return_value = response(200)
    kwargs.setdefault("retry_backoff", 0)
    client = SplunkHECClient(
        "https://splunk:8088/services/collector", "tok", session=session, **kwargs
    )
    return client, session


def posted_events(session: mock.Mock, call: int = 0) -> list[dict[str, Any]]:
    payload = session.post.call_args_list[call].kwargs["data"].decode()
    return [json.loads(line) for line in payload.splitlines()]


def test_session_setup() -> None:
    client, session = make_client(verify="/ca.pem")
    assert client.endpoint == "https://splunk:8088/services/collector/event"
    assert session.headers["Authorization"] == "Splunk tok"
    assert session.verify == "/ca.pem"


def test_envelope_omits_none_fields() -> None:
    client, session = make_client()
    client.send({"a": 1}, sourcetype="st", index="idx", timestamp=12.5)
    client.send({"b": 2})
    client.flush()

    assert posted_events(session) == [
        {"event": {"a": 1}, "time": 12.5, "sourcetype": "st", "index": "idx"},
        {"event": {"b": 2}},
    ]
    assert client.sent_count == 2
    assert client.pending == 0


def test_flush_splits_into_batches() -> None:
    client, session = make_client(batch_size=2)
    for i in range(5):
        client.send(i)
    client.flush()
    assert session.post.call_count == 3
    assert [len(posted_events(session, i)) for i in range(3)] == [2, 2, 1]


def test_non_serializable_values_are_stringified() -> None:
    client, session = make_client()
    client.send({"when": object.__new__(type("X", (), {"__str__": lambda self: "x"}))})
    client.flush()
    assert posted_events(session)[0]["event"] == {"when": "x"}


def test_circular_event_is_dropped(caplog: pytest.LogCaptureFixture) -> None:
    client, _ = make_client()
    loop: dict[str, Any] = {}
    loop["self"] = loop
    client.send(loop)
    assert client.pending == 0
    assert "cannot be serialized" in caplog.text


def test_queue_bound_drops_oldest(caplog: pytest.LogCaptureFixture) -> None:
    client, session = make_client(max_queue_size=3, batch_size=10)
    for i in range(5):
        client.send(i)
    assert client.dropped_count == 2
    client.flush()
    assert [e["event"] for e in posted_events(session)] == [2, 3, 4]
    assert "queue full" in caplog.text


def test_retry_on_transient_errors() -> None:
    client, session = make_client(
        requests.ConnectionError("boom"), response(503, "busy"), response(200), max_retries=3
    )
    client.send(1)
    client.flush()
    assert session.post.call_count == 3
    assert client.sent_count == 1
    assert client.dropped_count == 0


def test_gives_up_after_max_retries(caplog: pytest.LogCaptureFixture) -> None:
    client, session = make_client(*[response(500)] * 3, max_retries=2)
    client.send(1)
    client.flush()
    assert session.post.call_count == 3
    assert client.dropped_count == 1
    assert "Dropped 1 events" in caplog.text


def test_no_retry_on_client_error() -> None:
    client, session = make_client(response(403, '{"text":"Invalid token","code":4}'))
    client.send(1)
    client.flush()
    assert session.post.call_count == 1
    assert client.dropped_count == 1


def test_worker_flushes_on_batch_size() -> None:
    client, session = make_client(batch_size=2, batch_timeout=60)
    client.start()
    client.start()  # idempotent
    try:
        client.send(1)
        client.send(2)
        deadline = time.monotonic() + 5
        while session.post.call_count == 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert session.post.call_count == 1
    finally:
        client.close()


def test_worker_flushes_on_timeout() -> None:
    client, session = make_client(batch_size=100, batch_timeout=0.05)
    client.start()
    try:
        client.send(1)
        deadline = time.monotonic() + 5
        while session.post.call_count == 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert session.post.call_count == 1
    finally:
        client.close()


def test_close_flushes_and_closes_session() -> None:
    client, session = make_client(batch_timeout=60)
    client.start()
    client.send(1)
    client.close(timeout=5)
    assert session.post.call_count == 1
    session.close.assert_called_once()


def test_worker_survives_unexpected_errors(caplog: pytest.LogCaptureFixture) -> None:
    done = threading.Event()

    def post(*_args: Any, **_kwargs: Any) -> Any:
        if not done.is_set():
            done.set()
            raise RuntimeError("unexpected")
        return response(200)

    client, session = make_client(batch_size=1, batch_timeout=0.01)
    session.post.side_effect = post
    client.start()
    try:
        client.send(1)
        assert done.wait(5)
        client.send(2)
        deadline = time.monotonic() + 5
        while client.sent_count == 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert client.sent_count == 1
    finally:
        client.close()
    assert "Unexpected error in Splunk flush worker" in caplog.text
