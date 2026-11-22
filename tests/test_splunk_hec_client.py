from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
import requests

from icinga2_splunk import splunk_hec_client as hec
from icinga2_splunk.splunk_hec_client import SplunkHECClient
from icinga2_splunk.spool import DiskSpool


def response(status: int, body: dict[str, Any] | None = None) -> mock.Mock:
    resp = mock.Mock(spec=requests.Response)
    resp.status_code = status
    resp.ok = 200 <= status < 400
    resp.text = json.dumps(body) if body is not None else ""
    if body is None:
        resp.json.side_effect = ValueError("no json")
    else:
        resp.json.return_value = body
    return resp


OK = {"text": "Success", "code": 0}
Handler = Callable[[list[dict[str, Any]]], Any]


class FakeSplunk:
    """Fake requests.Session; ``handler`` maps the posted events to a response."""

    def __init__(self, handler: Handler | None = None) -> None:
        self.headers: dict[str, str] = {}
        self.verify: Any = True
        self.handler: Handler = handler or (lambda _events: response(200, OK))
        self.posts: list[list[dict[str, Any]]] = []
        self.received: list[Any] = []
        self.closed = False
        self.lock = threading.Lock()

    def post(self, url: str, data: bytes, timeout: float, *, allow_redirects: bool = True) -> Any:
        assert allow_redirects is False
        events = [json.loads(line) for line in data.decode().splitlines()]
        with self.lock:
            self.posts.append(events)
        result = self.handler(events)
        if isinstance(result, Exception):
            raise result
        if result.ok:
            with self.lock:
                self.received.extend(e["event"] for e in events)
        return result

    def close(self) -> None:
        self.closed = True


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def down(_events: list[dict[str, Any]]) -> Exception:
    return requests.ConnectionError("connection refused")


def make_client(
    fake: FakeSplunk | None = None, **kwargs: Any
) -> tuple[SplunkHECClient, FakeSplunk]:
    fake = fake or FakeSplunk()
    client = SplunkHECClient(
        "https://splunk:8088/services/collector",
        "tok",
        session=fake,  # type: ignore[arg-type]
        **kwargs,
    )
    return client, fake


_open_spools: list[DiskSpool] = []


def make_spool(tmp_path: Path, **kwargs: Any) -> DiskSpool:
    kwargs.setdefault("max_bytes", 10 * 1024 * 1024)
    kwargs.setdefault("max_age", 86400)
    spool = DiskSpool(tmp_path / "spool", **kwargs)
    _open_spools.append(spool)
    return spool


@pytest.fixture(autouse=True)
def _close_spools() -> Any:
    yield
    while _open_spools:
        _open_spools.pop().close()


# ------------------------------------------------------------------- basics


def test_session_setup() -> None:
    client, fake = make_client(verify="/ca.pem")
    assert client.endpoint == "https://splunk:8088/services/collector/event"
    assert fake.headers["Authorization"] == "Splunk tok"
    assert fake.verify == "/ca.pem"


def test_envelope_omits_none_fields() -> None:
    client, fake = make_client()
    client.send({"a": 1}, sourcetype="st", index="idx", timestamp=12.5)
    client.send({"b": 2})
    assert client.flush() is True
    assert fake.posts[0] == [
        {"event": {"a": 1}, "time": 12.5, "sourcetype": "st", "index": "idx"},
        {"event": {"b": 2}},
    ]
    assert client.sent_count == 2
    assert client.pending == 0


def test_flush_splits_into_batches() -> None:
    client, fake = make_client(batch_size=2)
    for i in range(5):
        client.send(i)
    client.flush()
    assert [len(p) for p in fake.posts] == [2, 2, 1]


def test_non_serializable_values_are_stringified() -> None:
    client, fake = make_client()
    client.send({"when": object.__new__(type("X", (), {"__str__": lambda self: "x"}))})
    client.flush()
    assert fake.received == [{"when": "x"}]


def test_circular_event_is_dropped(caplog: pytest.LogCaptureFixture) -> None:
    client, _ = make_client()
    loop: dict[str, Any] = {}
    loop["self"] = loop
    client.send(loop)
    assert client.pending == 0
    assert "cannot be serialized" in caplog.text


def test_memory_queue_bound_drops_oldest(caplog: pytest.LogCaptureFixture) -> None:
    client, fake = make_client(max_queue_size=3, batch_size=10)
    for i in range(5):
        client.send(i)
    assert client.dropped_count == 2
    client.flush()
    assert fake.received == [2, 3, 4]
    assert "queue full" in caplog.text


# ------------------------------------------------------------ bad payloads


def test_invalid_event_number_skips_only_that_event(caplog: pytest.LogCaptureFixture) -> None:
    def handler(events: list[dict[str, Any]]) -> Any:
        for i, e in enumerate(events):
            if e["event"] == "BAD":
                # HEC indexes events before the bad one, then stops.
                return response(
                    400, {"text": "Invalid data format", "code": 6, "invalid-event-number": i}
                )
        return response(200, OK)

    accepted: list[Any] = []
    fake = FakeSplunk(handler)
    client, _ = make_client(fake)
    original_post = fake.post

    def post(url: str, data: bytes, timeout: float, *, allow_redirects: bool = True) -> Any:
        result = original_post(url, data, timeout, allow_redirects=allow_redirects)
        if not result.ok:
            events = [json.loads(line)["event"] for line in data.decode().splitlines()]
            accepted.extend(events[: json.loads(result.text)["invalid-event-number"]])
        return result

    fake.post = post  # type: ignore[method-assign]
    for e in [1, 2, "BAD", 3, "BAD", 4]:
        client.send(e)
    assert client.flush() is True
    assert accepted + fake.received == [1, 2, 3, 4]
    assert client.sent_count == 4
    assert client.dropped_count == 2
    assert "discarded" in caplog.text


def test_bad_request_without_event_number_bisects() -> None:
    def handler(events: list[dict[str, Any]]) -> Any:
        if any(e["event"] == "BAD" for e in events):
            return response(400, {"text": "Invalid data format", "code": 6})
        return response(200, OK)

    client, fake = make_client(FakeSplunk(handler), batch_size=8)
    for e in [0, 1, 2, "BAD", 4, 5, 6, 7]:
        client.send(e)
    assert client.flush() is True
    assert fake.received == [0, 1, 2, 4, 5, 6, 7]
    assert client.dropped_count == 1


def test_payload_too_large_is_split() -> None:
    def handler(events: list[dict[str, Any]]) -> Any:
        return response(413) if len(events) > 2 else response(200, OK)

    client, fake = make_client(FakeSplunk(handler), batch_size=8)
    for i in range(8):
        client.send(i)
    assert client.flush() is True
    assert fake.received == list(range(8))
    assert client.dropped_count == 0


def test_single_oversized_event_is_discarded() -> None:
    client, _ = make_client(FakeSplunk(lambda _e: response(413)))
    client.send("huge")
    assert client.flush() is True
    assert client.dropped_count == 1


@pytest.mark.parametrize(
    ("status", "body"),
    [
        (400, {"text": "Incorrect index", "code": 7, "invalid-event-number": 0}),
        (400, {"text": "Data channel is missing", "code": 10}),
        (400, None),
        (401, {"text": "Token is required", "code": 2}),
        (403, {"text": "Invalid token", "code": 4}),
        (404, None),
        (429, None),
        (500, None),
        (503, {"text": "Server is busy", "code": 9}),
    ],
)
def test_unavailable_responses_keep_events(status: int, body: Any) -> None:
    fake = FakeSplunk(lambda _e: response(status, body))
    client, _ = make_client(fake)
    client.send(1)
    client.send(2)
    assert client.flush() is False
    assert client.in_outage
    assert client.dropped_count == 0
    assert client.pending == 2

    fake.handler = lambda _e: response(200, OK)
    assert client.flush() is True
    assert fake.received == [1, 2]


def test_partial_bisect_failure_keeps_remainder() -> None:
    calls = {"n": 0}

    def handler(events: list[dict[str, Any]]) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            return response(413)
        if calls["n"] == 2:
            return response(200, OK)  # first half accepted
        return requests.ConnectionError("gone")  # then Splunk goes away

    client, fake = make_client(FakeSplunk(handler), batch_size=4)
    for i in range(4):
        client.send(i)
    assert client.flush() is False
    assert fake.received == [0, 1]
    assert client.pending == 2  # only the undelivered half is kept


# ------------------------------------------------------------ outage logic


def test_backoff_doubles_and_is_capped(caplog: pytest.LogCaptureFixture) -> None:
    clock = Clock()
    client, _ = make_client(FakeSplunk(down), clock=clock, retry_initial=1.0, retry_max=8.0)
    client.send(1)
    waits = []
    for _ in range(6):
        clock.now = client._next_attempt
        client._cycle()
        waits.append(client._next_attempt - clock.now)
    assert waits == [1.0, 2.0, 4.0, 8.0, 8.0, 8.0]
    assert caplog.text.count("Splunk unavailable") == 1


def test_no_requests_before_next_attempt() -> None:
    clock = Clock()
    client, fake = make_client(FakeSplunk(down), clock=clock, retry_initial=10.0)
    client.send(1)
    client._cycle()
    posts = len(fake.posts)
    clock.now += 5
    client.send(2)
    client._cycle()
    assert len(fake.posts) == posts


def test_recovery_logs_and_resets_backoff(caplog: pytest.LogCaptureFixture) -> None:
    clock = Clock()
    fake = FakeSplunk(down)
    client, _ = make_client(fake, clock=clock, retry_initial=1.0)
    client.send(1)
    client._cycle()
    clock.now = client._next_attempt
    client._cycle()
    fake.handler = lambda _e: response(200, OK)
    clock.now = client._next_attempt
    with caplog.at_level(logging.INFO):
        client._cycle()
    assert not client.in_outage
    assert fake.received == [1]
    assert "available again" in caplog.text
    assert client._backoff == 1.0


def test_outage_status_report(caplog: pytest.LogCaptureFixture, tmp_path: Path) -> None:
    clock = Clock()
    client, _ = make_client(
        FakeSplunk(down), clock=clock, retry_initial=1000.0, spool=make_spool(tmp_path)
    )
    client.send(1)
    client._cycle()
    clock.now += hec.OUTAGE_REPORT_INTERVAL + 1
    client._cycle()
    assert "still unavailable" in caplog.text
    assert "MB spooled" in caplog.text


def test_auth_failure_logged_as_error(caplog: pytest.LogCaptureFixture) -> None:
    client, _ = make_client(FakeSplunk(lambda _e: response(403, {"code": 4})))
    client.send(1)
    client.flush()
    assert any(r.levelno == logging.ERROR for r in caplog.records)


def test_repeated_failures_logged_quietly(caplog: pytest.LogCaptureFixture) -> None:
    client, _ = make_client(FakeSplunk(down))
    client.send(1)
    client.flush()
    client.flush()
    assert caplog.text.count("Could not deliver") == 1


# ------------------------------------------------------------- disk spool


def test_outage_spills_to_disk_and_replays_in_order(tmp_path: Path) -> None:
    clock = Clock()
    spool = make_spool(tmp_path)
    fake = FakeSplunk(down)
    client, _ = make_client(fake, clock=clock, spool=spool, batch_size=3)
    for i in range(5):
        client.send(i)
    client._cycle()
    assert client.pending == 0
    assert spool.size_bytes > 0

    # More events arrive during the outage; they are spooled as well.
    client.send(5)
    clock.now += 0.1
    client._cycle()
    assert client.pending == 0

    fake.handler = lambda _e: response(200, OK)
    client.send(6)
    clock.now = client._next_attempt
    client._cycle()
    assert fake.received == list(range(7))
    assert not spool
    assert not list((tmp_path / "spool").glob("*.jsonl"))


def test_spool_survives_restart(tmp_path: Path) -> None:
    client, _ = make_client(FakeSplunk(down), spool=make_spool(tmp_path))
    for i in range(3):
        client.send(i)
    client.close()
    assert client.dropped_count == 0

    client2, fake2 = make_client(spool=make_spool(tmp_path))
    client2.send("new")
    assert client2.flush() is True
    assert fake2.received == [0, 1, 2, "new"]


def test_close_when_healthy_delivers(tmp_path: Path) -> None:
    client, fake = make_client(spool=make_spool(tmp_path), batch_timeout=60)
    client.start()
    client.send(1)
    client.close(timeout=5)
    assert fake.received == [1]
    assert fake.closed


def test_close_without_spool_during_outage_counts_loss(caplog: pytest.LogCaptureFixture) -> None:
    client, _ = make_client(FakeSplunk(down))
    client.send(1)
    client.flush()
    client.send(2)
    client.close()
    assert client.dropped_count == 2
    assert "no spool configured" in caplog.text


def test_close_with_stuck_worker(caplog: pytest.LogCaptureFixture) -> None:
    release = threading.Event()

    def handler(_events: list[dict[str, Any]]) -> Any:
        release.wait(5)
        return response(200, OK)

    client, _ = make_client(FakeSplunk(handler), batch_size=1)
    client.start()
    client.send(1)
    time.sleep(0.05)
    client.close(timeout=0.05)
    release.set()
    assert "did not stop in time" in caplog.text


def test_replay_keeps_memory_bounded(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    for i in range(10):
        spool.append([json.dumps({"event": i})])
    spool.close()

    fake = FakeSplunk()
    client, _ = make_client(fake, spool=spool, batch_size=1, max_queue_size=4)

    def handler(_events: list[dict[str, Any]]) -> Any:
        if len(fake.posts) <= 10:
            client.send("live")  # live events keep arriving during replay
        return response(200, OK)

    fake.handler = handler
    assert client.flush() is True
    assert client.dropped_count == 0
    assert [e for e in fake.received if e != "live"] == list(range(10))
    assert fake.received.count("live") == 10


# ------------------------------------------------------------ worker thread


def wait_for(predicate: Callable[[], bool], timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def test_worker_flushes_on_batch_size() -> None:
    client, fake = make_client(batch_size=2, batch_timeout=60)
    client.start()
    client.start()  # idempotent
    try:
        client.send(1)
        client.send(2)
        assert wait_for(lambda: len(fake.posts) == 1)
    finally:
        client.close()


def test_worker_flushes_on_timeout() -> None:
    client, fake = make_client(batch_timeout=0.05)
    client.start()
    try:
        client.send(1)
        assert wait_for(lambda: fake.received == [1])
    finally:
        client.close()


def test_worker_recovers_after_outage(tmp_path: Path) -> None:
    fake = FakeSplunk(down)
    client, _ = make_client(
        fake,
        spool=make_spool(tmp_path),
        batch_size=2,
        batch_timeout=0.02,
        retry_initial=0.02,
        retry_max=0.05,
    )
    client.start()
    try:
        for i in range(5):
            client.send(i)
        assert wait_for(lambda: client.in_outage)
        fake.handler = lambda _e: response(200, OK)
        assert wait_for(lambda: len(fake.received) == 5)
        assert sorted(fake.received) == list(range(5))
        assert wait_for(lambda: not client.in_outage)
    finally:
        client.close()


def test_worker_survives_unexpected_errors(caplog: pytest.LogCaptureFixture) -> None:
    calls = {"n": 0}

    def handler(_events: list[dict[str, Any]]) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            return RuntimeError("unexpected")
        return response(200, OK)

    client, fake = make_client(
        FakeSplunk(handler), batch_size=1, batch_timeout=0.01, retry_initial=0.01
    )
    client.start()
    try:
        client.send(1)
        assert wait_for(lambda: fake.received == [1])
    finally:
        client.close()
    assert "Unexpected error in Splunk delivery worker" in caplog.text


# ------------------------------------------------------------- edge cases


def test_flush_fails_while_spool_cannot_be_drained(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    spool.append([json.dumps({"event": "old"})])
    client, fake = make_client(FakeSplunk(down), spool=spool)
    client.send("new")
    assert client.flush() is False
    assert client.pending == 0  # the outage moved the new event to disk too
    fake.handler = lambda _e: response(200, OK)
    assert client.flush() is True
    assert fake.received == ["old", "new"]


def test_cycle_stops_when_spool_replay_fails(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    spool.append([json.dumps({"event": "old"})])
    client, fake = make_client(FakeSplunk(down), spool=spool, clock=Clock())
    client._cycle()
    assert client.in_outage
    assert len(fake.posts) == 1


def test_drain_stops_when_closing(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    spool.append([json.dumps({"event": "old"})])
    client, fake = make_client(spool=spool)
    client._closed.set()
    assert client._drain_spool(stop_on_close=True) is False
    assert fake.posts == []


def test_invalid_last_event_in_batch() -> None:
    def handler(events: list[dict[str, Any]]) -> Any:
        if events[-1]["event"] == "BAD":
            return response(
                400, {"text": "Invalid data format", "code": 6, "invalid-event-number": 1}
            )
        return response(200, OK)

    client, _ = make_client(FakeSplunk(handler))
    client.send("good")
    client.send("BAD")
    assert client.flush() is True
    assert client.sent_count == 1
    assert client.dropped_count == 1


def test_bisect_stops_if_first_half_hits_outage() -> None:
    calls = {"n": 0}

    def handler(_events: list[dict[str, Any]]) -> Any:
        calls["n"] += 1
        return response(413) if calls["n"] == 1 else requests.ConnectionError("gone")

    client, fake = make_client(FakeSplunk(handler), batch_size=4)
    for i in range(4):
        client.send(i)
    assert client.flush() is False
    assert len(fake.posts) == 2  # second half never attempted
    assert client.pending == 4


def test_requeue_without_spool_respects_memory_limit() -> None:
    fake = FakeSplunk()
    client, _ = make_client(fake, batch_size=2, max_queue_size=3)

    def handler(_events: list[dict[str, Any]]) -> Any:
        for i in range(3):
            client.send(f"live{i}")  # queue fills up while the request is in flight
        return requests.ConnectionError("down")

    fake.handler = handler
    client.send("a")
    client.send("b")
    assert client.flush() is False
    assert client.pending == 3
    assert client.dropped_count == 2


def test_nan_event_is_dropped(caplog: pytest.LogCaptureFixture) -> None:
    client, _ = make_client()
    client.send({"v": float("nan")})
    assert client.pending == 0
    assert "cannot be serialized" in caplog.text


def test_many_invalid_events_do_not_recurse() -> None:
    def handler(events: list[dict[str, Any]]) -> Any:
        if events[0]["event"] == "BAD":
            return response(
                400, {"text": "Invalid data format", "code": 6, "invalid-event-number": 0}
            )
        return response(200, OK)

    client, fake = make_client(FakeSplunk(handler), batch_size=600)
    for _ in range(550):
        client.send("BAD")
    client.send("good")
    assert client.flush() is True
    assert client.dropped_count == 550
    assert fake.received == ["good"]


def test_outage_without_spool_does_not_busy_spin() -> None:
    client, _ = make_client(
        FakeSplunk(down), batch_size=2, batch_timeout=5.0, retry_initial=5.0, retry_max=5.0
    )
    cycles = 0
    original_cycle = client._cycle

    def counting_cycle() -> None:
        nonlocal cycles
        cycles += 1
        original_cycle()

    client._cycle = counting_cycle  # type: ignore[method-assign]
    client.start()
    try:
        client.send(1)
        client.send(2)
        assert wait_for(lambda: client.in_outage)
        time.sleep(0.05)
        assert cycles == 1
    finally:
        client.close()


def test_spool_write_error_keeps_worker_alive(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    spool = make_spool(tmp_path)
    fake = FakeSplunk(down)
    client, _ = make_client(
        fake,
        spool=spool,
        batch_size=1,
        batch_timeout=0.02,
        retry_initial=0.02,
        retry_max=0.02,
    )
    with mock.patch.object(spool, "append", side_effect=OSError("No space left on device")):
        client.start()
        try:
            client.send(1)
            assert wait_for(lambda: client.in_outage and client._spool_error)
            assert client._worker is not None and client._worker.is_alive()
            assert "Failed to write" in caplog.text
            # Second cycle hits _spill_memory while spool still fails.
            client.send(2)
            time.sleep(0.05)
            assert client._worker.is_alive()
            fake.handler = lambda _e: response(200, OK)
            assert wait_for(lambda: sorted(fake.received) == [1, 2])
            assert not client._spool_error
        finally:
            client.close()


def test_close_respects_deadline_and_spills_remainder(tmp_path: Path) -> None:
    clock = Clock()
    spool = make_spool(tmp_path)

    def slow_ok(_events: list[dict[str, Any]]) -> Any:
        clock.now += 10.0
        return response(200, OK)

    client, fake = make_client(FakeSplunk(slow_ok), spool=spool, clock=clock, batch_size=1)
    client.send(1)
    client.send(2)
    client.send(3)
    client.close(timeout=5.0)
    assert fake.received == [1]
    assert spool.size_bytes > 0


# ------------------------------------------------------- redirects & disk


def test_redirect_is_not_a_delivery(caplog: pytest.LogCaptureFixture) -> None:
    resp = response(301)
    resp.headers = {"Location": "https://elsewhere/"}
    fake = FakeSplunk(lambda _e: resp)
    client, _ = make_client(fake)
    client.send(1)
    assert client.flush() is False
    assert client.sent_count == 0
    assert client.in_outage
    assert client.pending == 1
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert errors and "redirect to 'https://elsewhere/'" in errors[0].getMessage()


def test_discarded_event_content_only_at_debug(caplog: pytest.LogCaptureFixture) -> None:
    client, _ = make_client(FakeSplunk(lambda _e: response(413)))
    client.send("secret-output")
    with caplog.at_level(logging.DEBUG):
        client.flush()
    error = next(r for r in caplog.records if r.levelno == logging.ERROR)
    assert "secret-output" not in error.getMessage()
    assert any(
        "secret-output" in r.getMessage() for r in caplog.records if r.levelno == logging.DEBUG
    )


def test_spool_close_error_is_logged(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    spool = make_spool(tmp_path)
    client, _ = make_client(spool=spool)
    with mock.patch.object(spool, "close", side_effect=OSError("EIO")):
        client.close()
    assert "Failed to close the disk spool" in caplog.text


def test_enforce_limits_error_is_logged(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    spool = make_spool(tmp_path)
    client, fake = make_client(spool=spool, clock=Clock())
    client.send(1)
    with mock.patch.object(spool, "enforce_limits", side_effect=OSError("EIO")):
        client._cycle()
    assert "Failed to apply disk spool limits" in caplog.text
    assert fake.received == [1]


def test_unreadable_spool_does_not_block_live_events(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    spool = make_spool(tmp_path)
    spool.append([json.dumps({"event": "old"})])
    client, fake = make_client(spool=spool)
    client.send("live")
    with mock.patch.object(spool, "oldest", side_effect=PermissionError("denied")):
        assert client.flush() is True
    assert fake.received == ["live"]
    assert "Failed to read the disk spool" in caplog.text
    assert client.flush() is True  # the backlog is picked up once readable
    assert fake.received == ["live", "old"]


def test_segment_dropped_mid_replay_is_not_resent(tmp_path: Path) -> None:
    spool = make_spool(tmp_path)
    spool.append([json.dumps({"event": i}) for i in range(4)])
    calls = {"n": 0}

    def handler(_events: list[dict[str, Any]]) -> Any:
        calls["n"] += 1
        return response(200, OK) if calls["n"] == 1 else requests.ConnectionError("down")

    fake = FakeSplunk(handler)
    client, _ = make_client(fake, spool=spool, batch_size=2)
    assert client.flush() is False  # 2 delivered, then Splunk goes away
    assert client._replay is not None
    path = client._replay[0]
    spool._drop(path, "test")  # the age/size limit removes the segment meanwhile
    assert spool.dropped_count == 2  # only the undelivered half
    fake.handler = lambda _e: response(200, OK)
    assert client.flush() is True
    assert fake.received == [0, 1]
