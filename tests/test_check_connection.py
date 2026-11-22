from __future__ import annotations

import json
from typing import Any
from unittest import mock

import pytest
import requests

from icinga2_splunk import check_connection as cc


def response(status: int, body: Any = None, lines: list[bytes] | Exception = ()) -> mock.MagicMock:
    resp = mock.MagicMock()
    resp.status_code = status
    resp.ok = 200 <= status < 400
    resp.text = json.dumps(body) if body is not None else ""
    if body is None:
        resp.json.side_effect = ValueError("no json")
    else:
        resp.json.return_value = body
    if isinstance(lines, Exception):
        resp.iter_lines.side_effect = lines
    else:
        resp.iter_lines.return_value = iter(lines)
    resp.__enter__.return_value = resp
    return resp


STATUS = {"results": [{"status": {"icingaapplication": {"app": {"version": "r2.14.2"}}}}]}


def test_icinga2_api_ok(make_config, capsys) -> None:
    with mock.patch.object(cc.requests, "get", return_value=response(200, STATUS)) as get:
        assert cc.check_icinga2_api(make_config()) is True
    assert "r2.14.2" in capsys.readouterr().out
    assert get.call_args.kwargs["auth"] == ("splunk", "secret")


def test_icinga2_api_unexpected_body(make_config) -> None:
    with mock.patch.object(cc.requests, "get", return_value=response(200, {"results": []})):
        assert cc.check_icinga2_api(make_config()) is True


@pytest.mark.parametrize(("status", "hint"), [(401, "username"), (403, "permissions"), (500, "")])
def test_icinga2_api_http_errors(make_config, capsys, status: int, hint: str) -> None:
    with mock.patch.object(cc.requests, "get", return_value=response(status)):
        assert cc.check_icinga2_api(make_config()) is False
    assert hint in capsys.readouterr().out


def test_icinga2_api_connection_error(make_config) -> None:
    with mock.patch.object(cc.requests, "get", side_effect=requests.ConnectionError("x")):
        assert cc.check_icinga2_api(make_config()) is False


@pytest.mark.parametrize(
    ("resp", "expected", "text"),
    [
        (response(200, lines=[b"", b"{}"]), True, "Received an event"),
        (response(200, lines=requests.ConnectionError("timeout")), True, "No events"),
        (response(403), False, "permissions"),
    ],
)
def test_event_stream(make_config, capsys, resp, expected: bool, text: str) -> None:
    with mock.patch.object(cc.requests, "post", return_value=resp):
        assert cc.check_event_stream(make_config()) is expected
    assert text in capsys.readouterr().out


def test_event_stream_connection_error(make_config) -> None:
    with mock.patch.object(cc.requests, "post", side_effect=requests.ConnectionError("x")):
        assert cc.check_event_stream(make_config()) is False


@pytest.mark.parametrize(
    ("resp", "expected"),
    [
        (response(400, {"text": "No data", "code": 5}), True),
        (response(200, {"text": "Success", "code": 0}), True),
        (response(403, {"text": "Invalid token", "code": 4}), False),
        (response(404), False),
    ],
)
def test_splunk_hec(make_config, resp, expected: bool) -> None:
    with mock.patch.object(cc.requests, "post", return_value=resp) as post:
        assert cc.check_splunk_hec(make_config()) is expected
    assert post.call_args.args[0] == "https://splunk.example.com:8088/services/collector/event"


def test_splunk_hec_connection_error(make_config) -> None:
    with mock.patch.object(cc.requests, "post", side_effect=requests.ConnectionError("x")):
        assert cc.check_splunk_hec(make_config()) is False


def test_main_all_pass(tmp_path, base_settings, monkeypatch) -> None:
    path = tmp_path / "c.json"
    path.write_text(json.dumps(base_settings))
    path.chmod(0o600)
    monkeypatch.setattr(cc, "CHECKS", {"a": lambda _c: True})
    assert cc.main(["-c", str(path)]) == 0
    monkeypatch.setattr(cc, "CHECKS", {"a": lambda _c: True, "b": lambda _c: False})
    assert cc.main(["-c", str(path)]) == 1


def test_main_config_error(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("SPLUNK_HEC_TOKEN", raising=False)
    assert cc.main(["-c", str(tmp_path / "missing.json")]) == 1


def test_event_stream_ends_without_event(make_config, capsys) -> None:
    with mock.patch.object(cc.requests, "post", return_value=response(200, lines=[])):
        assert cc.check_event_stream(make_config()) is True
    assert "Received an event" not in capsys.readouterr().out
