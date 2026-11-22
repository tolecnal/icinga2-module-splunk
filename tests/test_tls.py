"""TLS tests against a real local HTTPS server (certificates from trustme)."""

from __future__ import annotations

import json
import ssl
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
import requests

from icinga2_splunk import check_connection as cc
from icinga2_splunk.tls import (
    ServerNameAdapter,
    certificate_names,
    icinga2_session,
    is_hostname_mismatch,
)

trustme = pytest.importorskip("trustme")

NODE = "icinga-node.example"
STATUS = {"results": [{"status": {"icingaapplication": {"app": {"version": "r2.14.2"}}}}]}


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = json.dumps(STATUS).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args: Any) -> None:
        pass


@pytest.fixture(scope="module")
def ca() -> Any:
    return trustme.CA()


@pytest.fixture(scope="module")
def ca_file(ca: Any, tmp_path_factory: pytest.TempPathFactory) -> str:
    path = tmp_path_factory.mktemp("ca") / "ca.crt"
    ca.cert_pem.write_to_path(str(path))
    return str(path)


@pytest.fixture(scope="module")
def server(ca: Any) -> Iterator[int]:
    """HTTPS server on 127.0.0.1 whose certificate is only valid for NODE."""
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert(NODE).configure_cert(context)
    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield httpd.server_address[1]
    httpd.shutdown()
    httpd.server_close()


def config(make_config: Any, port: int, ca_file: str, **overrides: Any) -> Any:
    return make_config(
        icinga2_api_url=f"https://127.0.0.1:{port}",
        icinga2_api_verify_ssl=ca_file,
        **overrides,
    )


def get(cfg: Any) -> requests.Response:
    with icinga2_session(cfg) as session:
        return session.get(
            f"{cfg.icinga2_api_url}/v1/status", verify=cfg.icinga2_api_verify_ssl, timeout=5
        )


# ------------------------------------------------------------- server name


def test_hostname_mismatch_without_server_name(make_config, server: int, ca_file: str) -> None:
    with pytest.raises(requests.exceptions.SSLError) as excinfo:
        get(config(make_config, server, ca_file))
    assert is_hostname_mismatch(excinfo.value)


def test_server_name_verifies_against_node_name(make_config, server: int, ca_file: str) -> None:
    cfg = config(make_config, server, ca_file, icinga2_api_tls_server_name=NODE)
    assert get(cfg).json() == STATUS


def test_wrong_server_name_still_fails(make_config, server: int, ca_file: str) -> None:
    cfg = config(make_config, server, ca_file, icinga2_api_tls_server_name="other.example")
    with pytest.raises(requests.exceptions.SSLError):
        get(cfg)


def test_server_name_does_not_skip_chain_verification(
    make_config, server: int, tmp_path: Path
) -> None:
    untrusted = tmp_path / "other-ca.crt"
    trustme.CA().cert_pem.write_to_path(str(untrusted))
    cfg = config(make_config, server, str(untrusted), icinga2_api_tls_server_name=NODE)
    with pytest.raises(requests.exceptions.SSLError) as excinfo:
        get(cfg)
    assert not is_hostname_mismatch(excinfo.value)


def test_session_without_server_name_uses_default_adapter(make_config) -> None:
    session = icinga2_session(make_config())
    assert not isinstance(session.get_adapter("https://x"), ServerNameAdapter)


def test_event_stream_client_uses_server_name(make_config, server: int, ca_file: str) -> None:
    from icinga2_splunk.event_stream_client import EventStreamClient

    cfg = config(make_config, server, ca_file, icinga2_api_tls_server_name=NODE, spool_dir="")
    client = EventStreamClient(cfg)
    session = client._session_factory()
    assert isinstance(session.get_adapter(cfg.icinga2_api_url), ServerNameAdapter)


# -------------------------------------------------------- certificate names


def test_certificate_names(server: int, ca_file: str) -> None:
    assert certificate_names("127.0.0.1", server, ca_file) == [NODE]


def test_certificate_names_with_ip_and_common_name(ca: Any, ca_file: str) -> None:
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("127.0.0.1", common_name="node-cn").configure_cert(context)
    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        names = certificate_names("127.0.0.1", httpd.server_address[1], ca_file)
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert names == ["127.0.0.1", "node-cn"]


@pytest.mark.parametrize("verify", [False, True])
def test_certificate_names_untrusted_or_disabled(server: int, verify: bool) -> None:
    # True means the system CA store, which doesn't trust the test CA.
    assert certificate_names("127.0.0.1", server, verify) is None


def test_certificate_names_unreachable(ca_file: str) -> None:
    assert certificate_names("127.0.0.1", 1, ca_file, timeout=1) is None


# ---------------------------------------------------------- check command


def test_check_prints_certificate_names(make_config, server: int, ca_file: str, capsys) -> None:
    assert cc.check_icinga2_api(config(make_config, server, ca_file)) is False
    out = capsys.readouterr().out
    assert f"certificate is valid for: {NODE}" in out
    assert f'"icinga2_api_tls_server_name": "{NODE}"' in out


def test_check_passes_with_server_name(make_config, server: int, ca_file: str, capsys) -> None:
    cfg = config(make_config, server, ca_file, icinga2_api_tls_server_name=NODE)
    assert cc.check_icinga2_api(cfg) is True
    assert "r2.14.2" in capsys.readouterr().out


def test_check_generic_hint_when_names_unknown(
    make_config, server: int, ca_file: str, capsys
) -> None:
    with mock.patch.object(cc, "certificate_names", return_value=None):
        assert cc.check_icinga2_api(config(make_config, server, ca_file)) is False
    assert "use the Icinga2 node name" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("Hostname mismatch, certificate is not valid for 'x'.", True),
        ("IP address mismatch, certificate is not valid for '10.0.0.1'.", True),
        ("hostname '127.0.0.1' doesn't match 'icinga-node.example'", True),
        ("hostname 'a' doesn't match either of 'b', 'c'", True),
        ("certificate verify failed: unable to get local issuer certificate", False),
    ],
)
def test_is_hostname_mismatch_messages(message: str, expected: bool) -> None:
    assert is_hostname_mismatch(Exception(message)) is expected
