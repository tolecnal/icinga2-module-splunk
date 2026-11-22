"""TLS helpers for connections to the Icinga2 API."""

from __future__ import annotations

import socket
import ssl
from typing import Any

import requests
from requests.adapters import HTTPAdapter

from icinga2_splunk.config import Config, VerifySetting


class ServerNameAdapter(HTTPAdapter):
    """Verify the server certificate against a fixed name instead of the URL host.

    Icinga2 certificates are issued for the node name. This lets the API be
    reached through another name (an alias, IP address or load balancer) while
    the certificate is still fully verified - against ``server_name``. The name
    is also sent as SNI.
    """

    def __init__(self, server_name: str, **kwargs: Any) -> None:
        """Create the adapter; ``kwargs`` are passed to :class:`HTTPAdapter`."""
        self.server_name = server_name
        super().__init__(**kwargs)

    def init_poolmanager(self, *args: Any, **pool_kwargs: Any) -> None:
        """Add the expected server name to every connection pool."""
        pool_kwargs["assert_hostname"] = self.server_name
        pool_kwargs["server_hostname"] = self.server_name
        super().init_poolmanager(*args, **pool_kwargs)


def icinga2_session(config: Config) -> requests.Session:
    """Return a session for the Icinga2 API, honouring ``icinga2_api_tls_server_name``."""
    session = requests.Session()
    if config.icinga2_api_tls_server_name:
        session.mount("https://", ServerNameAdapter(config.icinga2_api_tls_server_name))
    return session


def is_hostname_mismatch(exc: BaseException) -> bool:
    """Whether ``exc`` (e.g. a requests.SSLError) failed only on the certificate name."""
    text = str(exc)
    # Python/OpenSSL (urllib3 2.x): "Hostname mismatch, certificate is not valid
    # for 'x'" or "IP address mismatch, ...". urllib3 1.26 (Debian 12, Ubuntu
    # 22.04) matches names itself: "hostname 'x' doesn't match 'y'".
    return "mismatch, certificate is not valid for" in text or (
        "hostname" in text and "doesn't match" in text
    )


def certificate_names(
    host: str, port: int, verify: VerifySetting, timeout: float = 10.0
) -> list[str] | None:
    """Return the DNS names and IPs the server's certificate is valid for.

    The certificate chain is verified with ``verify`` (CA bundle path or the
    system store); only the hostname check is skipped. Returns None if the
    certificate can't be fetched or isn't trusted.
    """
    if verify is False:
        return None
    context = ssl.create_default_context(cafile=verify if isinstance(verify, str) else None)
    context.check_hostname = False
    try:
        with (
            socket.create_connection((host, port), timeout=timeout) as sock,
            context.wrap_socket(sock, server_hostname=host) as tls,
        ):
            # With certificate verification on, this is always a populated dict.
            cert = tls.getpeercert() or {}
    except (OSError, ValueError):
        return None

    # typeshed types getpeercert() loosely; the shapes are documented in ssl.
    san: Any = cert.get("subjectAltName", ())
    subject: Any = cert.get("subject", ())
    names: list[str] = [value for kind, value in san if kind in ("DNS", "IP Address")]
    for rdn in subject:
        for key, value in rdn:
            if key == "commonName" and value not in names:
                names.append(value)
    return names
