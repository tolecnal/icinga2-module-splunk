"""Check connectivity to the Icinga2 API, its event stream, and Splunk HEC.

Run this (``icinga2-splunk-check``) before enabling the service to validate
configuration, credentials and TLS settings.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from urllib.parse import urlsplit

import requests

from icinga2_splunk.config import Config, ConfigError
from icinga2_splunk.event_stream_client import (
    silence_insecure_request_warnings,
    subscription_payload,
)
from icinga2_splunk.tls import certificate_names, icinga2_session, is_hostname_mismatch

TIMEOUT = 10.0
OK, FAIL, INFO = "✓", "✗", "i"


def _icinga2_get(config: Config, path: str) -> requests.Response:
    with icinga2_session(config) as session:
        return session.get(
            f"{config.icinga2_api_url}{path}",
            auth=config.icinga2_auth,
            cert=config.icinga2_cert,
            verify=config.icinga2_api_verify_ssl,
            headers={"Accept": "application/json"},
            allow_redirects=False,
            timeout=TIMEOUT,
        )


def _icinga2_connection_error(config: Config, exc: requests.RequestException) -> None:
    print(f"{FAIL} Connection error: {exc}")
    if not is_hostname_mismatch(exc):
        return
    url = urlsplit(config.icinga2_api_url)
    names = certificate_names(url.hostname or "", url.port or 443, config.icinga2_api_verify_ssl)
    if names:
        print(f"  Hint: the Icinga2 certificate is valid for: {', '.join(names)}")
        print(
            "        Use one of these as the host in icinga2_api_url, or set "
            f'"icinga2_api_tls_server_name": "{names[0]}"'
        )
    else:
        print(
            "  Hint: use the Icinga2 node name (the certificate's CN) as the host in "
            "icinga2_api_url, or set icinga2_api_tls_server_name"
        )


def _fail_response(response: requests.Response) -> None:
    print(f"{FAIL} HTTP {response.status_code}")
    print(f"  Response: {response.text[:500]}")
    if 300 <= response.status_code < 400:  # noqa: PLR2004
        print(
            f"  Hint: redirected to {response.headers.get('Location', '?')}; redirects are "
            "not followed, so configure the final URL"
        )
    elif response.status_code == 401:  # noqa: PLR2004
        print("  Hint: check the API username/password or client certificate")
    elif response.status_code == 403:  # noqa: PLR2004
        print("  Hint: the API user lacks the required permissions")


def check_icinga2_api(config: Config) -> bool:
    """Check basic Icinga2 API connectivity using ``/v1/status``."""
    print("\n=== Icinga2 API ===")
    try:
        response = _icinga2_get(config, "/v1/status/IcingaApplication")
    except requests.RequestException as exc:
        _icinga2_connection_error(config, exc)
        return False

    if response.status_code != 200:  # noqa: PLR2004
        _fail_response(response)
        return False

    print(f"{OK} Connected to {config.icinga2_api_url}")
    try:
        app = response.json()["results"][0]["status"]["icingaapplication"]["app"]
        print(f"  Icinga2 version: {app.get('version', 'unknown')}")
    except (ValueError, KeyError, IndexError, TypeError):
        pass
    return True


def check_event_stream(config: Config) -> bool:
    """Check that the API user may subscribe to the configured event types."""
    print("\n=== Icinga2 event stream ===")
    payload = subscription_payload(config, queue="icinga2-splunk-check")
    try:
        with (
            icinga2_session(config) as session,
            session.post(
                f"{config.icinga2_api_url}/v1/events",
                json=payload,
                auth=config.icinga2_auth,
                cert=config.icinga2_cert,
                verify=config.icinga2_api_verify_ssl,
                headers={"Accept": "application/json"},
                allow_redirects=False,
                timeout=TIMEOUT,
                stream=True,
            ) as response,
        ):
            if response.status_code != 200:  # noqa: PLR2004
                _fail_response(response)
                return False
            print(f"{OK} Subscribed to event stream; waiting up to {TIMEOUT:.0f}s for an event")
            try:
                for line in response.iter_lines():
                    if line:
                        print(f"{OK} Received an event")
                        break
            except requests.RequestException:
                print(f"{INFO} No events received within the timeout (normal if idle)")
    except requests.RequestException as exc:
        _icinga2_connection_error(config, exc)
        return False
    return True


def check_splunk_hec(config: Config) -> bool:
    """Check that Splunk HEC accepts the token (sends an empty POST to the event endpoint)."""
    print("\n=== Splunk HEC ===")
    headers = {"Authorization": f"Splunk {config.splunk_hec_token}"}
    url = f"{config.splunk_hec_url}/services/collector/event"
    try:
        # An empty POST is rejected with "No data" (HTTP 400, code 5) when the
        # token is valid, without indexing anything.
        response = requests.post(
            url,
            headers=headers,
            data=b"",
            verify=config.splunk_verify_ssl,
            allow_redirects=False,
            timeout=TIMEOUT,
        )
    except requests.RequestException as exc:
        print(f"{FAIL} Connection error: {exc}")
        return False

    try:
        body = response.json()
    except ValueError:
        body = None
    code = body.get("code") if isinstance(body, dict) else None

    accepted = 200 <= response.status_code < 300  # noqa: PLR2004
    no_data = response.status_code == 400 and code == 5  # noqa: PLR2004
    if accepted or no_data:
        print(f"{OK} Splunk HEC at {config.splunk_hec_url} accepted the token")
        return True

    _fail_response(response)
    if response.status_code == 404:  # noqa: PLR2004
        print("  Hint: splunk_hec_url should look like https://splunk.example.com:8088")
    return False


CHECKS: dict[str, Callable[[Config], bool]] = {
    "Icinga2 API": check_icinga2_api,
    "Event stream": check_event_stream,
    "Splunk HEC": check_splunk_hec,
}


def main(argv: list[str] | None = None) -> int:
    """Command line entry point (``icinga2-splunk-check``)."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("-c", "--config", help="path to the JSON configuration file")
    args = parser.parse_args(argv)

    print("Icinga2 -> Splunk connection check")
    print("=" * 40)
    try:
        config = Config.load(args.config)
    except ConfigError as exc:
        print(f"{FAIL} Configuration error: {exc}")
        return 1
    silence_insecure_request_warnings(config)
    file_errors = config.file_errors()
    for error in file_errors:
        print(f"{FAIL} {error}")
    if file_errors:
        return 1
    print(f"{OK} Configuration loaded")

    results = {name: check(config) for name, check in CHECKS.items()}

    print("\n" + "=" * 40)
    for name, passed in results.items():
        print(f"  {name}: {'PASS' if passed else 'FAIL'}")
    if all(results.values()):
        print(f"\n{OK} All checks passed")
        return 0
    print(f"\n{FAIL} Some checks failed")
    return 1


if __name__ == "__main__":
    sys.exit(main())
