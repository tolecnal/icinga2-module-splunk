"""Icinga2 event stream -> Splunk HEC forwarder.

Subscribes to the Icinga2 API event stream (``/v1/events``), converts each
event into a Splunk event, and forwards performance data from check results as
individual metric events to a separate index.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import signal
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from types import FrameType
from typing import Any

import requests

from icinga2_splunk import __version__
from icinga2_splunk.config import Config, ConfigError
from icinga2_splunk.perfdata_parser import parse_perfdata
from icinga2_splunk.splunk_hec_client import SplunkHECClient
from icinga2_splunk.spool import DiskSpool

logger = logging.getLogger(__name__)

Event = Mapping[str, Any]

CONNECT_TIMEOUT = 10.0
RECONNECT_DELAY_MIN = 1.0
RECONNECT_DELAY_MAX = 60.0
MAX_EVENT_LINE_BYTES = 10 * 1024 * 1024  # 10 MiB

# Exit code for configuration errors (sysexits.h EX_CONFIG); the systemd unit
# uses RestartPreventExitStatus so a broken config doesn't cause a restart loop.
EXIT_CONFIG_ERROR = 78

HOST_STATES = {0: "UP", 1: "DOWN"}
SERVICE_STATES = {0: "OK", 1: "WARNING", 2: "CRITICAL", 3: "UNKNOWN"}
STATE_TYPES = {0: "SOFT", 1: "HARD"}


def subscription_payload(config: Config, queue: str | None = None) -> dict[str, Any]:
    """Return the request body used to subscribe to the Icinga2 event stream."""
    types = list(dict.fromkeys(config.event_types))
    wants_check_results = config.include_check_results or config.include_perfdata
    if wants_check_results and "CheckResult" not in types:
        types.append("CheckResult")

    payload: dict[str, Any] = {"queue": queue or config.event_stream_queue, "types": types}
    if config.event_stream_filter:
        payload["filter"] = config.event_stream_filter
    return payload


def _iter_bounded_lines(
    response: requests.Response, max_bytes: int = MAX_EVENT_LINE_BYTES
) -> Iterator[bytes]:
    """Yield lines from a streaming response, discarding any longer than ``max_bytes``."""
    buf = bytearray()
    dropping = False
    for chunk in response.iter_content(chunk_size=8192):
        if not chunk:
            continue
        start = 0
        while (nl := chunk.find(b"\n", start)) != -1:
            part = chunk[start:nl]
            start = nl + 1
            if dropping:
                dropping = False
                continue
            if len(buf) + len(part) > max_bytes:
                logger.error("Ignoring event line exceeding %d bytes", max_bytes)
                buf.clear()
                continue
            buf.extend(part)
            yield bytes(buf).rstrip(b"\r")
            buf.clear()
        tail = chunk[start:]
        if dropping:
            continue
        if len(buf) + len(tail) > max_bytes:
            logger.error("Ignoring event line exceeding %d bytes", max_bytes)
            buf.clear()
            dropping = True
        else:
            buf.extend(tail)
    if buf and not dropping:
        yield bytes(buf).rstrip(b"\r")


def _object_name(host: str | None, service: str | None) -> str | None:
    return f"{host}!{service}" if service else host


def _state_name(state: Any, service: str | None) -> str | None:
    if isinstance(state, float) and state.is_integer():
        state = int(state)
    names = SERVICE_STATES if service else HOST_STATES
    return names.get(state) if isinstance(state, int) else None


def _state_type_name(state_type: Any) -> str | None:
    if isinstance(state_type, float) and state_type.is_integer():
        state_type = int(state_type)
    return STATE_TYPES.get(state_type) if isinstance(state_type, int) else None


def _as_dict(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def build_splunk_client(config: Config) -> SplunkHECClient:
    """Create the HEC client (and its disk spool, if enabled) from the config.

    Raises:
        OSError: If the spool directory cannot be created or written.
    """
    spool = None
    if config.spool_dir:
        spool = DiskSpool(
            config.spool_dir,
            max_bytes=int(config.spool_max_size_mb * 1_048_576),
            max_age=config.spool_max_age_days * 86_400,
        )
    else:
        logger.warning("No spool_dir configured; events are only buffered in memory")
    return SplunkHECClient(
        config.splunk_hec_url,
        config.splunk_hec_token,
        verify=config.splunk_verify_ssl,
        batch_size=config.batch_size,
        batch_timeout=config.batch_timeout,
        max_queue_size=config.max_queue_size,
        spool=spool,
        retry_max=config.retry_max_interval,
    )


class EventStreamClient:
    """Consume the Icinga2 event stream and forward events to Splunk.

    Args:
        config: Validated configuration.
        splunk: HEC client; created from ``config`` if omitted.
        session_factory: Callable returning a :class:`requests.Session`
            (used for the Icinga2 connection; injectable for tests).
    """

    def __init__(
        self,
        config: Config,
        splunk: SplunkHECClient | None = None,
        session_factory: Callable[[], requests.Session] = requests.Session,
    ) -> None:
        """Set up the client; call :meth:`run` to start consuming."""
        self.config = config
        self.splunk = splunk or build_splunk_client(config)
        self._session_factory = session_factory
        self._stop = threading.Event()
        self._handlers: dict[str, Callable[[Event], None]] = {
            "CheckResult": self._handle_check_result,
            "StateChange": self._handle_state_change,
            "Notification": self._handle_notification,
            "AcknowledgementSet": self._handle_acknowledgement,
            "AcknowledgementCleared": self._handle_acknowledgement,
            "CommentAdded": self._handle_comment,
            "CommentRemoved": self._handle_comment,
            "DowntimeAdded": self._handle_downtime,
            "DowntimeRemoved": self._handle_downtime,
            "DowntimeStarted": self._handle_downtime,
            "DowntimeTriggered": self._handle_downtime,
        }

    # -------------------------------------------------------------- lifecycle

    def run(self) -> None:
        """Consume the event stream until :meth:`stop` is called, reconnecting on failure."""
        logger.info("Starting Icinga2 -> Splunk event stream client %s", __version__)
        self.splunk.start()
        delay = RECONNECT_DELAY_MIN
        attempt = 0

        while not self._stop.is_set():
            connected = self._consume()
            if self._stop.is_set():
                break
            if connected:
                delay, attempt = RECONNECT_DELAY_MIN, 0
            attempt += 1
            logger.warning(
                "Event stream disconnected; reconnecting in %.0f s (attempt %d)", delay, attempt
            )
            self._stop.wait(delay)
            delay = min(delay * 2, RECONNECT_DELAY_MAX)

        logger.info("Event stream client stopped")

    def stop(self) -> None:
        """Request the main loop to exit."""
        self._stop.set()

    def close(self) -> None:
        """Stop the client and flush all pending events to Splunk."""
        self.stop()
        self.splunk.close()
        spool = getattr(self.splunk, "spool", None)
        dropped = self.splunk.dropped_count + (spool.dropped_count if spool is not None else 0)
        logger.info(
            "Splunk client closed: %d events sent, %d dropped",
            self.splunk.sent_count,
            dropped,
        )

    # ------------------------------------------------------------ connection

    def subscription(self) -> dict[str, Any]:
        """Return the request body used to subscribe to the event stream."""
        return subscription_payload(self.config)

    def _consume(self) -> bool:
        """Open one event stream connection and process it until it ends.

        Returns:
            True if the connection was established (so backoff can be reset).
        """
        url = f"{self.config.icinga2_api_url}/v1/events"
        payload = self.subscription()
        logger.info("Connecting to %s (types: %s)", url, ", ".join(payload["types"]))

        connected = False
        with self._session_factory() as session:
            try:
                # The read timeout doubles as a heartbeat: if Icinga2 sends
                # nothing for heartbeat_timeout seconds the connection is
                # considered dead and re-established.
                with session.post(
                    url,
                    json=payload,
                    headers={"Accept": "application/json"},
                    auth=self.config.icinga2_auth,
                    cert=self.config.icinga2_cert,
                    verify=self.config.icinga2_api_verify_ssl,
                    stream=True,
                    allow_redirects=False,
                    timeout=(CONNECT_TIMEOUT, self.config.heartbeat_timeout),
                ) as response:
                    if response.status_code != 200:  # noqa: PLR2004
                        logger.error(
                            "Event stream subscription failed: HTTP %s - %s",
                            response.status_code,
                            response.text[:500],
                        )
                        return False

                    connected = True
                    logger.info("Connected to Icinga2 event stream")
                    for line in _iter_bounded_lines(response):
                        if self._stop.is_set():
                            break
                        if line:
                            self._process_line(line)
                    else:
                        logger.warning("Event stream closed by Icinga2")
            except requests.RequestException as exc:
                # Includes the read timeout used as heartbeat.
                logger.error("Event stream connection error: %s", exc)  # noqa: TRY400
        return connected

    def _process_line(self, line: bytes) -> None:
        try:
            event = json.loads(line)
        except ValueError as exc:
            logger.error("Failed to decode event JSON: %s", exc)  # noqa: TRY400
            return
        if not isinstance(event, dict):
            logger.warning("Ignoring non-object event: %.200r", event)
            return
        try:
            self.handle_event(event)
        except Exception:  # one bad event must not kill the stream
            logger.exception("Error processing %s event", event.get("type"))

    # --------------------------------------------------------------- dispatch

    def handle_event(self, event: Event) -> None:
        """Convert a single Icinga2 event and queue it for Splunk."""
        event_type = event.get("type")
        if not isinstance(event_type, str):
            return
        handler = self._handlers.get(event_type, self._handle_generic)
        handler(event)

    def _send(self, data: dict[str, Any], sourcetype: str, timestamp: float) -> None:
        self.splunk.send(
            data,
            sourcetype=sourcetype,
            host=data.get("host"),
            timestamp=timestamp,
            index=self.config.splunk_index_events,
        )

    @staticmethod
    def _timestamp(event: Event) -> float:
        ts = event.get("timestamp")
        if isinstance(ts, (int, float)) and not isinstance(ts, bool) and math.isfinite(ts):
            return float(ts)
        return time.time()

    @staticmethod
    def _base(event: Event, host: str | None, service: str | None) -> dict[str, Any]:
        return {
            "event_type": event.get("type"),
            "host": host,
            "service": service,
            "object_name": _object_name(host, service),
            "timestamp": EventStreamClient._timestamp(event),
        }

    # --------------------------------------------------------------- handlers

    def _handle_check_result(self, event: Event) -> None:
        host, service = event.get("host"), event.get("service")
        check_result = _as_dict(event.get("check_result"))
        end = check_result.get("execution_end")
        timestamp = (
            float(end)
            if isinstance(end, (int, float)) and not isinstance(end, bool) and math.isfinite(end)
            else self._timestamp(event)
        )
        state = check_result.get("state")

        state_filter = self.config.check_result_state_filter
        if self.config.include_check_results and (not state_filter or state in state_filter):
            data = self._base(event, host, service)
            data.update(
                {
                    "timestamp": timestamp,
                    "state": state,
                    "state_name": _state_name(state, service),
                    "output": check_result.get("output", ""),
                    "exit_status": check_result.get("exit_status"),
                    "check_source": check_result.get("check_source"),
                    "execution_start": check_result.get("execution_start"),
                    "execution_end": end,
                    "schedule_start": check_result.get("schedule_start"),
                    "schedule_end": check_result.get("schedule_end"),
                    "active": check_result.get("active"),
                    "downtime_depth": event.get("downtime_depth"),
                    "acknowledgement": event.get("acknowledgement"),
                }
            )
            # The check command line is deliberately not forwarded: plugin
            # arguments frequently contain credentials.
            self._send(data, "icinga2:checkresult", timestamp)

        if self.config.include_perfdata:
            self._send_perfdata(check_result.get("performance_data"), host, service, timestamp)

    def _send_perfdata(
        self, perfdata: Any, host: str | None, service: str | None, timestamp: float
    ) -> None:
        if not perfdata:
            return
        object_name = _object_name(host, service)
        for metric in parse_perfdata(perfdata):
            self.splunk.send(
                {
                    "host": host,
                    "service": service,
                    "object_name": object_name,
                    "timestamp": timestamp,
                    "metric": metric.label,
                    "value": metric.value,
                    "unit": metric.unit,
                    "warning": metric.warn,
                    "critical": metric.crit,
                    "min": metric.min,
                    "max": metric.max,
                },
                sourcetype="icinga2:perfdata",
                host=host,
                timestamp=timestamp,
                index=self.config.splunk_index_perfdata,
            )

    def _handle_state_change(self, event: Event) -> None:
        host, service = event.get("host"), event.get("service")
        check_result = _as_dict(event.get("check_result"))
        before = _as_dict(check_result.get("vars_before"))
        after = _as_dict(check_result.get("vars_after"))
        state, state_type = event.get("state"), event.get("state_type")

        data = self._base(event, host, service)
        data.update(
            {
                "state": state,
                "state_name": _state_name(state, service),
                "state_type": state_type,
                "state_type_name": _state_type_name(state_type),
                "previous_state": before.get("state"),
                "previous_state_name": _state_name(before.get("state"), service),
                "attempt": after.get("attempt"),
                "output": check_result.get("output", ""),
                "downtime_depth": event.get("downtime_depth"),
                "acknowledgement": event.get("acknowledgement"),
            }
        )
        # Perfdata is not sent here: the same check result also arrives as a
        # CheckResult event, so sending it twice would double-count metrics.
        self._send(data, "icinga2:statechange", data["timestamp"])

    def _handle_notification(self, event: Event) -> None:
        host, service = event.get("host"), event.get("service")
        check_result = _as_dict(event.get("check_result"))
        data = self._base(event, host, service)
        data.update(
            {
                "notification_type": event.get("notification_type"),
                "command": event.get("command"),
                "users": event.get("users", []),
                "author": event.get("author"),
                "text": event.get("text"),
                "state": check_result.get("state"),
                "state_name": _state_name(check_result.get("state"), service),
                "output": check_result.get("output", ""),
            }
        )
        self._send(data, "icinga2:notification", data["timestamp"])

    def _handle_acknowledgement(self, event: Event) -> None:
        host, service = event.get("host"), event.get("service")
        data = self._base(event, host, service)
        data.update(
            {
                "state": event.get("state"),
                "state_name": _state_name(event.get("state"), service),
                "state_type": event.get("state_type"),
                "author": event.get("author"),
                "comment": event.get("comment"),
                "acknowledgement_type": event.get("acknowledgement_type"),
                "notify": event.get("notify"),
                "expiry": event.get("expiry"),
            }
        )
        if "acknowledgement_cleared_by" in event:
            data["cleared_by"] = event.get("acknowledgement_cleared_by")
        self._send(data, "icinga2:acknowledgement", data["timestamp"])

    def _handle_comment(self, event: Event) -> None:
        comment = _as_dict(event.get("comment"))
        host, service = comment.get("host_name"), comment.get("service_name") or None
        data = self._base(event, host, service)
        data.update(
            {
                "name": comment.get("name"),
                "author": comment.get("author"),
                "text": comment.get("text"),
                "entry_type": comment.get("entry_type"),
                "entry_time": comment.get("entry_time"),
                "expire_time": comment.get("expire_time"),
                "persistent": comment.get("persistent"),
            }
        )
        self._send(data, "icinga2:comment", data["timestamp"])

    def _handle_downtime(self, event: Event) -> None:
        downtime = _as_dict(event.get("downtime"))
        host, service = downtime.get("host_name"), downtime.get("service_name") or None
        data = self._base(event, host, service)
        data.update(
            {
                "name": downtime.get("name"),
                "author": downtime.get("author"),
                "comment": downtime.get("comment"),
                "start_time": downtime.get("start_time"),
                "end_time": downtime.get("end_time"),
                "duration": downtime.get("duration"),
                "fixed": downtime.get("fixed"),
                "entry_time": downtime.get("entry_time"),
                "trigger_time": downtime.get("trigger_time"),
                "triggered_by": downtime.get("triggered_by"),
            }
        )
        self._send(data, "icinga2:downtime", data["timestamp"])

    def _handle_generic(self, event: Event) -> None:
        """Forward any other subscribed event type unchanged."""
        event_type = str(event.get("type"))
        data = dict(event)
        data["event_type"] = event_type
        self.splunk.send(
            data,
            sourcetype=f"icinga2:{event_type.lower()}",
            host=event.get("host") if isinstance(event.get("host"), str) else None,
            timestamp=self._timestamp(event),
            index=self.config.splunk_index_events,
        )


class _ShutdownRequestedError(BaseException):
    """Raised from the signal handler to interrupt blocking socket reads.

    Derives from BaseException so generic ``except Exception`` handlers in the
    event processing path cannot swallow it.
    """


def _raise_shutdown(signum: int, _frame: FrameType | None) -> None:
    raise _ShutdownRequestedError(signal.Signals(signum).name)


def main(argv: list[str] | None = None) -> int:
    """Command line entry point (``icinga2-splunk``)."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("-c", "--config", help="path to the JSON configuration file")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
    try:
        config = Config.load(args.config)
    except ConfigError as exc:
        logger.critical("%s", exc)
        return EXIT_CONFIG_ERROR
    logging.getLogger().setLevel(config.log_level)

    try:
        client = EventStreamClient(config)
    except OSError as exc:
        logger.critical("Cannot open spool directory %s: %s", config.spool_dir, exc)
        return EXIT_CONFIG_ERROR
    signal.signal(signal.SIGTERM, _raise_shutdown)
    signal.signal(signal.SIGINT, _raise_shutdown)
    try:
        client.run()
    except _ShutdownRequestedError as exc:
        logger.info("Received %s, shutting down", exc)
    finally:
        # Don't let a second signal interrupt the final flush.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
