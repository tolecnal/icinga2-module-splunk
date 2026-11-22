"""Batching client for the Splunk HTTP Event Collector (HEC).

Delivery model
--------------
Events are queued in memory by :meth:`SplunkHECClient.send` and delivered by a
single worker thread in batches. Each HEC response is classified:

* **Success** (2xx only) - the batch is done.
* **Bad data** (HTTP 400 with a known bad-event code, or 413 too large) -
  retrying the same request can never succeed. If HEC reports which event was
  invalid, the events before it were indexed, that event is discarded and the
  rest is resent. Otherwise the batch is split in half until the offending
  event is isolated and discarded. Other events are not lost.
* **Splunk unavailable** - connection errors, timeouts, HTTP 5xx/429, and also
  3xx (redirects are not followed), 401/403/404, and any other 400 such as
  "incorrect index" (configuration problems that an admin can fix). The client
  enters *outage* mode: nothing is discarded, delivery is retried with
  exponential backoff (``retry_initial`` .. ``retry_max``), and
  pending events are written to the optional :class:`DiskSpool` so they
  survive long outages and restarts.

When Splunk is reachable again, spooled events are replayed oldest first,
followed by events from memory. Without a spool, at most ``max_queue_size``
events are kept in memory and the oldest are dropped beyond that.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from collections.abc import Callable
from pathlib import Path
from typing import Any

import requests

from icinga2_splunk.config import VerifySetting, normalize_hec_url
from icinga2_splunk.spool import DiskSpool

logger = logging.getLogger(__name__)

# HEC error codes in a 400 response that mean "this event is malformed".
_BAD_EVENT_CODES = frozenset({6, 12, 13, 15, 16, 17, 18, 19, 20, 21})
_HTTP_OK_MIN, _HTTP_OK_MAX = 200, 300
_HTTP_REDIRECT_MIN, _HTTP_REDIRECT_MAX = 300, 400
_HTTP_BAD_REQUEST = 400
_HTTP_TOO_LARGE = 413

ENFORCE_LIMITS_INTERVAL = 60.0
OUTAGE_REPORT_INTERVAL = 300.0


class SplunkHECClient:
    """Thread-safe, batching, outage-tolerant client for Splunk HEC.

    Call :meth:`start` before sending and :meth:`close` on shutdown. See the
    module docstring for the delivery and retry model.
    """

    def __init__(
        self,
        hec_url: str,
        hec_token: str,
        *,
        verify: VerifySetting = True,
        batch_size: int = 100,
        batch_timeout: float = 10.0,
        max_queue_size: int = 10_000,
        spool: DiskSpool | None = None,
        retry_initial: float = 1.0,
        retry_max: float = 60.0,
        request_timeout: float = 30.0,
        session: requests.Session | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create the client. No network activity happens until events are delivered."""
        self.endpoint = f"{normalize_hec_url(hec_url)}/services/collector/event"
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.retry_initial = retry_initial
        self.retry_max = retry_max
        self.request_timeout = request_timeout
        self.spool = spool

        self._session = session or requests.Session()
        self._session.headers.update(
            {"Authorization": f"Splunk {hec_token}", "Content-Type": "application/json"}
        )
        self._session.verify = verify
        self._clock = clock

        self._queue: deque[str] = deque()
        self._max_queue_size = max_queue_size
        self._cond = threading.Condition()
        self._closed = threading.Event()
        self._worker: threading.Thread | None = None

        # Outage state; only touched by the worker (or by close() after it stopped).
        self._outage_since: float | None = None
        self._backoff = retry_initial
        self._next_attempt = 0.0
        self._last_report = 0.0
        self._last_limits_check = 0.0
        self._spool_error = False
        # Spool segment being replayed: (path, lines, number already delivered).
        self._replay: tuple[Path, list[str], int] | None = None

        self.sent_count = 0
        self.dropped_count = 0

    # --------------------------------------------------------------- lifecycle

    def start(self) -> None:
        """Start the background delivery worker (idempotent)."""
        if self._worker is not None and self._worker.is_alive():
            return
        self._closed.clear()
        self._worker = threading.Thread(target=self._run, name="splunk-hec", daemon=True)
        self._worker.start()

    def close(self, timeout: float | None = 30.0) -> None:
        """Stop the worker, try a last delivery and persist anything left over."""
        deadline = None if timeout is None else self._clock() + timeout
        self._closed.set()
        with self._cond:
            self._cond.notify_all()
        if self._worker is not None:
            self._worker.join(timeout)
            if self._worker.is_alive():
                # It is stuck in a request; touching the spool now could race.
                logger.error(
                    "Delivery worker did not stop in time; %d queued events not saved",
                    self.pending,
                )
                self._session.close()
                return
            self._worker = None

        if not self.in_outage:
            self._deliver_memory(stop_on_close=False, deadline=deadline)
        if self.spool is not None:
            self._spill_memory()
            try:
                self.spool.close()
            except OSError as exc:
                logger.error("Failed to close the disk spool cleanly: %s", exc)  # noqa: TRY400
            if self.spool:
                logger.info(
                    "%.1f MB of events remain spooled for delivery on next start",
                    self.spool.size_bytes / 1_048_576,
                )
        else:
            lost = self._take_all()
            if lost:
                self.dropped_count += len(lost)
                logger.error("Discarding %d undelivered events (no spool configured)", len(lost))
        self._session.close()

    # ----------------------------------------------------------------- sending

    def send(
        self,
        event: Any,
        *,
        sourcetype: str | None = None,
        host: str | None = None,
        timestamp: float | None = None,
        index: str | None = None,
        source: str | None = None,
    ) -> None:
        """Queue an event for delivery to Splunk.

        Args:
            event: JSON-serializable event payload.
            sourcetype: Splunk sourcetype.
            host: Splunk host field.
            timestamp: Event time in epoch seconds.
            index: Target Splunk index (HEC token default if omitted).
            source: Splunk source field.
        """
        envelope: dict[str, Any] = {"event": event}
        for key, value in (
            ("time", timestamp),
            ("host", host),
            ("source", source),
            ("sourcetype", sourcetype),
            ("index", index),
        ):
            if value is not None:
                envelope[key] = value

        try:
            line = json.dumps(envelope, separators=(",", ":"), default=str, allow_nan=False)
        except (TypeError, ValueError):
            logger.exception("Dropping event that cannot be serialized to JSON")
            return

        with self._cond:
            if len(self._queue) >= self._max_queue_size:
                # Only reached without a spool, or if the worker can't keep up.
                self._queue.popleft()
                self.dropped_count += 1
                if self.dropped_count == 1 or self.dropped_count % 1000 == 0:
                    logger.warning(
                        "In-memory queue full (%d events); dropped %d events so far",
                        self._max_queue_size,
                        self.dropped_count,
                    )
            self._queue.append(line)
            if len(self._queue) >= self.batch_size:
                self._cond.notify()

    def flush(self) -> bool:
        """Synchronously try to deliver spooled and queued events once.

        Must not be called while the worker is running.

        Returns:
            True if everything was delivered.
        """
        if self.spool is not None and not self._drain_spool(stop_on_close=False):
            return False
        return self._deliver_memory(stop_on_close=False)

    @property
    def pending(self) -> int:
        """Number of events waiting in memory."""
        with self._cond:
            return len(self._queue)

    @property
    def in_outage(self) -> bool:
        """Whether Splunk is currently considered unavailable."""
        return self._outage_since is not None

    # ------------------------------------------------------------------ worker

    def _run(self) -> None:
        while not self._closed.is_set():
            with self._cond:
                deadline = self._clock() + self.batch_timeout
                if self.in_outage:
                    deadline = min(deadline, self._next_attempt)
                while not self._closed.is_set():
                    if len(self._queue) >= self.batch_size and not (
                        self.in_outage and (self.spool is None or self._spool_error)
                    ):
                        break
                    remaining = deadline - self._clock()
                    if remaining <= 0:
                        break
                    self._cond.wait(remaining)
            if self._closed.is_set():
                return
            try:
                self._cycle()
            except Exception:  # keep the worker alive whatever happens
                logger.exception("Unexpected error in Splunk delivery worker")
                self._enter_outage()

    def _cycle(self) -> None:
        now = self._clock()
        if self.spool is not None and now - self._last_limits_check >= ENFORCE_LIMITS_INTERVAL:
            self._last_limits_check = now
            try:
                self.spool.enforce_limits()
            except OSError as exc:
                logger.error("Failed to apply disk spool limits: %s", exc)  # noqa: TRY400

        if self.in_outage and now < self._next_attempt:
            self._spill_memory()
            self._report_outage(now)
            return

        if self.spool is not None and not self._drain_spool(stop_on_close=True):
            return
        if self._deliver_memory(stop_on_close=True):
            self._leave_outage()

    # ---------------------------------------------------------------- delivery

    def _drain_spool(self, *, stop_on_close: bool) -> bool:
        """Replay spooled segments oldest first.

        Returns:
            True once the spool is empty; False on outage, or if ``stop_on_close``
            is set and the client is closing.
        """
        assert self.spool is not None  # noqa: S101 - for type narrowing
        while not (stop_on_close and self._closed.is_set()):
            if self._replay is None:
                try:
                    oldest = self.spool.oldest()
                except OSError as exc:
                    # Don't block live events on an unreadable spool; the
                    # backlog is retried on the next cycle.
                    logger.error("Failed to read the disk spool: %s", exc)  # noqa: TRY400
                    return True
                if oldest is None:
                    return True
                self._replay = (oldest[0], oldest[1], 0)

            path, lines, done = self._replay
            if path not in self.spool:
                # Dropped by the spool's age or size limit during the replay.
                self._replay = None
                continue
            if done >= len(lines):
                self.spool.remove(path)
                self._replay = None
                continue

            batch = lines[done : done + self.batch_size]
            delivered = self._deliver(batch)
            self._replay = (path, lines, done + delivered)
            self.spool.mark_progress(path, done + delivered)
            if delivered < len(batch):
                self._enter_outage()
                return False
            self._leave_outage()
            # Keep memory bounded while a long backlog is replayed.
            if self.pending >= self._max_queue_size // 2:
                self._spill_memory()
        return False

    def _deliver_memory(self, *, stop_on_close: bool, deadline: float | None = None) -> bool:
        """Send everything queued in memory. Returns False on outage or deadline."""
        while not (stop_on_close and self._closed.is_set()):
            if deadline is not None and self._clock() >= deadline:
                return False
            batch = self._take_batch()
            if not batch:
                return True
            try:
                delivered = self._deliver(batch)
            except Exception:
                self._requeue_front(batch)  # never lose a batch to a bug
                raise
            if delivered < len(batch):
                self._requeue_front(batch[delivered:])
                self._enter_outage()
                return False
        return True

    def _deliver(self, lines: list[str]) -> int:
        """POST events to HEC and return how many leading events are resolved.

        "Resolved" means indexed, or discarded as malformed. A return value
        below ``len(lines)`` means Splunk is unavailable and the remaining
        events must be kept.
        """
        resolved = 0
        while resolved < len(lines):
            chunk = lines[resolved:]
            payload = "\n".join(chunk).encode("utf-8")
            try:
                response = self._session.post(
                    self.endpoint,
                    data=payload,
                    timeout=self.request_timeout,
                    allow_redirects=False,
                )
            except OSError as exc:  # RequestException, or e.g. an unreadable CA bundle
                self._log_failure(len(chunk), str(exc))
                return resolved

            if _HTTP_OK_MIN <= response.status_code < _HTTP_OK_MAX:
                self.sent_count += len(chunk)
                logger.debug("Sent %d events to Splunk", len(chunk))
                return resolved + len(chunk)

            if response.status_code == _HTTP_BAD_REQUEST:
                body = _json_body(response)
                if body.get("code") in _BAD_EVENT_CODES:
                    bad = body.get("invalid-event-number")
                    if isinstance(bad, int) and not isinstance(bad, bool) and 0 <= bad < len(chunk):
                        # HEC indexed the events before the invalid one.
                        self.sent_count += bad
                        self._discard(chunk[bad], response)
                        resolved += bad + 1
                        continue
                    return resolved + self._bisect(chunk, response)
            if response.status_code == _HTTP_TOO_LARGE:
                return resolved + self._bisect(chunk, response)

            if _HTTP_REDIRECT_MIN <= response.status_code < _HTTP_REDIRECT_MAX:
                # Redirects are not followed (they could leak the token); a
                # 3xx is never a delivery.
                reason = (
                    f"HTTP {response.status_code} redirect to "
                    f"{response.headers.get('Location', '?')!r}; check splunk_hec_url"
                )
            else:
                reason = f"HTTP {response.status_code} - {response.text[:300]}"
            self._log_failure(len(chunk), reason, response)
            return resolved
        return resolved

    def _bisect(self, lines: list[str], response: requests.Response) -> int:
        if len(lines) == 1:
            self._discard(lines[0], response)
            return 1
        mid = len(lines) // 2
        first = self._deliver(lines[:mid])
        if first < mid:
            return first
        return mid + self._deliver(lines[mid:])

    def _discard(self, line: str, response: requests.Response) -> None:
        with self._cond:
            self.dropped_count += 1
        logger.error(
            "Splunk rejected an event as invalid and it was discarded: HTTP %s - %s",
            response.status_code,
            response.text[:300],
        )
        # The event may contain check output; only show it when debugging.
        logger.debug("Discarded event: %.300s", line)

    def _log_failure(
        self, count: int, reason: str, response: requests.Response | None = None
    ) -> None:
        level = logging.WARNING
        if response is not None and (
            _HTTP_REDIRECT_MIN <= response.status_code < _HTTP_REDIRECT_MAX
            or response.status_code in (400, 401, 403, 404)
        ):
            level = logging.ERROR  # configuration problem; needs an admin
        if not self.in_outage or level == logging.ERROR:
            logger.log(level, "Could not deliver %d events to Splunk: %s", count, reason)
        else:
            logger.debug("Retry failed for %d events: %s", count, reason)

    # ---------------------------------------------------------- outage state

    def _enter_outage(self) -> None:
        now = self._clock()
        if self._outage_since is None:
            self._outage_since = now
            self._last_report = now
            self._backoff = self.retry_initial
            logger.warning(
                "Splunk unavailable; buffering events%s and retrying",
                " to disk" if self.spool is not None else " in memory",
            )
        else:
            self._backoff = min(self._backoff * 2, self.retry_max)
        self._next_attempt = now + self._backoff
        self._spill_memory()

    def _leave_outage(self) -> None:
        if self._outage_since is None:
            return
        duration = self._clock() - self._outage_since
        self._outage_since = None
        self._spool_error = False
        self._backoff = self.retry_initial
        logger.info("Splunk available again after %.0f s; delivering buffered events", duration)

    def _report_outage(self, now: float) -> None:
        if self._outage_since is None or now - self._last_report < OUTAGE_REPORT_INTERVAL:
            return
        self._last_report = now
        spooled = self.spool.size_bytes / 1_048_576 if self.spool is not None else 0.0
        logger.warning(
            "Splunk still unavailable after %.0f s: %d events in memory, %.1f MB spooled",
            now - self._outage_since,
            self.pending,
            spooled,
        )

    # ------------------------------------------------------------ queue ops

    def _take_batch(self) -> list[str]:
        with self._cond:
            count = min(self.batch_size, len(self._queue))
            return [self._queue.popleft() for _ in range(count)]

    def _take_all(self) -> list[str]:
        with self._cond:
            lines = list(self._queue)
            self._queue.clear()
            return lines

    def _requeue_front(self, lines: list[str]) -> None:
        if self.spool is not None and self._append_spool(lines):
            return
        self._requeue_memory(lines)

    def _requeue_memory(self, lines: list[str]) -> None:
        with self._cond:
            self._queue.extendleft(reversed(lines))
            while len(self._queue) > self._max_queue_size:
                self._queue.popleft()
                self.dropped_count += 1

    def _spill_memory(self) -> None:
        if self.spool is None:
            return
        lines = self._take_all()
        if lines and not self._append_spool(lines):
            self._requeue_memory(lines)

    def _append_spool(self, lines: list[str]) -> bool:
        assert self.spool is not None  # noqa: S101 - for type narrowing
        try:
            self.spool.append(lines)
        except OSError as exc:
            self._spool_error = True
            logger.error("Failed to write %d events to disk spool: %s", len(lines), exc)  # noqa: TRY400
            return False
        self._spool_error = False
        return True


def _json_body(response: requests.Response) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}
