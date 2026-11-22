"""Batching client for the Splunk HTTP Event Collector (HEC)."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from typing import Any

import requests

from icinga2_splunk.config import VerifySetting, normalize_hec_url

logger = logging.getLogger(__name__)

# HEC status codes worth retrying: throttling and transient server errors.
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class SplunkHECClient:
    """Thread-safe, batching client for Splunk HEC.

    Events are queued by :meth:`send` and delivered by a single background
    worker thread, either once ``batch_size`` events are pending or every
    ``batch_timeout`` seconds. The queue is bounded by ``max_queue_size``;
    when it is full the oldest events are dropped so a Splunk outage cannot
    exhaust memory. Transient failures (connection errors, HTTP 429/5xx) are
    retried with exponential backoff; other failures drop the batch.

    Call :meth:`start` before sending and :meth:`close` on shutdown to flush
    pending events.
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
        max_retries: int = 3,
        retry_backoff: float = 1.0,
        request_timeout: float = 30.0,
        session: requests.Session | None = None,
    ) -> None:
        """Create the client. No network activity happens until events are flushed."""
        self.endpoint = f"{normalize_hec_url(hec_url)}/services/collector/event"
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.request_timeout = request_timeout

        self._session = session or requests.Session()
        self._session.headers.update(
            {"Authorization": f"Splunk {hec_token}", "Content-Type": "application/json"}
        )
        self._session.verify = verify

        self._queue: deque[str] = deque()
        self._max_queue_size = max_queue_size
        self._cond = threading.Condition()
        self._send_lock = threading.Lock()
        self._closed = threading.Event()
        self._worker: threading.Thread | None = None

        self.sent_count = 0
        self.dropped_count = 0

    # --------------------------------------------------------------- lifecycle

    def start(self) -> None:
        """Start the background flush worker (idempotent)."""
        if self._worker is not None and self._worker.is_alive():
            return
        self._closed.clear()
        self._worker = threading.Thread(target=self._run, name="splunk-hec-flush", daemon=True)
        self._worker.start()

    def close(self, timeout: float | None = 30.0) -> None:
        """Stop the worker and flush any pending events."""
        self._closed.set()
        with self._cond:
            self._cond.notify_all()
        if self._worker is not None:
            self._worker.join(timeout)
            self._worker = None
        self.flush()
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
            line = json.dumps(envelope, separators=(",", ":"), default=str)
        except (TypeError, ValueError):
            logger.exception("Dropping event that cannot be serialized to JSON")
            return

        with self._cond:
            if len(self._queue) >= self._max_queue_size:
                self._queue.popleft()
                self.dropped_count += 1
                if self.dropped_count == 1 or self.dropped_count % 1000 == 0:
                    logger.warning(
                        "Splunk send queue full (%d events); dropped %d events so far",
                        self._max_queue_size,
                        self.dropped_count,
                    )
            self._queue.append(line)
            if len(self._queue) >= self.batch_size:
                self._cond.notify()

    def flush(self) -> None:
        """Synchronously deliver all queued events."""
        while True:
            batch = self._take_batch()
            if not batch:
                return
            self._post_batch(batch)

    @property
    def pending(self) -> int:
        """Number of events waiting to be sent."""
        with self._cond:
            return len(self._queue)

    # --------------------------------------------------------------- internals

    def _take_batch(self) -> list[str]:
        with self._cond:
            count = min(self.batch_size, len(self._queue))
            return [self._queue.popleft() for _ in range(count)]

    def _run(self) -> None:
        while not self._closed.is_set():
            with self._cond:
                deadline = time.monotonic() + self.batch_timeout
                while len(self._queue) < self.batch_size and not self._closed.is_set():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    self._cond.wait(remaining)
            if self._closed.is_set():
                return
            try:
                batch = self._take_batch()
                if batch:
                    self._post_batch(batch)
            except Exception:  # keep the worker alive whatever happens
                logger.exception("Unexpected error in Splunk flush worker")

    def _post_batch(self, batch: list[str]) -> bool:
        payload = "\n".join(batch).encode("utf-8")
        attempts = self.max_retries + 1

        with self._send_lock:
            for attempt in range(1, attempts + 1):
                retry = False
                try:
                    response = self._session.post(
                        self.endpoint, data=payload, timeout=self.request_timeout
                    )
                except requests.RequestException as exc:
                    logger.warning(
                        "Error sending %d events to Splunk (attempt %d/%d): %s",
                        len(batch),
                        attempt,
                        attempts,
                        exc,
                    )
                    retry = True
                else:
                    if response.ok:
                        self.sent_count += len(batch)
                        logger.debug("Sent %d events to Splunk", len(batch))
                        return True
                    retry = response.status_code in _RETRYABLE_STATUS
                    logger.log(
                        logging.WARNING if retry else logging.ERROR,
                        "Splunk HEC rejected %d events (attempt %d/%d): HTTP %s - %s",
                        len(batch),
                        attempt,
                        attempts,
                        response.status_code,
                        response.text[:500],
                    )

                if not retry or attempt == attempts:
                    break
                # Don't delay retries while shutting down.
                self._closed.wait(self.retry_backoff * 2 ** (attempt - 1))

        self.dropped_count += len(batch)
        logger.error("Dropped %d events after failing to deliver them to Splunk", len(batch))
        return False
