#!/usr/bin/env python3
"""
Icinga2 Event Stream to Splunk Integration Client
"""

import json
import time
import threading
import logging
import urllib3
import requests


logger = logging.getLogger(__name__)


class SplunkHECClient:
    """Client for sending events and performance metrics to Splunk HEC.

    This client batches events and perfdata and flushes them either when the
    configured batch size is reached or when a periodic flush timer fires.
    It is safe to call from multiple threads; internal locks protect each
    batch queue.
    """

    def __init__(
        self, hec_url, hec_token, verify_ssl=True, batch_size=100, batch_timeout=10
    ):
        self.hec_url = hec_url.rstrip("/")
        self.hec_token = hec_token
        self.verify_ssl = verify_ssl
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout

        if not verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.events_batch = []
        self.perfdata_batch = []
        self.events_lock = threading.Lock()
        self.perfdata_lock = threading.Lock()

        # Start flush timers
        self._start_flush_timers()

    def _start_flush_timers(self):
        """Start background threads to flush batches periodically"""

        def flush_events_timer():
            while True:
                time.sleep(self.batch_timeout)
                self._flush_events()

        def flush_perfdata_timer():
            while True:
                time.sleep(self.batch_timeout)
                self._flush_perfdata()

        events_thread = threading.Thread(target=flush_events_timer, daemon=True)
        perfdata_thread = threading.Thread(target=flush_perfdata_timer, daemon=True)
        events_thread.start()
        perfdata_thread.start()

    def send_event(
        self, event_data, host=None, sourcetype=None, timestamp=None, index_name=None
    ):
        """Add event to the internal batch queue.

        Args:
            event_data (dict): Event payload to send to Splunk HEC.
            host (str, optional): Host field for the event.
            sourcetype (str, optional): Splunk sourcetype to tag the event with.
            timestamp (float, optional): Event timestamp (epoch seconds).
            index_name (str, optional): Splunk index name override.
        """
        should_flush = False

        with self.events_lock:
            self.events_batch.append(
                {
                    "event_data": event_data,
                    "host": host,
                    "sourcetype": sourcetype,
                    "timestamp": timestamp,
                    "index_name": index_name,
                }
            )

            if len(self.events_batch) >= self.batch_size:
                should_flush = True

        if should_flush:
            self._flush_events()

    def send_perfdata(self, perfdata, host=None, timestamp=None, index_name=None):
        """Add parsed performance metric to the perfdata batch queue.

        Args:
            perfdata (dict): Metric payload prepared for Splunk.
            host (str, optional): Host name associated with the metric.
            timestamp (float, optional): Event timestamp (epoch seconds).
            index_name (str, optional): Splunk index name override.
        """
        should_flush = False
        batch_count = 0

        with self.perfdata_lock:
            self.perfdata_batch.append(
                {
                    "perfdata": perfdata,
                    "host": host,
                    "timestamp": timestamp,
                    "index_name": index_name,
                }
            )

            batch_count = len(self.perfdata_batch)
            logger.debug(
                "[BATCH] Added perfdata for %s, batch now has %s items",
                host,
                batch_count,
            )

            if batch_count >= self.batch_size:
                should_flush = True

        if should_flush:
            logger.debug(
                "[BATCH] Batch size reached (%s), flushing perfdata", batch_count
            )
            self._flush_perfdata()

    def _flush_events(self):
        """Flush the accumulated events batch to Splunk HEC.

        This builds a newline-delimited payload and posts it to the HEC
        collector endpoint. Network and payload errors are handled and logged.
        """
        with self.events_lock:
            if not self.events_batch:
                return

            batch = self.events_batch.copy()
            self.events_batch.clear()

        try:
            # Build payload for batch sending
            payload_lines = []
            for item in batch:
                event = {
                    "event": item["event_data"],
                    "sourcetype": item.get("sourcetype", "icinga2:event"),
                    "host": item.get("host", "icinga2"),
                }

                if item.get("timestamp"):
                    event["time"] = item["timestamp"]

                if item.get("index_name"):
                    event["index"] = item["index_name"]

                payload_lines.append(json.dumps(event))

            payload = "\n".join(payload_lines)

            logger.info(
                "Sending %s events to Splunk (index: %s)",
                len(batch),
                batch[0].get("index_name", "default"),
            )

            response = requests.post(
                f"{self.hec_url}/services/collector/event",
                headers={
                    "Authorization": f"Splunk {self.hec_token}",
                    "Content-Type": "application/json",
                },
                data=payload,
                verify=self.verify_ssl,
                timeout=30,
            )

            if response.status_code == 200:
                logger.info("Successfully sent %s events to Splunk", len(batch))
            else:
                logger.error(
                    "Failed to send events to Splunk: %s - %s",
                    response.status_code,
                    response.text,
                )

        except (requests.exceptions.RequestException, ValueError) as e:
            logger.error("Error flushing events to Splunk: %s", e, exc_info=True)

    def _flush_perfdata(self):
        """Flush the accumulated performance metrics batch to Splunk HEC.

        Similar to `_flush_events`, this prepares newline-delimited JSON and
        sends it to the collector endpoint. Network errors are logged.
        """
        with self.perfdata_lock:
            if not self.perfdata_batch:
                return

            batch = self.perfdata_batch.copy()
            self.perfdata_batch.clear()

        logger.debug(
            "[FLUSH] Starting flush of %s perfdata items to Splunk", len(batch)
        )

        try:
            # Build payload for batch sending
            payload_lines = []
            for item in batch:
                event = {
                    "event": item["perfdata"],
                    "sourcetype": "icinga2:perfdata",
                    "host": item.get("host", "icinga2"),
                }

                if item.get("timestamp"):
                    event["time"] = item["timestamp"]

                if item.get("index_name"):
                    event["index"] = item["index_name"]

                payload_lines.append(json.dumps(event))

            payload = "\n".join(payload_lines)

            logger.debug("[FLUSH] Sending POST request to Splunk HEC...")

            response = requests.post(
                f"{self.hec_url}/services/collector/event",
                headers={
                    "Authorization": f"Splunk {self.hec_token}",
                    "Content-Type": "application/json",
                },
                data=payload,
                verify=self.verify_ssl,
                timeout=30,
            )

            logger.debug(
                "[FLUSH] Splunk responded with status code: %s", response.status_code
            )

            if response.status_code == 200:
                logger.info(
                    "✓ Successfully sent %s performance metrics to Splunk", len(batch)
                )
            else:
                logger.error(
                    "Failed to send perfdata to Splunk: %s - %s",
                    response.status_code,
                    response.text,
                )

        except (requests.exceptions.RequestException, ValueError) as e:
            logger.error("Error flushing perfdata to Splunk: %s", e, exc_info=True)

    def flush(self):
        """Force flush of all pending events and performance metrics.

        This method synchronously flushes both the events and perfdata
        queues to Splunk HEC. It is safe to call from shutdown handlers
        and will attempt to send any buffered items immediately.
        """
        # Flush events first, then perfdata to preserve relative ordering
        self._flush_events()
        self._flush_perfdata()
