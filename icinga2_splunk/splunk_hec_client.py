"""
Splunk HTTP Event Collector (HEC) Client
Handles sending events to Splunk with batching and retry logic
"""

import json
import logging
import time
import requests
from typing import Dict, Any, Optional
from queue import Queue, Empty
import threading


class SplunkHECClient:
    """Client for sending events to Splunk HEC"""

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Event queue for batching
        self.event_queue = Queue(maxsize=10000)
        self.batch_size = config.splunk_batch_size
        self.batch_interval = config.splunk_batch_interval

        # Worker thread for sending events
        self.running = False
        self.worker_thread = None

        # Setup session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Splunk {config.splunk_hec_token}",
                "Content-Type": "application/json",
            }
        )

        # Start worker thread
        self.start()

    def start(self):
        """Start the worker thread"""
        if not self.running:
            self.running = True
            self.worker_thread = threading.Thread(target=self._worker, daemon=True)
            self.worker_thread.start()
            self.logger.info("Splunk HEC client started")

    def stop(self):
        """Stop the worker thread"""
        self.running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=5)
        self.logger.info("Splunk HEC client stopped")

    def shutdown(self):
        """Gracefully shutdown, flushing remaining events"""
        self.logger.info("Shutting down Splunk HEC client...")
        self.stop()

        # Flush remaining events
        if not self.event_queue.empty():
            self.logger.info(f"Flushing {self.event_queue.qsize()} remaining events")
            self._send_batch(self._get_batch(self.event_queue.qsize()))

    def send_event(
        self,
        event_data: Dict[str, Any],
        index: Optional[str] = None,
        sourcetype: Optional[str] = None,
        source: Optional[str] = None,
        host: Optional[str] = None,
        timestamp: Optional[float] = None,
    ):
        """
        Queue an event to be sent to Splunk

        Args:
            event_data: Event data dictionary
            index: Splunk index (optional, uses default if not specified)
            sourcetype: Splunk sourcetype
            source: Event source
            host: Host name
            timestamp: Event timestamp (Unix epoch)
        """
        event = {
            "event": event_data,
            "time": timestamp or time.time(),
        }

        if index:
            event["index"] = index
        if sourcetype:
            event["sourcetype"] = sourcetype
        if source:
            event["source"] = source
        if host:
            event["host"] = host

        try:
            self.event_queue.put(event, block=False)
        except:
            self.logger.error("Event queue is full, dropping event")

    def _worker(self):
        """Worker thread that batches and sends events"""
        last_send = time.time()

        while self.running:
            try:
                # Check if we should send based on size or time
                queue_size = self.event_queue.qsize()
                time_elapsed = time.time() - last_send

                should_send = queue_size >= self.batch_size or (
                    queue_size > 0 and time_elapsed >= self.batch_interval
                )

                if should_send:
                    batch = self._get_batch(min(queue_size, self.batch_size))
                    if batch:
                        self._send_batch(batch)
                        last_send = time.time()
                else:
                    time.sleep(0.1)  # Small sleep to prevent busy loop

            except Exception as e:
                self.logger.exception(f"Error in worker thread: {e}")
                time.sleep(1)

    def _get_batch(self, size: int) -> list:
        """Get a batch of events from the queue"""
        batch = []
        for _ in range(size):
            try:
                event = self.event_queue.get_nowait()
                batch.append(event)
            except Empty:
                break
        return batch

    def _send_batch(self, batch: list):
        """Send a batch of events to Splunk HEC"""
        if not batch:
            return

        # Convert batch to newline-delimited JSON
        payload = "\n".join([json.dumps(event) for event in batch])

        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    self.config.splunk_hec_url,
                    data=payload,
                    verify=self.config.splunk_verify_ssl,
                    timeout=10,
                )

                if response.status_code == 200:
                    self.logger.debug(
                        f"Successfully sent {len(batch)} events to Splunk"
                    )
                    return
                else:
                    self.logger.error(
                        f"Splunk HEC returned status {response.status_code}: {response.text}"
                    )

            except requests.exceptions.RequestException as e:
                self.logger.error(
                    f"Failed to send to Splunk (attempt {attempt + 1}/{max_retries}): {e}"
                )

            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff

        self.logger.error(
            f"Failed to send {len(batch)} events after {max_retries} attempts"
        )
