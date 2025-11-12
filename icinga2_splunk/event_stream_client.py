#!/usr/bin/env python3
"""
Icinga2 Event Stream Client for Splunk
Connects to Icinga2 API Event Streams and forwards events to Splunk HEC
"""

import json
import logging
import time
import requests
from requests.auth import HTTPBasicAuth
from typing import Dict, Any, Optional, Callable
import threading
from queue import Queue, Empty

from .config import Config
from .splunk_hec_client import SplunkHECClient
from .perfdata_parser import parse_perfdata


class EventStreamClient:
    """Client for consuming Icinga2 Event Streams and forwarding to Splunk"""

    def __init__(self, config: Config, splunk_client: SplunkHECClient):
        self.config = config
        self.splunk_client = splunk_client
        self.logger = logging.getLogger(__name__)
        self.running = False
        self.stream_thread = None

        # Event type handlers
        self.event_handlers = {
            "CheckResult": self._handle_check_result,
            "StateChange": self._handle_state_change,
            "Notification": self._handle_notification,
            "AcknowledgementSet": self._handle_acknowledgement,
            "CommentAdded": self._handle_comment,
            "DowntimeAdded": self._handle_downtime,
        }

    def start(self):
        """Start the event stream client"""
        self.logger.info("Starting Icinga2 Event Stream Client")
        self.running = True
        self.stream_thread = threading.Thread(target=self._consume_stream, daemon=True)
        self.stream_thread.start()

    def stop(self):
        """Stop the event stream client"""
        self.logger.info("Stopping Icinga2 Event Stream Client")
        self.running = False
        if self.stream_thread:
            self.stream_thread.join(timeout=5)

    def _consume_stream(self):
        """Main loop to consume event stream"""
        reconnect_delay = 1
        max_reconnect_delay = 60

        while self.running:
            try:
                self.logger.info(
                    f"Connecting to Icinga2 Event Stream at {self.config.icinga2_api_url}"
                )
                self._connect_and_process()
                reconnect_delay = 1  # Reset on successful connection

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Connection error: {e}")
                if self.running:
                    self.logger.info(f"Reconnecting in {reconnect_delay} seconds...")
                    time.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

            except Exception as e:
                self.logger.exception(f"Unexpected error: {e}")
                if self.running:
                    time.sleep(reconnect_delay)

    def _connect_and_process(self):
        """Establish connection and process events"""
        url = f"{self.config.icinga2_api_url}/v1/events"

        # Build event stream subscription payload
        payload = {
            "queue": self.config.event_stream_queue,
            "types": self.config.event_stream_types,
        }

        if self.config.event_stream_filter:
            payload["filter"] = self.config.event_stream_filter

        # Setup authentication
        auth = None
        cert = None

        if self.config.icinga2_api_username and self.config.icinga2_api_password:
            auth = HTTPBasicAuth(
                self.config.icinga2_api_username, self.config.icinga2_api_password
            )

        if self.config.icinga2_api_client_cert and self.config.icinga2_api_client_key:
            cert = (
                self.config.icinga2_api_client_cert,
                self.config.icinga2_api_client_key,
            )

        # Connect to event stream
        with requests.post(
            url,
            json=payload,
            auth=auth,
            cert=cert,
            verify=self.config.icinga2_api_verify_ssl,
            stream=True,
            timeout=30,
            headers={"Accept": "application/json", "X-HTTP-Method-Override": "GET"},
        ) as response:
            response.raise_for_status()
            self.logger.info("Successfully connected to Icinga2 Event Stream")

            # Process stream line by line
            for line in response.iter_lines():
                if not self.running:
                    break

                if line:
                    try:
                        event = json.loads(line.decode("utf-8"))
                        self._process_event(event)
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Failed to parse event JSON: {e}")
                    except Exception as e:
                        self.logger.exception(f"Error processing event: {e}")

    def _process_event(self, event: Dict[str, Any]):
        """Process a single event from the stream"""
        event_type = event.get("type")

        if not event_type:
            self.logger.warning(f"Event missing type field: {event}")
            return

        handler = self.event_handlers.get(event_type)

        if handler:
            try:
                handler(event)
            except Exception as e:
                self.logger.exception(f"Error in handler for {event_type}: {e}")
        else:
            self.logger.debug(f"No handler for event type: {event_type}")

    def _handle_check_result(self, event: Dict[str, Any]):
        """Handle CheckResult events - contains performance data"""
        check_result = event.get("check_result", {})

        # Extract basic info
        object_type = event.get("object_type", "Unknown")
        object_name = event.get("object_name", "Unknown")
        host = event.get("host", "Unknown")
        service = event.get("service", "")

        timestamp = check_result.get("execution_end", time.time())

        # Send check result event to events index
        event_data = {
            "event_type": "CheckResult",
            "object_type": object_type,
            "object_name": object_name,
            "host": host,
            "service": service,
            "state": check_result.get("state", 0),
            "output": check_result.get("output", ""),
            "execution_start": check_result.get("execution_start"),
            "execution_end": check_result.get("execution_end"),
            "check_source": check_result.get("check_source", ""),
            "command": check_result.get("command", []),
        }

        self.splunk_client.send_event(
            event_data,
            index=self.config.splunk_index_events,
            sourcetype="icinga2:checkresult",
            timestamp=timestamp,
        )

        # Parse and send performance data to perfdata index
        perfdata_str = check_result.get("performance_data", [])

        if perfdata_str:
            for perfdata_line in perfdata_str:
                metrics = parse_perfdata(perfdata_line)

                for metric in metrics:
                    perfdata_event = {
                        "event_type": "PerformanceData",
                        "object_type": object_type,
                        "object_name": object_name,
                        "host": host,
                        "service": service,
                        "metric": metric["label"],
                        "value": metric["value"],
                        "unit": metric.get("unit", ""),
                        "warning": metric.get("warning"),
                        "critical": metric.get("critical"),
                        "min": metric.get("min"),
                        "max": metric.get("max"),
                    }

                    self.splunk_client.send_event(
                        perfdata_event,
                        index=self.config.splunk_index_perfdata,
                        sourcetype="icinga2:perfdata",
                        timestamp=timestamp,
                    )

        self.logger.debug(f"Processed CheckResult for {object_name}")

    def _handle_state_change(self, event: Dict[str, Any]):
        """Handle StateChange events"""
        state_change = event.get("state_change", {})

        object_type = event.get("object_type", "Unknown")
        object_name = event.get("object_name", "Unknown")
        host = event.get("host", "Unknown")
        service = event.get("service", "")

        timestamp = event.get("timestamp", time.time())

        event_data = {
            "event_type": "StateChange",
            "object_type": object_type,
            "object_name": object_name,
            "host": host,
            "service": service,
            "state": state_change.get("state"),
            "state_type": state_change.get("state_type"),
            "previous_state": state_change.get("previous_state"),
            "check_attempt": state_change.get("check_attempt"),
            "max_check_attempts": state_change.get("max_check_attempts"),
            "output": state_change.get("output", ""),
        }

        self.splunk_client.send_event(
            event_data,
            index=self.config.splunk_index_events,
            sourcetype="icinga2:statechange",
            timestamp=timestamp,
        )

        self.logger.info(
            f"State change for {object_name}: {state_change.get('previous_state')} -> {state_change.get('state')}"
        )

    def _handle_notification(self, event: Dict[str, Any]):
        """Handle Notification events"""
        notification = event.get("notification", {})

        object_type = event.get("object_type", "Unknown")
        object_name = event.get("object_name", "Unknown")
        host = event.get("host", "Unknown")
        service = event.get("service", "")

        timestamp = event.get("timestamp", time.time())

        event_data = {
            "event_type": "Notification",
            "object_type": object_type,
            "object_name": object_name,
            "host": host,
            "service": service,
            "notification_type": notification.get("type"),
            "author": notification.get("author", ""),
            "text": notification.get("text", ""),
            "users": notification.get("users", []),
            "state": notification.get("state"),
        }

        self.splunk_client.send_event(
            event_data,
            index=self.config.splunk_index_events,
            sourcetype="icinga2:notification",
            timestamp=timestamp,
        )

        self.logger.info(
            f"Notification sent for {object_name} to {len(notification.get('users', []))} users"
        )

    def _handle_acknowledgement(self, event: Dict[str, Any]):
        """Handle AcknowledgementSet events"""
        acknowledgement = event.get("acknowledgement", {})

        object_type = event.get("object_type", "Unknown")
        object_name = event.get("object_name", "Unknown")
        host = event.get("host", "Unknown")
        service = event.get("service", "")

        timestamp = event.get("timestamp", time.time())

        event_data = {
            "event_type": "Acknowledgement",
            "object_type": object_type,
            "object_name": object_name,
            "host": host,
            "service": service,
            "author": acknowledgement.get("author", ""),
            "comment": acknowledgement.get("comment", ""),
            "notify": acknowledgement.get("notify", False),
            "persistent": acknowledgement.get("persistent", False),
            "expiry": acknowledgement.get("expiry"),
        }

        self.splunk_client.send_event(
            event_data,
            index=self.config.splunk_index_events,
            sourcetype="icinga2:acknowledgement",
            timestamp=timestamp,
        )

        self.logger.info(
            f"Acknowledgement set for {object_name} by {acknowledgement.get('author')}"
        )

    def _handle_comment(self, event: Dict[str, Any]):
        """Handle CommentAdded events"""
        comment = event.get("comment", {})

        object_type = event.get("object_type", "Unknown")
        object_name = event.get("object_name", "Unknown")
        host = event.get("host", "Unknown")
        service = event.get("service", "")

        timestamp = event.get("timestamp", time.time())

        event_data = {
            "event_type": "Comment",
            "object_type": object_type,
            "object_name": object_name,
            "host": host,
            "service": service,
            "author": comment.get("author", ""),
            "text": comment.get("text", ""),
            "entry_type": comment.get("entry_type"),
        }

        self.splunk_client.send_event(
            event_data,
            index=self.config.splunk_index_events,
            sourcetype="icinga2:comment",
            timestamp=timestamp,
        )

        self.logger.debug(f"Comment added to {object_name}")

    def _handle_downtime(self, event: Dict[str, Any]):
        """Handle DowntimeAdded events"""
        downtime = event.get("downtime", {})

        object_type = event.get("object_type", "Unknown")
        object_name = event.get("object_name", "Unknown")
        host = event.get("host", "Unknown")
        service = event.get("service", "")

        timestamp = event.get("timestamp", time.time())

        event_data = {
            "event_type": "Downtime",
            "object_type": object_type,
            "object_name": object_name,
            "host": host,
            "service": service,
            "author": downtime.get("author", ""),
            "comment": downtime.get("comment", ""),
            "start_time": downtime.get("start_time"),
            "end_time": downtime.get("end_time"),
            "duration": downtime.get("duration"),
            "fixed": downtime.get("fixed", False),
        }

        self.splunk_client.send_event(
            event_data,
            index=self.config.splunk_index_events,
            sourcetype="icinga2:downtime",
            timestamp=timestamp,
        )

        self.logger.info(f"Downtime scheduled for {object_name}")


def main():
    """Main entry point for running as a standalone service"""
    import sys

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("/var/log/icinga2/splunk.log"),
        ],
    )

    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        config = Config()
        logger.info("Configuration loaded successfully")

        # Create Splunk client
        splunk_client = SplunkHECClient(config)

        # Create and start event stream client
        event_client = EventStreamClient(config, splunk_client)
        event_client.start()

        logger.info("Icinga2 to Splunk integration is running. Press Ctrl+C to stop.")

        # Keep the main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
            event_client.stop()
            splunk_client.shutdown()
            logger.info("Shutdown complete")

    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
