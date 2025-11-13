#!/usr/bin/env python3
"""
Icinga2 Event Stream to Splunk Integration Client
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import logging
import signal
import threading
import time

import requests
import urllib3
from config import Config
from perfdata_parser import PerfdataParser
from splunk_hec_client import SplunkHECClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EventStreamClient:
    def __init__(self, config_dict):
        self.config = config_dict

        if not config_dict.get("icinga2_api_verify_ssl", True):
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.splunk_client = SplunkHECClient(
            hec_url=config_dict["splunk_hec_url"],
            hec_token=config_dict["splunk_hec_token"],
            verify_ssl=config_dict.get("splunk_verify_ssl", True),
            batch_size=config_dict.get("batch_size", 100),
            batch_timeout=config_dict.get("batch_timeout", 10),
        )

        self.perfdata_parser = PerfdataParser()
        self.running = False
        self.session = None
        self.last_event_time = time.time()
        self.heartbeat_timeout = config_dict.get(
            "heartbeat_timeout", 300
        )  # 5 minutes default

    def _should_process_event(self, event_type, event_data):
        """Determine if an event should be processed based on filters"""
        # Check if event type is enabled
        if event_type == "CheckResult" and not self.config.get(
            "include_check_results", False
        ):
            return False

        # Filter CheckResults by state if configured
        if event_type == "CheckResult" and "check_result_state_filter" in self.config:
            state_filter = self.config["check_result_state_filter"]
            check_result = event_data.get("check_result", {})
            state = check_result.get("state", 0)
            if state not in state_filter:
                return False

        return True

    def _connect_and_consume(self):
        """Connect to Icinga2 Event Stream and consume events"""
        url = f"{self.config['icinga2_api_url']}/v1/events"

        auth = None
        cert = None

        # Setup authentication
        if self.config.get("icinga2_api_username") and self.config.get(
            "icinga2_api_password"
        ):
            auth = (
                self.config["icinga2_api_username"],
                self.config["icinga2_api_password"],
            )
        elif self.config.get("icinga2_api_client_cert") and self.config.get(
            "icinga2_api_client_key"
        ):
            cert = (
                self.config["icinga2_api_client_cert"],
                self.config["icinga2_api_client_key"],
            )

        # Prepare subscription payload
        event_types = self.config.get("event_types") or self.config.get(
            "event_stream_types", ["StateChange", "Notification"]
        )
        event_types = event_types.copy()

        # Performance data comes from CheckResult events, so we need to subscribe even if we filter them
        if self.config.get("include_check_results", False) or self.config.get(
            "include_perfdata", True
        ):
            if "CheckResult" not in event_types:
                event_types.append("CheckResult")
                logger.info(
                    "Added CheckResult to subscription for performance data collection"
                )

        payload = {
            "queue": self.config.get("event_stream_queue", "splunk"),
            "types": event_types,
        }

        # Add filter if configured
        if "event_stream_filter" in self.config:
            payload["filter"] = self.config["event_stream_filter"]

        headers = {"Accept": "application/json"}

        logger.info("Connecting to Icinga2 Event Stream: %s", url)
        logger.info("Subscribed to event types: %s", payload["types"])

        try:
            self.session = requests.Session()
            self.last_event_time = time.time()

            response = self.session.post(
                url,
                auth=auth,
                cert=cert,
                headers=headers,
                json=payload,
                verify=self.config.get("icinga2_api_verify_ssl", True),
                stream=True,
                timeout=None,  # No timeout for persistent Event Stream
            )

            if response.status_code != 200:
                logger.error(
                    "Failed to connect to Event Stream: %s - %s",
                    response.status_code,
                    response.text,
                )
                return False

            logger.info("Successfully connected to Icinga2 Event Stream")

            heartbeat_thread = threading.Thread(
                target=self._heartbeat_monitor, daemon=True
            )
            heartbeat_thread.start()

            # Process events line by line
            for line in response.iter_lines():
                if not self.running:
                    logger.info("Stop signal received, closing connection")
                    break

                if not line:
                    continue

                self.last_event_time = time.time()

                try:
                    event = json.loads(line.decode("utf-8"))
                    self._process_event(event)
                except json.JSONDecodeError as e:
                    logger.error("Failed to parse event JSON: %s", e)
                except Exception as e:
                    logger.exception("Error processing event: %s", e)
            if self.running:
                logger.warning("Event Stream connection closed by server")

            return False  # Always return False to trigger reconnection

        except requests.exceptions.RequestException as e:
            logger.error("Connection error: %s", e)
            return False
        except Exception as e:
            logger.exception("Unexpected error in event stream: %s", e)
            return False
        finally:
            if self.session:
                try:
                    self.session.close()
                except:
                    pass

    def _heartbeat_monitor(self):
        """Monitor for silent connection failures by checking time since last event"""
        while self.running:
            time.sleep(30)  # Check every 30 seconds

            if not self.running:
                break

            time_since_last_event = time.time() - self.last_event_time

            if time_since_last_event > self.heartbeat_timeout:
                logger.error(
                    f"No events received for {int(time_since_last_event)} seconds (timeout: {self.heartbeat_timeout}s) - connection appears dead"
                )
                # Close the session to force reconnection
                if self.session:
                    try:
                        self.session.close()
                        logger.info("Closed stale session to force reconnection")
                    except Exception as e:
                        logger.error("Error closing stale session: %s", e)
                break

    def _process_event(self, event):
        """Process a single event from the stream"""
        event_type = event.get("type")

        if not event_type:
            return

        # Route to appropriate handler
        if event_type == "CheckResult":
            self._process_check_result(event)
        elif event_type == "StateChange":
            self._process_state_change(event)
        elif event_type == "Notification":
            self._process_notification(event)
        elif event_type in ["AcknowledgementSet", "AcknowledgementCleared"]:
            self._process_acknowledgement(event)
        elif event_type in ["CommentAdded", "CommentRemoved"]:
            self._process_comment(event)
        elif event_type in ["DowntimeAdded", "DowntimeRemoved", "DowntimeTriggered"]:
            self._process_downtime(event)
        else:
            logger.debug("Unhandled event type: %s", event_type)

    def _process_check_result(self, event):
        """Process CheckResult event"""
        check_result = event.get("check_result", {})
        host = event.get("host")
        service = event.get("service")

        send_event = self.config.get("include_check_results", False)

        # Apply state filter if configured
        if send_event and "check_result_state_filter" in self.config:
            state_filter = self.config["check_result_state_filter"]
            state = check_result.get("state", 0)
            if state not in state_filter:
                send_event = False

        # Extract basic info
        if send_event:
            event_data = {
                "event_type": "CheckResult",
                "host": host,
                "service": service,
                "object_name": f"{host}!{service}" if service else host,
                "state": check_result.get("state", 0),
                "output": check_result.get("output", ""),
                "execution_start": check_result.get("execution_start"),
                "execution_end": check_result.get("execution_end"),
                "schedule_start": check_result.get("schedule_start"),
                "schedule_end": check_result.get("schedule_end"),
                "active": check_result.get("active"),
                "command": check_result.get("command", []),
            }

            timestamp = check_result.get("execution_end", time.time())

            # Send event to Splunk
            self.splunk_client.send_event(
                event_data,
                host=host,
                sourcetype="icinga2:checkresult",
                timestamp=timestamp,
                index_name=self.config.get("splunk_index_events", "icinga2_events"),
            )

        if self.config.get("include_perfdata", True):
            perfdata = check_result.get("performance_data", [])
            timestamp = check_result.get("execution_end", time.time())
            logger.debug(
                "[PERFDATA DEBUG] Host: %s, Service: %s, Perfdata: %s",
                host,
                service,
                perfdata,
            )
            if perfdata:
                self._process_perfdata(perfdata, host, service, timestamp)
            else:
                logger.debug("[PERFDATA DEBUG] No perfdata for %s!%s", host, service)

    def _process_state_change(self, event):
        """Process StateChange event"""
        host = event.get("host")
        service = event.get("service")
        check_result = event.get("check_result", {})

        event_data = {
            "event_type": "StateChange",
            "host": host,
            "service": service,
            "object_name": f"{host}!{service}" if service else host,
            "state": event.get("state", 0),
            "state_type": event.get("state_type", 0),
            "previous_state": event.get("previous_state", 0),
            "output": check_result.get("output", ""),
            "timestamp": event.get("timestamp", time.time()),
        }

        # Send event to Splunk
        self.splunk_client.send_event(
            event_data,
            host=host,
            sourcetype="icinga2:statechange",
            timestamp=event.get("timestamp", time.time()),
            index_name=self.config.get("splunk_index_events", "icinga2_events"),
        )

        # Process performance data if present
        perfdata = check_result.get("performance_data", [])
        if perfdata:
            self._process_perfdata(
                perfdata, host, service, event.get("timestamp", time.time())
            )

    def _process_notification(self, event):
        """Process Notification event"""
        host = event.get("host")
        service = event.get("service")

        event_data = {
            "event_type": "Notification",
            "host": host,
            "service": service,
            "object_name": f"{host}!{service}" if service else host,
            "notification_type": event.get("notification_type"),
            "author": event.get("author"),
            "text": event.get("text"),
            "users": event.get("users", []),
            "timestamp": event.get("timestamp", time.time()),
        }

        self.splunk_client.send_event(
            event_data,
            host=host,
            sourcetype="icinga2:notification",
            timestamp=event.get("timestamp", time.time()),
            index_name=self.config.get("splunk_index_events", "icinga2_events"),
        )

    def _process_acknowledgement(self, event):
        """Process Acknowledgement events"""
        host = event.get("host")
        service = event.get("service")

        event_data = {
            "event_type": event.get("type"),
            "host": host,
            "service": service,
            "object_name": f"{host}!{service}" if service else host,
            "author": event.get("author"),
            "comment": event.get("comment"),
            "timestamp": event.get("timestamp", time.time()),
        }

        self.splunk_client.send_event(
            event_data,
            host=host,
            sourcetype="icinga2:acknowledgement",
            timestamp=event.get("timestamp", time.time()),
            index_name=self.config.get("splunk_index_events", "icinga2_events"),
        )

    def _process_comment(self, event):
        """Process Comment events"""
        host = event.get("host")
        service = event.get("service")

        event_data = {
            "event_type": event.get("type"),
            "host": host,
            "service": service,
            "object_name": f"{host}!{service}" if service else host,
            "author": event.get("author"),
            "text": event.get("text"),
            "timestamp": event.get("timestamp", time.time()),
        }

        self.splunk_client.send_event(
            event_data,
            host=host,
            sourcetype="icinga2:comment",
            timestamp=event.get("timestamp", time.time()),
            index_name=self.config.get("splunk_index_events", "icinga2_events"),
        )

    def _process_downtime(self, event):
        """Process Downtime events"""
        host = event.get("host")
        service = event.get("service")

        event_data = {
            "event_type": event.get("type"),
            "host": host,
            "service": service,
            "object_name": f"{host}!{service}" if service else host,
            "author": event.get("author"),
            "comment": event.get("comment"),
            "start_time": event.get("start_time"),
            "end_time": event.get("end_time"),
            "timestamp": event.get("timestamp", time.time()),
        }

        self.splunk_client.send_event(
            event_data,
            host=host,
            sourcetype="icinga2:downtime",
            timestamp=event.get("timestamp", time.time()),
            index_name=self.config.get("splunk_index_events", "icinga2_events"),
        )

    def _process_perfdata(self, perfdata, host, service, timestamp):
        """Process performance data from check results"""
        object_name = f"{host}!{service}" if service else host

        # Handle different perfdata formats
        perfdata_str = perfdata
        if isinstance(perfdata, list):
            if perfdata and isinstance(perfdata[0], dict):
                # Convert list of dicts to Nagios perfdata string
                perfdata_items = []
                for item in perfdata:
                    label = item.get("label", "")
                    value = item.get("value", "")
                    uom = item.get("unit", "")
                    warn = item.get("warn", "")
                    crit = item.get("crit", "")
                    min_val = item.get("min", "")
                    max_val = item.get("max", "")
                    perfdata_items.append(
                        f"{label}={value}{uom};{warn};{crit};{min_val};{max_val}"
                    )
                perfdata_str = " ".join(perfdata_items)
            else:
                # List of strings
                perfdata_str = " ".join(perfdata)

        # Parse perfdata
        try:
            metrics = self.perfdata_parser.parse(perfdata_str)

            for metric in metrics:
                perfdata_event = {
                    "host": host,
                    "service": service,
                    "object_name": object_name,
                    "timestamp": timestamp,
                    "metric": metric["label"],
                    "value": metric["value"],
                    "unit": metric["uom"],
                    "warning": metric["warn"],
                    "critical": metric["crit"],
                    "min": metric["min"],
                    "max": metric["max"],
                }

                self.splunk_client.send_perfdata(
                    perfdata_event,
                    host=host,
                    timestamp=timestamp,
                    index_name=self.config.get(
                        "splunk_index_perfdata", "icinga2_perfdata"
                    ),
                )
        except Exception as e:
            logger.error("Error parsing performance data: %s", e)

    def start(self):
        """Start the Event Stream client"""
        self.running = True
        logger.info("Starting Icinga2 to Splunk Event Stream client")

        reconnect_count = 0

        while self.running:
            try:
                if reconnect_count > 0:
                    logger.info("Reconnection attempt #%s", reconnect_count)

                success = self._connect_and_consume()

                if not self.running:
                    logger.info("Shutdown requested, exiting")
                    break

                reconnect_count += 1
                logger.warning(
                    "Connection lost, reconnecting in 10 seconds... (attempt #%s)",
                    reconnect_count,
                )
                time.sleep(10)

            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                self.stop()
                break
            except Exception as e:
                logger.exception("Unexpected error in main loop: %s", e)
                reconnect_count += 1
                logger.warning(
                    "Reconnecting in 10 seconds... (attempt #%s)", reconnect_count
                )
                time.sleep(10)

    def stop(self):
        """Stop the Event Stream client"""
        logger.info("Stopping Icinga2 to Splunk Event Stream client")
        self.running = False

        if self.session:
            self.session.close()

        # Flush any pending data
        self.splunk_client.flush()
        logger.info("Client stopped")


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("Received signal %s, shutting down...", signum)
    if "client" in globals():
        client.stop()
    sys.exit(0)


def main():
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Load configuration
    config = Config()

    # Create and start client
    client = EventStreamClient(config.config)
    client.start()


if __name__ == "__main__":
    main()
