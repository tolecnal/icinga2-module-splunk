#!/usr/bin/env python3
"""
Icinga2 Event Handler for Splunk
Processes state changes for hosts and services
"""

import os
import sys
import json
import logging
from typing import Dict, Any
from datetime import datetime
from splunk_hec_client import SplunkHECClient


class Icinga2EventHandler:
    """Handles Icinga2 host and service state change events"""

    def __init__(self, client: SplunkHECClient):
        self.client = client
        self.logger = logging.getLogger("icinga2_splunk.event_handler")

    def process_host_event(
        self,
        host_name: str,
        host_state: str,
        host_state_type: str,
        host_attempt: int,
        host_output: str,
        **kwargs,
    ) -> bool:
        """
        Process host state change event

        Args:
            host_name: Name of the host
            host_state: Current state (UP, DOWN)
            host_state_type: SOFT or HARD
            host_attempt: Current check attempt
            host_output: Plugin output
            **kwargs: Additional host attributes

        Returns:
            Boolean indicating success
        """
        event_data = {
            "event_type": "host_state_change",
            "timestamp": datetime.now().timestamp(),
            "host": host_name,
            "state": host_state,
            "state_type": host_state_type,
            "attempt": host_attempt,
            "output": host_output,
            "last_state": kwargs.get("last_host_state", "UNKNOWN"),
            "last_state_change": kwargs.get("last_state_change", ""),
            "downtime_depth": kwargs.get("host_downtime_depth", 0),
            "acknowledgement": kwargs.get("host_acknowledgement", 0),
        }

        # Add any additional attributes
        for key, value in kwargs.items():
            if key not in event_data:
                event_data[key] = value

        self.logger.info("Processing host event: %s -> %s", host_name, host_state)
        return self.client.send_event(event_data, sourcetype="icinga2:host:state")

    def process_service_event(
        self,
        host_name: str,
        service_name: str,
        service_state: str,
        service_state_type: str,
        service_attempt: int,
        service_output: str,
        **kwargs,
    ) -> bool:
        """
        Process service state change event

        Args:
            host_name: Name of the host
            service_name: Name of the service
            service_state: Current state (OK, WARNING, CRITICAL, UNKNOWN)
            service_state_type: SOFT or HARD
            service_attempt: Current check attempt
            service_output: Plugin output
            **kwargs: Additional service attributes

        Returns:
            Boolean indicating success
        """
        event_data = {
            "event_type": "service_state_change",
            "timestamp": datetime.now().timestamp(),
            "host": host_name,
            "service": service_name,
            "state": service_state,
            "state_type": service_state_type,
            "attempt": service_attempt,
            "output": service_output,
            "last_state": kwargs.get("last_service_state", "UNKNOWN"),
            "last_state_change": kwargs.get("last_state_change", ""),
            "downtime_depth": kwargs.get("service_downtime_depth", 0),
            "acknowledgement": kwargs.get("service_acknowledgement", 0),
        }

        # Add any additional attributes
        for key, value in kwargs.items():
            if key not in event_data:
                event_data[key] = value

        self.logger.info(
            "Processing service event: %s/%s -> %s",
            host_name,
            service_name,
            service_state,
        )
        return self.client.send_event(event_data, sourcetype="icinga2:service:state")


def main():
    """Main entry point for event handler script"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("/var/log/icinga2/splunk_events.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Configuration from environment or config file
    hec_url = os.getenv("SPLUNK_HEC_URL", "https://splunk.example.com:8088")
    hec_token = os.getenv("SPLUNK_HEC_TOKEN", "")
    verify_ssl = os.getenv("SPLUNK_VERIFY_SSL", "true").lower() == "true"

    if not hec_token:
        logging.error("SPLUNK_HEC_TOKEN not set")
        sys.exit(1)

    # Initialize client and handler
    client = SplunkHECClient(hec_url, hec_token, verify_ssl)
    handler = Icinga2EventHandler(client)

    # Determine event type and process accordingly
    event_type = os.getenv("ICINGA2_EVENT_TYPE", "service")

    if event_type == "host":
        success = handler.process_host_event(
            host_name=os.getenv("ICINGA2_HOSTNAME", ""),
            host_state=os.getenv("ICINGA2_HOSTSTATE", ""),
            host_state_type=os.getenv("ICINGA2_HOSTSTATETYPE", ""),
            host_attempt=int(os.getenv("ICINGA2_HOSTATTEMPT", "0")),
            host_output=os.getenv("ICINGA2_HOSTOUTPUT", ""),
            last_host_state=os.getenv("ICINGA2_LASTHOSTSTATE", ""),
            last_state_change=os.getenv("ICINGA2_LASTHOSTSTATECHANGE", ""),
            host_downtime_depth=int(os.getenv("ICINGA2_HOSTDOWNTIMEDEPTH", "0")),
            host_acknowledgement=int(os.getenv("ICINGA2_HOSTACKNOWLEDGEMENT", "0")),
        )
    else:  # service
        success = handler.process_service_event(
            host_name=os.getenv("ICINGA2_HOSTNAME", ""),
            service_name=os.getenv("ICINGA2_SERVICENAME", ""),
            service_state=os.getenv("ICINGA2_SERVICESTATE", ""),
            service_state_type=os.getenv("ICINGA2_SERVICESTATETYPE", ""),
            service_attempt=int(os.getenv("ICINGA2_SERVICEATTEMPT", "0")),
            service_output=os.getenv("ICINGA2_SERVICEOUTPUT", ""),
            last_service_state=os.getenv("ICINGA2_LASTSERVICESTATE", ""),
            last_state_change=os.getenv("ICINGA2_LASTSERVICESTATECHANGE", ""),
            service_downtime_depth=int(os.getenv("ICINGA2_SERVICEDOWNTIMEDEPTH", "0")),
            service_acknowledgement=int(
                os.getenv("ICINGA2_SERVICEACKNOWLEDGEMENT", "0")
            ),
        )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
