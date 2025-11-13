#!/usr/bin/env python3
"""
Icinga2 Performance Data Handler for Splunk
Processes performance metrics from hosts and services
"""

import os
import sys
import re
import logging
from typing import Dict, List, Any
from datetime import datetime
from splunk_hec_client import SplunkHECClient


class PerformanceDataParser:
    """Parse Icinga2 performance data format"""

    # Regex pattern for performance data format
    # label=value[UOM];[warn];[crit];[min];[max]
    PERFDATA_PATTERN = re.compile(
        r"'?([^'=]+)'?=([\d\.\-]+)([\w%]*);?([\d\.\-]*);?([\d\.\-]*);?([\d\.\-]*);?([\d\.\-]*)"
    )

    @staticmethod
    def parse(perfdata_string: str) -> List[Dict[str, Any]]:
        """
        Parse performance data string into structured metrics

        Args:
            perfdata_string: Performance data string from Icinga2

        Returns:
            List of metric dictionaries
        """
        metrics = []

        if not perfdata_string:
            return metrics

        for match in PerformanceDataParser.PERFDATA_PATTERN.finditer(perfdata_string):
            label = match.group(1).strip()
            value = match.group(2)
            uom = match.group(3) or ""
            warn = match.group(4) or ""
            crit = match.group(5) or ""
            min_val = match.group(6) or ""
            max_val = match.group(7) or ""

            metric = {
                "label": label,
                "value": float(value) if value else 0.0,
                "unit": uom,
            }

            if warn:
                metric["warning"] = float(warn)
            if crit:
                metric["critical"] = float(crit)
            if min_val:
                metric["min"] = float(min_val)
            if max_val:
                metric["max"] = float(max_val)

            metrics.append(metric)

        return metrics


class Icinga2PerfdataHandler:
    """Handles Icinga2 performance data"""

    def __init__(self, client: SplunkHECClient):
        self.client = client
        self.parser = PerformanceDataParser()
        self.logger = logging.getLogger("icinga2_splunk.perfdata_handler")

    def process_host_perfdata(
        self, host_name: str, perfdata: str, check_command: str = "", **kwargs
    ) -> bool:
        """
        Process host performance data

        Args:
            host_name: Name of the host
            perfdata: Performance data string
            check_command: Check command used
            **kwargs: Additional attributes

        Returns:
            Boolean indicating success
        """
        metrics = self.parser.parse(perfdata)

        if not metrics:
            self.logger.warning("No metrics parsed from perfdata: %", perfdata)
            return False

        timestamp = datetime.now().timestamp()
        success_count = 0

        for metric in metrics:
            metric_data = {
                "metric_type": "host_perfdata",
                "timestamp": timestamp,
                "host": host_name,
                "check_command": check_command,
                "metric_name": metric["label"],
                "metric_value": metric["value"],
                "metric_unit": metric["unit"],
            }

            # Add thresholds if available
            if "warning" in metric:
                metric_data["warning_threshold"] = metric["warning"]
            if "critical" in metric:
                metric_data["critical_threshold"] = metric["critical"]
            if "min" in metric:
                metric_data["min_value"] = metric["min"]
            if "max" in metric:
                metric_data["max_value"] = metric["max"]

            # Add any additional attributes
            for key, value in kwargs.items():
                if key not in metric_data:
                    metric_data[key] = value

            if self.client.send_metric(metric_data, sourcetype="icinga2:host:perfdata"):
                success_count += 1

        self.logger.info(
            "Sent %s/%s host metrics for %s",
            success_count,
            len(metrics),
            host_name,
        )
        return success_count > 0

    def process_service_perfdata(
        self,
        host_name: str,
        service_name: str,
        perfdata: str,
        check_command: str = "",
        **kwargs,
    ) -> bool:
        """
        Process service performance data

        Args:
            host_name: Name of the host
            service_name: Name of the service
            perfdata: Performance data string
            check_command: Check command used
            **kwargs: Additional attributes

        Returns:
            Boolean indicating success
        """
        metrics = self.parser.parse(perfdata)

        if not metrics:
            self.logger.warning("No metrics parsed from perfdata: %s", perfdata)
            return False

        timestamp = datetime.now().timestamp()
        success_count = 0

        for metric in metrics:
            metric_data = {
                "metric_type": "service_perfdata",
                "timestamp": timestamp,
                "host": host_name,
                "service": service_name,
                "check_command": check_command,
                "metric_name": metric["label"],
                "metric_value": metric["value"],
                "metric_unit": metric["unit"],
            }

            # Add thresholds if available
            if "warning" in metric:
                metric_data["warning_threshold"] = metric["warning"]
            if "critical" in metric:
                metric_data["critical_threshold"] = metric["critical"]
            if "min" in metric:
                metric_data["min_value"] = metric["min"]
            if "max" in metric:
                metric_data["max_value"] = metric["max"]

            # Add any additional attributes
            for key, value in kwargs.items():
                if key not in metric_data:
                    metric_data[key] = value

            if self.client.send_metric(
                metric_data, sourcetype="icinga2:service:perfdata"
            ):
                success_count += 1

        self.logger.info(
            "Sent %s/%s service metrics for %s/%s",
            success_count,
            len(metrics),
            host_name,
            service_name,
        )
        return success_count > 0


def main():
    """Main entry point for perfdata handler script"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("/var/log/icinga2/splunk_perfdata.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Configuration from environment
    hec_url = os.getenv("SPLUNK_HEC_URL", "https://splunk.example.com:8088")
    hec_token = os.getenv("SPLUNK_HEC_TOKEN", "")
    verify_ssl = os.getenv("SPLUNK_VERIFY_SSL", "true").lower() == "true"

    if not hec_token:
        logging.error("SPLUNK_HEC_TOKEN not set")
        sys.exit(1)

    # Initialize client and handler
    client = SplunkHECClient(hec_url, hec_token, verify_ssl)
    handler = Icinga2PerfdataHandler(client)

    # Determine data type and process accordingly
    data_type = os.getenv("ICINGA2_DATA_TYPE", "service")

    if data_type == "host":
        success = handler.process_host_perfdata(
            host_name=os.getenv("ICINGA2_HOSTNAME", ""),
            perfdata=os.getenv("ICINGA2_HOSTPERFDATA", ""),
            check_command=os.getenv("ICINGA2_HOSTCHECKCOMMAND", ""),
        )
    else:  # service
        success = handler.process_service_perfdata(
            host_name=os.getenv("ICINGA2_HOSTNAME", ""),
            service_name=os.getenv("ICINGA2_SERVICENAME", ""),
            perfdata=os.getenv("ICINGA2_SERVICEPERFDATA", ""),
            check_command=os.getenv("ICINGA2_SERVICECHECKCOMMAND", ""),
        )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
