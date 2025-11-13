#!/usr/bin/env python3
"""
Performance Data Parser for Icinga2/Nagios format
"""

import re
import logging

logger = logging.getLogger(__name__)


class PerfdataParser:
    """
    Parse Nagios/Icinga2 performance data format

    Format: 'label'=value[UOM];[warn];[crit];[min];[max]
    Example: 'cpu-usage'=45.2%;80;90;0;100
    """

    # Regex pattern for Nagios perfdata format
    PERFDATA_PATTERN = re.compile(
        r"'?([^'=]+)'?="  # label (with optional quotes)
        r"([-0-9.]+)"  # value
        r"([^;]*)"  # unit of measurement (optional)
        r"(?:;([-0-9.]*))"  # warning threshold (optional)
        r"?(?:;([-0-9.]*))?"  # critical threshold (optional)
        r"(?:;([-0-9.]*))?"  # min value (optional)
        r"(?:;([-0-9.]*))?",  # max value (optional)
        re.IGNORECASE,
    )

    def parse(self, perfdata_string):
        """
        Parse performance data string into structured metrics

        Args:
            perfdata_string: Nagios/Icinga2 perfdata format string

        Returns:
            List of metric dictionaries with keys: label, value, uom, warn, crit, min, max
        """
        if not perfdata_string or not isinstance(perfdata_string, str):
            return []

        metrics = []

        # Split by whitespace to get individual metrics
        for match in self.PERFDATA_PATTERN.finditer(perfdata_string):
            try:
                label = match.group(1).strip()
                value = self._parse_number(match.group(2))
                uom = match.group(3) if match.group(3) else ""
                warn = self._parse_number(match.group(4)) if match.group(4) else None
                crit = self._parse_number(match.group(5)) if match.group(5) else None
                min_val = self._parse_number(match.group(6)) if match.group(6) else None
                max_val = self._parse_number(match.group(7)) if match.group(7) else None

                metrics.append(
                    {
                        "label": label,
                        "value": value,
                        "uom": uom,
                        "warn": warn,
                        "crit": crit,
                        "min": min_val,
                        "max": max_val,
                    }
                )
            except Exception as e:
                logger.warning("Failed to parse metric: %s - %s", match.group(0), e)
                continue

        return metrics

    def _parse_number(self, value_str):
        """
        Parse a numeric value, handling both int and float
        """
        if not value_str:
            return None

        try:
            if "." in value_str:
                return float(value_str)
            else:
                return int(value_str)
        except ValueError:
            return None
