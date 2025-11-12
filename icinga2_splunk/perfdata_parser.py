"""
Performance Data Parser for Icinga2
Parses Nagios/Icinga2 performance data format
"""

import re
from typing import List, Dict, Any, Optional


def parse_perfdata(perfdata_string: str) -> List[Dict[str, Any]]:
    """
    Parse Icinga2/Nagios performance data string into structured metrics

    Format: 'label'=value[UOM];[warn];[crit];[min];[max]

    Examples:
        time=0.123s;0.5;1.0;0;10
        'disk usage'=85%;80;90;0;100
        bytes_sent=1234567B;;;0;

    Args:
        perfdata_string: Performance data string from check result

    Returns:
        List of dictionaries containing parsed metric data
    """
    metrics = []

    # Regular expression to match performance data format
    # Handles both quoted and unquoted labels
    pattern = r"(?:'([^']+)'|([^\s=]+))=([0-9.]+)([a-zA-Z%]*);?([0-9.]*);?([0-9.]*);?([0-9.]*);?([0-9.]*)"

    matches = re.finditer(pattern, perfdata_string)

    for match in matches:
        # Extract label (either quoted or unquoted)
        label = match.group(1) if match.group(1) else match.group(2)

        # Parse value and unit
        value_str = match.group(3)
        unit = match.group(4) if match.group(4) else ""

        try:
            value = float(value_str)
        except ValueError:
            continue  # Skip invalid values

        # Parse thresholds and ranges
        warning = _parse_threshold(match.group(5))
        critical = _parse_threshold(match.group(6))
        min_val = _parse_threshold(match.group(7))
        max_val = _parse_threshold(match.group(8))

        metric = {
            "label": label,
            "value": value,
            "unit": unit,
            "warning": warning,
            "critical": critical,
            "min": min_val,
            "max": max_val,
        }

        metrics.append(metric)

    return metrics


def _parse_threshold(threshold_str: str) -> Optional[float]:
    """
    Parse a threshold value

    Args:
        threshold_str: Threshold string (may be empty)

    Returns:
        Float value or None if empty/invalid
    """
    if not threshold_str:
        return None

    try:
        # Handle range notation (e.g., "10:20", "@10:20")
        # For now, we'll extract the upper bound
        if ":" in threshold_str:
            parts = threshold_str.lstrip("@").split(":")
            if len(parts) == 2 and parts[1]:
                return float(parts[1])
            elif len(parts) == 2 and not parts[1]:
                # Open-ended range like "10:"
                return None

        return float(threshold_str)
    except (ValueError, IndexError):
        return None


def format_metric_for_display(metric: Dict[str, Any]) -> str:
    """
    Format a parsed metric for human-readable display

    Args:
        metric: Parsed metric dictionary

    Returns:
        Formatted string representation
    """
    parts = [f"{metric['label']}={metric['value']}{metric['unit']}"]

    if metric["warning"] is not None:
        parts.append(f"warn={metric['warning']}")

    if metric["critical"] is not None:
        parts.append(f"crit={metric['critical']}")

    if metric["min"] is not None:
        parts.append(f"min={metric['min']}")

    if metric["max"] is not None:
        parts.append(f"max={metric['max']}")

    return " ".join(parts)


# Example usage and tests
if __name__ == "__main__":
    test_cases = [
        "time=0.123s;0.5;1.0;0;10",
        "'disk usage'=85%;80;90;0;100",
        "bytes_sent=1234567B;;;0;",
        "rta=0.123ms;100;200;0; pl=0%;20;40;0;100",
        "'C:\\ Used Space'=12.5GB;20;25;0;50",
    ]

    for test in test_cases:
        print(f"\nInput: {test}")
        metrics = parse_perfdata(test)
        for metric in metrics:
            print(f"  {format_metric_for_display(metric)}")
