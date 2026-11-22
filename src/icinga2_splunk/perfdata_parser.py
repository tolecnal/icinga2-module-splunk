"""Parser for Nagios/Icinga2 performance data.

Format of a single metric::

    'label'=value[UOM];[warn];[crit];[min];[max]

Labels containing spaces must be quoted with single quotes; a literal single
quote inside a quoted label is written as two single quotes. ``warn`` and
``crit`` may be plain numbers or Nagios ranges such as ``10:20``, ``~:5`` or
``@0:10`` - plain numbers are returned as floats, ranges as strings.
"""

from __future__ import annotations

import logging
import math
import re
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import asdict, dataclass
from typing import Any, TypeAlias

logger = logging.getLogger(__name__)

# A label is either single-quoted ('' escapes a quote) or a run of characters
# without whitespace, '=' or quotes; the rest of the token runs to whitespace.
_TOKEN_RE = re.compile(r"(?:'((?:[^']|'')*)'|([^\s=']+))=(\S*)")
_VALUE_RE = re.compile(r"^([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)(.*)$")

#: A threshold is either a plain number or a Nagios range string.
Threshold: TypeAlias = float | str | None

#: Accepted input: a perfdata string, or a list of strings and/or
#: serialized Icinga2 ``PerfdataValue`` dictionaries.
PerfdataInput: TypeAlias = str | Iterable[str | Mapping[str, Any]] | None


@dataclass(frozen=True)
class Metric:
    """A single parsed performance data metric."""

    label: str
    value: float
    unit: str = ""
    warn: Threshold = None
    crit: Threshold = None
    min: float | None = None
    max: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return the metric as a plain dictionary."""
        return asdict(self)


def _to_float(text: str | None) -> float | None:
    if text is None or text == "":
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _to_threshold(text: str | None) -> Threshold:
    if text is None or text == "":
        return None
    number = _to_float(text)
    return number if number is not None else text


def _split_tokens(perfdata: str) -> Iterator[tuple[str, str]]:
    """Yield ``(label, rest)`` pairs from a whitespace-separated perfdata string."""
    for match in _TOKEN_RE.finditer(perfdata):
        quoted, bare, rest = match.groups()
        label = quoted.replace("''", "'") if quoted is not None else bare
        yield label, rest


def _parse_metric(label: str, rest: str) -> Metric | None:
    fields = rest.split(";")
    fields += [""] * (5 - len(fields))
    raw_value, warn, crit, min_val, max_val = fields[:5]

    match = _VALUE_RE.match(raw_value)
    if not label or not match:
        # Includes the Nagios "U" (unknown) value.
        logger.debug("Skipping perfdata metric %r with unusable value %r", label, raw_value)
        return None

    value = _to_float(match.group(1))
    if value is None:
        return None

    return Metric(
        label=label,
        value=value,
        unit=match.group(2),
        warn=_to_threshold(warn),
        crit=_to_threshold(crit),
        min=_to_float(min_val),
        max=_to_float(max_val),
    )


def _from_mapping(item: Mapping[str, Any]) -> Metric | None:
    """Convert a serialized Icinga2 ``PerfdataValue`` dictionary to a metric."""
    label = item.get("label")
    value = item.get("value")
    if (
        not label
        or isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        return None

    def number(key: str) -> float | None:
        raw = item.get(key)
        if isinstance(raw, bool) or not isinstance(raw, (int, float)) or not math.isfinite(raw):
            return None
        return float(raw)

    def threshold(key: str) -> Threshold:
        raw = item.get(key)
        if isinstance(raw, str):
            return _to_threshold(raw)
        return number(key)

    return Metric(
        label=str(label),
        value=float(value),
        unit=str(item.get("unit") or ""),
        warn=threshold("warn"),
        crit=threshold("crit"),
        min=number("min"),
        max=number("max"),
    )


def parse_perfdata(perfdata: PerfdataInput) -> list[Metric]:
    """Parse performance data into a list of :class:`Metric` objects.

    Args:
        perfdata: A perfdata string, or an iterable of perfdata strings and/or
            serialized ``PerfdataValue`` dictionaries as returned by the
            Icinga2 API in ``check_result.performance_data``.

    Returns:
        The successfully parsed metrics. Malformed entries are skipped.
    """
    if not perfdata:
        return []

    # Typed as object: the API payload is untrusted and may contain anything.
    items: Iterable[object] = [perfdata] if isinstance(perfdata, str) else perfdata

    metrics: list[Metric] = []
    for item in items:
        if isinstance(item, str):
            for label, rest in _split_tokens(item):
                metric = _parse_metric(label, rest)
                if metric is not None:
                    metrics.append(metric)
        elif isinstance(item, Mapping):
            metric = _from_mapping(item)
            if metric is not None:
                metrics.append(metric)
        else:
            logger.debug("Ignoring perfdata entry of unexpected type %s", type(item).__name__)
    return metrics
