from __future__ import annotations

import pytest

from icinga2_splunk.perfdata_parser import Metric, parse_perfdata


def test_full_metric() -> None:
    assert parse_perfdata("'cpu-usage'=45.2%;80;90;0;100") == [
        Metric("cpu-usage", 45.2, "%", 80.0, 90.0, 0.0, 100.0)
    ]


def test_multiple_metrics_without_trailing_semicolons() -> None:
    # Regression: the old regex swallowed following metrics into the unit.
    metrics = parse_perfdata("time=0.5s size=100B users=3")
    assert [(m.label, m.value, m.unit) for m in metrics] == [
        ("time", 0.5, "s"),
        ("size", 100.0, "B"),
        ("users", 3.0, ""),
    ]


def test_range_thresholds_kept_as_strings() -> None:
    # Regression: ranges used to corrupt the label of the following metric.
    metrics = parse_perfdata("load=1.5;5:10;@0:20;0 temp=40C;~:50;60")
    assert metrics[0] == Metric("load", 1.5, "", "5:10", "@0:20", 0.0, None)
    assert metrics[1] == Metric("temp", 40.0, "C", "~:50", 60.0, None, None)


def test_quoted_labels_with_spaces_and_escaped_quotes() -> None:
    metrics = parse_perfdata("'disk usage /'=10GB;;;0;100 'it''s'=1")
    assert [m.label for m in metrics] == ["disk usage /", "it's"]
    assert metrics[0].max == 100.0


@pytest.mark.parametrize(
    ("raw", "value"),
    [("-5", -5.0), ("+5", 5.0), (".5", 0.5), ("1e3", 1000.0), ("1.5E-2", 0.015)],
)
def test_number_formats(raw: str, value: float) -> None:
    assert parse_perfdata(f"x={raw}")[0].value == value


@pytest.mark.parametrize("perfdata", ["", None, [], "garbage", "x=U", "x=", "'=1", "x=abc"])
def test_unparseable_input_returns_nothing(perfdata: object) -> None:
    assert parse_perfdata(perfdata) == []  # type: ignore[arg-type]


def test_list_of_strings_and_dicts() -> None:
    metrics = parse_perfdata(
        [
            "rta=0.5ms;100;500;0",
            {
                "type": "PerfdataValue",
                "label": "pl",
                "value": 0,
                "unit": "%",
                "warn": 5,
                "crit": 10,
                "min": 0,
                "max": None,
            },
            {"label": "bad", "value": "nope"},
            {"label": "flag", "value": True},
            42,
        ]
    )
    assert metrics == [
        Metric("rta", 0.5, "ms", 100.0, 500.0, 0.0, None),
        Metric("pl", 0.0, "%", 5.0, 10.0, 0.0, None),
    ]


def test_dict_with_string_range_threshold() -> None:
    [metric] = parse_perfdata([{"label": "x", "value": 1, "warn": "10:20", "crit": "nan"}])
    assert metric.warn == "10:20"
    assert metric.crit == "nan"


def test_non_finite_values_are_dropped() -> None:
    assert parse_perfdata("x=1;inf;nan;-inf;nan")[0] == Metric("x", 1.0, "", "inf", "nan")


def test_to_dict() -> None:
    assert parse_perfdata("a=1")[0].to_dict() == {
        "label": "a",
        "value": 1.0,
        "unit": "",
        "warn": None,
        "crit": None,
        "min": None,
        "max": None,
    }
