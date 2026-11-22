from __future__ import annotations

import pytest

import icinga2_splunk


def test_lazy_exports() -> None:
    from icinga2_splunk.config import Config

    assert icinga2_splunk.Config is Config
    assert callable(icinga2_splunk.parse_perfdata)
    assert set(icinga2_splunk.__all__) <= set(dir(icinga2_splunk))


def test_unknown_attribute() -> None:
    with pytest.raises(AttributeError):
        _ = icinga2_splunk.nope
