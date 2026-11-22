from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import pytest

from icinga2_splunk.config import Config, ConfigError, normalize_hec_url


def write_config(tmp_path: Path, data: Any, mode: int = 0o600) -> Path:
    path = tmp_path / "splunk_config.json"
    path.write_text(json.dumps(data))
    path.chmod(mode)
    return path


def test_load_file_with_defaults(tmp_path: Path, base_settings: dict[str, Any]) -> None:
    config = Config.load(write_config(tmp_path, base_settings), environ={})
    assert config.icinga2_auth == ("splunk", "secret")
    assert config.icinga2_cert is None
    assert config.batch_size == 100
    assert config.include_perfdata is True
    assert "AcknowledgementCleared" in config.event_types


def test_path_from_environment(tmp_path: Path, base_settings: dict[str, Any]) -> None:
    path = write_config(tmp_path, base_settings)
    config = Config.load(environ={"ICINGA2_SPLUNK_CONFIG": str(path)})
    assert config.splunk_hec_token == "token-123"


def test_missing_file_uses_environment(tmp_path: Path) -> None:
    env = {
        "ICINGA2_API_URL": "https://icinga:5665/",
        "ICINGA2_API_CLIENT_CERT": "/c.pem",
        "ICINGA2_API_CLIENT_KEY": "/k.pem",
        "ICINGA2_API_VERIFY_SSL": "/var/lib/icinga2/certs/ca.crt",
        "SPLUNK_HEC_URL": "https://splunk:8088/services/collector",
        "SPLUNK_HEC_TOKEN": "12345",
        "SPLUNK_VERIFY_SSL": "false",
        "BATCH_SIZE": "50",
        "BATCH_TIMEOUT": "2.5",
        "EVENT_TYPES": "StateChange, Notification",
        "CHECK_RESULT_STATE_FILTER": "2,3",
        "INCLUDE_PERFDATA": "no",
        "LOG_LEVEL": "debug",
        "SPOOL_DIR": "",
        "SPOOL_MAX_SIZE_MB": "512",
        "SPOOL_MAX_AGE_DAYS": "1.5",
        "RETRY_MAX_INTERVAL": "120",
    }
    config = Config.load(tmp_path / "missing.json", environ=env)
    assert config.icinga2_api_url == "https://icinga:5665"
    assert config.icinga2_cert == ("/c.pem", "/k.pem")
    assert config.icinga2_api_verify_ssl == "/var/lib/icinga2/certs/ca.crt"
    assert config.splunk_hec_url == "https://splunk:8088"
    # Regression: numeric secrets must stay strings.
    assert config.splunk_hec_token == "12345"
    assert config.splunk_verify_ssl is False
    assert config.batch_size == 50
    assert config.batch_timeout == 2.5
    assert config.event_types == ["StateChange", "Notification"]
    assert config.check_result_state_filter == [2, 3]
    assert config.include_perfdata is False
    assert config.log_level == "DEBUG"
    assert config.spool_dir == ""
    assert config.spool_max_size_mb == 512
    assert config.spool_max_age_days == 1.5
    assert config.retry_max_interval == 120


def test_numeric_password_from_env_stays_string(
    tmp_path: Path, base_settings: dict[str, Any]
) -> None:
    path = write_config(tmp_path, base_settings)
    config = Config.load(path, environ={"ICINGA2_API_PASSWORD": "true"})
    assert config.icinga2_api_password == "true"


def test_invalid_env_value(tmp_path: Path, base_settings: dict[str, Any]) -> None:
    path = write_config(tmp_path, base_settings)
    with pytest.raises(ConfigError, match="BATCH_SIZE"):
        Config.load(path, environ={"BATCH_SIZE": "lots"})
    with pytest.raises(ConfigError, match="INCLUDE_PERFDATA"):
        Config.load(path, environ={"INCLUDE_PERFDATA": "maybe"})


def test_legacy_keys(
    tmp_path: Path, base_settings: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    data = {**base_settings, "event_stream_types": ["StateChange"], "splunk_batch_size": 7}
    config = Config.load(write_config(tmp_path, data), environ={})
    assert config.event_types == ["StateChange"]
    assert config.batch_size == 7
    assert "deprecated" in caplog.text


def test_legacy_key_ignored_when_new_key_present(
    tmp_path: Path, base_settings: dict[str, Any]
) -> None:
    data = {**base_settings, "batch_size": 5, "splunk_batch_size": 7}
    assert Config.load(write_config(tmp_path, data), environ={}).batch_size == 5


def test_unknown_keys_warn(make_config, caplog: pytest.LogCaptureFixture) -> None:
    make_config(bacth_size=5)
    assert "bacth_size" in caplog.text


def test_world_readable_file_warns(
    tmp_path: Path, base_settings: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    Config.load(write_config(tmp_path, base_settings, mode=0o644), environ={})
    assert "world-readable" in caplog.text


def test_world_writable_file_warns(
    tmp_path: Path, base_settings: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    Config.load(write_config(tmp_path, base_settings, mode=0o602), environ={})
    assert "world-readable/writable" in caplog.text


@pytest.mark.parametrize("content", ["{not json", "[1, 2]"])
def test_bad_file_content(tmp_path: Path, content: str) -> None:
    path = tmp_path / "c.json"
    path.write_text(content)
    with pytest.raises(ConfigError):
        Config.load(path, environ={})


def test_missing_required(base_settings: dict[str, Any]) -> None:
    del base_settings["splunk_hec_token"]
    with pytest.raises(ConfigError, match="splunk_hec_token"):
        Config.from_dict(base_settings)


def test_auth_required(base_settings: dict[str, Any]) -> None:
    del base_settings["icinga2_api_password"]
    with pytest.raises(ConfigError, match="authentication required"):
        Config.from_dict(base_settings)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"icinga2_api_url": "icinga:5665"}, "icinga2_api_url must be"),
        ({"splunk_hec_url": "ftp://x"}, "splunk_hec_url must be"),
        ({"icinga2_api_url": "https://user:pass@icinga:5665"}, "embedded credentials"),
        ({"splunk_hec_url": "https://user@splunk:8088"}, "embedded credentials"),
        ({"batch_size": 0}, "batch_size"),
        ({"batch_size": True}, "batch_size"),
        ({"max_queue_size": "10"}, "max_queue_size"),
        ({"retry_max_interval": 0.5}, "retry_max_interval"),
        ({"retry_max_interval": float("inf")}, "retry_max_interval"),
        ({"spool_dir": "relative/path"}, "spool_dir"),
        ({"spool_dir": 5}, "spool_dir"),
        ({"spool_max_size_mb": 0}, "spool_max_size_mb"),
        ({"spool_max_size_mb": float("inf")}, "spool_max_size_mb"),
        ({"spool_max_age_days": -1}, "spool_max_age_days"),
        ({"batch_timeout": 0}, "batch_timeout"),
        ({"heartbeat_timeout": "x"}, "heartbeat_timeout"),
        ({"include_perfdata": "yes"}, "include_perfdata"),
        ({"include_check_results": 1}, "include_check_results"),
        ({"splunk_verify_ssl": 1}, "splunk_verify_ssl"),
        ({"splunk_verify_ssl": ""}, "splunk_verify_ssl"),
        ({"icinga2_api_verify_ssl": "   "}, "icinga2_api_verify_ssl"),
        ({"event_types": "StateChange"}, "event_types"),
        ({"check_result_state_filter": ["2"]}, "check_result_state_filter"),
        ({"log_level": "LOUD"}, "log_level"),
        ({"icinga2_api_url": 5}, "Invalid configuration"),
    ],
)
def test_validation_errors(make_config, overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        make_config(**overrides)


def test_insecure_settings_warn(make_config, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        make_config(
            icinga2_api_url="http://icinga:5665",
            splunk_hec_url="http://splunk:8088",
            icinga2_api_verify_ssl=False,
            splunk_verify_ssl=False,
        )
    assert caplog.text.count("TLS verification") == 2
    assert caplog.text.count("plain HTTP") == 2


def test_repr_hides_secrets(make_config) -> None:
    text = repr(make_config())
    assert "secret" not in text
    assert "token-123" not in text


@pytest.mark.parametrize(
    "url",
    [
        "https://s:8088",
        "https://s:8088/",
        "https://s:8088/services/collector",
        "https://s:8088/services/collector/event",
        "https://s:8088/services/collector/event/1.0/",
        "https://s:8088/services/collector/raw",
    ],
)
def test_normalize_hec_url(url: str) -> None:
    assert normalize_hec_url(url) == "https://s:8088"


def test_removed_key_warns(
    tmp_path: Path, base_settings: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    config = Config.load(write_config(tmp_path, {**base_settings, "max_retries": 3}), environ={})
    assert not hasattr(config, "max_retries")
    assert "'max_retries' is no longer used" in caplog.text


def test_spool_defaults(make_config) -> None:
    config = make_config()
    assert config.spool_dir == "/var/lib/icinga2-splunk/spool"
    assert config.spool_max_size_mb == 1024
    assert config.spool_max_age_days == 7


def test_boolean_env_true(tmp_path: Path, base_settings: dict[str, Any]) -> None:
    path = write_config(tmp_path, base_settings)
    config = Config.load(path, environ={"INCLUDE_CHECK_RESULTS": "Yes"})
    assert config.include_check_results is True


# ------------------------------------------------------------ file checks


def test_file_errors_ok_for_readable_files_and_booleans(make_config, tmp_path: Path) -> None:
    ca = tmp_path / "ca.crt"
    ca.write_text("x")
    config = make_config(icinga2_api_verify_ssl=str(ca), splunk_verify_ssl=True)
    assert config.file_errors() == []


def test_file_errors_reports_missing_files(make_config, tmp_path: Path) -> None:
    config = make_config(
        icinga2_api_client_cert=str(tmp_path / "c.pem"),
        icinga2_api_client_key=str(tmp_path / "k.pem"),
        splunk_verify_ssl=str(tmp_path),  # a directory is not a CA bundle
    )
    errors = config.file_errors()
    assert len(errors) == 3
    assert all("is not a file readable by user" in e for e in errors)


def test_file_errors_hint_for_icinga2_private_certs(make_config) -> None:
    config = make_config(icinga2_api_verify_ssl="/var/lib/icinga2/certs/does-not-exist.crt")
    [error] = config.file_errors()
    assert "copy the file to e.g. /etc/icinga2-splunk/" in error


def test_file_errors_unsearchable_directory(make_config, tmp_path: Path) -> None:
    private = tmp_path / "certs"
    private.mkdir()
    (private / "ca.crt").write_text("x")
    private.chmod(0o000)
    try:
        if os.access(private / "ca.crt", os.R_OK):  # running as root
            pytest.skip("cannot test permissions as root")
        config = make_config(icinga2_api_verify_ssl=str(private / "ca.crt"))
        assert len(config.file_errors()) == 1
    finally:
        private.chmod(0o700)


def test_current_user_without_passwd_entry(make_config, monkeypatch) -> None:
    import icinga2_splunk.config as config_module

    def missing(_uid: int) -> Any:
        raise KeyError(_uid)

    monkeypatch.setattr(config_module.pwd, "getpwuid", missing)
    config = make_config(splunk_verify_ssl="/nonexistent/ca.crt")
    assert f"uid {os.geteuid()}" in config.file_errors()[0]
