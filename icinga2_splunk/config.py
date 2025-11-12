"""
Configuration management for Icinga2 to Splunk integration
"""

import json
import os
from typing import Optional, List


class Config:
    """Configuration for Icinga2 to Splunk integration"""

    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration from file or environment variables

        Args:
            config_file: Path to JSON configuration file (optional)
        """
        # Default values
        self.config = {
            # Icinga2 API settings
            "icinga2_api_url": "https://localhost:5665",
            "icinga2_api_username": "",
            "icinga2_api_password": "",
            "icinga2_api_verify_ssl": False,
            "icinga2_api_client_cert": None,
            "icinga2_api_client_key": None,
            # Event Stream settings
            "event_stream_queue": "splunk-integration",
            "event_stream_types": [
                "CheckResult",
                "StateChange",
                "Notification",
                "AcknowledgementSet",
                "CommentAdded",
                "DowntimeAdded",
            ],
            "event_stream_filter": None,
            # Splunk HEC settings
            "splunk_hec_url": "https://localhost:8088/services/collector",
            "splunk_hec_token": "",
            "splunk_verify_ssl": True,
            "splunk_index_events": "icinga2_events",
            "splunk_index_perfdata": "icinga2_perfdata",
            # Batching settings
            "splunk_batch_size": 100,
            "splunk_batch_interval": 5,
        }

        # Load from file if provided
        if config_file and os.path.exists(config_file):
            self._load_from_file(config_file)

        # Override with environment variables
        self._load_from_env()

        # Validate configuration
        self._validate()

    def _load_from_file(self, config_file: str):
        """Load configuration from JSON file"""
        with open(config_file, "r") as f:
            file_config = json.load(f)
            self.config.update(file_config)

    def _load_from_env(self):
        """Load configuration from environment variables"""
        env_mapping = {
            "ICINGA2_API_URL": "icinga2_api_url",
            "ICINGA2_API_USERNAME": "icinga2_api_username",
            "ICINGA2_API_PASSWORD": "icinga2_api_password",
            "ICINGA2_API_VERIFY_SSL": "icinga2_api_verify_ssl",
            "ICINGA2_API_CLIENT_CERT": "icinga2_api_client_cert",
            "ICINGA2_API_CLIENT_KEY": "icinga2_api_client_key",
            "EVENT_STREAM_QUEUE": "event_stream_queue",
            "EVENT_STREAM_FILTER": "event_stream_filter",
            "SPLUNK_HEC_URL": "splunk_hec_url",
            "SPLUNK_HEC_TOKEN": "splunk_hec_token",
            "SPLUNK_VERIFY_SSL": "splunk_verify_ssl",
            "SPLUNK_INDEX_EVENTS": "splunk_index_events",
            "SPLUNK_INDEX_PERFDATA": "splunk_index_perfdata",
            "SPLUNK_BATCH_SIZE": "splunk_batch_size",
            "SPLUNK_BATCH_INTERVAL": "splunk_batch_interval",
        }

        for env_var, config_key in env_mapping.items():
            value = os.environ.get(env_var)
            if value is not None:
                # Convert boolean strings
                if value.lower() in ("true", "false"):
                    value = value.lower() == "true"
                # Convert numeric strings
                elif value.isdigit():
                    value = int(value)

                self.config[config_key] = value

    def _validate(self):
        """Validate required configuration"""
        required = [
            "icinga2_api_url",
            "splunk_hec_url",
            "splunk_hec_token",
        ]

        # Check authentication (either username/password or client cert)
        has_basic_auth = (
            self.config["icinga2_api_username"] and self.config["icinga2_api_password"]
        )
        has_cert_auth = (
            self.config["icinga2_api_client_cert"]
            and self.config["icinga2_api_client_key"]
        )

        if not (has_basic_auth or has_cert_auth):
            raise ValueError(
                "Icinga2 API authentication required: "
                "provide either username/password or client certificate"
            )

        for key in required:
            if not self.config.get(key):
                raise ValueError(f"Missing required configuration: {key}")

    def __getattr__(self, name):
        """Allow accessing config values as attributes"""
        if name in self.config:
            return self.config[name]
        raise AttributeError(f"Config has no attribute '{name}'")

    def to_dict(self) -> dict:
        """Return configuration as dictionary"""
        return self.config.copy()
