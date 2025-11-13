import json
import os
import logging

logger = logging.getLogger(__name__)

class Config:
    def __init__(self, config_path=None):
        if config_path is None:
            config_path = os.environ.get(
                'ICINGA2_SPLUNK_CONFIG',
                '/etc/icinga2/splunk_config.json'
            )
        
        self.config_path = config_path
        self.config = self._load_config()
        self._apply_env_overrides()
        self._set_defaults()
        self.validate()
        
    def _load_config(self):
        """Load configuration from JSON file"""
        if not os.path.exists(self.config_path):
            logger.warning(f"Config file not found: {self.config_path}, using environment variables")
            return {}
        
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"Failed to load config from {self.config_path}: {e}")
    
    def _apply_env_overrides(self):
        """Override config with environment variables"""
        env_mappings = {
            'ICINGA2_API_URL': 'icinga2_api_url',
            'ICINGA2_API_USERNAME': 'icinga2_api_username',
            'ICINGA2_API_PASSWORD': 'icinga2_api_password',
            'ICINGA2_API_VERIFY_SSL': 'icinga2_api_verify_ssl',
            'SPLUNK_HEC_URL': 'splunk_hec_url',
            'SPLUNK_HEC_TOKEN': 'splunk_hec_token',
            'SPLUNK_VERIFY_SSL': 'splunk_verify_ssl',
            'SPLUNK_INDEX_EVENTS': 'splunk_index_events',
            'SPLUNK_INDEX_PERFDATA': 'splunk_index_perfdata',
            'BATCH_SIZE': 'batch_size',
            'BATCH_TIMEOUT': 'batch_timeout',
            'LOG_LEVEL': 'log_level',
        }
        
        for env_var, config_key in env_mappings.items():
            value = os.environ.get(env_var)
            if value is not None:
                # Convert boolean strings
                if value.lower() in ('true', 'false'):
                    value = value.lower() == 'true'
                # Convert numeric strings
                elif value.isdigit():
                    value = int(value)
                self.config[config_key] = value
    
    def _set_defaults(self):
        """Set default values for optional config"""
        defaults = {
            'icinga2_api_verify_ssl': True,
            'splunk_verify_ssl': True,
            'splunk_index_events': 'icinga2_events',
            'splunk_index_perfdata': 'icinga2_perfdata',
            'event_types': ['StateChange', 'Notification', 'AcknowledgementSet', 'DowntimeAdded', 'DowntimeRemoved'],
            'include_check_results': False,
            'check_result_state_filter': [1, 2, 3],  # WARNING, CRITICAL, UNKNOWN (skip OK)
            'event_stream_filter': '',
            'batch_size': 100,
            'batch_timeout': 10,
            'log_level': 'INFO',
        }
        
        for key, value in defaults.items():
            if key not in self.config:
                self.config[key] = value
    
    def validate(self):
        """Validate required configuration"""
        required = [
            'icinga2_api_url',
            'splunk_hec_url',
            'splunk_hec_token',
        ]
        
        missing = [k for k in required if k not in self.config]
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        
        # Require either username/password or client cert for Icinga2 API
        has_userpass = self.config.get('icinga2_api_username') and self.config.get('icinga2_api_password')
        has_cert = self.config.get('icinga2_api_client_cert') and self.config.get('icinga2_api_client_key')
        
        if not (has_userpass or has_cert):
            raise ValueError("Icinga2 API authentication required: provide either username/password or client certificate")
    
    def __getattr__(self, name):
        """Allow accessing config values as attributes"""
        if name in self.config:
            return self.config[name]
        raise AttributeError(f"Config has no attribute '{name}'")
    
    def __getitem__(self, key):
        """Allow dict-style access"""
        return self.config[key]
    
    def get(self, key, default=None):
        """Get config value with default"""
        return self.config.get(key, default)

