import os
import yaml
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    _instance = None
    _config = {}

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_config_loaded'):
            self._load_config()
            self._config_loaded = True

    def _load_config(self) -> None:
        """Load configuration from YAML file and environment variables."""
        try:
            # Load .env from project root
            env_path = Path(__file__).parent.parent.parent / '.env'
            print(f"Looking for .env at: {env_path}")
            print(f".env exists: {env_path.exists()}")
            
            if env_path.exists():
                with open(env_path) as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, value = line.split('=', 1)
                            os.environ[key] = value
                            print(f"Loaded env var: {key}={value}")
            
            # Load YAML config
            config_path = Path(__file__).parent / 'config.yaml'
            if not config_path.exists():
                logger.warning(f"Config file not found at {config_path}")
                self._config = {}
                return
            
            with open(config_path, 'r') as f:
                config_str = f.read()
                config_str = self._expand_env_vars(config_str)
                self._config = yaml.safe_load(config_str) or {}
                
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            self._config = {}

    def get_connector_config(self, category: str, name: str) -> dict:
        """
        Get configuration for a specific connector.
        First checks if environment variables exist, otherwise uses config.yaml.
        """
        connector_config = self._config.get(category, {}).get(name, {})

        # Override with env vars if present
        env_map = {
            'host': f'{name.upper()}_HOST',
            'port': f'{name.upper()}_PORT',
            'username': f'{name.upper()}_USERNAME',
            'password': f'{name.upper()}_PASSWORD',
            'use_ssl': f'{name.upper()}_USE_SSL',
            'mailbox': f'{name.upper()}_MAILBOX',
        }

        for key, env_key in env_map.items():
            if env_key in os.environ:
                val = os.environ[env_key]
                if key == 'port':
                    val = int(val)
                elif key == 'use_ssl':
                    val = val.lower() in ('true', '1', 'yes')
                connector_config[key] = val

        return connector_config

    def _expand_env_vars(self, config_str: str) -> str:
        """Replace ${ENV_VAR} in YAML with actual environment variable."""
        import re
        pattern = re.compile(r'\$\{(\w+)\}')
        return pattern.sub(lambda m: os.environ.get(m.group(1), m.group(0)), config_str)

    def get(self, key: str, default=None):
        return self._config.get(key, default)


# Provide a function to get a singleton instance
def get_config() -> ConfigManager:
    return ConfigManager()
