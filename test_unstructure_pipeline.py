import yaml
import json
from src.python.txtai.pipeline.connectors.email import GmailConnector
from src.python.txtai.pipeline.extractors.email import GmailExtractor


class GmailElasticsearchIntegration:
    def __init__(self, config_path: str):
        """Initialize integration with configuration."""
        self.config_path = config_path
        self.config = self._load_config()
        self.gmail_connector = None
        self.gmail_extractor = None

    def _load_config(self) -> dict:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML configuration: {e}")

    def initialize_gmail_connector(self):
        """Initialize Gmail connector with config."""
        gmail_cfg = self.config.get("gmail", {})
        self.gmail_connector = GmailConnector(
            credentials_path=gmail_cfg.get("credentials_path"),
            token_path=gmail_cfg.get("token_path")
        )
        self.gmail_connector.connect()

    def initialize_extractor(self):
        """Initialize Gmail extractor."""
        self.gmail_extractor = GmailExtractor()

    def run(self):
        """Simple test run: connect + fetch messages."""
        self.initialize_gmail_connector()
        self.initialize_extractor()

        service = self.gmail_connector.service
        messages = self.gmail_extractor.get_messages(service, max_results=5)

        print(f"Fetched {len(messages)} messages!")
        return messages

if __name__ == "__main__":
    pipeline = GmailElasticsearchIntegration("pipeline_config.yaml")
    messages = pipeline.run()

    print("\n=== OUTPUT ===")
    print(f"Total messages fetched: {len(messages)}")

    print("full raw json")
    print(json.dumps(messages[0], indent=2))
