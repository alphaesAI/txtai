import yaml
import json
from src.python.txtai.pipeline.connectors.email import GmailConnector
from src.python.txtai.pipeline.extractors.email import GmailExtractor
from src.python.txtai.pipeline.transformers.data_transformer import DataTransformer

class GmailElasticsearchIntegration:
    def __init__(self, config_path: str):
        """Initialize integration with configuration."""
        self.config_path = config_path
        self.config = self._load_config()
        self.gmail_connector = None
        self.gmail_extractor = None
        self.data_transformer = None

    def _load_config(self) -> dict:
        try:
            with open(self.config_path, 'r') as f:
                cfg = yaml.safe_load(f)
                print("Loaded config:", cfg)  # <- debug line
                return cfg
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

    def initialize_transformer(self):
        """Initialize DataTransformer with Textractor config from YAML."""
        textractor_cfg = self.config.get("transformer", {}).get("textractor", {})
        filetohtml_cfg = self.config.get("filetohtml", {})
        self.data_transformer = DataTransformer(textractor_config=textractor_cfg, filetohtml_config=filetohtml_cfg)



    def run(self):
        """Connect, extract, and transform emails."""
        try:
            # Initialize all components
            self.initialize_gmail_connector()
            self.initialize_extractor()
            self.initialize_transformer()

            service = self.gmail_connector.service

            # Extract emails (metadata, html, attachments)
            extracted_data = self.gmail_extractor.extract(service)
            print(f"\n=== EXTRACTION SUMMARY ===")
            print(f"Fetched & processed {len(extracted_data)} messages!")
            
            # Show sample of extracted data
            if extracted_data:
                print("\nSample extracted email:")
                sample = extracted_data[0]
                print(f"Subject: {sample.get('subject', 'N/A')}")
                print(f"From: {sample.get('from', 'N/A')}")
                print(f"Date: {sample.get('date', 'N/A')}")
                print(f"Has attachments: {bool(sample.get('attachments'))}")

            # Transform data
            print("\n=== TRANSFORMATION ===")
            transformed_data = self.data_transformer.transform(extracted_data)
            
            print(f"\nTotal transformed segments: {len(transformed_data)}")
            
            # Show sample transformed segments
            if transformed_data:
                print("\nSample transformed segments:")
                for i, (doc_id, text, tags) in enumerate(transformed_data[:3]):  # Show first 3 segments
                    print(f"\nSegment {i+1}:")
                    print(f"  ID: {doc_id}")
                    print(f"  Text: {text[:200]}{'...' if len(text) > 200 else ''}")
                    if tags:
                        print("  Tags:")
                        for key, value in tags.items():
                            if value:  # Only show non-None values
                                print(f"    {key}: {value}")

            return transformed_data

        except Exception as e:
            print(f"Error in pipeline execution: {str(e)}")
            raise



if __name__ == "__main__":
    pipeline = GmailElasticsearchIntegration("pipeline_config.yaml")
    pipeline.run()
