"""Simple test for Gmail connector using config file."""

import os
import sys
import tempfile
import unittest
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Import directly without going through __init__.py
import yaml
from etl_pipeline.unstructure.connector.factory import ConnectorFactory


class TestGmailConnector(unittest.TestCase):
    """Test Gmail connector with real config."""

    def setUp(self):
        """Set up test using config file."""
        # Load config from yaml
        config_path = Path(__file__).parent.parent / "config" / "config.yaml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        
        self.gmail_config = config["unstructure"]["gmail"]
        self.connector = ConnectorFactory.create("gmail", self.gmail_config)

    def test_connector_creation(self):
        """Test connector can be created from config."""
        self.assertIsNotNone(self.connector)
        self.assertEqual(self.connector.credentials_path, self.gmail_config["credentials_path"])
        self.assertEqual(self.connector.token_path, self.gmail_config["token_path"])

    def test_connection_and_basic_fetch(self):
        """Test connection and fetching unread messages."""
        # Connect
        self.connector.connect()
        self.assertTrue(self.connector.is_connected)
        
        try:
            # Fetch unread messages
            messages = self.connector.fetch_unread_messages()
            self.assertIsInstance(messages, list)
            
            if messages:  # Only test if there are unread messages
                msg = messages[0]
                
                # Test metadata parsing
                metadata = self.connector.fetch_message_metadata(msg)
                required_fields = ["id", "threadId", "subject", "from", "date"]
                for field in required_fields:
                    self.assertIn(field, metadata)
                
                # Test body parsing
                body = self.connector.fetch_message_body(msg)
                self.assertIsInstance(body, dict)
                self.assertIn("text", body)
                self.assertIn("html", body)
                
                # Test attachment download
                with tempfile.TemporaryDirectory() as temp_dir:
                    attachments = self.connector.download_attachments(msg, temp_dir)
                    self.assertIsInstance(attachments, list)
                    
                    # Verify downloaded files exist
                    for attachment_path in attachments:
                        self.assertTrue(os.path.exists(attachment_path))
        
        finally:
            # Always disconnect
            self.connector.disconnect()
            self.assertFalse(self.connector.is_connected)


if __name__ == "__main__":
    unittest.main()
