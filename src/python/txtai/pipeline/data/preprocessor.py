"""Preprocessor for ETL pipeline data processing."""

import tempfile
import logging
from typing import Any, Dict, List, Optional

from ..connectors.factory import ConnectorFactory


class PreProcessor:
    """
    Middle-layer that connects a connector (like Gmail)
    to the Textractor input format.
    """

    def __init__(self, connector_type: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize preprocessor.
        
        Args:
            connector_type: Type of connector to use (e.g., "gmail")
            config: Configuration for the connector
        """
        if config is None:
            config = {}
        
        self.connector = ConnectorFactory.create(connector_type, config)
        self.textractor = None
        
        # Setup logging
        self.logger = logging.getLogger(__name__)

    def set_textractor(self, textractor) -> None:
        """Set the textractor instance for processing."""
        self.textractor = textractor

    def fetch(self) -> Dict[str, List[str]]:
        """
        Fetches data from connector and prepares:
        - file paths (temp files created for attachments)
        - html bodies
        - raw text bodies

        Returns:
            {
                "files": ["/tmp/file1.pdf", ...],
                "html": ["<html>...</html>"],
                "text": ["some plaintext"]
            }
        """
        self.connector.connect()

        try:
            messages = self.connector.fetch_unread_messages()

            results = {"files": [], "html": [], "text": []}

            for msg in messages:
                # 1️⃣ Save attachments to temp files
                attachments = self.connector.download_attachments(msg, tempfile.gettempdir())
                results["files"].extend(attachments)

                # 2️⃣ HTML parts
                body = self.connector.fetch_message_body(msg)
                if body.get("html"):
                    results["html"].append(body["html"])

                # 3️⃣ Raw text parts
                if body.get("text"):
                    results["text"].append(body["text"])

            return results

        except Exception as e:
            self.logger.error(f"Error fetching data from connector: {e}")
            raise
        finally:
            self.connector.disconnect()

    def process(self, data: Optional[Dict[str, List[str]]] = None) -> List[Dict[str, Any]]:
        """
        Process fetched data through textractor.
        
        Args:
            data: Optional pre-fetched data. If None, will fetch first.
            
        Returns:
            List of processed documents
        """
        if data is None:
            data = self.fetch()

        if not self.textractor:
            raise RuntimeError("Textractor not set. Call set_textractor() first.")

        processed_documents = []

        # Process files
        for file_path in data.get("files", []):
            try:
                doc = self.textractor.extract_file(file_path)
                processed_documents.append(doc)
            except Exception as e:
                self.logger.error(f"Error processing file {file_path}: {e}")

        # Process HTML
        for html_content in data.get("html", []):
            try:
                doc = self.textractor.extract_html(html_content)
                processed_documents.append(doc)
            except Exception as e:
                self.logger.error(f"Error processing HTML content: {e}")

        # Process text
        for text_content in data.get("text", []):
            try:
                doc = self.textractor.extract_text(text_content)
                processed_documents.append(doc)
            except Exception as e:
                self.logger.error(f"Error processing text content: {e}")

        return processed_documents

    def process_and_store(self, output_path: str, data: Optional[Dict[str, List[str]]] = None) -> bool:
        """
        Process fetched data and store to file.
        
        Args:
            output_path: Path to store processed documents
            data: Optional pre-fetched data
            
        Returns:
            True if successful
        """
        try:
            documents = self.process(data)
            
            import json
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(documents, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Processed {len(documents)} documents to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing and storing documents: {e}")
            return False

    def get_stats(self) -> Dict[str, Any]:
        """
        Get processing statistics.
        
        Returns:
            Dictionary with statistics
        """
        return {
            "connector_type": type(self.connector).__name__,
            "textractor_set": self.textractor is not None,
            "textractor_type": type(self.textractor).__name__ if self.textractor else None
        }
