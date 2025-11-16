import tempfile
from ...connector.factory import ConnectorFactory

class PreProcessor:
    """
    Middle-layer that connects a connector (like Gmail)
    to the Textractor input format.
    """

    def __init__(self, connector_type, config=None):
        # Example: connector_type="gmail"
        if config is None:
            config = {}
        self.connector = ConnectorFactory.create(connector_type, config)
        self.textractor = None

    def set_textractor(self, textractor):
        """Set the textractor instance for processing."""
        self.textractor = textractor

    def fetch(self):
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

    def process_and_extract(self):
        """
        Fetch data from connector and process through textractor.
        
        Returns:
            List of extracted text content ready for embeddings
        """
        if not self.textractor:
            raise RuntimeError("Textractor not set. Call set_textractor() first.")
        
        data = self.fetch()
        extracted_text = []
        
        # Process files through textractor
        for file_path in data["files"]:
            try:
                text = self.textractor.text(file_path)
                if text.strip():
                    extracted_text.append(text)
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
        
        # Process HTML through textractor
        for html in data["html"]:
            try:
                text = self.textractor.text(html)
                if text.strip():
                    extracted_text.append(text)
            except Exception as e:
                print(f"Error processing HTML content: {e}")
        
        # Process raw text (pass through as-is)
        for text in data["text"]:
            if text.strip():
                extracted_text.append(text)
        
        return extracted_text
