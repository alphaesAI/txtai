import base64
import os
import uuid
from typing import Dict, List, Any
from bs4 import BeautifulSoup


class GmailExtractor:
    """
    Extractor that pulls ALL Gmail messages and returns:
      - metadata JSON
      - cleaned HTML body
      - downloaded attachments (file paths)
    """

    def __init__(self, download_dir: str = "downloads"):
        self.download_dir = download_dir
        os.makedirs(self.download_dir, exist_ok=True)

    def extract(self, service) -> List[Dict[str, Any]]:
        """
        Main entry point.
        Fetches ALL messages and extracts their content.
        """
        results = []

        msg_list = service.users().messages().list(
            userId="me"
        ).execute()

        messages = msg_list.get("messages", [])
        if not messages:
            return []

        for msg in messages:
            msg_id = msg["id"]
            results.append(self._process_single_email(service, msg_id))

        return results

    def _process_single_email(self, service, msg_id: str) -> Dict[str, Any]:
        """
        Extracts ONE email (metadata, HTML, attachment file paths).
        """
        metadata = self.get_metadata(service, msg_id)
        html = self.get_html(metadata)
        attachments = self.get_attachments(service, metadata)

        return {
            "metadata": metadata,
            "html": html,
            "attachments": attachments  # only file paths
        }

    def get_metadata(self, service, msg_id: str) -> Dict[str, Any]:
        """
        Returns metadata JSON from Gmail.
        """
        message = service.users().messages().get(
            userId="me",
            id=msg_id,
            format="full"
        ).execute()

        payload = message.get("payload", {})
        headers = payload.get("headers", [])

        metadata = {
            "id": msg_id,
            "subject": self._get_header(headers, "Subject"),
            "from": self._get_header(headers, "From"),
            "to": self._get_header(headers, "To"),
            "date": self._get_header(headers, "Date"),
            "snippet": message.get("snippet", ""),
            "parts": payload.get("parts", [])
        }

        return metadata

    def _get_header(self, headers, name):
        for h in headers:
            if h["name"].lower() == name.lower():
                return h["value"]
        return None

    def get_html(self, metadata: Dict[str, Any]) -> str:
        """
        Extract HTML or text content from metadata.
        """
        parts = metadata.get("parts", [])
        html_content = None

        for part in parts:
            mime = part.get("mimeType", "")

            if mime == "text/html":
                html_content = part["body"].get("data")
                break

            elif mime == "text/plain" and html_content is None:
                html_content = self._convert_text_to_html(part["body"].get("data"))

        if not html_content:
            return ""

        decoded = base64.urlsafe_b64decode(html_content).decode("utf-8", errors="ignore")
        cleaned = self._clean_html(decoded)
        return cleaned

    def _convert_text_to_html(self, encoded_text):
        if not encoded_text:
            return ""
        text = base64.urlsafe_b64decode(encoded_text).decode("utf-8", errors="ignore")
        return f"<pre>{text}</pre>"

    def _clean_html(self, html):
        soup = BeautifulSoup(html, "html.parser")
        return str(soup)

    def get_attachments(self, service, metadata: Dict[str, Any]) -> List[str]:
        """
        Downloads attachments and returns file paths only.
        """
        parts = metadata.get("parts", [])
        file_paths = []

        for part in parts:
            filename = part.get("filename")
            body = part.get("body", {})

            if not filename or "attachmentId" not in body:
                continue

            attachment_id = body["attachmentId"]
            file_data = service.users().messages().attachments().get(
                userId="me",
                messageId=metadata["id"],
                id=attachment_id
            ).execute()

            data = file_data.get("data")
            if not data:
                continue

            decoded = base64.urlsafe_b64decode(data)
            file_path = os.path.join(self.download_dir, f"{uuid.uuid4()}-{filename}")

            with open(file_path, "wb") as f:
                f.write(decoded)

            file_paths.append(file_path)

        return file_paths
