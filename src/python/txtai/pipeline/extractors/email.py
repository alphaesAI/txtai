"""Email extraction logic for ETL pipeline."""

import base64
from typing import Any, Dict, List
from googleapiclient.discovery import Resource


class GmailExtractor:
    """Extracts messages and attachments from email services."""

    def get_messages(self, service: Resource, query: str = "", max_results: int = 10) -> List[Dict[str, Any]]:
        """Fetch full emails."""
        try:
            result = service.users().messages().list(
                userId="me", q=query, maxResults=max_results
            ).execute()

            messages = result.get("messages", [])
            full_messages = []

            for msg in messages:
                msg_detail = service.users().messages().get(
                    userId="me", id=msg["id"], format="full"
                ).execute()
                full_messages.append(msg_detail)

            return full_messages

        except Exception as e:
            raise RuntimeError(f"Failed to get messages: {e}")

    def get_attachments(self, service: Resource, message_id: str) -> List[Dict[str, Any]]:
        """Extract attachments from a Gmail message."""
        try:
            message = service.users().messages().get(
                userId="me", id=message_id, format="full"
            ).execute()

            attachments = []
            parts = message.get("payload", {}).get("parts", [])

            for part in parts:
                if part.get("filename") and part.get("body", {}).get("attachmentId"):
                    attachment_id = part["body"]["attachmentId"]
                    attachment = service.users().messages().attachments().get(
                        userId="me", messageId=message_id, id=attachment_id
                    ).execute()

                    attachments.append({
                        "filename": part["filename"],
                        "mimeType": part.get("mimeType"),
                        "data": base64.urlsafe_b64decode(attachment["data"]),
                        "size": attachment.get("size", 0)
                    })

            return attachments

        except Exception as e:
            raise RuntimeError(f"Failed to get attachments: {e}")
