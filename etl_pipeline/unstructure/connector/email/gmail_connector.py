"""Gmail API connector implementation."""

import base64
import os
import pickle
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build

from ..base import BaseConnector


class GmailConnector(BaseConnector):
    """Gmail API connector for fetching emails and attachments."""

    SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
    
    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize Gmail connector.
        
        Args:
            config: Configuration dictionary containing credentials_path and token_path.
        """
        super().__init__(config)
        self.service: Resource | None = None
        self.credentials_path = config.get("credentials_path", "credentials.json")
        self.token_path = config.get("token_path", "token.json")

    def connect(self) -> None:
        """Authenticate and build Gmail service client."""
        creds = None
        
        # Load existing token if available
        if os.path.exists(self.token_path):
            with open(self.token_path, "rb") as token:
                creds = pickle.load(token)
        
        # If no valid credentials, get new ones
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, self.SCOPES
                )
                creds = flow.run_local_server(port=0)
            
            # Save credentials for next run
            with open(self.token_path, "wb") as token:
                pickle.dump(creds, token)
        
        self.service = build("gmail", "v1", credentials=creds)
        self._connected = True

    def disconnect(self) -> None:
        """Close Gmail service connection."""
        self.service = None
        self._connected = False

    def fetch_unread_messages(self) -> List[Dict[str, Any]]:
        """Fetch unread messages from Gmail.
        
        Returns:
            List of message dictionaries with id and threadId.
        """
        if not self.service:
            raise RuntimeError("Not connected to Gmail service")
        
        result = self.service.users().messages().list(
            userId="me", q="is:unread"
        ).execute()
        
        messages = result.get("messages", [])
        return [{"id": msg["id"], "threadId": msg["threadId"]} for msg in messages]

    def fetch_message_metadata(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch detailed metadata for a specific message.
        
        Args:
            msg: Message dictionary with id and threadId.
            
        Returns:
            Dictionary containing message metadata.
        """
        if not self.service:
            raise RuntimeError("Not connected to Gmail service")
        
        message = self.service.users().messages().get(
            userId="me", id=msg["id"], format="metadata"
        ).execute()
        
        headers = message["payload"]["headers"]
        metadata = {
            "id": message["id"],
            "threadId": message["threadId"],
            "subject": self._get_header(headers, "Subject"),
            "from": self._get_header(headers, "From"),
            "date": self._get_header(headers, "Date"),
            "to": self._get_header(headers, "To"),
            "cc": self._get_header(headers, "Cc"),
            "message_id": self._get_header(headers, "Message-ID"),
        }
        
        return metadata

    def fetch_message_body(self, msg: Dict[str, Any]) -> Dict[str, str]:
        """Fetch message body content.
        
        Args:
            msg: Message dictionary with id and threadId.
            
        Returns:
            Dictionary containing 'text' and 'html' body content.
        """
        if not self.service:
            raise RuntimeError("Not connected to Gmail service")
        
        message = self.service.users().messages().get(
            userId="me", id=msg["id"], format="full"
        ).execute()
        
        body = {"text": "", "html": ""}
        self._extract_body(message["payload"], body)
        
        return body

    def download_attachments(self, msg: Dict[str, Any], output_dir: str) -> List[str]:
        """Download attachments from a message.
        
        Args:
            msg: Message dictionary with id and threadId.
            output_dir: Directory to save attachments.
            
        Returns:
            List of downloaded file paths.
        """
        if not self.service:
            raise RuntimeError("Not connected to Gmail service")
        
        message = self.service.users().messages().get(
            userId="me", id=msg["id"], format="full"
        ).execute()
        
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        downloaded_files = []
        
        self._save_attachments(message["payload"], output_dir, downloaded_files, msg["id"])
        
        return downloaded_files

    def _get_header(self, headers: List[Dict[str, str]], name: str) -> str:
        """Extract header value by name.
        
        Args:
            headers: List of header dictionaries.
            name: Header name to search for.
            
        Returns:
            Header value or empty string if not found.
        """
        for header in headers:
            if header["name"] == name:
                return header["value"]
        return ""

    def _extract_body(self, payload: Dict[str, Any], body: Dict[str, str]) -> None:
        """Recursively extract body content from message payload.
        
        Args:
            payload: Message payload part.
            body: Dictionary to store extracted content.
        """
        if "parts" in payload:
            for part in payload["parts"]:
                self._extract_body(part, body)
        else:
            mime_type = payload.get("mimeType", "")
            data = payload.get("body", {}).get("data", "")
            
            if data:
                decoded_data = base64.urlsafe_b64decode(data).decode("utf-8")
                
                if mime_type == "text/plain":
                    body["text"] = decoded_data
                elif mime_type == "text/html":
                    body["html"] = decoded_data

    def _save_attachments(
        self, payload: Dict[str, Any], output_dir: str, downloaded_files: List[str], message_id: str
    ) -> None:
        """Recursively save attachments from message payload.
        
        Args:
            payload: Message payload part.
            output_dir: Directory to save attachments.
            downloaded_files: List to track downloaded file paths.
            message_id: Gmail message ID for attachment retrieval.
        """
        if "parts" in payload:
            for part in payload["parts"]:
                self._save_attachments(part, output_dir, downloaded_files, message_id)
        else:
            filename = payload.get("filename", "")
            attachment_id = payload.get("body", {}).get("attachmentId", "")
            
            if filename and attachment_id and self.service:
                attachment = self.service.users().messages().attachments().get(
                    userId="me", messageId=message_id, id=attachment_id
                ).execute()
                
                data = attachment.get("data", "")
                if data:
                    file_data = base64.urlsafe_b64decode(data)
                    file_path = os.path.join(output_dir, filename)
                    
                    with open(file_path, "wb") as f:
                        f.write(file_data)
                    
                    downloaded_files.append(file_path)
