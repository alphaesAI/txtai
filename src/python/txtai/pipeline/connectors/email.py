"""Email connectors for ETL pipeline."""

import os
import pickle
from typing import Any, Dict, List, Optional
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build, Resource

from .base import BaseConnector


class GmailConnector(BaseConnector):
    """Gmail API connector for fetching emails and attachments."""

    SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
    
    def __init__(self, credentials_path: str = "credentials.json", token_path: str = "token.json", **kwargs) -> None:
        """Initialize Gmail connector.
        
        Args:
            credentials_path: Path to Google API credentials file
            token_path: Path to store authentication token
            **kwargs: Additional connection parameters
        """
        super().__init__("", **kwargs)
        self.service: Optional[Resource] = None
        self.credentials_path = credentials_path
        self.token_path = token_path

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
            
            # Save the credentials for the next run
            with open(self.token_path, "wb") as token:
                pickle.dump(creds, token)
        
        # Build Gmail service
        self.service = build("gmail", "v1", credentials=creds)

    def disconnect(self) -> None:
        """Close Gmail service connection."""
        self.service = None

    def test_connection(self) -> bool:
        """Test if Gmail connection is working."""
        try:
            if not self.service:
                return False
            
            # Try to get user profile
            profile = self.service.users().getProfile(userId="me").execute()
            return profile is not None
        except Exception:
            return False

    def get_messages(self, query: str = "", max_results: int = 10) -> List[Dict[str, Any]]:
        """Get Gmail messages.
        
        Args:
            query: Gmail search query
            max_results: Maximum number of messages to retrieve
            
        Returns:
            List of message dictionaries
        """
        if not self.service:
            raise RuntimeError("Connector not connected. Call connect() first.")
        
        try:
            # List messages
            result = self.service.users().messages().list(
                userId="me", q=query, maxResults=max_results
            ).execute()
            
            messages = result.get("messages", [])
            
            # Get full message details
            full_messages = []
            for msg in messages:
                msg_detail = self.service.users().messages().get(
                    userId="me", id=msg["id"], format="full"
                ).execute()
                full_messages.append(msg_detail)
            
            return full_messages
        except Exception as e:
            raise RuntimeError(f"Failed to get messages: {e}")

    def get_attachments(self, message_id: str) -> List[Dict[str, Any]]:
        """Get attachments from a message.
        
        Args:
            message_id: Gmail message ID
            
        Returns:
            List of attachment dictionaries
        """
        if not self.service:
            raise RuntimeError("Connector not connected. Call connect() first.")
        
        try:
            message = self.service.users().messages().get(
                userId="me", id=message_id, format="full"
            ).execute()
            
            attachments = []
            parts = message.get("payload", {}).get("parts", [])
            
            for part in parts:
                if part.get("filename") and part.get("body", {}).get("attachmentId"):
                    attachment_id = part["body"]["attachmentId"]
                    attachment = self.service.users().messages().attachments().get(
                        userId="me", messageId=message_id, id=attachment_id
                    ).execute()
                    
                    attachments.append({
                        "filename": part["filename"],
                        "mimeType": part.get("mimeType"),
                        "data": attachment["data"],
                        "size": attachment.get("size", 0)
                    })
            
            return attachments
        except Exception as e:
            raise RuntimeError(f"Failed to get attachments: {e}")
