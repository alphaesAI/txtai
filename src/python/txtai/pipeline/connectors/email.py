"""Email connectors for ETL pipeline."""

import os
import pickle
from typing import Optional
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build, Resource

from .base import BaseConnector


class GmailConnector(BaseConnector):
    """Gmail API connector for fetching emails and attachments."""

    SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

    def __init__(self, credentials_path: str, token_path: str, **kwargs) -> None:
        super().__init__("", **kwargs)
        self.service: Optional[Resource] = None
        self.credentials_path = credentials_path
        self.token_path = token_path

    def connect(self) -> None:
        """Authenticate and build Gmail service client."""
        creds = None

        if os.path.exists(self.token_path):
            with open(self.token_path, "rb") as token:
                creds = pickle.load(token)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, self.SCOPES
                )
                creds = flow.run_local_server(port=0)

            with open(self.token_path, "wb") as token:
                pickle.dump(creds, token)

        self.service = build("gmail", "v1", credentials=creds)

    def disconnect(self) -> None:
        self.service = None

    def test_connection(self) -> bool:
        """Test Gmail connection."""
        try:
            if not self.service:
                return False
            profile = self.service.users().getProfile(userId="me").execute()
            return profile is not None
        except Exception:
            return False
