"""Connector registration module."""

from .email.gmail_connector import GmailConnector
from .factory import ConnectorFactory

# Register available connectors
ConnectorFactory.register("gmail", GmailConnector)
