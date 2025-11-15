"""Base connector interface for unstructured data sources."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseConnector(ABC):
    """Abstract base class for data connectors."""

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize connector with configuration.
        
        Args:
            config: Configuration dictionary for the connector.
        """
        self.config = config
        self._connected = False

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data source."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the data source."""
        pass

    @abstractmethod
    def fetch_unread_messages(self) -> List[Dict[str, Any]]:
        """Fetch unread messages from the data source.
        
        Returns:
            List of message dictionaries containing basic metadata.
        """
        pass

    @abstractmethod
    def fetch_message_metadata(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch detailed metadata for a specific message.
        
        Args:
            msg: Message dictionary with basic info.
            
        Returns:
            Dictionary containing message metadata (subject, from, date, etc.).
        """
        pass

    @abstractmethod
    def fetch_message_body(self, msg: Dict[str, Any]) -> Dict[str, str]:
        """Fetch message body content.
        
        Args:
            msg: Message dictionary with basic info.
            
        Returns:
            Dictionary containing 'text' and 'html' body content.
        """
        pass

    @abstractmethod
    def download_attachments(self, msg: Dict[str, Any], output_dir: str) -> List[str]:
        """Download attachments from a message.
        
        Args:
            msg: Message dictionary with basic info.
            output_dir: Directory to save attachments.
            
        Returns:
            List of downloaded file paths.
        """
        pass

    @property
    def is_connected(self) -> bool:
        """Check if connector is connected."""
        return self._connected