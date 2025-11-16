"""Base connector class for ETL pipeline."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BaseConnector(ABC):
    """
    Abstract base class for all connectors.
    Defines the interface that all concrete connectors must implement.
    """
    
    def __init__(self, connection_string: str, **kwargs):
        """
        Initialize the base connector.
        
        Args:
            connection_string: Connection string for the data source
            **kwargs: Additional connection parameters
        """
        self.connection_string = connection_string
        self.connection_params = kwargs
        self._connection = None
        self._engine = None
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data source."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the data source."""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test if connection is working."""
        pass
    
    @property
    def connection(self):
        """Get the current connection object."""
        return self._connection
    
    @property
    def engine(self):
        """Get the connection engine."""
        return self._engine
