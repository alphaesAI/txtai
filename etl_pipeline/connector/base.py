"""
Base connector class for all database and service connectors.
Implements the Abstract Factory pattern.
"""
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
            self.connection_string: Connection string for the data source "postgresql://user:pass[host/db"](cci:4://file://host/db"
            **kwargs: Additional connection parameters
            self._connection: connection object
            self._engine: connection factory responsible for creating connections
        """
        self.connection_string = connection_string
        self.connection_params = kwargs
        self._connection = None
        self._engine = None
    
    @abstractmethod
    def connect(self) -> Any:
        """
        Establish connection to the data source.
        
        Returns:
            Connection object
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """
        Close the connection to the data source.
        """
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if connection is active.
        
        Returns:
            True if connected, False otherwise
        """
        pass
    
    @abstractmethod
    def get_connection(self) -> Any:
        """
        Get the active connection object.
        
        Returns:
            Connection object
        """
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """
        Test if the connection is valid.
        
        Returns:
            True if connection is valid, False otherwise
        """
        pass
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False
