"""Base extractor class for ETL pipeline."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional


class BaseExtractor(ABC):
    """
    Abstract base class for all extractors.
    Defines the interface for data extraction.
    """
    
    def __init__(self, connector: Any, **kwargs):
        """
        Initialize the base extractor.
        
        Args:
            connector: Database connector instance
            **kwargs: Additional extractor parameters
        """
        self.connector = connector
        self.config = kwargs
    
    @abstractmethod
    def extract(
        self,
        source: str,
        query: Optional[str] = None,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """
        Extract data from source.
        
        Args:
            source: Data source (table name, file path, etc.)
            query: Optional query/filter
            **kwargs: Additional extraction parameters
            
        Yields:
            Data records as dictionaries
        """
        pass
    
    @abstractmethod
    def get_schema(self, source: str) -> Dict[str, Any]:
        """
        Get schema information for the source.
        
        Args:
            source: Data source
            
        Returns:
            Schema information
        """
        pass
    
    def validate_connection(self) -> bool:
        """
        Validate that the connector is properly connected.
        
        Returns:
            True if connection is valid
        """
        if hasattr(self.connector, 'test_connection'):
            return self.connector.test_connection()
        return True
