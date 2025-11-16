"""Base loader class for ETL pipeline."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseLoader(ABC):
    """
    Abstract base class for all loaders.
    Defines the interface for data loading.
    """
    
    def __init__(self, connector: Any, **kwargs):
        """
        Initialize the base loader.
        
        Args:
            connector: Connection to the target system
            **kwargs: Additional loader parameters
        """
        self.connector = connector
        self.config = kwargs
    
    @abstractmethod
    def load(self, data: Any, **kwargs) -> bool:
        """
        Load data to the target system.
        
        Args:
            data: Data to load (can be single record, list, or generator)
            **kwargs: Additional loading parameters
            
        Returns:
            True if loading was successful
        """
        pass
    
    @abstractmethod
    def batch_load(self, data_batch: List[Any], **kwargs) -> bool:
        """
        Load a batch of data to the target system.
        
        Args:
            data_batch: List of data records to load
            **kwargs: Additional loading parameters
            
        Returns:
            True if batch loading was successful
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
    
    def get_load_stats(self) -> Dict[str, Any]:
        """
        Get loading statistics.
        
        Returns:
            Dictionary with loading statistics
        """
        return {
            "total_loaded": getattr(self, '_total_loaded', 0),
            "total_failed": getattr(self, '_total_failed', 0),
            "last_load_time": getattr(self, '_last_load_time', None)
        }
