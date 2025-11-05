"""
Base loader class for data loading.
Implements the Strategy pattern for different loading strategies.
"""
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
            data: Data to load
            **kwargs: Additional loading parameters
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def load_batch(self, data: List[Any], **kwargs) -> bool:
        """
        Load data in batch mode.
        
        Args:
            data: List of data items to load
            **kwargs: Additional loading parameters
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def validate(self, data: Any) -> bool:
        """
        Validate data before loading.
        
        Args:
            data: Data to validate
            
        Returns:
            True if valid, False otherwise
        """
        pass
