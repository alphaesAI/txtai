"""
Base transformer class for data transformation.
Implements the Strategy pattern for different transformation strategies.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import pandas as pd


class BaseTransformer(ABC):
    """
    Abstract base class for all transformers.
    Defines the interface for data transformation.
    """
    
    def __init__(self, **kwargs):
        """
        Initialize the base transformer.
        
        Args:
            **kwargs: Additional transformer parameters
        """
        self.config = kwargs
    
    @abstractmethod
    def transform(self, data: pd.DataFrame, **kwargs) -> Any:
        """
        Transform the input data.
        
        Args:
            data: Input data as Pandas DataFrame
            **kwargs: Additional transformation parameters
            
        Returns:
            Transformed data
        """
        pass
    
    @abstractmethod
    def validate(self, data: pd.DataFrame) -> bool:
        """
        Validate input data before transformation.
        
        Args:
            data: Input data to validate
            
        Returns:
            True if valid, False otherwise
        """
        pass
    
    def preprocess(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess data before transformation.
        
        Args:
            data: Input data
            
        Returns:
            Preprocessed data
        """
        # Default implementation - can be overridden
        return data
    
    def postprocess(self, data: Any) -> Any:
        """
        Postprocess data after transformation.
        
        Args:
            data: Transformed data
            
        Returns:
            Postprocessed data
        """
        # Default implementation - can be overridden
        return data
