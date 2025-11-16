"""Base transformer class for ETL pipeline."""

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
            data: Input data as pandas DataFrame
            **kwargs: Additional transformation parameters
            
        Returns:
            Transformed data
        """
        pass
    
    @abstractmethod
    def get_output_schema(self, input_schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get the output schema based on input schema.
        
        Args:
            input_schema: Schema of input data
            
        Returns:
            Schema of output data
        """
        pass
    
    def validate_input(self, data: pd.DataFrame) -> bool:
        """
        Validate input data.
        
        Args:
            data: Input data to validate
            
        Returns:
            True if data is valid
        """
        return data is not None and not data.empty
    
    def get_transform_stats(self) -> Dict[str, Any]:
        """
        Get transformation statistics.
        
        Returns:
            Dictionary with transformation statistics
        """
        return {
            "total_transformed": getattr(self, '_total_transformed', 0),
            "total_failed": getattr(self, '_total_failed', 0),
            "last_transform_time": getattr(self, '_last_transform_time', None)
        }
