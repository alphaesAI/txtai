"""
Base extractor class for data extraction.
Implements the Strategy pattern for different extraction strategies.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import pandas as pd


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
        table_name: str,
        columns: Optional[List[str]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Extract data from the data source.
        
        Args:
            table_name: Name of the table to extract
            columns: List of columns to extract
            **kwargs: Additional extraction parameters
            
        Returns:
            Pandas DataFrame containing extracted data
        """
        pass
    
    @abstractmethod
    def validate_extraction_config(self, config: Dict) -> bool:
        """
        Validate extraction configuration.
        
        Args:
            config: Extraction configuration dictionary
            
        Returns:
            True if valid, False otherwise
        """
        pass
    
    def _build_column_list(
        self,
        columns: Optional[List[str]] = None
    ) -> str:
        """
        Build SQL column list.
        
        Args:
            columns: List of columns
            
        Returns:
            Comma-separated column string or '*'
        """
        if columns:
            return ', '.join(columns)
        return '*'
