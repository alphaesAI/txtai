"""
JSON transformer for converting DataFrames to JSON format.
"""
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import json
import logging
from datetime import datetime, date
from decimal import Decimal
import numpy as np

from .base import BaseTransformer


logger = logging.getLogger(__name__)


class JSONTransformer(BaseTransformer):
    """
    Transformer to convert Pandas DataFrame to JSON format.
    Supports various JSON output formats and handles data type conversions.
    """
    
    def __init__(
        self,
        orient: str = 'records',
        date_format: str = 'iso',
        handle_nan: str = 'null',
        include_index: bool = False,
        indent: Optional[int] = None,
        **kwargs
    ):
        """
        Initialize JSON transformer.
        
        Args:
            orient: JSON format orientation ('records', 'index', 'columns', 'values', 'split', 'table')
            date_format: Date format ('iso', 'epoch', 'custom')
            handle_nan: How to handle NaN values ('null', 'drop', 'string')
            include_index: Include DataFrame index in output
            indent: JSON indentation level (None for compact)
            **kwargs: Additional transformer parameters
        """
        super().__init__(**kwargs)
        self.orient = orient
        self.date_format = date_format
        self.handle_nan = handle_nan
        self.include_index = include_index
        self.indent = indent
    
    def transform(
        self,
        data: pd.DataFrame,
        **kwargs
    ) -> Union[List[Dict], str]:
        """
        Transform DataFrame to JSON format.
        
        Args:
            data: Input DataFrame
            **kwargs: Additional transformation parameters
            
        Returns:
            JSON data (list of dicts or JSON string)
        """
        if not self.validate(data):
            raise ValueError("Invalid input data")
        
        # Preprocess data
        data = self.preprocess(data)
        
        # Convert to JSON
        try:
            # Handle NaN values
            if self.handle_nan == 'drop':
                data = data.dropna()
            elif self.handle_nan == 'null':
                data = data.replace({np.nan: None})
            elif self.handle_nan == 'string':
                data = data.fillna('null')
            
            # Convert DataFrame to JSON
            if kwargs.get('as_string', False):
                # Return as JSON string
                json_data = data.to_json(
                    orient=self.orient,
                    date_format=self.date_format,
                    indent=self.indent,
                    **kwargs.get('to_json_kwargs', {})
                )
            else:
                # Return as Python objects (list of dicts)
                json_data = data.to_dict(orient=self.orient)
                
                # Apply custom serialization for special types
                json_data = self._serialize_special_types(json_data)
            
            # Postprocess
            json_data = self.postprocess(json_data)
            
            logger.info(f"Transformed {len(data)} rows to JSON format")
            return json_data
            
        except Exception as e:
            logger.error(f"Error transforming to JSON: {e}")
            raise
    
    def _serialize_special_types(self, data: Any) -> Any:
        """
        Serialize special data types to JSON-compatible formats.
        
        Args:
            data: Data to serialize
            
        Returns:
            Serialized data
        """
        if isinstance(data, dict):
            return {key: self._serialize_special_types(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._serialize_special_types(item) for item in data]
        elif isinstance(data, (datetime, date)):
            return data.isoformat()
        elif isinstance(data, Decimal):
            return float(data)
        elif isinstance(data, (np.integer, np.floating)):
            return data.item()
        elif pd.isna(data):
            return None
        else:
            return data
    
    def validate(self, data: pd.DataFrame) -> bool:
        """
        Validate input DataFrame.
        
        Args:
            data: Input DataFrame to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not isinstance(data, pd.DataFrame):
            logger.error("Input data must be a Pandas DataFrame")
            return False
        
        if data.empty:
            logger.warning("Input DataFrame is empty")
            return True  # Empty is valid but logged
        
        return True
    
    def preprocess(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess DataFrame before JSON conversion.
        
        Args:
            data: Input DataFrame
            
        Returns:
            Preprocessed DataFrame
        """
        # Create a copy to avoid modifying original
        df = data.copy()
        
        # Convert datetime columns to appropriate format
        for col in df.select_dtypes(include=['datetime64']).columns:
            if self.date_format == 'iso':
                df[col] = df[col].apply(
                    lambda x: x.isoformat() if pd.notna(x) else None
                )
            elif self.date_format == 'epoch':
                df[col] = df[col].apply(
                    lambda x: int(x.timestamp()) if pd.notna(x) else None
                )
        
        # Handle binary/bytes columns
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].apply(lambda x: isinstance(x, bytes)).any():
                df[col] = df[col].apply(
                    lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
                )
        
        return df
    
    def transform_batch(
        self,
        data: pd.DataFrame,
        batch_size: int = 1000,
        **kwargs
    ) -> List[List[Dict]]:
        """
        Transform DataFrame in batches.
        
        Args:
            data: Input DataFrame
            batch_size: Size of each batch
            **kwargs: Additional transformation parameters
            
        Returns:
            List of batches (each batch is a list of dicts)
        """
        batches = []
        
        for start_idx in range(0, len(data), batch_size):
            end_idx = min(start_idx + batch_size, len(data))
            batch_df = data.iloc[start_idx:end_idx]
            
            batch_json = self.transform(batch_df, **kwargs)
            batches.append(batch_json)
            
            logger.debug(f"Transformed batch {start_idx}-{end_idx}")
        
        logger.info(f"Transformed {len(batches)} batches")
        return batches
    
    def add_metadata(
        self,
        data: List[Dict],
        metadata: Dict[str, Any]
    ) -> List[Dict]:
        """
        Add metadata fields to each record.
        
        Args:
            data: List of records
            metadata: Metadata to add to each record
            
        Returns:
            Records with metadata
        """
        for record in data:
            record.update(metadata)
        
        return data
    
    def flatten_nested(
        self,
        data: List[Dict],
        separator: str = '_'
    ) -> List[Dict]:
        """
        Flatten nested dictionaries.
        
        Args:
            data: List of records with nested dicts
            separator: Separator for flattened keys
            
        Returns:
            Flattened records
        """
        def flatten_dict(d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(flatten_dict(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
            return dict(items)
        
        return [flatten_dict(record, sep=separator) for record in data]