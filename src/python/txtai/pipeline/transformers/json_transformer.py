"""JSON transformer for ETL pipeline."""

import json
import pandas as pd
from typing import Any, Dict, Optional
import logging

from .base import BaseTransformer


class JsonTransformer(BaseTransformer):
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
        
        # Statistics tracking
        self._total_transformed = 0
        self._total_failed = 0
        self._last_transform_time = None
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
    
    def transform(self, data: pd.DataFrame, **kwargs) -> Any:
        """
        Transform DataFrame to JSON format.
        
        Args:
            data: Input DataFrame
            **kwargs: Additional transformation parameters
            
        Returns:
            JSON string or list/dict depending on orient
        """
        if not self.validate_input(data):
            raise ValueError("Invalid input data")
        
        try:
            # Handle NaN values
            data = self._handle_nan_values(data)
            
            # Convert timestamps based on date_format
            data = self._convert_timestamps(data)
            
            # Convert to JSON
            if self.orient == 'records':
                result = data.to_dict(orient='records')
            elif self.orient == 'records_string':
                result = data.to_json(
                    orient='records',
                    date_format=self.date_format,
                    date_unit='s',
                    indent=self.indent
                )
            else:
                result = data.to_json(
                    orient=self.orient,
                    date_format=self.date_format,
                    date_unit='s',
                    indent=self.indent
                )
            
            # Update statistics
            self._total_transformed += len(data)
            self._last_transform_time = pd.Timestamp.now().timestamp()
            
            return result
            
        except Exception as e:
            self._total_failed += len(data) if data is not None else 0
            self.logger.error(f"Failed to transform data to JSON: {e}")
            raise
    
    def _handle_nan_values(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Handle NaN values according to configuration.
        
        Args:
            data: Input DataFrame
            
        Returns:
            DataFrame with NaN values handled
        """
        if self.handle_nan == 'drop':
            return data.dropna()
        elif self.handle_nan == 'string':
            return data.fillna('NaN')
        else:  # 'null' (default)
            return data
    
    def _convert_timestamps(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Convert timestamp columns based on date_format setting.
        
        Args:
            data: Input DataFrame
            
        Returns:
            DataFrame with converted timestamps
        """
        data = data.copy()
        
        for col in data.columns:
            if pd.api.types.is_datetime64_any_dtype(data[col]):
                if self.date_format == 'iso':
                    data[col] = data[col].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                elif self.date_format == 'epoch':
                    data[col] = data[col].astype('int64') // 10**9
        
        return data
    
    def get_output_schema(self, input_schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get the output schema based on input schema.
        
        Args:
            input_schema: Schema of input data
            
        Returns:
            Schema of output data
        """
        if self.orient == 'records':
            return {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        col["name"]: {"type": "string"} for col in input_schema.get("columns", [])
                    }
                }
            }
        else:
            return {
                "type": "object",
                "properties": {
                    "data": {"type": "array"},
                    "columns": {"type": "array"},
                    "index": {"type": "array"}
                }
            }
    
    def transform_to_file(self, data: pd.DataFrame, file_path: str, **kwargs) -> bool:
        """
        Transform DataFrame and save to JSON file.
        
        Args:
            data: Input DataFrame
            file_path: Output file path
            **kwargs: Additional transformation parameters
            
        Returns:
            True if successful
        """
        try:
            json_data = self.transform(data, **kwargs)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                if isinstance(json_data, str):
                    f.write(json_data)
                else:
                    json.dump(json_data, f, indent=self.indent, ensure_ascii=False)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save JSON to file {file_path}: {e}")
            return False
    
    def transform_batch(self, data_frames: list[pd.DataFrame], **kwargs) -> list[Any]:
        """
        Transform multiple DataFrames to JSON.
        
        Args:
            data_frames: List of DataFrames to transform
            **kwargs: Additional transformation parameters
            
        Returns:
            List of JSON results
        """
        results = []
        
        for df in data_frames:
            try:
                result = self.transform(df, **kwargs)
                results.append(result)
            except Exception as e:
                self.logger.error(f"Failed to transform batch DataFrame: {e}")
                results.append(None)
        
        return results
