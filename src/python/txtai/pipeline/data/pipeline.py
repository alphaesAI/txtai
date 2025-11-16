"""Pipeline class for ETL data processing."""

import os
import logging
from typing import Any, Dict, List, Optional, Union
import pandas as pd

try:
    import pandas as pd
    PANDAS = True
except ImportError:
    PANDAS = False


class Pipeline:
    """
    Base pipeline class for data processing operations.
    """

    def __init__(self, **kwargs):
        """
        Initialize the pipeline.
        
        Args:
            **kwargs: Pipeline configuration parameters
        """
        self.config = kwargs
        self.logger = logging.getLogger(__name__)
    
    def __call__(self, data: Any) -> Any:
        """
        Process the input data.
        
        Args:
            data: Input data to process
            
        Returns:
            Processed data
        """
        return self.process(data)
    
    def process(self, data: Any) -> Any:
        """
        Process the input data. Override in subclasses.
        
        Args:
            data: Input data to process
            
        Returns:
            Processed data
        """
        raise NotImplementedError("Subclasses must implement process method")


        return self.process(df)

    def get_columns(self, data: Any) -> List[str]:
        """
        Get column names from input data.

        Args:
            data: input data

        Returns:
            list of column names
        """

        if isinstance(data, str):
            _, extension = os.path.splitext(data)
            extension = extension.replace(".", "").lower()

            if extension == "csv":
                df = pd.read_csv(data)
            elif extension in ["xlsx", "xls"]:
                df = pd.read_excel(data)
            elif extension == "json":
                df = pd.read_json(data)
            elif extension == "parquet":
                df = pd.read_parquet(data)
            else:
                raise ValueError(f"Unsupported file format: {extension}")

        elif isinstance(data, pd.DataFrame):
            df = data

        elif isinstance(data, dict):
            df = pd.DataFrame([data])

        elif isinstance(data, list) and data and isinstance(data[0], dict):
            df = pd.DataFrame(data)

        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

        return list(df.columns)

    def validate_config(self) -> bool:
        """
        Validate pipeline configuration.

        Returns:
            True if configuration is valid
        """

        if self.textcolumns and not isinstance(self.textcolumns, list):
            self.logger.error("textcolumns must be a list")
            return False

        if self.content and isinstance(self.content, list) and not all(isinstance(col, str) for col in self.content):
            self.logger.error("content list must contain only strings")
            return False

        return True
