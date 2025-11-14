"""
PostgreSQL extractor with incremental extraction support.
"""
from typing import Dict, List, Optional, Any
import pandas as pd
from sqlalchemy import text, MetaData, Table
import logging
from datetime import datetime

from .base import BaseExtractor
from .config import TableConfig, ExtractionMode


logger = logging.getLogger(__name__)


class PostgresExtractor(BaseExtractor):
    """
    PostgreSQL data extractor with support for full and incremental date-based
    extraction strategies.
    """
    
    def __init__(self, connector: Any, **kwargs):
        """
        Initialize PostgreSQL extractor.
        
        Args:
            connector: PostgreSQL connector instance
            **kwargs: Additional extractor parameters
        """
        super().__init__(connector, **kwargs)
        self.metadata = MetaData()

    def _serialize_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert all pandas Timestamp columns in the DataFrame to ISO strings.
        """
        # Create a copy to avoid modifying the original DataFrame
        df = df.copy()
        
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
        return df
    
    def extract(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Extract data from PostgreSQL table.
        
        Args:
            table_name: Name of the table to extract
            columns: List of columns to extract
            **kwargs: Additional extraction parameters
            
        Returns:
            Pandas DataFrame containing extracted data
        """
        # Build TableConfig from parameters
        table_config = TableConfig(
            table_name=table_name,
            columns=columns,
            **kwargs
        )
        
        return self.extract_with_config(table_config)
    
    def extract_with_config(self, config: TableConfig) -> pd.DataFrame:
        """
        Extract data using TableConfig.
        
        Args:
            config: Table extraction configuration
            
        Returns:
            Pandas DataFrame containing extracted data
        """
        if not self.validate_extraction_config(config.__dict__):
            raise ValueError("Invalid extraction configuration")
        
        # Choose extraction strategy based on mode
        if config.extraction_mode == ExtractionMode.FULL:
            return self._extract_full(config)
        elif config.extraction_mode == ExtractionMode.INCREMENTAL_DATE:
            return self._extract_incremental_date(config)
        else:
            raise ValueError(f"Unsupported extraction mode: {config.extraction_mode}")
    
    def _extract_full(self, config: TableConfig) -> pd.DataFrame:
        """
        Perform full table extraction.
        
        Args:
            config: Table extraction configuration
            
        Returns:
            Pandas DataFrame containing all table data
        """
        logger.info(f"Performing full extraction from {config.table_name}")
        
        # Build query
        columns_str = self._build_column_list(config.columns)
        table_name = config.get_full_table_name()
        
        query = f"SELECT {columns_str} FROM {table_name}"
        
        if config.where_clause:
            query += f" WHERE {config.where_clause}"
        
        if config.order_by:
            query += f" ORDER BY {config.order_by}"
        
        # Execute query and load into DataFrame
        try:
            engine = self.connector.get_connection()
            
            # Get raw DBAPI connection for pandas compatibility
            with engine.connect() as conn:
                # Access the underlying DBAPI connection
                raw_conn = conn.connection
                df = pd.read_sql(query, raw_conn)
                df = self._serialize_timestamps(df)
            
            logger.info(f"Extracted {len(df)} rows from {config.table_name}")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting from {config.table_name}: {e}")
            raise
    
    def _extract_incremental_date(self, config: TableConfig) -> pd.DataFrame:
        """
        Perform incremental date-based extraction.
        
        Args:
            config: Table extraction configuration
            
        Returns:
            Pandas DataFrame containing incremental data
        """
        logger.info(
            f"Performing incremental date extraction from {config.table_name} "
            f"using column {config.date_column}"
        )
        
        # Build query
        columns_str = self._build_column_list(config.columns)
        table_name = config.get_full_table_name()
        
        query = f"SELECT {columns_str} FROM {table_name}"
        
        # Build WHERE clause for date range
        where_conditions = []
        
        if config.start_date:
            where_conditions.append(
                f"{config.date_column} > '{config.start_date.isoformat()}'"  # Changed >= to > to avoid duplicates
            )
        
        if config.end_date:
            where_conditions.append(
                f"{config.date_column} < '{config.end_date.isoformat()}'"
            )
        
        if config.where_clause:
            where_conditions.append(f"({config.where_clause})")
        
        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)
        
        if config.order_by:
            query += f" ORDER BY {config.order_by}"
        else:
            # Default order by date column
            query += f" ORDER BY {config.date_column}"
        
        # Execute query
        try:
            engine = self.connector.get_connection()
            
            # Get raw DBAPI connection for pandas compatibility
            with engine.connect() as conn:
                raw_conn = conn.connection
                df = pd.read_sql(query, raw_conn)
                df = self._serialize_timestamps(df)
            
            logger.info(
                f"Extracted {len(df)} rows from {config.table_name} "
                f"(date range: {config.start_date.isoformat() if config.start_date else 'None'} to {config.end_date.isoformat() if config.end_date else 'None'})"
            )
            logger.debug(f"SQL Query: {query}")
            return df
            
        except Exception as e:
            logger.error(f"Error in incremental date extraction: {e}")
            raise
    
    
    def validate_extraction_config(self, config: Dict) -> bool:
        """
        Validate extraction configuration.
        
        Args:
            config: Extraction configuration dictionary
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ['table_name']
        
        for field in required_fields:
            if field not in config or config[field] is None:
                logger.error(f"Missing required field: {field}")
                return False
        
        return True
    
    def get_max_value(
        self,
        table_name: str,
        column: str,
        schema: Optional[str] = None
    ) -> Any:
        """
        Get maximum value of a column (useful for CDC).
        
        Args:
            table_name: Name of the table
            column: Name of the column
            schema: Schema name
            
        Returns:
            Maximum value of the column
        """
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        query = f"SELECT MAX({column}) as max_value FROM {full_table_name}"
        
        try:
            engine = self.connector.get_connection()
            
            # Get raw DBAPI connection for pandas compatibility
            with engine.connect() as conn:
                raw_conn = conn.connection
                result = pd.read_sql(query, raw_conn)
                max_value = result['max_value'].iloc[0]
            
            logger.info(
                f"Maximum {column} value in {full_table_name}: {max_value}"
            )
            return max_value
            
        except Exception as e:
            logger.error(f"Error getting max value: {e}")
            raise
    
    def get_table_columns(
        self,
        table_name: str,
        schema: Optional[str] = None
    ) -> List[str]:
        """
        Get list of columns in a table.
        
        Args:
            table_name: Name of the table
            schema: Schema name
            
        Returns:
            List of column names
        """
        try:
            engine = self.connector.get_connection()
            
            # Reflect table metadata
            table = Table(
                table_name,
                self.metadata,
                autoload_with=engine,
                schema=schema
            )
            
            columns = [col.name for col in table.columns]
            logger.info(f"Columns in {table_name}: {columns}")
            
            return columns
            
        except Exception as e:
            logger.error(f"Error getting table columns: {e}")
            raise
