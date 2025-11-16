"""PostgreSQL extractor for ETL pipeline."""

import pandas as pd
from sqlalchemy import MetaData, inspect, text
from typing import Any, Dict, Iterator, Optional
import logging

from .base import BaseExtractor


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
        self.logger = logging.getLogger(__name__)

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
        query: Optional[str] = None,
        batch_size: int = 1000,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """
        Extract data from PostgreSQL table.
        
        Args:
            table_name: Name of the table to extract from
            query: Optional custom SQL query
            batch_size: Number of rows to fetch per batch
            **kwargs: Additional extraction parameters
            
        Yields:
            Data records as dictionaries
        """
        if not self.validate_connection():
            raise RuntimeError("Database connection is not valid")
        
        try:
            if query:
                # Use custom query
                sql = text(query)
            else:
                # Use table name with optional filters
                sql = text(f"SELECT * FROM {table_name}")
                if kwargs.get('where_clause'):
                    sql = text(f"SELECT * FROM {table_name} WHERE {kwargs['where_clause']}")
            
            # Get database session
            session = self.connector.get_session()
            
            # Execute query in batches
            offset = 0
            while True:
                batch_sql = sql.limit(batch_size).offset(offset)
                result = session.execute(batch_sql)
                rows = result.fetchall()
                
                if not rows:
                    break
                
                # Convert rows to dictionaries
                columns = result.keys()
                for row in rows:
                    yield dict(zip(columns, row))
                
                offset += batch_size
                
                # Break if we got fewer rows than batch_size
                if len(rows) < batch_size:
                    break
                    
        except Exception as e:
            self.logger.error(f"Failed to extract data from {table_name}: {e}")
            raise
        finally:
            if 'session' in locals():
                session.close()
    
    def extract_full(
        self,
        table_name: str,
        batch_size: int = 1000,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """
        Extract full table data.
        
        Args:
            table_name: Name of the table
            batch_size: Batch size for extraction
            **kwargs: Additional parameters
            
        Yields:
            Data records as dictionaries
        """
        yield from self.extract(table_name, batch_size=batch_size, **kwargs)
    
    def extract_incremental(
        self,
        table_name: str,
        timestamp_column: str,
        last_extracted: Optional[str] = None,
        batch_size: int = 1000,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """
        Extract incremental data based on timestamp.
        
        Args:
            table_name: Name of the table
            timestamp_column: Column name for timestamp filtering
            last_extracted: Last extracted timestamp (ISO format)
            batch_size: Batch size for extraction
            **kwargs: Additional parameters
            
        Yields:
            Data records as dictionaries
        """
        where_clause = None
        if last_extracted:
            where_clause = f"{timestamp_column} > '{last_extracted}'"
        
        yield from self.extract(
            table_name,
            batch_size=batch_size,
            where_clause=where_clause,
            **kwargs
        )
    
    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get table schema information.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Schema information
        """
        if not self.validate_connection():
            raise RuntimeError("Database connection is not valid")
        
        try:
            inspector = inspect(self.connector.engine)
            columns = inspector.get_columns(table_name)
            
            schema = {
                "table_name": table_name,
                "columns": []
            }
            
            for column in columns:
                schema["columns"].append({
                    "name": column["name"],
                    "type": str(column["type"]),
                    "nullable": column["nullable"],
                    "default": column.get("default")
                })
            
            return schema
            
        except Exception as e:
            self.logger.error(f"Failed to get schema for {table_name}: {e}")
            raise
    
    def get_table_list(self) -> List[str]:
        """
        Get list of all tables in the database.
        
        Returns:
            List of table names
        """
        if not self.validate_connection():
            raise RuntimeError("Database connection is not valid")
        
        try:
            inspector = inspect(self.connector.engine)
            return inspector.get_table_names()
        except Exception as e:
            self.logger.error(f"Failed to get table list: {e}")
            raise
