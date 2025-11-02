from typing import List, Optional
import pandas as pd
from datetime import datetime
import logging

from .base import ExtractionStrategy

class CDCExtractionStrategy(ExtractionStrategy):
    """CDC-based extraction strategy"""
    def extract(self, connection, table: str, columns: List[str], 
                last_extraction_date: Optional[datetime]) -> pd.DataFrame:
        column_str = ", ".join(columns)
        query = f"""
            SELECT {column_str}
            FROM {table}
            WHERE last_modified_timestamp > %s
        """
        try:
            df = pd.read_sql_query(
                query,
                connection,
                params=(last_extraction_date,)
            )
            return df
        except Exception as e:
            logging.error(f"Error extracting data from {table}: {str(e)}")
            raise

class DateBasedExtractionStrategy(ExtractionStrategy):
    """Date-based extraction strategy"""
    def extract(self, connection, table: str, columns: List[str], 
                last_extraction_date: Optional[datetime]) -> pd.DataFrame:
        column_str = ", ".join(columns)
        query = f"""
            SELECT {column_str}
            FROM {table}
            WHERE created_date > %s
        """
        try:
            df = pd.read_sql_query(
                query,
                connection,
                params=(last_extraction_date,)
            )
            return df
        except Exception as e:
            logging.error(f"Error extracting data from {table}: {str(e)}")
            raise

class DataExtractor:
    """Main extractor class using Strategy pattern"""
    def __init__(self, strategy: ExtractionStrategy):
        self.strategy = strategy

    def extract_data(self, connection, table: str, columns: List[str], 
                     last_extraction_date: Optional[datetime]) -> pd.DataFrame:
        return self.strategy.extract(connection, table, columns, last_extraction_date)