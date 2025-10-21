import logging
import pandas as pd
from sqlalchemy import text
from .base import BaseExtractor
from .repository import SQLAlchemyRepository

logger = logging.getLogger(__name__)

class DBExtractor(BaseExtractor):
    """Database extractor implementation for PostgreSQL"""

    def __init__(self, database):
        """
        Initialize the extractor with a database connectino.
        
        Args:
            database: Database object implementing BaseDatabase
        """
        self.database = database
        self.repository = None
        logger.info("DBExtractor initialized with database connection.")

    def __call__(self):
        """
        Prepare the extractor
        """
        if not self.database:
            raise ValueError("Database connection not provided.")
        logger.info("DBExtractor is ready to extract data.")
        return self
    
    def extract_table(self, table_name: str, to_csv: bool=False):
        """
        Extract all rows from a given table as a DataFrame.
        Optionally save to CSV.
        """
        try:
            with self.database.get_session() as session:
                query = text(f"SELECT * FROM {table_name}")
                result = session.execute(query)
                df = pd.DataFrame(result.fetchall(), columns=result.keys())

                if df.empty:
                    logger.warning(f"Table '{table_name}' is empty.")
                else:
                    logger.info(f"Extracted {len(df)} rows from '{table_name}'.")

                if to_csv:
                    file_path = f"{table_name}_export.csv"
                    df.to_csv(file_path, index=False)
                    logger.info(f"Data exported to CSV: {file_path}")

                return df
            
        except Exception as e:
            logger.error(f"Error extracting data from {table_name}: {e}")
            raise