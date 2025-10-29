import logging
import pandas as pd
from sqlalchemy import text
from .base import BaseExtractor

logger = logging.getLogger(__name__)

class DBExtractor(BaseExtractor):
    def __init__(self, database):
        if not database:
            raise ValueError("Database connection not provided.")
        self.database = database
        logger.info("DBExtractor initialized with a valid database connection.")

    def __call__(self):
        return self

    def extract_table(self, table_name: str, to_csv: bool = False):
        try:
            with self.database.get_session() as session:
                query = text(f"SELECT * FROM {table_name}")
                result = session.execute(query)
                rows = result.fetchall()
                columns = result.keys()
                df = pd.DataFrame(rows, columns=columns)
            
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
            logger.error(f"Error extracting data from table '{table_name}': {e}")
            raise