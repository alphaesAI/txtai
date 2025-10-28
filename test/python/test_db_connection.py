import os
import sys
from pathlib import Path
from sqlalchemy import text

sys.path.append(os.path.abspath("paper_curator/"))

from src.db.interfaces.postgresql import PostgreSQLDatabase
from src.schemas.database.config import PostgreSQLSettings
from src.extractor.dbextractor import DBExtractor
from src.transmission import DataTransmitter
from src.elasticsearch.connection import ElasticsearchConfig, ElasticsearchClientFactory

def setup_database():
    """ 
     Create table and insert sample data if missing.
    """
    config = PostgreSQLSettings(
        database_url="postgresql://rag_user:rag_password@localhost:5432/paper_curator",
        echo_sql=True,
    )

    db = PostgreSQLDatabase(config)
    db.startup()
    print("Database connection successful")
    return db

def extract_data(db):
    extractor = DBExtractor(db)
    df = extractor.extract_table("students", to_csv=True)
    print("\nExtracted DataFrame: ")
    print(df)
    print("\nData successfully extracted and exported to 'students_export.csv'")
    return df

def transmission(df):
    json_output_path = Path("students_export.json")
    transmitter = DataTransmitter(df, save_to_file=True, output_path=json_output_path)
    json_str = transmitter()
    print("\nTransmitter JSON string: ")
    print(json_str)


if __name__ == "__main__":
    db = setup_database()
    df = extract_data(db)
    transmission(df)