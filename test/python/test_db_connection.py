import os
import sys
import json
from pathlib import Path
from sqlalchemy import text

sys.path.append(os.path.abspath("esai_flow/"))

from src.connector.interfaces import RDBMSDatabase
from src.schemas.database.config import DatabaseConfig
from src.extractor.dbextractor import DBExtractor
from src.transmission import DataTransmitter
from src.elasticsearch.connection import ElasticsearchConfig, ElasticsearchClientFactory
from src.elasticsearch.loader import ElasticsearchLoader, BulkIngestion

def setup_database():
    """ 
     Create table and insert sample data if missing.
    """
    config = DatabaseConfig(
        database_url="postgresql://rag_user:rag_password@localhost:5432/paper_curator",
        echo_sql=True,
    )

    db = RDBMSDatabase(config)
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
    return json_str

def setup_elasticsearch():
    es_config = ElasticsearchConfig(
        host="http://localhost:9200",
        username="elastic",
        password="q+nR3Kse*QW5kpoacWn3",
        verify_certs=False,
    )

    loader = ElasticsearchLoader(es_config, index_name="students_index")
    loader.connect()
    loader.index_exists()
    print("Elasticsearch connection established and index ensured.")
    return loader

def ingest_to_elasticsearch(loader, json_str):
    try:
        data = json.loads(json_str)
        if isinstance(data, list):
            doc = data[0]
        else:
            doc = data

        response = loader.ingest_doc(doc)
        print("\nIngestion response:")
        print(response)
    except Exception as e:
        print(f"Error during ingestion: {e}")

def ingest_bulk(loader, json_str):
    try:
        bulk_ingestor = BulkIngestion(loader.client)
        response = bulk_ingestor.ingest_json(json_str, loader.index_name)
        print("\nBulk ingestion response:")
        print(response)
    except Exception as e:
        print(f"Error during ingestion: {e}")

if __name__ == "__main__":
    db = setup_database()
    df = extract_data(db)
    json_str = transmission(df)
    loader = setup_elasticsearch()
    ingest_bulk(loader, json_str)
