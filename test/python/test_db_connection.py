import sys
import os
import pandas as pd
from pathlib import Path
from sqlalchemy import text
sys.path.append(os.path.abspath("paper_curator/"))

from src.db.interfaces.postgresql import PostgreSQLDatabase
from src.schemas.database.config import PostgreSQLSettings
from src.extractor.dbextractor import DBExtractor
from src.transmission import DataTransmitter
from src.storage import ESStorage

config = PostgreSQLSettings(
    database_url="postgresql://rag_user:rag_password@localhost:5432/paper_curator",
    echo_sql=True,
)

db = PostgreSQLDatabase(config)

db.startup()
print("database connection successful")

with db.get_session() as session:
    session.execute(
        text("""
        CREATE TABLE IF NOT EXISTS students (
             id SERIAL PRIMARY KEY,
             name VARCHAR(50),
             department VARCHAR(50),
             marks INTEGER
        )
        """)
    )
    session.commit()

    result = session.execute(text("SELECT COUNT(*) FROM students"))
    count = result.scalar()

    if count == 0:
        print("Inserting sample records...")
        session.execute(
            text("""
            INSERT INTO students (name, department, marks)
                 VALUES
                 ('Logidhasan', 'AI', 95)
                 """)
        )
        session.commit()
        print("sample records inserted.")
    else:
        print("sample data already exists.")

extractor = DBExtractor(db)()
df = extractor.extract_table("students", to_csv=True)

print("\nExtracted dataframe:")
print(df)

print("\nData successfully extracted and exported to 'students_export.csv'")

json_output_path = Path("students_export.json")
transmitter = DataTransmitter(df, save_to_file=True, output_path=json_output_path)
json_str = transmitter()
print("\nTransmitter JSON string:")
print(json_str)

es_storage = ESStorage(host="localhost", port=9200)
es_storage.startup()

es_storage.insert_json(index_name="students", json_str=json_str)

db.teardown()
print("Database connection closed.")