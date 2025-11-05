from datetime import datetime, timedelta
import pandas as pd

from connectors.factory import ConnectorFactory
from extractors.postgres import CDCExtractionStrategy, DateBasedExtractionStrategy, DataExtractor

# PostgreSQL connection setup
pg_params = {
    "host": "localhost",
    "port": 5432,
    "database": "paper_curator",
    "user": "rag_user",
    "password": "rag_password"
}

connector = ConnectorFactory.create_connector("postgres", pg_params)

try:
    connector.connect()
    print("Connected to Postgres")

    # Use last 2 days as extraction window
    last_extraction_date = datetime.now() - timedelta(hours=2)
    
    # Define common test setup
    table = "students"   # should exist with proper columns
    columns = ["id", "name", "department", "marks", "created_date", "last_modified_timestamp"]

    # --- CDC Strategy ---
    cdc_strategy = CDCExtractionStrategy()
    extractor = DataExtractor(cdc_strategy)
    try:
        df_cdc = extractor.extract_data(
            connector.connection,
            table,
            columns,
            last_extraction_date
        )
        print("\nCDC Extraction Successful:")
        print(df_cdc if not df_cdc.empty else "No CDC data found after given date.")
    except Exception as e:
        print("CDC extraction failed:", e)

    # --- Date-based Strategy ---
    date_strategy = DateBasedExtractionStrategy()
    extractor = DataExtractor(date_strategy)
    try:
        df_date = extractor.extract_data(
            connector.connection,
            table,
            columns,
            last_extraction_date
        )
        print("\nDate-based Extraction Successful:")
        print(df_date if not df_date.empty else "No new rows found after given date.")
    except Exception as e:
        print("Date-based extraction failed:", e)

finally:
    connector.disconnect()
    print("Disconnected from Postgres")
