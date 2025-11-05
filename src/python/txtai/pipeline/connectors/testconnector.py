# connectors/testconnector.py

from .postgres import PostgresConnector  # adjust if your file has a different name
import logging

def test_postgres_connector():
    """Manual integration test for PostgresConnector"""
    logging.basicConfig(level=logging.INFO)

    # Fill these with your actual database values
    config = {
        "host": "localhost",
        "port": 5432,
        "database": "testdb",
        "user": "postgres",
        "password": "password"
    }

    try:
        print("\n--- Initializing PostgresConnector ---")
        connector = PostgresConnector(config)

        print("\n--- Connecting to database ---")
        connector.connect()

        print("\n--- Testing connection ---")
        success = connector.test_connection()
        if success:
            print("Connection test passed ✅")
        else:
            print("Connection test failed ❌")

    except Exception as e:
        print(f"Error during test: {e}")

    finally:
        print("\n--- Disconnecting ---")
        try:
            connector.disconnect()
        except Exception as e:
            print(f"Error during disconnect: {e}")

if __name__ == "__main__":
    test_postgres_connector()
