from loaders.elasticsearch import SingleDocumentLoader, BulkDocumentLoader
from connectors.elasticsearch import ElasticsearchConnector

def test_single_loader():
    print("\n--- Running SingleDocumentLoader Test ---")

    connection_params = {
    "hosts": 'http://localhost:9200',
    "basic_auth": ("elastic", "q+nR3Kse*QW5kpoacWn3"),
    "verify_certs": False
    }
    connector = ElasticsearchConnector(connection_params)
    es_client = connector.connect()

    loader = SingleDocumentLoader(es_client)
    data = [
        {"_index": "students_index", "_source": {"id": 1, "name": "Logidhasan", "department": "AI", "marks": 95}},
        {"_index": "students_index", "_source": {"id": 2, "name": "Nisha", "department": "Data Science", "marks": 90}},
    ]

    loader.load(data)
    print("SingleDocumentLoader test completed successfully.")
    connector.disconnect()

def test_bulk_loader():
    print("\n--- Running BulkDocumentLoader Test ---")

    connection_params = {
        "host": "localhost",
        "port": 9200,
        "scheme": "http",
        "username": "elastic",
        "password": "your_password"
    }

    connector = ElasticsearchConnector(connection_params)
    es_client = connector.connect()

    loader = BulkDocumentLoader(es_client)
    data = [
        {"_index": "students_index", "_source": {"id": 3, "name": "Deva", "department": "Tamil", "marks": 100}},
        {"_index": "students_index", "_source": {"id": 4, "name": "Arun", "department": "CSE", "marks": 85}},
    ]

    loader.load(data)
    print("BulkDocumentLoader test completed successfully.")
    connector.disconnect()

if __name__ == "__main__":
    test_single_loader()
    test_bulk_loader()
