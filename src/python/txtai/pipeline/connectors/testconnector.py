# from .postgres import PostgresConnector
# from .elasticsearch import ElasticsearchConnector

# connection_params = {
#     "host": "localhost",
#     "port": 5432,
#     "database": "paper_curator",
#     "user": "rag_user",
#     "password": "rag_password"
# }

# connector = PostgresConnector(connection_params)

# try:
#     connector.connect()
#     print("connection successful")
#     print("testing connection:", connector.test_connection())
# finally:
#     connector.disconnect()
#     print("connection disclosed")


# es_connection_params = {
#     "hosts": 'http://localhost:9200',
#     "basic_auth": ("elastic", "q+nR3Kse*QW5kpoacWn3"),
#     "verify_certs": False
# }

# es_connector = ElasticsearchConnector(es_connection_params)

# try:
#     es_connector.connect()
#     print("connection successful")
# finally:
#     es_connector.disconnect()

# print("okay")


# from .factory import ConnectorFactory

# # PostgreSQL test
# pg_params = {
#     "host": "localhost",
#     "port": 5432,
#     "database": "paper_curator",
#     "user": "rag_user",
#     "password": "rag_password"
# }

# pg_connector = ConnectorFactory.create_connector("postgres", pg_params)

# try:
#     pg_connector.connect()
#     print("Postgres connected successfully")
#     print("Test connection:", pg_connector.test_connection())
# finally:
#     pg_connector.disconnect()
#     print("Postgres disconnected")

# # Elasticsearch test
# es_params = {
#     "hosts": ["http://localhost:9200"],
#     "basic_auth": ("elastic", "q+nR3Kse*QW5kpoacWn3"),
#     "verify_certs": False
# }

# es_connector = ConnectorFactory.create_connector("elasticsearch", es_params)

# try:
#     es_connector.connect()
#     print("Elasticsearch connected successfully")
#     print("Test connection:", es_connector.test_connection())
# finally:
#     es_connector.disconnect()
#     print("Elasticsearch disconnected")

# print("All connectors tested successfully")


from .postgres import PostgresConnector

def main():
    config = {
        "host": "localhost",
        "port": 5432,
        "database": "testdb",
        "user": "postgres",
        "password": "password"
    }

    connector = PostgresConnector(config)
    connector.connect()         # ← this line is mandatory before testing
    connector.test_connection() # now it’ll work fine
    connector.disconnect()

if __name__ == "__main__":
    main()
