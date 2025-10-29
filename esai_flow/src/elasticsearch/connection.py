import os
from elasticsearch import Elasticsearch, exceptions

class ElasticsearchConfig:
    def __init__(self, host: str, username: str, password: str, verify_certs: bool = True):
        self.host = host
        self.username = username
        self.password = password
        self.verify_certs = verify_certs

class ElasticsearchClientFactory:
    def __init__(self, config: ElasticsearchConfig):
        self.config = config

    def create_client(self) -> Elasticsearch:
        try:
            client = Elasticsearch(
                hosts=[self.config.host],
                # basic_auth=(self.config.username, self.config.password),
                verify_certs=self.config.verify_certs
            )
            client.info()
            return client
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Elasticsearch: {e}")
        
        
