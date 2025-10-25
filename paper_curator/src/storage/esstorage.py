import json
import logging
from typing import Optional

from elasticsearch import Elasticsearch, helpers

logger = logging.getLogger(__name__)

class ESStorage:
    """
    Elasticsearch storage handler for inserting JSON data.
    """

    def __init__(self, host: str="localhost", port: int=9200):
        self.host = host
        self.port = port
        self.es: Optional[Elasticsearch] = None

    def startup(self) -> None:
        """
        Initialize Elasticsearch connection.
        """
        try:
            self.es = Elasticsearch([{"host": self.host, "port": self.port, "scheme": "http"}])
            if self.es.ping():
                logger.info(f"Connected to Elasticsearch at {self.host}:{self.port}")
            else:
                raise ConnectionError("Elasticsearch ping failed")
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {e}")
            raise

    def teardown(self) -> None:
        """
        Close Elasticsearch connection.
        """
        if self.es:
            self.es.transport.close()
            logger.info("Elasticsearch connection closed")

    def insert_json(self, index_name: str, json_str: str) -> None:
        """
        Insert JSON string into Elasticsearch index.
        
        Args:
            index_name: the name of the ES index
            json_str: JSON string representing list of documents
        """
        if not self.es:
            raise RuntimeError("Elasticsearch not initialized. Call startup() first.")
        
        try:
            data = json.loads(json_str)
            if not isinstance(data, list):
                data = [data]

            actions = [
                {
                    "_index": index_name,
                    "_id": doc.get("id"),
                    "_source": doc,
                }
                for doc in data
            ]

            helpers.bulk(self.es, actions)
            logger.info(f"Inserted {len(actions)} documents into index '{index_name}' ")
        except Exception as e:
            logger.error(f"Failed to insert data into Elasticsearch: {e}")
            raise