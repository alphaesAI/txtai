from typing import Dict, Any
from elasticsearch import Elasticsearch
import logging

from .base import BaseConnector


class ElasticsearchConnector(BaseConnector):
    """Elasticsearch connector."""

    def __init__(self, connection_params: Dict[str, Any]):
        """
        connection_params required:
            - hosts: list or string
        """
        if "hosts" not in connection_params:
            raise ValueError("Missing required param: hosts")

        self.hosts = connection_params["hosts"]
        self.client = None

    def connect(self):
        try:
            self.client = Elasticsearch(self.hosts)
            logging.info("Elasticsearch client initialized.")
        except Exception as e:
            logging.error(f"Elasticsearch connection init failed: {e}")
            raise

    def test_connection(self) -> bool:
        if not self.client:
            raise RuntimeError("Elasticsearch: connect() must be called first.")
        try:
            return self.client.ping()
        except Exception as e:
            logging.error(f"Elasticsearch ping failed: {e}")
            return False

    def disconnect(self):
        self.client = None
        logging.info("Elasticsearch connection closed (client released).")
