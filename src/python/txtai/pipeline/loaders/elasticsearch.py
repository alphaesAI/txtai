from abc import ABC, abstractmethod
from typing import List, Dict, Any
from elasticsearch import Elasticsearch, helpers
import logging

from .base import BaseElasticsearchLoader

class SingleDocumentLoader(BaseElasticsearchLoader):
    """Single document loading strategy"""
    def load(self, data: List[Dict[str, Any]]) -> None:
        for document in data:
            try:
                self.es_client.index(
                    index=document["_index"],
                    document=document["_source"]
                )
            except Exception as e:
                logging.error(f"Error loading document to Elasticsearch: {str(e)}")
                raise

class BulkDocumentLoader(BaseElasticsearchLoader):
    """Bulk loading strategy"""
    def load(self, data: List[Dict[str, Any]]) -> None:
        try:
            helpers.bulk(self.es_client, data)
        except Exception as e:
            logging.error(f"Error bulk loading documents to Elasticsearch: {str(e)}")
            raise