import logging
import json
from typing import Dict, Any
from .connection import ElasticsearchClientFactory
from elasticsearch import helpers, Elasticsearch

logger = logging.getLogger(__name__)

class ElasticsearchLoader:
    def __init__(self, es_config, index_name: str):
        self.es_config = es_config
        self.index_name = index_name
        self.client = None

    def connect(self):
        try:
            factory = ElasticsearchClientFactory(self.es_config)
            self.client = factory.create_client()
            logger.info("Connected to Elasticsearch successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {e}")
            raise

    def index_exists(self):
        try:
            if not self.client.indices.exists(index=self.index_name):
                self.client.indices.create(index=self.index_name)
                logger.info(f"Created new index: {self.index_name}")
            else:
                logger.info(f"Index already exists: {self.index_name}")
        except Exception as e:
            logger.error(f"Error ensuring index existence: {e}")
            raise

    def ingest_doc(self, document: Dict[str, Any]):
        """
        Perform a single-document ingestion.
        """
        try:
            if not self.client:
                raise ValueError("Elasticsearch client is not initialized. call connect() first.")
            
            response = self.client.index(index=self.index_name, document=document)
            logger.info(f"Document ingested successfully with ID: {response['_id']}")
            return response
        
        except Exception as e:
            logger.error(f"Error ingesting document: {e}")
            raise

class BulkIngestion:
    def __init__(self, client: Elasticsearch):
        self.client = client

    def ingest_json(self, json_string: str, index_name: str):
        try:
            documents = json.loads(json_string)

            if isinstance(documents, dict):
                documents = [documents]
            elif not isinstance(documents, list):
                raise ValueError("Invalid JSON format: must be a list or an object")
            
            actions = [
                {
                    "_index": index_name,
                    "_source": doc
                }
                for doc in documents
            ]

            success, errors = helpers.bulk(self.client, actions, raise_on_error=False)

            return {
                "status": "success",
                "indexed_count": success,
                "errors": errors
            }
        
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON string provided")
        except Exception as e:
            raise RuntimeError(f"Bulk ingestion failed: {e}")