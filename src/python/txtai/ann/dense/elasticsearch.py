"""
Elasticsearch ANN backend for txtai embeddings.

This module provides an Elasticsearch-based implementation of the ANN interface
for txtai, supporting both dense vector search and content storage.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple, Union
import numpy as np
from elasticsearch import Elasticsearch as ESClient, NotFoundError
from elasticsearch.helpers import bulk

from ..base import ANN
from ...pipeline.connectors.elasticsearch import ElasticsearchConnector

logger = logging.getLogger(__name__)

class Elasticsearch(ANN):
    """
    Elasticsearch backend for Approximate Nearest Neighbor (ANN) search.
    
    This implementation uses Elasticsearch's dense vector search capabilities
    to provide fast similarity search over large collections of vectors.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Elasticsearch ANN backend.
        
        Args:
            config: Configuration dictionary containing Elasticsearch settings
        """
        super().__init__(config)
        
        # Get Elasticsearch configuration with defaults
        es_config = config.get("elasticsearch", {})
        self.hosts = es_config.get("hosts", ["http://localhost:9200"])
        self.index_name = es_config.get("index", "txtai_embeddings")
        self.dimension = es_config.get("dimension", 768)
        self.similarity = es_config.get("similarity", "cosine").lower()
        self.batch_size = es_config.get("batch_size", 1000)
        
        # Initialize Elasticsearch connector
        self.connector = ElasticsearchConnector(
            connection_string=",".join(self.hosts) if isinstance(self.hosts, list) else self.hosts,
            verify_certs=es_config.get("verify_certs", True),
            timeout=es_config.get("timeout", 30),
            max_retries=es_config.get("max_retries", 3)
        )
        
        # Initialize Elasticsearch client
        self.client = None


    def connect(self) -> None:
        """
        Initialize connection to Elasticsearch and create the index if it doesn't exist.
        
        Raises:
            ConnectionError: If unable to connect to Elasticsearch
            ValueError: If required configuration is missing
        """
        if self.client is not None:
            return
            
        # Connect using the connector and get the client
        self.connector.connect()
        self.client = self.connector.connection
        
        # Check if index exists, create if it doesn't
        if not self.client.indices.exists(index=self.index_name):
            self._create_index()
            
        logger.info(f"Connected to Elasticsearch at {self.hosts}")
    
    def _create_index(self) -> None:
        """Create the Elasticsearch index with appropriate mappings for vector search."""
        mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "text": {"type": "text"},
                    "vector": {
                        "type": "dense_vector",
                        "dims": self.dimension,
                        "index": True,
                        "similarity": self.similarity
                    },
                    "tags": {
                        "type": "object",
                        "enabled": True
                    },
                    "metadata": {
                        "type": "object",
                        "enabled": True
                    }
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "30s"
            }
        }
        
        try:
            self.client.indices.create(index=self.index_name, body=mapping)
            logger.info(f"Created index '{self.index_name}' with vector search enabled")
        except Exception as e:
            logger.error(f"Failed to create index '{self.index_name}': {str(e)}")
            raise
    
    def index(self, documents: List[Tuple[str, np.ndarray, Dict[str, Any]]]) -> None:
        """
        Index documents with their vector embeddings.
        
        Args:
            documents: List of (id, vector, metadata) tuples to index
        """
        if not documents:
            return
            
        self.connect()
        
        def generate_actions():
            for doc_id, vector, metadata in documents:
                if not isinstance(vector, np.ndarray):
                    vector = np.array(vector, dtype=np.float32)
                    
                if vector.shape[0] != self.dimension:
                    raise ValueError(f"Vector dimension mismatch. Expected {self.dimension}, got {vector.shape[0]}")
                
                # Extract tags and text from metadata
                tags = metadata.pop('tags', {}) if isinstance(metadata, dict) else {}
                text = metadata.pop('text', '') if isinstance(metadata, dict) else ''
                
                doc = {
                    "_index": self.index_name,
                    "_id": str(doc_id),
                    "_source": {
                        "id": str(doc_id),
                        "text": text,
                        "vector": vector.tolist(),
                        "tags": tags,
                        "metadata": metadata
                    }
                }
                yield doc
        
        try:
            # Use bulk indexing for better performance
            success, failed = bulk(
                self.client,
                generate_actions(),
                index=self.index_name,
                chunk_size=self.batch_size,
                raise_on_error=False
            )
            
            if failed:
                logger.warning(f"Failed to index {len(failed)} documents")
                
            logger.info(f"Indexed {success} documents successfully")
            
            # Refresh the index to make documents searchable immediately
            self.client.indices.refresh(index=self.index_name)
            
        except Exception as e:
            logger.error(f"Error during bulk indexing: {str(e)}")
            raise
    
    def search(self, query: np.ndarray, limit: int = 10) -> List[Tuple[str, float]]:
        """
        Search for similar vectors in the index.
        
        Args:
            query: Query vector
            limit: Maximum number of results to return
            
        Returns:
            List of (id, score) tuples sorted by relevance
        """
        self.connect()
        
        if not isinstance(query, np.ndarray):
            query = np.array(query, dtype=np.float32)
            
        if query.shape[0] != self.dimension:
            raise ValueError(f"Query vector dimension mismatch. Expected {self.dimension}, got {query.shape[0]}")
        
        search_body = {
            "query": {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": f"cosineSimilarity(params.query_vector, 'vector') + 1.0",
                        "params": {"query_vector": query.tolist()}
                    }
                }
            },
            "size": limit,
            "_source": ["id", "text", "tags"]
        }
        
        try:
            response = self.client.search(
                index=self.index_name,
                body=search_body
            )
            
            results = []
            for hit in response['hits']['hits']:
                doc_id = hit['_source']['id']
                score = hit['_score']
                results.append((doc_id, score))
                
            return results
            
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            raise
    
    def delete(self, ids: List[str]) -> None:
        """
        Delete documents from the index by their IDs.
        
        Args:
            ids: List of document IDs to delete
        """
        if not ids:
            return
            
        self.connect()
        
        try:
            body = [
                {'delete': {'_index': self.index_name, '_id': doc_id}}
                for doc_id in ids
            ]
            
            response = self.client.bulk(body=body, refresh=True)
            
            if response['errors']:
                logger.warning(f"Failed to delete some documents: {response}")
                
            logger.info(f"Deleted {len(ids)} documents")
            
        except Exception as e:
            logger.error(f"Error deleting documents: {str(e)}")
            raise
    
    def count(self) -> int:
        """
        Get the total number of documents in the index.
        
        Returns:
            Number of documents in the index
        """
        self.connect()
        
        try:
            response = self.client.count(index=self.index_name)
            return response['count']
        except Exception as e:
            logger.error(f"Error getting document count: {str(e)}")
            return 0
    
    def close(self) -> None:
        """Close the Elasticsearch connection."""
        if hasattr(self, 'client') and self.client is not None:
            self.client.close()
            self.client = None
            logger.info("Closed Elasticsearch connection")
    
    def __del__(self):
        """Ensure resources are cleaned up when the object is destroyed."""
        self.close()

    def load(self, path=None):
        """No-op for Elasticsearch backend."""
        self.connect()

    def index(self, embeddings):
        """Index embeddings with associated text and tags."""
        self.connect()

        documents = []
        for i, item in enumerate(embeddings):
            try:
                doc = None

                # Dict input
                if isinstance(item, dict):
                    doc_id = str(item["id"])
                    text = item.get("text", "")
                    tags = item.get("tags", [])
                    vector = item.get("embedding")

                    if vector is None:
                        logger.warning(f"Skipping item {i}: No embedding vector provided")
                        continue

                    # Convert any vector type to list with detailed logging
                    try:
                        if hasattr(vector, 'tolist'):
                            vector = vector.tolist()
                        elif hasattr(vector, 'numpy'):  # Handle PyTorch tensors
                            vector = vector.numpy().tolist()
                        elif isinstance(vector, (list, tuple)):
                            vector = list(vector)  # Ensure it's a list
                        else:
                            logger.warning(f"Unsupported vector type {type(vector)}. Converting to list.")
                            vector = list(vector)
                            
                        # Verify the vector is a list of numbers
                        if not all(isinstance(x, (int, float)) for x in vector):
                            logger.warning(f"Vector contains non-numeric values: {vector[:5]}...")
                            
                    except Exception as e:
                        logger.error(f"Error converting vector to list: {e}")
                        continue

                    doc = {
                        "_index": self.index_name,
                        "_id": doc_id,
                        "_source": {
                            "id": doc_id,
                            "text": text,
                            "tags": tags,
                            "embedding": vector
                        }
                    }

                # Tuple input
                elif isinstance(item, tuple):
                    if len(item) == 4:  # (id, text, tags, embedding)
                        doc_id, text, tags, vector = item
                        
                        # Convert numpy.memmap or numpy.ndarray to list
                        if hasattr(vector, 'tolist'):
                            vector = vector.tolist()
                        elif hasattr(vector, 'numpy'):  # Handle PyTorch tensors
                            vector = vector.numpy().tolist()
                            
                        doc = {
                            "_index": self.index_name,
                            "_id": str(doc_id),
                            "_source": {
                                "id": str(doc_id),
                                "text": text,
                                "tags": tags,
                                "embedding": vector
                            }
                        }
                    else:
                        logger.warning(f"Skipping item {i}: Unexpected tuple length {len(item)}. Expected 4 (id, text, tags, embedding)")
                        continue
                else:
                    logger.warning(f"Skipping item {i}: Unsupported type {type(item)}")
                    continue

                if doc:
                    documents.append(doc)

            except Exception as e:
                logger.error(f"Error processing item {i}: {e}")
                continue

        if documents:
            try:
                success, _ = bulk(self.client, documents)
                logger.info(f"Successfully indexed {success} documents")
                return success > 0
            except Exception as e:
                logger.error(f"Error during bulk indexing: {e}")
                raise
        else:
            logger.warning("No valid documents to index")
            return False

    def append(self, embeddings):
        """Append embeddings to the existing index."""
        return self.index(embeddings)

    def delete(self, ids):
        """Delete documents by IDs."""
        self.connect()

        actions = [
            {"_op_type": "delete", "_index": self.index_name, "_id": str(doc_id)}
            for doc_id in ids
        ]

        if actions:
            try:
                bulk(self.client, actions)
                logger.info(f"Deleted {len(actions)} documents")
            except Exception as e:
                logger.error(f"Error deleting documents: {e}")
                raise

    def search(self, queries, limit=10):
        """Search for similar documents using vector similarity."""
        self.connect()
        results = []

        for query in queries:
            vector = query.tolist() if isinstance(query, np.ndarray) else query
            search_body = {
                "size": limit,
                "query": {
                    "script_score": {
                        "query": {"match_all": {}},
                        "script": {
                            "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                            "params": {"query_vector": vector}
                        }
                    }
                }
            }

            response = self.client.search(index=self.index_name, body=search_body)
            query_results = [(hit["_source"]["id"], hit["_score"]) for hit in response["hits"]["hits"]]
            results.append(query_results)

        return results

    def count(self):
        """Return total number of documents in the index."""
        self.connect()
        response = self.client.count(index=self.index_name)
        return response.get("count", 0)

    def save(self, path=None):
        """No-op for Elasticsearch backend."""
        pass

    def close(self):
        """Close Elasticsearch connection."""
        if hasattr(self, 'connector'):
            self.connector.disconnect()
        self.client = None
        logger.info("Elasticsearch connection closed")
