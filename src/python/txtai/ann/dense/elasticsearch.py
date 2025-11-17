"""Elasticsearch ANN backend for txtai embeddings."""

import json
import numpy as np
from elasticsearch import Elasticsearch as ESClient
from elasticsearch.helpers import bulk

from ..base import ANN


class Elasticsearch(ANN):
    """
    Builds an ANN index using Elasticsearch.
    Stores vectors and performs similarity search using Elasticsearch's vector search capabilities.
    """

    def __init__(self, config):
        super().__init__(config)
        
        # Elasticsearch configuration
        self.hosts = config.get("hosts", [])
        self.use_ssl = config.get("use_ssl", False)
        self.verify_certs = config.get("verify_certs", True)
        self.timeout = config.get("timeout", 30)
        self.max_retries = config.get("max_retries", 3)
        self.index_name = config.get("index", "embeddings")
        self.dimension = config.get("dimension", 384)
        self.similarity = config.get("similarity", "cosine")
        
        # Elasticsearch client
        self.client = None
        
    def connect(self):
        """Initialize Elasticsearch connection."""
        if not self.client:
            self.client = ESClient(
                hosts=self.hosts,
                verify_certs=self.verify_certs,
                request_timeout=self.timeout,
                max_retries=self.max_retries
            )
            
            # Create index if it doesn't exist
            self._create_index()
            
    def _create_index(self):
        """Create Elasticsearch index with vector mapping."""
        if not self.client.indices.exists(index=self.index_name):
            mapping = {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "text": {"type": "text"},
                        "embedding": {
                            "type": "dense_vector",
                            "dims": self.dimension,
                            "index": True,
                            "similarity": self.similarity
                        },
                        "metadata": {"type": "object", "dynamic": True}
                    }
                }
            }
            
            self.client.indices.create(index=self.index_name, body=mapping)
            
    def load(self, path):
        """Load is not applicable for Elasticsearch backend."""
        self.connect()
        
    def index(self, embeddings):
        """Builds an ANN index in Elasticsearch.
        
        Args:
            embeddings: numpy array of embedding vectors
        """
        self.connect()
        
        # Check if we have access to the embeddings database to get document info
        # For now, we'll store just the vectors with array indices as IDs
        # The document content and ID mapping is handled by the database system
        documents = []
        for i, embedding in enumerate(embeddings):
            doc = {
                "_index": self.index_name,
                "_id": str(i),
                "_source": {
                    "id": str(i),
                    "embedding": embedding.tolist() if isinstance(embedding, np.ndarray) else embedding
                }
            }
            documents.append(doc)
            
        # Bulk index documents
        if documents:
            bulk(self.client, documents)
            
    def append(self, embeddings):
        """Append documents to existing index."""
        self.index(embeddings)
        
    def delete(self, ids):
        """Delete documents from index."""
        self.connect()
        
        # Prepare bulk delete
        actions = []
        for doc_id in ids:
            action = {
                "_op_type": "delete",
                "_index": self.index_name,
                "_id": str(doc_id)
            }
            actions.append(action)
            
        if actions:
            bulk(self.client, actions)
            
    def search(self, queries, limit):
        """Search for similar documents.
        
        Args:
            queries: query embeddings
            limit: maximum number of results
            
        Returns:
            list of (id, score) tuples
        """
        self.connect()
        
        results = []
        for query in queries:
            search_body = {
                "size": limit,
                "query": {
                    "script_score": {
                        "query": {"match_all": {}},
                        "script": {
                            "source": f"cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                            "params": {"query_vector": query.tolist() if isinstance(query, np.ndarray) else query}
                        }
                    }
                }
            }
            
            response = self.client.search(index=self.index_name, body=search_body)
            
            query_results = []
            for hit in response["hits"]["hits"]:
                query_results.append((hit["_source"]["id"], hit["_score"]))
            
            results.append(query_results)
            
        return results
        
    def count(self):
        """Get total number of documents in index."""
        self.connect()
        response = self.client.count(index=self.index_name)
        return response["count"]
        
    def save(self, path):
        """Save is not applicable for Elasticsearch backend."""
        pass
        
    def close(self):
        """Close Elasticsearch connection."""
        if self.client:
            self.client.close()
            self.client = None