"""
Elasticsearch database implementation
"""
import numpy as np
import json
from typing import List, Optional, Tuple, Any

from elasticsearch import Elasticsearch as ESClient
from elasticsearch.helpers import bulk

from .base import Database


class Elasticsearch(Database):
    """
    Elasticsearch database for storing document content and metadata.
    """

    def __init__(self, config):
        """
        Initialize Elasticsearch database.

        Args:
            config: database configuration
        """
        super().__init__(config)

        # Elasticsearch configuration
        self.hosts = config.get("hosts")
        if not self.hosts:
            raise ValueError("Elasticsearch 'hosts' configuration is required")
        # Use content_index if available, otherwise fall back to index or default
        self.index_name = config.get("content_index", config.get("index", "documents"))
        self.verify_certs = config.get("verify_certs", True)
        self.timeout = config.get("timeout", 30)
        self.max_retries = config.get("max_retries", 3)
        
        # Initialize client
        self.client = None

    def connect(self):
        """Initialize Elasticsearch connection and ensure index exists."""
        if not self.client:
            self.client = ESClient(
                hosts=self.hosts,
                verify_certs=self.verify_certs,
                request_timeout=self.timeout,
                max_retries=self.max_retries
            )
            # Create index with stable mapping if needed
            self._create_index()

    def _create_index(self):
        """Create the index with a stable mapping if it doesn't exist."""
        if not self.client.indices.exists(index=self.index_name):
            mapping = {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "text": {"type": "text"},
                        "metadata": {"type": "object", "dynamic": True}
                    }
                }
            }
            self.client.indices.create(index=self.index_name, body=mapping)
    
    def insert(self, documents, index=0):
        """
        Insert documents into the database.

        Args:
            documents: list of documents to save
            index: indexid offset, used for internal ids
        """
        self.connect()
        
        # Prepare bulk documents
        bulk_docs = []
        for doc_id, text, metadata in documents:
            # Ensure metadata is a dictionary to avoid mapping conflicts
            if metadata is None:
                metadata = {}
            elif not isinstance(metadata, dict):
                metadata = {"value": metadata}

            # Handle nested text structure
            if isinstance(text, dict) and "text" in text:
                doc_text = text["text"]
                tags = text.get("tags", {})
            else:
                doc_text = text
                tags = {}

            doc = {
                "_index": self.index_name,
                "_id": str(doc_id),
                "_source": {
                    "id": str(doc_id),
                    "text": doc_text,
                    "tags": tags,
                    "metadata": metadata
                }
            }
            bulk_docs.append(doc)
        
        # Debug: Print the first document being indexed
        if bulk_docs:
            print("First document being indexed:", json.dumps(bulk_docs[0], indent=2))
        
        # Bulk insert with error handling
        if bulk_docs:
            try:
                from elasticsearch.helpers import bulk, BulkIndexError
                # Use the bulk helper function directly
                success, errors = bulk(
                    self.client,
                    bulk_docs,
                    raise_on_error=False,
                    stats_only=False
                )
                
                if errors:
                    print(f"Bulk insert completed with {len(errors)} errors.")
                    for error in errors[:5]:  # Print first 5 errors to avoid flooding logs
                        print("Error:", error)
                    raise BulkIndexError(f"{len(errors)} document(s) failed to index.", errors)
                else:
                    print(f"Successfully indexed {success} documents.")
                    
            except Exception as e:
                print(f"Error during bulk insert: {e}")
                raise

    def delete(self, ids: List[int]) -> None:
        """
        Delete documents by ids.

        Args:
            ids: list of ids to delete
        """
        self.connect()
        
        # Prepare bulk delete
        bulk_docs = []
        for doc_id in ids:
            bulk_docs.append({
                "_op_type": "delete",
                "_index": self.index_name,
                "_id": str(doc_id)
            })
        
        # Bulk delete
        if bulk_docs:
            bulk(self.client, bulk_docs)

    def update(self, documents: List[Tuple[int, str, dict]]) -> None:
        """
        Update documents by ids.

        Args:
            documents: list of (id, text, metadata) tuples
        """
        self.connect()
        
        # Prepare bulk update
        bulk_docs = []
        for doc_id, text, metadata in documents:
            bulk_docs.append({
                "_op_type": "update",
                "_index": self.index_name,
                "_id": str(doc_id),
                "_source": {
                    "doc": {
                        "text": text,
                        "metadata": metadata or {}
                    }
                }
            })
        
        # Bulk update
        if bulk_docs:
            bulk(self.client, bulk_docs)

    def get(self, ids: List[int]) -> List[Tuple[str, str, dict]]:
        """
        Get documents by ids.

        Args:
            ids: list of ids to retrieve

        Returns:
            list of (id, text, metadata) tuples
        """
        self.connect()
        
        results = []
        for doc_id in ids:
            try:
                response = self.client.get(index=self.index_name, id=str(doc_id))
                source = response["_source"]
                results.append((
                    source["id"],
                    source["text"],
                    source.get("metadata", {})
                ))
            except:
                # Document not found
                results.append((str(doc_id), "", {}))
        
        return results

    def search(self, query, limit=10, weights=None, index=None, parameters=None):
        """
        Search the database.
        
        Args:
            query: query string or vector
            limit: maximum results to return
            weights: optional weights for result scoring
            index: index name to search in
            parameters: additional search parameters
            
        Returns:
            list of (id, score) tuples
        """
        self.connect()
        
        # Determine if this is a vector or text search
        is_vector = isinstance(query, (list, np.ndarray, np.generic))
        index_name = index or self.index_name
        
        try:
            if is_vector:
                # Vector similarity search
                script_query = {
                    "script_score": {
                        "query": {"match_all": {}},
                        "script": {
                            "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                            "params": {"query_vector": query}
                        }
                    }
                }
                
                response = self.client.search(
                    index=index_name,
                    body={
                        "size": int(limit),  # Ensure limit is an integer
                        "query": script_query,
                        "_source": False
                    }
                )
                
                # Extract results
                hits = response.get('hits', {}).get('hits', [])
                return [(hit['_id'], hit['_score']) for hit in hits]
                
            else:
                # Text search - fixed query structure
                response = self.client.search(
                    index=index_name,
                    body={
                        "query": {
                            "multi_match": {
                                "query": query,
                                "fields": ["text", "tags^2"],
                                "type": "best_fields"
                            }
                        },
                        "size": int(limit),  # Moved size to the root level
                        "_source": False
                    }
                )
                
                # Extract results
                hits = response.get('hits', {}).get('hits', [])
                return [(hit['_id'], hit['_score']) for hit in hits]
                
        except Exception as e:
            print(f"Error during search: {e}")
            return []

    def count(self) -> int:
        """
        Get total number of documents.

        Returns:
            total document count
        """
        self.connect()
        
        response = self.client.count(index=self.index_name)
        return response["count"]

    def ids(self, ids=None):
        """
        Returns ids in the database.
        
        Args:
            ids: list of ids to check for existence
            
        Returns:
            list of ids that exist in the database
        """
        self.connect()
        
        if ids:
            # Filter ids to only those that exist
            response = self.client.search(
                index=self.index_name,
                body={
                    "query": {
                        "ids": {
                            "values": [str(id) for id in ids]
                        }
                    },
                    "_source": False,
                    "size": len(ids)
                }
            )
            return [hit["_id"] for hit in response["hits"]["hits"]]
        else:
            # Return all ids
            response = self.client.search(
                index=self.index_name,
                body={
                    "query": {
                        "match_all": {}
                    },
                    "_source": False,
                    "size": 10000  # Adjust based on expected dataset size
                }
            )
            return [hit["_id"] for hit in response["hits"]["hits"]]

    def close(self):
        """Close Elasticsearch connection."""
        if self.client:
            self.client.close()
            self.client = None
