"""
Elasticsearch database implementation
"""

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
        """Initialize Elasticsearch connection."""
        if not self.client:
            self.client = ESClient(
                hosts=self.hosts,
                verify_certs=self.verify_certs,
                request_timeout=self.timeout,
                max_retries=self.max_retries
            )

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
            doc = {
                "_index": self.index_name,
                "_id": str(doc_id),
                "_source": {
                    "id": str(doc_id),
                    "text": text,
                    "metadata": metadata or {}
                }
            }
            bulk_docs.append(doc)
        
        # Bulk insert
        if bulk_docs:
            bulk(self.client, bulk_docs)

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

    def search(self, query: str, limit: int = 10) -> List[Tuple[int, float]]:
        """
        Search documents by text.

        Args:
            query: search query
            limit: maximum results

        Returns:
            list of (id, score) tuples
        """
        self.connect()
        
        response = self.client.search(
            index=self.index_name,
            body={
                "query": {
                    "match": {
                        "text": query
                    }
                },
                "size": limit
            }
        )
        
        results = []
        for hit in response["hits"]["hits"]:
            results.append((int(hit["_source"]["id"]), hit["_score"]))
        
        return results

    def count(self) -> int:
        """
        Get total number of documents.

        Returns:
            total document count
        """
        self.connect()
        
        response = self.client.count(index=self.index_name)
        return response["count"]

    def close(self):
        """Close Elasticsearch connection."""
        if self.client:
            self.client.close()
            self.client = None
