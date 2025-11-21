"""Elasticsearch connector for ETL pipeline."""

from typing import Any, Dict, List, Optional
from elasticsearch import Elasticsearch as ESClient
from elasticsearch.helpers import bulk
import logging

from .base import BaseConnector


class ElasticsearchConnector(BaseConnector):
    """
    Elasticsearch connector for indexing and searching documents.
    """
    
    def __init__(
        self,
        connection_string: str,
        verify_certs: bool = True,
        timeout: int = 30,
        max_retries: int = 3,
        retry_on_timeout: bool = True,
        **kwargs
    ):
        """
        Initialize Elasticsearch connector.
        
        Args:
            connection_string: Elasticsearch connection URL(s)
            verify_certs: Verify SSL certificates
            timeout: Connection timeout in seconds
            max_retries: Maximum number of retries
            retry_on_timeout: Retry on timeout errors
            **kwargs: Additional connection parameters (api_key, basic_auth, etc.)
        """
        super().__init__(connection_string, **kwargs)
        self.verify_certs = verify_certs
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_on_timeout = retry_on_timeout
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> None:
        """Establish connection to Elasticsearch."""
        try:
            # Parse connection string for multiple hosts
            hosts = self.connection_string.split(",") if "," in self.connection_string else [self.connection_string]
            
            # Create Elasticsearch client with modern parameters
            self._connection = ESClient(
                hosts=hosts,
                verify_certs=self.verify_certs,
                request_timeout=self.timeout,
                max_retries=self.max_retries,
                **self.connection_params
            )
            
            # Test connection
            if self.test_connection():
                self.logger.info("Elasticsearch connection established successfully")
            else:
                raise ConnectionError("Failed to connect to Elasticsearch")
                
        except Exception as e:
            self.logger.error(f"Failed to connect to Elasticsearch: {e}")
            raise
    
    @property
    def connection(self):
        """Get the Elasticsearch client instance."""
        if self._connection is None:
            self.connect()
        return self._connection

    def disconnect(self) -> None:
        """Close the Elasticsearch connection."""
        if self._connection is not None:
            try:
                self._connection.close()
                self.logger.info("Closed Elasticsearch connection")
            except Exception as e:
                self.logger.error(f"Error closing Elasticsearch connection: {e}")
            finally:
                self._connection = None

    def test_connection(self) -> bool:
        """Test if Elasticsearch connection is working."""
        try:
            if self._connection is None:
                return False
            return self._connection.ping()
        except Exception as e:
            self.logger.error(f"Elasticsearch connection test failed: {e}")
            return False
    
    def index_document(self, index: str, doc_id: str, document: Dict[str, Any]) -> Dict[str, Any]:
        """Index a document.
        
        Args:
            index: Index name
            doc_id: Document ID
            document: Document to index
            
        Returns:
            Index response
        """
        if not self._connection:
            raise RuntimeError("Connector not connected. Call connect() first.")
        
        return self._connection.index(
            index=index,
            id=doc_id,
            body=document
        )
    
    def bulk_index(self, index: str, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Bulk index documents.
        
        Args:
            index: Index name
            documents: List of documents to index
            
        Returns:
            Bulk index response
        """
        if not self._connection:
            raise RuntimeError("Connector not connected. Call connect() first.")
        
        # Prepare bulk actions
        actions = []
        for doc in documents:
            actions.append({
                "_index": index,
                "_id": doc.get("id"),
                "_source": doc
            })
        
        return bulk(self._connection, actions)
    
    def search(self, index: str, query: Dict[str, Any], size: int = 10) -> Dict[str, Any]:
        """Search documents.
        
        Args:
            index: Index name
            query: Search query
            size: Number of results to return
            
        Returns:
            Search results
        """
        if not self._connection:
            raise RuntimeError("Connector not connected. Call connect() first.")
        
        return self._connection.search(
            index=index,
            body=query,
            size=size
        )
    
    def get_document(self, index: str, doc_id: str) -> Dict[str, Any]:
        """Get a document by ID.
        
        Args:
            index: Index name
            doc_id: Document ID
            
        Returns:
            Document
        """
        if not self._connection:
            raise RuntimeError("Connector not connected. Call connect() first.")
        
        return self._connection.get(
            index=index,
            id=doc_id
        )
    
    def delete_document(self, index: str, doc_id: str) -> Dict[str, Any]:
        """Delete a document.
        
        Args:
            index: Index name
            doc_id: Document ID
            
        Returns:
            Delete response
        """
        if not self._connection:
            raise RuntimeError("Connector not connected. Call connect() first.")
        
        return self._connection.delete(
            index=index,
            id=doc_id
        )
