"""
Elasticsearch connector implementation.
"""
from typing import Any, Dict, List, Optional
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import logging

from .base import BaseConnector


logger = logging.getLogger(__name__)


class ElasticsearchConnector(BaseConnector):
    """
    Elasticsearch connector for indexing and searching documents.
    """
    
    def __init__(
        self,
        connection_string: str,
        use_ssl: bool = True,
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
            use_ssl: Use SSL for connection
            verify_certs: Verify SSL certificates
            timeout: Connection timeout in seconds
            max_retries: Maximum number of retries
            retry_on_timeout: Retry on timeout errors
            **kwargs: Additional connection parameters (api_key, basic_auth, etc.)
        """
        super().__init__(connection_string, **kwargs)
        self.use_ssl = use_ssl
        self.verify_certs = verify_certs
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_on_timeout = retry_on_timeout
    
    def connect(self) -> Elasticsearch:
        """
        Establish connection to Elasticsearch cluster.
        
        Returns:
            Elasticsearch client instance
        """
        if self._connection is None:
            try:
                # Parse connection string
                hosts = self.connection_string.split(',')
                
                # Prepare connection parameters
                conn_params = {
                    'hosts': hosts,
                    'timeout': self.timeout,
                    'max_retries': self.max_retries,
                    'retry_on_timeout': self.retry_on_timeout,
                }
                
                # Add SSL settings
                if self.use_ssl:
                    conn_params['use_ssl'] = self.use_ssl
                    conn_params['verify_certs'] = self.verify_certs
                
                # Add additional parameters (api_key, basic_auth, etc.)
                conn_params.update(self.connection_params)
                
                self._connection = Elasticsearch(**conn_params)
                
                # Test connection
                if not self._connection.ping():
                    raise ConnectionError("Failed to ping Elasticsearch cluster")
                
                logger.info("Elasticsearch connection established successfully")
                
            except Exception as e:
                logger.error(f"Failed to connect to Elasticsearch: {e}")
                raise
        
        return self._connection
    
    def disconnect(self) -> None:
        """
        Close the Elasticsearch connection.
        """
        if self._connection is not None:
            try:
                self._connection.close()
                self._connection = None
                logger.info("Elasticsearch connection closed")
            except Exception as e:
                logger.error(f"Error closing Elasticsearch connection: {e}")
                raise
    
    def is_connected(self) -> bool:
        """
        Check if connection is active.
        
        Returns:
            True if connected, False otherwise
        """
        if self._connection is None:
            return False
        
        try:
            return self._connection.ping()
        except Exception:
            return False
    
    def get_connection(self) -> Elasticsearch:
        """
        Get the active Elasticsearch client.
        
        Returns:
            Elasticsearch client instance
        """
        if self._connection is None:
            self.connect()
        return self._connection
    
    def test_connection(self) -> bool:
        """
        Test if the Elasticsearch connection is valid.
        
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            client = self.get_connection()
            info = client.info()
            logger.info(f"Elasticsearch connection test successful: {info['version']['number']}")
            return True
        except Exception as e:
            logger.error(f"Elasticsearch connection test failed: {e}")
            return False
    
    def create_index(
        self,
        index_name: str,
        mappings: Optional[Dict] = None,
        settings: Optional[Dict] = None
    ) -> bool:
        """
        Create an Elasticsearch index.
        
        Args:
            index_name: Name of the index
            mappings: Index mappings
            settings: Index settings
            
        Returns:
            True if successful, False otherwise
        """
        try:
            client = self.get_connection()
            
            if client.indices.exists(index=index_name):
                logger.info(f"Index {index_name} already exists")
                return True
            
            body = {}
            if mappings:
                body['mappings'] = mappings
            if settings:
                body['settings'] = settings
            
            client.indices.create(index=index_name, body=body)
            logger.info(f"Index {index_name} created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating index {index_name}: {e}")
            return False
    
    def delete_index(self, index_name: str) -> bool:
        """
        Delete an Elasticsearch index.
        
        Args:
            index_name: Name of the index
            
        Returns:
            True if successful, False otherwise
        """
        try:
            client = self.get_connection()
            
            if not client.indices.exists(index=index_name):
                logger.info(f"Index {index_name} does not exist")
                return True
            
            client.indices.delete(index=index_name)
            logger.info(f"Index {index_name} deleted successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting index {index_name}: {e}")
            return False
    
    def index_exists(self, index_name: str) -> bool:
        """
        Check if an index exists.
        
        Args:
            index_name: Name of the index
            
        Returns:
            True if exists, False otherwise
        """
        try:
            client = self.get_connection()
            return client.indices.exists(index=index_name)
        except Exception as e:
            logger.error(f"Error checking index existence: {e}")
            return False
