import logging

from typing import Any, Optional, Dict
from elasticsearch import Elasticsearch

from .base import BaseConnector

class ElasticsearchConnector(BaseConnector):
    """Elasticsearch specific connector implementation"""
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize Elasticsearch connector
        
        Args:
            connection_params (dict): Dictionary containing:
                - hosts: list of host URLs
                - username: username (optional)
                - password: password (optional)
                - timeout: connection timeout in seconds (optional)
                - use_ssl: boolean to enable SSL (optional)
                - verify_certs: boolean to verify certificates (optional)
                - ca_certs: path to CA certificates (optional)
        """
        super().__init__()
        self._connection_params = connection_params
        self._validate_params()
    
    def _validate_params(self) -> None:
        """Validate required connection parameters"""
        if 'hosts' not in self._connection_params:
            raise ValueError("Missing required parameter: hosts")
    
    def connect(self) -> None:
        """Establish Elasticsearch connection"""
        try:
            if not self._connection:
                self._connection = Elasticsearch(**self._connection_params)
                if not self._connection.ping():
                    raise ConnectionError("Could not ping Elasticsearch cluster")
                logging.info("Successfully connected to Elasticsearch")
        except Exception as e:
            logging.error(f"Failed to connect to Elasticsearch: {str(e)}")
            raise ConnectionError(f"Elasticsearch connection failed: {str(e)}")
    
    def disconnect(self) -> None:
        """Close Elasticsearch connection"""
        try:
            if self._connection:
                self._connection.close()
                self._connection = None
                logging.info("Successfully disconnected from Elasticsearch")
        except Exception as e:
            logging.error(f"Error disconnecting from Elasticsearch: {str(e)}")
            raise
    
    def test_connection(self) -> bool:
        """Test Elasticsearch connection"""
        try:
            return bool(self.connection.ping())
        except Exception as e:
            logging.error(f"Elasticsearch connection test failed: {str(e)}")
            return False