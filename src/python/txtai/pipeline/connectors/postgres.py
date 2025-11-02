from typing import Any, Optional, Dict
import logging

import psycopg2
from psycopg2.extras import RealDictCursor
import urllib.parse

from .base import BaseConnector


class PostgresConnector(BaseConnector):
    """PostgreSQL specific connector implementation"""
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize PostgreSQL connector
        
        Args:
            connection_params (dict): Dictionary containing:
                - host: database host
                - port: database port
                - database: database name
                - user: username
                - password: password
                - sslmode: SSL mode (optional)
                - timeout: connection timeout in seconds (optional)
        """
        super().__init__()
        self._connection_params = connection_params
        self._validate_params()
        
    def _validate_params(self) -> None:
        """Validate required connection parameters"""
        required_params = ['host', 'port', 'database', 'user', 'password']
        missing_params = [param for param in required_params 
                         if param not in self._connection_params]
        
        if missing_params:
            raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")
    
    def _build_connection_string(self) -> str:
        """Build PostgreSQL connection string"""
        params = self._connection_params.copy()
        password = urllib.parse.quote_plus(params.pop('password'))
        user = urllib.parse.quote_plus(params.pop('user'))
        host = params.pop('host')
        port = params.pop('port')
        database = params.pop('database')
        
        # Build connection string with remaining optional parameters
        optional_params = '&'.join(f"{k}={v}" for k, v in params.items())
        
        return (f"postgresql://{user}:{password}@{host}:{port}/{database}"
                f"{('?' + optional_params) if optional_params else ''}")
    
    def connect(self) -> None:
        """Establish PostgreSQL connection"""
        try:
            if not self._connection or self._connection.closed:
                self._connection = psycopg2.connect(
                    self._build_connection_string(),
                    cursor_factory=RealDictCursor
                )
                logging.info("Successfully connected to PostgreSQL database")
        except Exception as e:
            logging.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise ConnectionError(f"PostgreSQL connection failed: {str(e)}")
    
    def disconnect(self) -> None:
        """Close PostgreSQL connection"""
        try:
            if self._connection and not self._connection.closed:
                self._connection.close()
                self._connection = None
                logging.info("Successfully disconnected from PostgreSQL database")
        except Exception as e:
            logging.error(f"Error disconnecting from PostgreSQL: {str(e)}")
            raise
    
    def test_connection(self) -> bool:
        """Test PostgreSQL connection"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except Exception as e:
            logging.error(f"PostgreSQL connection test failed: {str(e)}")
            return False