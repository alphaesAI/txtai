from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

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
        """Validate required connection parameters or db_url"""
        # If user passed a SQLAlchemy-style connection URL, skip the detailed check
        if "db_url" in self._connection_params:
            return

        required_params = ['host', 'port', 'database', 'user', 'password']
        missing_params = [
            param for param in required_params 
            if param not in self._connection_params
        ]
        
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
    
 
    def connect(self):
        """Initialize SQLAlchemy engine"""
        try:
            conn_str = (
                f"postgresql+psycopg2://{self.connection_params['user']}:"
                f"{self.connection_params['password']}@"
                f"{self.connection_params['host']}:{self.connection_params['port']}/"
                f"{self.connection_params['database']}"
            )
            self.engine = create_engine(conn_str, pool_pre_ping=True)
            self.session_factory = sessionmaker(bind=self.engine)
            self.connection = self.engine
            print("Postgres connection established (not yet tested)")
        except Exception as e:
            logging.error(f"Error initializing PostgreSQL connection: {e}")
            raise

    def test_connection(self):
        """Test if the connection works"""
        if not self.engine:
            raise RuntimeError("Engine not initialized. Call connect() first.")
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Postgres test connection successful")
            return True
        except Exception as e:
            logging.error(f"PostgreSQL connection test failed: {e}")
            return False

    def disconnect(self):
        """Close SQLAlchemy engine"""
        if self.engine:
            self.engine.dispose()
            print("Postgres disconnected")