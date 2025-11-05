from typing import Dict, Any, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
import logging

from .base import BaseConnector


class PostgresConnector(BaseConnector):
    """PostgreSQL connector using SQLAlchemy."""

    def __init__(self, connection_params: Dict[str, Any]):
        """
        connection_params can be:
            - db_url (e.g., postgresql+psycopg2://user:pass@host:port/database)
            OR
            - host, port, database, user, password (individual parameters)
        """
        if "db_url" in connection_params:
            self.db_url = connection_params["db_url"]
        else:
            # Build connection string from individual parameters
            host = connection_params.get("host", "localhost")
            port = connection_params.get("port", 5432)
            database = connection_params.get("database")
            user = connection_params.get("user")
            password = connection_params.get("password")
            
            if not all([database, user, password]):
                raise ValueError("Missing required params: database, user, and password")
            
            self.db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        
        self.engine = None
        self.session_factory: Optional[sessionmaker] = None

    def connect(self):
        try:
            self.engine = create_engine(self.db_url, pool_pre_ping=True)
            self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)
            logging.info("Postgres: Engine initialized.")
        except Exception as e:
            logging.error(f"Postgres connection init failed: {e}")
            raise

    def get_session(self) -> Session:
        if not self.session_factory:
            raise RuntimeError("Postgres: connect() must be called first.")
        return self.session_factory()

    def test_connection(self) -> bool:
        if not self.engine:
            raise RuntimeError("Postgres: connect() must be called first.")
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logging.info("Postgres connection test successful.")
            return True
        except Exception as e:
            logging.error(f"Postgres connection test failed: {e}")
            return False

    def disconnect(self):
        if self.engine:
            self.engine.dispose()
            logging.info("Postgres connection closed.")
    
    @property
    def connection(self):
        """
        Returns the SQLAlchemy engine for compatibility with existing code.
        """
        if not self.engine:
            raise RuntimeError("Postgres: connect() must be called first.")
        return self.engine
