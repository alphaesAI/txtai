"""
PostgreSQL connector implementation using SQLAlchemy ORM.
"""
from typing import Any, Optional
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
import logging

from .base import BaseConnector


logger = logging.getLogger(__name__)


class PostgresConnector(BaseConnector):
    """
    PostgreSQL database connector using SQLAlchemy ORM.
    Implements connection pooling and event listeners.
    """
    
    def __init__(
        self,
        connection_string: str,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 3600,
        echo: bool = False,
        **kwargs
    ):
        """
        Initialize PostgreSQL connector.
        
        Args:
            connection_string: PostgreSQL connection string
            pool_size: Number of connections to maintain in pool
            max_overflow: Maximum overflow size of connection pool
            pool_timeout: Connection timeout in seconds
            pool_recycle: Recycle connections after this many seconds
            echo: Echo SQL statements for debugging
            **kwargs: Additional connection parameters
        """
        super().__init__(connection_string, **kwargs)
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle
        self.echo = echo
        self._session_factory = None
        self._setup_event_listeners()
    
    def _setup_event_listeners(self) -> None:
        """
        Setup SQLAlchemy event listeners for connection lifecycle.
        """
        @event.listens_for(Engine, "connect")
        def receive_connect(dbapi_conn, connection_record):
            """Event listener for new connections."""
            logger.info("New connection established")
        
        @event.listens_for(Engine, "checkout")
        def receive_checkout(dbapi_conn, connection_record, connection_proxy):
            """Event listener for connection checkout from pool."""
            logger.debug("Connection checked out from pool")
        
        @event.listens_for(Engine, "checkin")
        def receive_checkin(dbapi_conn, connection_record):
            """Event listener for connection checkin to pool."""
            logger.debug("Connection returned to pool")
    
    def connect(self) -> Engine:
        """
        Establish connection to PostgreSQL database.
        
        Returns:
            SQLAlchemy Engine instance
        """
        if self._engine is None:
            try:
                self._engine = create_engine(
                    self.connection_string,
                    poolclass=QueuePool,
                    pool_size=self.pool_size,
                    max_overflow=self.max_overflow,
                    pool_timeout=self.pool_timeout,
                    pool_recycle=self.pool_recycle,
                    echo=self.echo,
                    **self.connection_params
                )
                
                # Create session factory
                self._session_factory = sessionmaker(bind=self._engine)
                
                # Test connection
                with self._engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                
                logger.info("PostgreSQL connection established successfully")
                
            except Exception as e:
                logger.error(f"Failed to connect to PostgreSQL: {e}")
                raise
        
        return self._engine
    
    def disconnect(self) -> None:
        """
        Close the PostgreSQL connection and dispose of the engine.
        """
        if self._engine is not None:
            try:
                self._engine.dispose()
                self._engine = None
                self._session_factory = None
                logger.info("PostgreSQL connection closed")
            except Exception as e:
                logger.error(f"Error closing PostgreSQL connection: {e}")
                raise
    
    def is_connected(self) -> bool:
        """
        Check if connection is active.
        
        Returns:
            True if connected, False otherwise
        """
        if self._engine is None:
            return False
        
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
    
    def get_connection(self) -> Engine:
        """
        Get the active SQLAlchemy engine.
        
        Returns:
            SQLAlchemy Engine instance
        """
        if self._engine is None:
            self.connect()
        return self._engine
    
    @property
    def connection(self):
        """
        Returns the SQLAlchemy engine for compatibility with existing code.
        """
        if not self._engine:
            raise RuntimeError("Postgres: connect() must be called first.")
        return self._engine
    
    def get_db_url(self) -> str:
        """
        Returns the database connection URL string.
        For use with pandas which prefers connection strings.
        """
        return self.connection_string
    
    def get_session(self) -> Session:
        """
        Get a new SQLAlchemy session.
        
        Returns:
            SQLAlchemy Session instance
        """
        if self._session_factory is None:
            self.connect()
        return self._session_factory()
    
    def test_connection(self) -> bool:
        """
        Test if the PostgreSQL connection is valid.
        
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            with self.get_connection().connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.info(f"PostgreSQL connection test successful: {version}")
                return True
        except Exception as e:
            logger.error(f"PostgreSQL connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[dict] = None) -> Any:
        """
        Execute a SQL query.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Query result
        """
        try:
            with self.get_connection().connect() as conn:
                result = conn.execute(text(query), params or {})
                return result
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
