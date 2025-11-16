"""PostgreSQL connector using Airflow PostgresHook."""

import logging
from typing import Any, Optional

try:
    from airflow.hooks.postgres_hook import PostgresHook
    from sqlalchemy.engine import Engine
    AIRFLOW_AVAILABLE = True
except ImportError:
    PostgresHook = None
    Engine = None
    AIRFLOW_AVAILABLE = False

try:
    from .base import BaseConnector
except ImportError:
    # Fallback for direct testing
    from txtai.pipeline.connectors.base import BaseConnector


logger = logging.getLogger(__name__)


class PostgresConnector(BaseConnector):
    """
    PostgreSQL connector implementation using Airflow PostgresHook.
    Uses Airflow connection IDs instead of direct connection strings.
    """

    def __init__(
        self,
        conn_id: str,
        echo: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        Initialize PostgreSQL connector using Airflow connection.
        
        Args:
            conn_id: Airflow connection ID for PostgreSQL
            echo: Whether to echo SQL statements
            **kwargs: Additional connection parameters
        """
        if not AIRFLOW_AVAILABLE:
            raise ImportError("Apache Airflow is required for PostgresConnector. Install with: pip install apache-airflow")
        
        # BaseConnector expects a connection_string, but Airflow manages it internally
        super().__init__(connection_string="", **kwargs)
        self.conn_id = conn_id
        self.echo = echo
        self._hook: Optional[PostgresHook] = None

    def _get_hook(self) -> PostgresHook:
        """Get or create Airflow PostgresHook."""
        if self._hook is None:
            self._hook = PostgresHook(postgres_conn_id=self.conn_id)
        return self._hook

    def connect(self) -> Engine:
        """
        Establish connection using Airflow PostgresHook.
        
        Returns:
            SQLAlchemy engine instance
        """
        if self._engine is None:
            try:
                hook = self._get_hook()
                self._engine = hook.get_sqlalchemy_engine()

                # Test connection
                with self._engine.connect() as conn:
                    conn.execute("SELECT 1")

                logger.info(f"Airflow Postgres connection established successfully using conn_id: {self.conn_id}")

            except Exception as e:
                logger.error(f"Failed to connect via Airflow PostgresHook (conn_id: {self.conn_id}): {e}")
                raise

        return self._engine

    def disconnect(self) -> None:
        """Close the connection."""
        if self._engine is not None:
            try:
                self._engine.dispose()
                self._engine = None
                logger.info("Airflow Postgres connection closed")
            except Exception as e:
                logger.error(f"Error closing Airflow Postgres connection: {e}")
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
                conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    def get_connection(self) -> Engine:
        """
        Get SQLAlchemy engine, creating connection if needed.
        
        Returns:
            SQLAlchemy engine instance
        """
        if self._engine is None:
            self.connect()
        return self._engine

    def test_connection(self) -> bool:
        """
        Test the PostgreSQL connection.
        
        Returns:
            True if connection test succeeds, False otherwise
        """
        try:
            engine = self.get_connection()
            with engine.connect() as conn:
                result = conn.execute("SELECT version()")
                _ = result.fetchone()[0]
            logger.info(f"Airflow Postgres connection test successful (conn_id: {self.conn_id})")
            return True
        except Exception as e:
            logger.error(f"Airflow Postgres connection test failed (conn_id: {self.conn_id}): {e}")
            return False

    def get_session(self):
        """
        Get SQLAlchemy session.
        
        Returns:
            SQLAlchemy session
        """
        from sqlalchemy.orm import sessionmaker
        
        engine = self.get_connection()
        Session = sessionmaker(bind=engine)
        return Session()

    def execute_query(self, query: str, params: Optional[dict] = None):
        """
        Execute a SQL query.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Query result
        """
        engine = self.get_connection()
        with engine.connect() as conn:
            if params:
                result = conn.execute(query, params)
            else:
                result = conn.execute(query)
            return result

    def get_table_info(self, table_name: str) -> dict:
        """
        Get table information.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Table information dictionary
        """
        try:
            # Get column information
            columns_query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = :table_name
            ORDER BY ordinal_position
            """
            
            result = self.execute_query(columns_query, {"table_name": table_name})
            columns = []
            
            for row in result:
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "default": row[3]
                })
            
            # Get row count
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            count_result = self.execute_query(count_query)
            row_count = count_result.fetchone()[0]
            
            return {
                "table_name": table_name,
                "columns": columns,
                "row_count": row_count
            }
            
        except Exception as e:
            logger.error(f"Error getting table info for {table_name}: {e}")
            return {}
