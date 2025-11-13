from typing import Any, Optional
import logging

from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy.engine import Engine

from ..base import BaseConnector


logger = logging.getLogger(__name__)


class AirflowPostgresConnector(BaseConnector):
    """PostgreSQL connector implementation using Airflow PostgresHook."""

    def __init__(
        self,
        conn_id: str,
        echo: bool = False,
        **kwargs: Any,
    ) -> None:
        # BaseConnector expects a connection_string, but Airflow manages it internally.
        super().__init__(connection_string="", **kwargs)
        self.conn_id = conn_id
        self.echo = echo
        self._hook: Optional[PostgresHook] = None

    def _get_hook(self) -> PostgresHook:
        if self._hook is None:
            self._hook = PostgresHook(postgres_conn_id=self.conn_id)
        return self._hook

    def connect(self) -> Engine:
        if self._engine is None:
            try:
                hook = self._get_hook()
                self._engine = hook.get_sqlalchemy_engine()

                # Test connection
                with self._engine.connect() as conn:
                    conn.execute("SELECT 1")

                logger.info("Airflow Postgres connection established successfully")

            except Exception as e:
                logger.error(f"Failed to connect via Airflow PostgresHook: {e}")
                raise

        return self._engine

    def disconnect(self) -> None:
        if self._engine is not None:
            try:
                self._engine.dispose()
                self._engine = None
                logger.info("Airflow Postgres connection closed")
            except Exception as e:
                logger.error(f"Error closing Airflow Postgres connection: {e}")
                raise

    def is_connected(self) -> bool:
        if self._engine is None:
            return False

        try:
            with self._engine.connect() as conn:
                conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    def get_connection(self) -> Engine:
        if self._engine is None:
            self.connect()
        return self._engine

    def test_connection(self) -> bool:
        try:
            engine = self.get_connection()
            with engine.connect() as conn:
                result = conn.execute("SELECT version()")
                _ = result.fetchone()[0]
            logger.info("Airflow Postgres connection test successful")
            return True
        except Exception as e:
            logger.error(f"Airflow Postgres connection test failed: {e}")
            return False
