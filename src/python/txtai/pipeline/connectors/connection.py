from typing import Optional
from elasticsearch import Elasticsearch
import psycopg2
from psycopg2.extras import RealDictCursor

class DatabaseConnection:
    """Singleton pattern for database connections"""
    _postgres_instance = None
    _elasticsearch_instance = None

    @classmethod
    def get_postgres_connection(cls, connection_string: str) -> psycopg2.extensions.connection:
        """Get PostgreSQL connection using singleton pattern"""
        if cls._postgres_instance is None:
            try:
                cls._postgres_instance = psycopg2.connect(
                    connection_string,
                    cursor_factory=RealDictCursor
                )
            except Exception as e:
                raise ConnectionError(f"Failed to connect to PostgreSQL: {str(e)}")
        return cls._postgres_instance

    @classmethod
    def get_elasticsearch_connection(cls, connection_string: str) -> Elasticsearch:
        """Get Elasticsearch connection using singleton pattern"""
        if cls._elasticsearch_instance is None:
            try:
                cls._elasticsearch_instance = Elasticsearch(connection_string)
            except Exception as e:
                raise ConnectionError(f"Failed to connect to Elasticsearch: {str(e)}")
        return cls._elasticsearch_instance