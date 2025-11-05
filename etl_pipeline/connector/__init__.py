"""
Connector module for database and service connections.
Provides factory pattern for creating connectors.
"""
from .base import BaseConnector
from .connection import ConnectionManager
from .postgres import PostgresConnector
from .elasticsearch import ElasticsearchConnector
from .factory import ConnectorFactory, ConnectorType


__all__ = [
    'BaseConnector',
    'ConnectionManager',
    'PostgresConnector',
    'ElasticsearchConnector',
    'ConnectorFactory',
    'ConnectorType',
]
