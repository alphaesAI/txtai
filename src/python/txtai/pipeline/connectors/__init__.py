"""ETL Connectors for txtai pipeline system."""

from .base import BaseConnector
from .factory import ConnectorFactory
from .postgres import PostgresConnector
from .elasticsearch import ElasticsearchConnector
from .email import GmailConnector

# Auto-register connectors
ConnectorFactory.register("postgres", PostgresConnector)
ConnectorFactory.register("elasticsearch", ElasticsearchConnector)
ConnectorFactory.register("gmail", GmailConnector)

__all__ = ["BaseConnector", "ConnectorFactory", "PostgresConnector", "ElasticsearchConnector", "GmailConnector"]
