"""
Factory pattern implementation for creating connectors.
"""
from typing import Dict, Any, Optional
from enum import Enum
import logging

from .base import BaseConnector
from .postgres import PostgresConnector
from .elasticsearch import ElasticsearchConnector


logger = logging.getLogger(__name__)


class ConnectorType(Enum):
    """Enumeration of supported connector types."""
    POSTGRES = "postgres"
    #POSTGRESQL = "postgresql"
    ELASTICSEARCH = "elasticsearch"
    #ES = "es"


class ConnectorFactory:
    """
    Factory class for creating connector instances.
    Implements the Factory Method pattern.
    """
    
    _connector_registry: Dict[ConnectorType, type] = {
        ConnectorType.POSTGRES: PostgresConnector,
        ConnectorType.POSTGRESQL: PostgresConnector,
        ConnectorType.ELASTICSEARCH: ElasticsearchConnector,
        ConnectorType.ES: ElasticsearchConnector,
    }
    
    @classmethod
    def create_connector(
        cls,
        connector_type: str,
        connection_string: str,
        **kwargs
    ) -> BaseConnector:
        """
        Create a connector instance based on type.
        
        Args:
            connector_type: Type of connector to create
            connection_string: Connection string for the connector
            **kwargs: Additional connector-specific parameters
            
        Returns:
            Connector instance
            
        Raises:
            ValueError: If connector type is not supported
        """
        try:
            # Normalize connector type
            conn_type = ConnectorType(connector_type.lower())
            
            # Get connector class from registry
            connector_class = cls._connector_registry.get(conn_type)
            
            if connector_class is None:
                raise ValueError(f"Unsupported connector type: {connector_type}")
            
            # Create and return connector instance
            connector = connector_class(connection_string, **kwargs)
            logger.info(f"Created {connector_type} connector")
            
            return connector
            
        except ValueError as e:
            logger.error(f"Invalid connector type: {e}")
            raise
        except Exception as e:
            logger.error(f"Error creating connector: {e}")
            raise
    
    @classmethod
    def register_connector(
        cls,
        connector_type: ConnectorType,
        connector_class: type
    ) -> None:
        """
        Register a new connector type.
        
        Args:
            connector_type: Connector type enum
            connector_class: Connector class to register
        """
        if not issubclass(connector_class, BaseConnector):
            raise ValueError("Connector class must inherit from BaseConnector")
        
        cls._connector_registry[connector_type] = connector_class
        logger.info(f"Registered connector type: {connector_type.value}")
    
    @classmethod
    def list_supported_connectors(cls) -> list:
        """
        List all supported connector types.
        
        Returns:
            List of connector type strings
        """
        return [conn_type.value for conn_type in cls._connector_registry.keys()]
