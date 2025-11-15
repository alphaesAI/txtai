"""Factory for creating connector instances."""

from typing import Any, Dict, Type

from .base import BaseConnector


class ConnectorFactory:
    """Factory class for creating connector instances."""

    _connectors: Dict[str, Type[BaseConnector]] = {}

    @classmethod
    def register(cls, connector_type: str, connector_class: Type[BaseConnector]) -> None:
        """Register a connector type.
        
        Args:
            connector_type: String identifier for the connector.
            connector_class: Connector class to register.
        """
        cls._connectors[connector_type] = connector_class

    @classmethod
    def create(cls, connector_type: str, config: Dict[str, Any]) -> BaseConnector:
        """Create a connector instance.
        
        Args:
            connector_type: Type of connector to create.
            config: Configuration for the connector.
            
        Returns:
            Connector instance.
            
        Raises:
            ValueError: If connector type is not registered.
        """
        if connector_type not in cls._connectors:
            raise ValueError(f"Unknown connector type: {connector_type}")
        
        return cls._connectors[connector_type](config)

    @classmethod
    def get_available_connectors(cls) -> list[str]:
        """Get list of available connector types.
        
        Returns:
            List of registered connector type names.
        """
        return list(cls._connectors.keys())
