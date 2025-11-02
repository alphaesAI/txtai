from typing import Any, Optional, Dict

from .base import BaseConnector
from .postgres import PostgresConnector
from .elasticsearch import ElasticsearchConnector

class ConnectorFactory:
    """Factory class for creating database connectors"""
    
    @staticmethod
    def create_connector(connector_type: str, connection_params: Dict[str, Any]) -> BaseConnector:
        """
        Create and return a specific connector instance
        
        Args:
            connector_type (str): Type of connector ('postgres' or 'elasticsearch')
            connection_params (dict): Connection parameters
            
        Returns:
            BaseConnector: Instance of the specified connector
        """
        connectors = {
            'postgres': PostgresConnector,
            'elasticsearch': ElasticsearchConnector
        }
        
        if connector_type not in connectors:
            raise ValueError(f"Unsupported connector type: {connector_type}")
        
        return connectors[connector_type](connection_params)
