from typing import Dict, Any, Union
from .postgres import PostgresConnector
from .elasticsearch import ElasticsearchConnector


class ConnectorFactory:
    """Factory to create connectors without hardcoding."""

    @staticmethod
    def create_postgres(db_url: str) -> PostgresConnector:
        connector = PostgresConnector({"db_url": db_url})
        connector.connect()
        return connector

    @staticmethod
    def create_elasticsearch(hosts: Any) -> ElasticsearchConnector:
        connector = ElasticsearchConnector({"hosts": hosts})
        connector.connect()
        return connector
    
    @staticmethod
    def create_connector(connector_type: str, params: Dict[str, Any]) -> Union[PostgresConnector, ElasticsearchConnector]:
        """
        Unified factory method to create any connector by type.
        
        Args:
            connector_type: Type of connector ('postgres', 'elasticsearch')
            params: Connection parameters dictionary
            
        Returns:
            Connector instance
            
        Raises:
            ValueError: If connector type is not supported
        """
        connector_type = connector_type.lower()
        
        if connector_type in ['postgres', 'postgresql']:
            # For PostgreSQL, params should contain connection details
            connector = PostgresConnector(params)
            return connector
        elif connector_type in ['elasticsearch', 'es']:
            connector = ElasticsearchConnector(params)
            return connector
        else:
            raise ValueError(f"Unsupported connector type: {connector_type}")
