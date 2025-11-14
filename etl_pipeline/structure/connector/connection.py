"""
Connection manager implementing Singleton pattern to manage connection pooling.
"""
from typing import Dict, Any, Optional
import threading


class ConnectionManager:
    """
    Singleton connection manager to maintain connection pools.
    Ensures only one instance manages all connections.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """
        Singleton pattern implementation using thread-safe approach.
        
        Ensures only one instance manages all connections.
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ConnectionManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """
        Initialize the connection manager.
        """
        if self._initialized:
            return
        
        self._connections: Dict[str, Any] = {}
        self._connection_configs: Dict[str, Dict] = {}
        self._initialized = True
    
    def register_connection(
        self,
        connection_id: str,
        connector: Any,
        config: Optional[Dict] = None
    ) -> None:
        """
        Register a new connection.
        
        Args:
            connection_id: Unique identifier for the connection
            connector: Connector instance
            config: Configuration dictionary
        """
        with self._lock:
            self._connections[connection_id] = connector
            if config:
                self._connection_configs[connection_id] = config
    
    def get_connection(self, connection_id: str) -> Optional[Any]:
        """
        Retrieve a registered connection.
        
        Args:
            connection_id: Unique identifier for the connection
            
        Returns:
            Connector instance or None
        """
        return self._connections.get(connection_id)
    
    def remove_connection(self, connection_id: str) -> None:
        """
        Remove and disconnect a connection.
        
        Args:
            connection_id: Unique identifier for the connection
        """
        with self._lock:
            if connection_id in self._connections:
                connector = self._connections[connection_id]
                if hasattr(connector, 'disconnect'):
                    connector.disconnect()
                del self._connections[connection_id]
                
            if connection_id in self._connection_configs:
                del self._connection_configs[connection_id]
    
    def close_all(self) -> None:
        """
        Close all registered connections.
        """
        with self._lock:
            for connection_id in list(self._connections.keys()):
                self.remove_connection(connection_id)
    
    def list_connections(self) -> list:
        """
        List all registered connection IDs.
        
        Returns:
            List of connection IDs
        """
        return list(self._connections.keys())
