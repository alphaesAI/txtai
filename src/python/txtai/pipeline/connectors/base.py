from abc import ABC, abstractmethod

class BaseConnector(ABC):
    """Abstract base connector class"""
    
    def __init__(self):
        self._connection = None
        self._connection_params = {}
        
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the database"""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection"""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the connection is valid"""
        pass
    
    @property
    def connection(self):
        """Get the database connection"""
        if not self._connection:
            self.connect()
        return self._connection