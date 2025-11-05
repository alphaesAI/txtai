from abc import ABC, abstractmethod
from typing import Any


class BaseConnector(ABC):
    """Base connector abstraction."""

    @abstractmethod
    def connect(self) -> Any:
        """Establish a connection or initialize a client."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close or dispose the connection."""
        pass
