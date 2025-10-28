from abc import ABC, abstractmethod
from typing import Any, Optional

class IConnector(ABC):
    """
    Interface for connecting elasticsearch
    """

    @abstractmethod
    def connect(self) -> None:
        """
        Establishes the connection to the external service.
        Must raise a connection error if the connection fails.
        """
        pass

    @abstractmethod
    def get_client(self) -> Optional[Any]:
        """
        Returns the initialized client object for the external service.
        """
        pass

    @abstractmethod
    def check_connection(self) -> bool:
        """
        Verifies that the connection is active
        """
        pass