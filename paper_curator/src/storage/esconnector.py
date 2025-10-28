import logging
from typing import Optional
from elasticsearch import Elasticsearch
from .interfaces.base import IConnector

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class ESConnector(IConnector):
    """
    Concrete implementation of IConnector for Elasticsearch.
    """

    def __init__(self, host: str, port: int, user: Optional[str] = None, password: Optional[str] = None, scheme: str = "http"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.scheme = scheme
        self._client: Optional[Elasticsearch] = None

    def connect(self) -> None:
        """
        Establishes the Elasticsearch connection.
        """
        connection_url = f"{self.scheme}://{self.host}:{self.port}"
        logger.info(f"Attempting to connect to Elasticsearch at {connection_url}")

        try:
            if self.user and self.password:
                self._client = Elasticsearch(
                    connection_url,
                    basic_auth=(self.user, self.password),
                    verify_certs=False
                )
            else:
                self._client = Elasticsearch(
                    connection_url,
                    verify_certs=False
                )

            if not self.check_connection():
                raise ConnectionError("Elasticsearch ping failed after initialization.")
            
            logger.info("Elasticsearch connection established and verified.")

        except Exception as e:
            self._client = None
            logger.error(f"Failed to connect to Elasticsearch. Error: {e}")
            raise ConnectionError(f"Elasticsearch connection failed: {e}")

    def get_client(self) -> Optional[Elasticsearch]:
        """
        Returns the initialized Elasticsearch client object
        """
        if not self._client:
            raise RuntimeError("Client is not initialized. Call connect() first.")
        return self._client

    def check_connection(self) -> bool:
        """
        Pings the service to verify it is responsive.
        """
        if not self._client:
            return False
        try:
            result = self._client.ping()
            logger.info(f"Ping response: {result}")
            return result
        except Exception as e:
            logger.error(f"Ping failed: {e}")
            return False
