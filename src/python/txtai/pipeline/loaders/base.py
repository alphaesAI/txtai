from abc import ABC, abstractmethod
from typing import Any, Dict, List
from elasticsearch import Elasticsearch

class BaseElasticsearchLoader(ABC):
    """Abstract base class for Elasticsearch loading strategies"""
    def __init__(self, es_client: Elasticsearch):
        self.es_client = es_client

    @abstractmethod
    def load(self, data: List[Dict[str, Any]]) -> None:
        pass