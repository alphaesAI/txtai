"""
Factory pattern for creating loaders.
"""
from typing import Any, Dict
from enum import Enum
import logging

from .base import BaseLoader
from .elasticsearch_loader import ElasticsearchLoader


logger = logging.getLogger(__name__)


class LoaderType(Enum):
    """Enumeration of supported loader types."""
    ELASTICSEARCH = "elasticsearch"
    ES = "es"


class LoaderFactory:
    """
    Factory class for creating loader instances.
    """
    
    _loader_registry: Dict[LoaderType, type] = {
        LoaderType.ELASTICSEARCH: ElasticsearchLoader,
        LoaderType.ES: ElasticsearchLoader,
    }
    
    @classmethod
    def create_loader(
        cls,
        loader_type: str,
        connector: Any,
        **kwargs
    ) -> BaseLoader:
        """
        Create a loader instance based on type.
        
        Args:
            loader_type: Type of loader to create
            connector: Connector instance
            **kwargs: Additional loader parameters
            
        Returns:
            Loader instance
            
        Raises:
            ValueError: If loader type is not supported
        """
        try:
            # Normalize loader type
            load_type = LoaderType(loader_type.lower())
            
            # Get loader class from registry
            loader_class = cls._loader_registry.get(load_type)
            
            if loader_class is None:
                raise ValueError(f"Unsupported loader type: {loader_type}")
            
            # Create and return loader instance
            loader = loader_class(connector, **kwargs)
            logger.info(f"Created {loader_type} loader")
            
            return loader
            
        except ValueError as e:
            logger.error(f"Invalid loader type: {e}")
            raise
        except Exception as e:
            logger.error(f"Error creating loader: {e}")
            raise
    
    @classmethod
    def register_loader(
        cls,
        loader_type: LoaderType,
        loader_class: type
    ) -> None:
        """
        Register a new loader type.
        
        Args:
            loader_type: Loader type enum
            loader_class: Loader class to register
        """
        if not issubclass(loader_class, BaseLoader):
            raise ValueError("Loader class must inherit from BaseLoader")
        
        cls._loader_registry[loader_type] = loader_class
        logger.info(f"Registered loader type: {loader_type.value}")
