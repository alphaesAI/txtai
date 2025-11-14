"""
Factory pattern for creating extractors.
"""
from typing import Any, Dict
from enum import Enum
import logging

from .base import BaseExtractor
from .postgres_extractor import PostgresExtractor


logger = logging.getLogger(__name__)


class ExtractorType(Enum):
    """Enumeration of supported extractor types."""
    POSTGRES = "postgres"
    POSTGRESQL = "postgresql"


class ExtractorFactory:
    """
    Factory class for creating extractor instances.
    """
    
    _extractor_registry: Dict[ExtractorType, type] = {
        ExtractorType.POSTGRES: PostgresExtractor,
        ExtractorType.POSTGRESQL: PostgresExtractor,
    }
    
    @classmethod
    def create_extractor(
        cls,
        extractor_type: str,
        connector: Any,
        **kwargs
    ) -> BaseExtractor:
        """
        Create an extractor instance based on type.
        
        Args:
            extractor_type: Type of extractor to create
            connector: Database connector instance
            **kwargs: Additional extractor parameters
            
        Returns:
            Extractor instance
            
        Raises:
            ValueError: If extractor type is not supported
        """
        try:
            # Normalize extractor type
            ext_type = ExtractorType(extractor_type.lower())
            
            # Get extractor class from registry
            extractor_class = cls._extractor_registry.get(ext_type)
            
            if extractor_class is None:
                raise ValueError(f"Unsupported extractor type: {extractor_type}")
            
            # Create and return extractor instance
            extractor = extractor_class(connector, **kwargs)
            logger.info(f"Created {extractor_type} extractor")
            
            return extractor
            
        except ValueError as e:
            logger.error(f"Invalid extractor type: {e}")
            raise
        except Exception as e:
            logger.error(f"Error creating extractor: {e}")
            raise
    
    @classmethod
    def register_extractor(
        cls,
        extractor_type: ExtractorType,
        extractor_class: type
    ) -> None:
        """
        Register a new extractor type.
        
        Args:
            extractor_type: Extractor type enum
            extractor_class: Extractor class to register
        """
        if not issubclass(extractor_class, BaseExtractor):
            raise ValueError("Extractor class must inherit from BaseExtractor")
        
        cls._extractor_registry[extractor_type] = extractor_class
        logger.info(f"Registered extractor type: {extractor_type.value}")
