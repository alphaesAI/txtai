"""
Factory pattern for creating transformers.
"""
from typing import Dict
from enum import Enum
import logging

from .base import BaseTransformer
from .json_transformer import JSONTransformer


logger = logging.getLogger(__name__)


class TransformerType(Enum):
    """Enumeration of supported transformer types."""
    JSON = "json"


class TransformerFactory:
    """
    Factory class for creating transformer instances.
    """
    
    _transformer_registry: Dict[TransformerType, type] = {
        TransformerType.JSON: JSONTransformer,
    }
    
    @classmethod
    def create_transformer(
        cls,
        transformer_type: str,
        **kwargs
    ) -> BaseTransformer:
        """
        Create a transformer instance based on type.
        
        Args:
            transformer_type: Type of transformer to create
            **kwargs: Additional transformer parameters
            
        Returns:
            Transformer instance
            
        Raises:
            ValueError: If transformer type is not supported
        """
        try:
            # Normalize transformer type
            trans_type = TransformerType(transformer_type.lower())
            
            # Get transformer class from registry
            transformer_class = cls._transformer_registry.get(trans_type)
            
            if transformer_class is None:
                raise ValueError(f"Unsupported transformer type: {transformer_type}")
            
            # Create and return transformer instance
            transformer = transformer_class(**kwargs)
            logger.info(f"Created {transformer_type} transformer")
            
            return transformer
            
        except ValueError as e:
            logger.error(f"Invalid transformer type: {e}")
            raise
        except Exception as e:
            logger.error(f"Error creating transformer: {e}")
            raise
    
    @classmethod
    def register_transformer(
        cls,
        transformer_type: TransformerType,
        transformer_class: type
    ) -> None:
        """
        Register a new transformer type.
        
        Args:
            transformer_type: Transformer type enum
            transformer_class: Transformer class to register
        """
        if not issubclass(transformer_class, BaseTransformer):
            raise ValueError("Transformer class must inherit from BaseTransformer")
        
        cls._transformer_registry[transformer_type] = transformer_class
        logger.info(f"Registered transformer type: {transformer_type.value}")
