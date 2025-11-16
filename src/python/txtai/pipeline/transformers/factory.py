"""Factory for creating transformer instances."""

from typing import Any, Dict, Type

from .base import BaseTransformer


class TransformerFactory:
    """Factory class for creating transformer instances."""

    _transformers: Dict[str, Type[BaseTransformer]] = {}

    @classmethod
    def register(cls, transformer_type: str, transformer_class: Type[BaseTransformer]) -> None:
        """Register a transformer type.
        
        Args:
            transformer_type: String identifier for the transformer.
            transformer_class: Transformer class to register.
        """
        cls._transformers[transformer_type] = transformer_class

    @classmethod
    def create(cls, transformer_type: str, config: Dict[str, Any]) -> BaseTransformer:
        """Create a transformer instance.
        
        Args:
            transformer_type: Type of transformer to create.
            config: Configuration for the transformer.
            
        Returns:
            Transformer instance.
            
        Raises:
            ValueError: If transformer type is not registered.
        """
        if transformer_type not in cls._transformers:
            raise ValueError(f"Unknown transformer type: {transformer_type}")
        
        transformer_class = cls._transformers[transformer_type]
        return transformer_class(**config)

    @classmethod
    def list_transformers(cls) -> Dict[str, Type[BaseTransformer]]:
        """List all registered transformer types.
        
        Returns:
            Dictionary of registered transformer types.
        """
        return cls._transformers.copy()
