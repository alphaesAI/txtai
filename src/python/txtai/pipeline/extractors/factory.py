"""Factory for creating extractor instances."""

from typing import Any, Dict, Type

from .base import BaseExtractor


class ExtractorFactory:
    """Factory class for creating extractor instances."""

    _extractors: Dict[str, Type[BaseExtractor]] = {}

    @classmethod
    def register(cls, extractor_type: str, extractor_class: Type[BaseExtractor]) -> None:
        """Register an extractor type.
        
        Args:
            extractor_type: String identifier for the extractor.
            extractor_class: Extractor class to register.
        """
        cls._extractors[extractor_type] = extractor_class

    @classmethod
    def create(cls, extractor_type: str, config: Dict[str, Any]) -> BaseExtractor:
        """Create an extractor instance.
        
        Args:
            extractor_type: Type of extractor to create.
            config: Configuration for the extractor.
            
        Returns:
            Extractor instance.
            
        Raises:
            ValueError: If extractor type is not registered.
        """
        if extractor_type not in cls._extractors:
            raise ValueError(f"Unknown extractor type: {extractor_type}")
        
        extractor_class = cls._extractors[extractor_type]
        return extractor_class(**config)

    @classmethod
    def list_extractors(cls) -> Dict[str, Type[BaseExtractor]]:
        """List all registered extractor types.
        
        Returns:
            Dictionary of registered extractor types.
        """
        return cls._extractors.copy()
