"""Factory for creating loader instances."""

from typing import Any, Dict, Type

from .base import BaseLoader


class LoaderFactory:
    """Factory class for creating loader instances."""

    _loaders: Dict[str, Type[BaseLoader]] = {}

    @classmethod
    def register(cls, loader_type: str, loader_class: Type[BaseLoader]) -> None:
        """Register a loader type.
        
        Args:
            loader_type: String identifier for the loader.
            loader_class: Loader class to register.
        """
        cls._loaders[loader_type] = loader_class

    @classmethod
    def create(cls, loader_type: str, config: Dict[str, Any]) -> BaseLoader:
        """Create a loader instance.
        
        Args:
            loader_type: Type of loader to create.
            config: Configuration for the loader.
            
        Returns:
            Loader instance.
            
        Raises:
            ValueError: If loader type is not registered.
        """
        if loader_type not in cls._loaders:
            raise ValueError(f"Unknown loader type: {loader_type}")
        
        loader_class = cls._loaders[loader_type]
        return loader_class(**config)

    @classmethod
    def list_loaders(cls) -> Dict[str, Type[BaseLoader]]:
        """List all registered loader types.
        
        Returns:
            Dictionary of registered loader types.
        """
        return cls._loaders.copy()
