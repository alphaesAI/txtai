"""
Loader module for loading data to target systems.
Supports various loading strategies including bulk operations.
"""
from .base import BaseLoader
from .elasticsearch_loader import ElasticsearchLoader
from .factory import LoaderFactory, LoaderType


__all__ = [
    'BaseLoader',
    'ElasticsearchLoader',
    'LoaderFactory',
    'LoaderType',
]
