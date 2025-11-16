"""ETL Loaders for txtai pipeline system."""

from .base import BaseLoader
from .factory import LoaderFactory
from .elasticsearch import ElasticsearchLoader

# Auto-register loaders
LoaderFactory.register("elasticsearch", ElasticsearchLoader)

__all__ = ["BaseLoader", "LoaderFactory", "ElasticsearchLoader"]
