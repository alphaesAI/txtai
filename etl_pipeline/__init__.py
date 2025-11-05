"""
ETL Pipeline Package

A modular ETL pipeline for extracting data from PostgreSQL,
transforming to JSON, and loading to Elasticsearch.

Modules:
    - connector: Database and service connectors
    - extractor: Data extraction with CDC and incremental support
    - transformer: Data transformation to various formats
    - loader: Data loading to target systems
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

from . import connector
from . import extractor
from . import transformer
from . import loader

__all__ = [
    'connector',
    'extractor',
    'transformer',
    'loader',
]
