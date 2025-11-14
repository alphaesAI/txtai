"""
Extractor module for data extraction from various sources.
Supports full, incremental date-based, and CDC-based extraction.
"""
from .base import BaseExtractor
from .config import (
    TableConfig,
    ExtractionConfig,
    ExtractionMode
)
from .postgres_extractor import PostgresExtractor
from .state_manager import StateManager
from .factory import ExtractorFactory, ExtractorType


__all__ = [
    'BaseExtractor',
    'TableConfig',
    'ExtractionConfig',
    'ExtractionMode',
    'PostgresExtractor',
    'StateManager',
    'ExtractorFactory',
    'ExtractorType',
]
