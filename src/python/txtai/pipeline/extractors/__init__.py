"""Extractor module for ETL pipeline."""

from .base import BaseExtractor
from .factory import ExtractorFactory
from .postgres import PostgresExtractor
from .textractor import TextractorExtractor

# Register extractors
ExtractorFactory.register("postgres", PostgresExtractor)
ExtractorFactory.register("textractor", TextractorExtractor)

__all__ = [
    "BaseExtractor",
    "ExtractorFactory", 
    "PostgresExtractor",
    "TextractorExtractor"
]
