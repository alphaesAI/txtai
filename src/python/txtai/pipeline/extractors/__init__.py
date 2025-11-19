"""Extractor module for ETL pipeline."""

from .base import BaseExtractor
from .factory import ExtractorFactory
from .postgres import PostgresExtractor
from .email import GmailExtractor

# Register extractors
ExtractorFactory.register("postgres", PostgresExtractor)
ExtractorFactory.register("gmail", GmailExtractor)

__all__ = [
    "BaseExtractor",
    "ExtractorFactory", 
    "PostgresExtractor",
    "GmailExtractor"
]
