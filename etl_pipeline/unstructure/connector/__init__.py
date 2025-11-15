"""Connector module for unstructured data sources."""

from .base import BaseConnector
from .factory import ConnectorFactory

__all__ = ["BaseConnector", "ConnectorFactory"]
