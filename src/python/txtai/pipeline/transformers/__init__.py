"""ETL Transformers for txtai pipeline system."""

from .base import BaseTransformer
from .factory import TransformerFactory
from .json_transformer import JsonTransformer

# Auto-register transformers
TransformerFactory.register("json", JsonTransformer)

__all__ = ["BaseTransformer", "TransformerFactory", "JsonTransformer"]
