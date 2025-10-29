from abc import ABC, abstractmethod
from typing import Any
import pandas as pd

class BaseExtractor(ABC):
    """
    Abstract base class for data extractors.
    """

    @abstractmethod
    def __init__(self, *args: Any, **kwargs: Any):
        """
        Initialize the extractor with necessary dependencies.
        """
        pass

    @abstractmethod
    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        Start the trigger the extractor.     
        """
        pass

    @abstractmethod
    def extract_table(self, table_name: str, to_csv: bool = False) -> pd.DataFrame:
        """
        Extract all rows from a table as a pandas DataFrame (optionally save as CSV).
        """
        pass