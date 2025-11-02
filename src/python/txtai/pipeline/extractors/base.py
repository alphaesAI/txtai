from abc import ABC, abstractmethod

import pandas as pd
from typing import Optional, List
from datetime import datetime

class ExtractionStrategy(ABC):
    """Abstract base class for extraction strategies"""
    @abstractmethod
    def extract(self, connection, table: str, columns: List[str], 
                last_extraction_date: Optional[datetime]) -> pd.DataFrame:
        pass